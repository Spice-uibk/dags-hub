from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

IMAGE = "python:3.9-slim"

SCRIPT_SOURCE = """
#!/usr/bin/env python3

import argparse
import json
import math
import multiprocessing
import os
import random
import time
import uuid
from datetime import datetime, timezone


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def detect_cpu_limit() -> int:
    try:
        with open("/sys/fs/cgroup/cpu.max") as f:
            quota_str, period_str = f.read().split()
        if quota_str != "max":
            quota, period = int(quota_str), int(period_str)
            if quota > 0 and period > 0:
                return max(1, math.ceil(quota / period))
    except (FileNotFoundError, ValueError, OSError):
        pass

    try:
        with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us") as f:
            quota = int(f.read().strip())
        with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us") as f:
            period = int(f.read().strip())
        if quota > 0 and period > 0:
            return max(1, math.ceil(quota / period))
    except (FileNotFoundError, ValueError, OSError):
        pass

    return os.cpu_count() or 1


def calibrate_ops_per_second(duration: float = 0.3) -> float:
    \"\"\"Rough single-core throughput of cpu_work_worker's inner operation,
    measured fresh on this pod right before the computation phases start.
    Used to size each phase's work_target so that, with no contention, wall
    time roughly matches the nominal phase_duration -- but if the node is
    actually contended DURING the phases, finishing that same amount of work
    takes measurably longer than that. Calibrating per-invocation (instead
    of a hardcoded ops/sec constant) keeps this self-normalizing across
    nodes with different CPU speeds.\"\"\"
    x = 0.0001
    count = 0
    end = time.time() + duration
    while time.time() < end:
        x = math.sin(x) * math.cos(x) + 1.0000001
        count += 1
    return count / duration


def cpu_work_worker(target_percent: float, period: float, work_counter, stop_event) -> None:
    \"\"\"Duty-cycles at target_percent of each period (busy, then idle the
    rest), doing real floating-point work during the busy portion and
    tallying how much got done into the shared work_counter. The
    coordinating process (run_phase / run_light_stage) decides when to stop
    based on work_counter, not on a fixed sleep.\"\"\"
    target_percent = max(0.0, min(100.0, target_percent))
    busy_time = period * (target_percent / 100.0)
    x = 0.0001
    while not stop_event.is_set():
        start = time.time()
        busy_until = start + busy_time
        local_work = 0
        while time.time() < busy_until:
            x = math.sin(x) * math.cos(x) + 1.0000001
            local_work += 1
        with work_counter.get_lock():
            work_counter.value += local_work
        elapsed = time.time() - start
        remaining = period - elapsed
        if remaining > 0:
            time.sleep(remaining)


def allocate_and_touch_memory(target_mb: float) -> bytearray:
    size_bytes = max(0, int(target_mb * 1024 * 1024))
    block = bytearray(size_bytes)
    page_size = 4096
    for i in range(0, size_bytes, page_size):
        block[i] = 1
    return block


def run_light_stage(name: str, cpu_percent: float, duration_s: float, cores: int, period: float,
                     max_duration_multiplier: float) -> dict:
    # Same work-bound approach as run_phase: nominal_duration_s only holds
    # if there's no real contention, so execution_time is a genuine signal
    # here too, not a fixed sleep.
    ops_per_second = calibrate_ops_per_second()
    cpu_percent = max(0.0, min(100.0, cpu_percent))
    work_target = max(1, int(ops_per_second * duration_s * (cpu_percent / 100.0) * cores))
    max_duration_s = duration_s * max_duration_multiplier

    print(f"[{name}] light load start cpu_target%={cpu_percent:.1f} nominal_duration_s={duration_s:.1f} "
          f"cores={cores} work_target={work_target}", flush=True)

    start_wall = time.time()
    work_counter = multiprocessing.Value("l", 0)
    stop_event = multiprocessing.Event()
    workers = [
        multiprocessing.Process(target=cpu_work_worker, args=(cpu_percent, period, work_counter, stop_event))
        for _ in range(cores)
    ]
    for w in workers:
        w.start()

    capped = False
    while True:
        if work_counter.value >= work_target:
            break
        if time.time() - start_wall > max_duration_s:
            capped = True
            break
        time.sleep(period)

    stop_event.set()
    for w in workers:
        w.join(timeout=5)
        if w.is_alive():
            w.terminate()

    actual_duration_s = time.time() - start_wall
    metrics = {
        "nominal_duration_s": round(duration_s, 2),
        "duration_s": round(actual_duration_s, 3),
        "ops_per_second_calibration": round(ops_per_second, 1),
        "work_target": work_target,
        "work_completed": work_counter.value,
        "capped": capped,
    }
    print(f"[{name}] light load done -> {json.dumps(metrics)}", flush=True)
    return metrics


def run_phase(phase_idx: int, target_cpu_percent: float, target_mem_mb: float,
              cores: int, phase_duration: float, period: float,
              ops_per_second: float, max_duration_multiplier: float) -> dict:
    # Work-bound, not time-bound: each phase has a work_target (sized from
    # ops_per_second so that, with no contention, wall time roughly matches
    # phase_duration) and runs until that work is done. If the node is
    # actually contended during the phase, finishing takes measurably
    # longer than phase_duration -- that's the point, execution_time should
    # be a real function of load, not a fixed sleep. max_duration_multiplier
    # is a safety cap so a starved phase can't hang the pod forever.
    start_ts = now_iso()
    start_wall = time.time()
    target_cpu_percent = max(0.0, min(100.0, target_cpu_percent))

    work_target = max(1, int(ops_per_second * phase_duration * (target_cpu_percent / 100.0) * cores))
    max_duration_s = phase_duration * max_duration_multiplier

    print(f"[phase {phase_idx}] start={start_ts} cpu_target%={target_cpu_percent:.1f} "
          f"mem_target_mb={target_mem_mb:.1f} cores={cores} nominal_duration_s={phase_duration:.1f} "
          f"work_target={work_target}",
          flush=True)

    work_counter = multiprocessing.Value("l", 0)
    stop_event = multiprocessing.Event()
    workers = [
        multiprocessing.Process(target=cpu_work_worker, args=(target_cpu_percent, period, work_counter, stop_event))
        for _ in range(cores)
    ]
    for w in workers:
        w.start()

    mem_block = allocate_and_touch_memory(target_mem_mb)

    capped = False
    while True:
        if work_counter.value >= work_target:
            break
        if time.time() - start_wall > max_duration_s:
            capped = True
            break
        time.sleep(period)

    stop_event.set()
    for w in workers:
        w.join(timeout=5)
        if w.is_alive():
            w.terminate()

    del mem_block

    actual_duration_s = time.time() - start_wall
    end_ts = now_iso()
    record = {
        "phase": phase_idx,
        "start": start_ts,
        "end": end_ts,
        "cpu_target_percent": round(target_cpu_percent, 2),
        "mem_target_mb": round(target_mem_mb, 2),
        "cores": cores,
        "nominal_duration_s": round(phase_duration, 2),
        "duration_s": round(actual_duration_s, 3),
        "work_target": work_target,
        "work_completed": work_counter.value,
        "capped": capped,
    }
    print(f"[phase {phase_idx}] done -> {json.dumps(record)}", flush=True)
    return record


def parse_profile(profile_str: str):
    phases = []
    for chunk in profile_str.split(","):
        cpu_s, mem_s = chunk.split(":")
        phases.append((float(cpu_s), float(mem_s)))
    return phases


def run_producer(args) -> dict:
    element = {
        "element_id": str(uuid.uuid4()),
        "input_size_bytes": int(random.uniform(args.input_size_min_kb, args.input_size_max_kb) * 1024),
        "produced_at": now_iso(),
    }
    print("[producer] " + json.dumps(element), flush=True)
    return element


def run_preprocessing(args) -> dict:
    cores = args.cores or detect_cpu_limit()
    light_metrics = run_light_stage("preprocessing", args.light_cpu_percent, args.light_duration, cores,
                                     args.period, args.max_duration_multiplier)
    element = {
        "element_id": str(uuid.uuid4()),
        "input_size_bytes": int(random.uniform(args.input_size_min_kb, args.input_size_max_kb) * 1024),
        "preprocessed_at": now_iso(),
        "light_stage": light_metrics,
    }
    print("[preprocessing] " + json.dumps(element), flush=True)
    return element


def run_computation(args) -> dict:
    cores = args.cores or detect_cpu_limit()
    ops_per_second = calibrate_ops_per_second()
    print(f"[computation] calibration: {ops_per_second:.0f} ops/s/core", flush=True)

    if args.profile:
        phases = parse_profile(args.profile)
        steps = len(phases)
    else:
        steps = max(1, args.steps)
        phases = None

    phase_duration = args.duration / steps
    records = []

    print(f"[computation] start={now_iso()} nominal_total_duration_s={args.duration} steps={steps} cores={cores}", flush=True)

    for i in range(steps):
        if phases:
            cpu_target, mem_target = phases[i]
        else:
            cpu_target = random.uniform(args.cpu_min, args.cpu_max)
            mem_target = random.uniform(args.mem_min, args.mem_max)
        records.append(run_phase(i, cpu_target, mem_target, cores, phase_duration, args.period,
                                  ops_per_second, args.max_duration_multiplier))

    result = {
        "element_id": str(uuid.uuid4()),
        "output_size_bytes": int(random.uniform(args.output_size_min_kb, args.output_size_max_kb) * 1024),
        "computed_at": now_iso(),
        "ops_per_second_calibration": round(ops_per_second, 1),
        "phases": records,
    }
    print("[computation] SUMMARY " + json.dumps(result), flush=True)
    return result


def run_postprocessing(args) -> dict:
    cores = args.cores or detect_cpu_limit()
    light_metrics = run_light_stage("postprocessing", args.light_cpu_percent, args.light_duration, cores,
                                     args.period, args.max_duration_multiplier)
    result = {
        "element_id": str(uuid.uuid4()),
        "postprocessed_at": now_iso(),
        "light_stage": light_metrics,
    }
    print("[postprocessing] FINAL " + json.dumps(result), flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description="Synthetic pipeline stage: producer/preprocessing/computation/postprocessing")
    parser.add_argument("--stage", choices=["producer", "preprocessing", "computation", "postprocessing"], required=True)

    parser.add_argument("--input-size-min-kb", type=float, default=16.0)
    parser.add_argument("--input-size-max-kb", type=float, default=2048.0)
    parser.add_argument("--output-size-min-kb", type=float, default=16.0)
    parser.add_argument("--output-size-max-kb", type=float, default=2048.0)

    parser.add_argument("--light-cpu-percent", type=float, default=30.0,
                         help="Target CPU%% for lightweight stages (preprocessing/postprocessing)")
    parser.add_argument("--light-duration", type=float, default=5.0,
                         help="Duration in seconds for lightweight stages")

    parser.add_argument("--duration", type=float, default=600,
                         help="Total duration in seconds for the computation stage (default 600 = 10 min)")
    parser.add_argument("--steps", type=int, default=8,
                         help="Number of phases the computation duration is split into")
    parser.add_argument("--cpu-min", type=float, default=10.0)
    parser.add_argument("--cpu-max", type=float, default=90.0)
    parser.add_argument("--mem-min", type=float, default=64.0)
    parser.add_argument("--mem-max", type=float, default=512.0)
    parser.add_argument("--cores", type=int, default=None,
                         help="Number of cores to load (default: the pod's cgroup CPU limit, auto-detected)")
    parser.add_argument("--max-duration-multiplier", type=float, default=4.0,
                         help="Safety cap for the computation stage: a phase is force-stopped after "
                              "phase_duration * this, even if its work_target isn't reached yet")
    parser.add_argument("--period", type=float, default=0.1)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--profile", type=str, default=None,
                         help="Fixed profile 'cpu:mem,cpu:mem,...' for the computation stage instead of random")

    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    if args.stage == "producer":
        run_producer(args)
    elif args.stage == "preprocessing":
        run_preprocessing(args)
    elif args.stage == "computation":
        run_computation(args)
    else:
        run_postprocessing(args)


if __name__ == "__main__":
    main()
"""

LIGHT_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "100m", "memory": "128Mi"},
    limits={"cpu": "250m", "memory": "256Mi"},
)

POD_CPU_LIMIT_CORES = 1
COMPUTATION_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "512Mi"},
    limits={"cpu": str(POD_CPU_LIMIT_CORES), "memory": "1Gi"},
)

default_args = {
    'owner': 'pgsantaclara',
    'depends_on_past': False,
    'email_on_failure': False,
    'start_date': datetime(2026, 8, 6),
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    't52_synthetic_pipeline_chain',
    default_args=default_args,
    description='Synthetic Producer->Preprocessing->Computation->Postprocessing pipeline for T5.2',
    schedule='0 */3 * * *',
    catchup=False,
    tags=['t52', 'synthetic', 'pipeline_predictor'],
)

producer = KubernetesPodOperator(
    task_id='producer_task',
    name='synthetic-producer',
    image=IMAGE,
    cmds=['python', '-c'],
    arguments=[
        SCRIPT_SOURCE,
        '--stage', 'producer',
        '--input-size-min-kb', '16', '--input-size-max-kb', '2048',
    ],
    container_resources=LIGHT_RESOURCES,
    dag=dag,
    get_logs=True,
)

preprocessing = KubernetesPodOperator(
    task_id='preprocessing_task',
    name='synthetic-preprocessing',
    image=IMAGE,
    cmds=['python', '-c'],
    arguments=[
        SCRIPT_SOURCE,
        '--stage', 'preprocessing',
        '--input-size-min-kb', '16', '--input-size-max-kb', '2048',
        '--light-cpu-percent', '30', '--light-duration', '5',
        '--cores', str(POD_CPU_LIMIT_CORES),
    ],
    container_resources=LIGHT_RESOURCES,
    dag=dag,
    get_logs=True,
)

computation = KubernetesPodOperator(
    task_id='computation_task',
    name='synthetic-computation',
    image=IMAGE,
    cmds=['python', '-c'],
    arguments=[
        SCRIPT_SOURCE,
        '--stage', 'computation',
        '--duration', '600', '--steps', '8',
        '--cpu-min', '10', '--cpu-max', '80',
        '--mem-min', '64', '--mem-max', '400',
        '--output-size-min-kb', '16', '--output-size-max-kb', '2048',
        '--cores', str(POD_CPU_LIMIT_CORES),
    ],
    container_resources=COMPUTATION_RESOURCES,
    dag=dag,
    get_logs=True,
)

postprocessing = KubernetesPodOperator(
    task_id='postprocessing_task',
    name='synthetic-postprocessing',
    image=IMAGE,
    cmds=['python', '-c'],
    arguments=[
        SCRIPT_SOURCE,
        '--stage', 'postprocessing',
        '--light-cpu-percent', '30', '--light-duration', '5',
        '--cores', str(POD_CPU_LIMIT_CORES),
    ],
    container_resources=LIGHT_RESOURCES,
    dag=dag,
    get_logs=True,
)

producer >> preprocessing >> computation >> postprocessing
