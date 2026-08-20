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


def cpu_burn_worker(target_percent: float, period: float, stop_event) -> None:
    target_percent = max(0.0, min(100.0, target_percent))
    busy_time = period * (target_percent / 100.0)
    while not stop_event.is_set():
        start = time.time()
        busy_until = start + busy_time
        while time.time() < busy_until:
            pass
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


def run_light_stage(name: str, cpu_percent: float, duration_s: float, cores: int, period: float) -> None:
    print(f"[{name}] light load start cpu_target%={cpu_percent:.1f} duration_s={duration_s:.1f} cores={cores}", flush=True)
    stop_event = multiprocessing.Event()
    workers = [
        multiprocessing.Process(target=cpu_burn_worker, args=(cpu_percent, period, stop_event))
        for _ in range(cores)
    ]
    for w in workers:
        w.start()
    time.sleep(duration_s)
    stop_event.set()
    for w in workers:
        w.join(timeout=5)
        if w.is_alive():
            w.terminate()
    print(f"[{name}] light load done", flush=True)


def run_phase(phase_idx: int, target_cpu_percent: float, target_mem_mb: float,
              cores: int, duration_s: float, period: float) -> dict:
    start_ts = now_iso()
    print(f"[phase {phase_idx}] start={start_ts} cpu_target%={target_cpu_percent:.1f} "
          f"mem_target_mb={target_mem_mb:.1f} cores={cores} duration_s={duration_s:.1f}",
          flush=True)

    stop_event = multiprocessing.Event()
    workers = [
        multiprocessing.Process(target=cpu_burn_worker, args=(target_cpu_percent, period, stop_event))
        for _ in range(cores)
    ]
    for w in workers:
        w.start()

    mem_block = allocate_and_touch_memory(target_mem_mb)
    time.sleep(duration_s)

    stop_event.set()
    for w in workers:
        w.join(timeout=5)
        if w.is_alive():
            w.terminate()

    del mem_block

    end_ts = now_iso()
    record = {
        "phase": phase_idx,
        "start": start_ts,
        "end": end_ts,
        "cpu_target_percent": round(target_cpu_percent, 2),
        "mem_target_mb": round(target_mem_mb, 2),
        "cores": cores,
        "duration_s": duration_s,
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
    run_light_stage("preprocessing", args.light_cpu_percent, args.light_duration, cores, args.period)
    element = {
        "element_id": str(uuid.uuid4()),
        "input_size_bytes": int(random.uniform(args.input_size_min_kb, args.input_size_max_kb) * 1024),
        "preprocessed_at": now_iso(),
    }
    print("[preprocessing] " + json.dumps(element), flush=True)
    return element


def run_computation(args) -> dict:
    cores = args.cores or detect_cpu_limit()

    if args.profile:
        phases = parse_profile(args.profile)
        steps = len(phases)
    else:
        steps = max(1, args.steps)
        phases = None

    phase_duration = args.duration / steps
    records = []

    print(f"[computation] start={now_iso()} total_duration_s={args.duration} steps={steps} cores={cores}", flush=True)

    for i in range(steps):
        if phases:
            cpu_target, mem_target = phases[i]
        else:
            cpu_target = random.uniform(args.cpu_min, args.cpu_max)
            mem_target = random.uniform(args.mem_min, args.mem_max)
        records.append(run_phase(i, cpu_target, mem_target, cores, phase_duration, args.period))

    result = {
        "element_id": str(uuid.uuid4()),
        "output_size_bytes": int(random.uniform(args.output_size_min_kb, args.output_size_max_kb) * 1024),
        "computed_at": now_iso(),
        "phases": records,
    }
    print("[computation] SUMMARY " + json.dumps(result), flush=True)
    return result


def run_postprocessing(args) -> dict:
    cores = args.cores or detect_cpu_limit()
    run_light_stage("postprocessing", args.light_cpu_percent, args.light_duration, cores, args.period)
    result = {
        "element_id": str(uuid.uuid4()),
        "postprocessed_at": now_iso(),
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
    schedule='0 6,10,14,18 * * *',
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
