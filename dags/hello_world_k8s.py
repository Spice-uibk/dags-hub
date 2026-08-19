from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator
from kubernetes.client import models as k8s

resources = k8s.V1ResourceRequirements(
    requests={
        "cpu": "250m",
        "memory": "216Mi"
    },
    limits={
        "cpu": "500m",
        "memory": "500Mi"
    }
)

default_args = {
    'owner': 'stefanpedratscher',
    'depends_on_past': False,
    'email_on_failure': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'hello_world_kubernetes',
    default_args=default_args,
    description='A simple Hello World DAG using Kubernetes operator',
    schedule='@once',
    catchup=False,
    tags=['hello_world', 'k8s'],
)

start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

k8s_hello_task = KubernetesPodOperator(
    task_id='k8s_hello',
    name='hello-pod',
    # namespace='stefan-dev',
    image='python:3.9-slim',
    cmds=['python', '-c'],
    arguments=['print("Hello world from Kubernetes Pod!")'],
    # in_cluster=True,  # Change to False if Airflow runs outside K8s
    dag=dag,
    get_logs=True,
    # node_selector={"kubernetes.io/hostname": "node1"},
)

k8s_date_task = KubernetesPodOperator(
    task_id='k8s_date',
    name='date-pod',
    # namespace='stefan-dev',
    image='python:3.9-slim',
    cmds=['python', '-c'],
    arguments=['import datetime; print(f"Current date and time: {datetime.datetime.now()}")'],
    # in_cluster=True,
    dag=dag,
    get_logs=True,
)

k8s_sleep_task = KubernetesPodOperator(
    task_id='k8s_sleep',
    name='sleep-pod',
    # namespace='stefan-dev',
    image='python:3.9-slim',
    cmds=['python', '-c'],
    container_resources=resources,
    arguments=['import time; time.sleep(120); print("Done sleeping!")'],
    # in_cluster=True,
    dag=dag,
    get_logs=True,
    # node_selector={"kubernetes.io/hostname": "node1"},
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag,
)

# Set task dependencies
start_task >> [k8s_hello_task, k8s_date_task, k8s_sleep_task] >> end_task
