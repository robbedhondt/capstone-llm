import os

from airflow import DAG

# from airflow.operators.bash import Operator
from airflow.providers.docker.operators.docker import DockerOperator

# from datetime import datetime

with DAG(
    dag_id="process_posts",
    # start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    run_process_posts = DockerOperator(
        task_id="docker_run",
        image="capstone-llm:latest",
        container_name="task___command_sleep",
        api_version="auto",
        auto_remove="force",
        environment={
            "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
            "AWS_SECRET_ACCESS_KEY": os.environ ["AWS_SECRET_ACCESS_KEY"],
        },
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

# docker run --name capstone-llm-task capstone-llm:latest
