import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.filesystem import FileSensor
from docker.types import Mount


HOST_PROJECT_PATH = os.environ.get(
    "HOST_PROJECT_PATH",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
)


SPARK_MOUNTS = [
    Mount(source=f"{HOST_PROJECT_PATH}/src", target="/opt/spark/src", type="bind"),
    Mount(source=f"{HOST_PROJECT_PATH}/data", target="/opt/spark/data", type="bind"),
]


SPARK_ENV = {
    "SPARK_HOME": "/opt/spark",
    "PYTHONPATH": "/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/spark/src:/opt/spark/src/gold",
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "planes_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 8, 28),
    schedule_interval="*/5 * * * *",
    catchup=False,
    tags=["planes", "spark", "silver", "gold"],
) as dag:

    check_bronze = FileSensor(
        task_id="check_bronze",
        filepath="/opt/airflow/data/bronze/output",
        poke_interval=30,
        timeout=600,
        mode="reschedule",
    )

    silver_step = DockerOperator(
        task_id="silver_transformation",
        image="apache/spark:3.5.1",
        api_version="auto",
        auto_remove="force",
        command="python3 /opt/spark/src/silver.py",
        docker_url="unix:///var/run/docker.sock",
        network_mode="sao_paulo_planes_default",
        mounts=SPARK_MOUNTS,
        mount_tmp_dir=False,
        environment=SPARK_ENV,
        user="0:0",
    )

    gold_step = DockerOperator(
        task_id="gold_marts",
        image="apache/spark:3.5.1",
        api_version="auto",
        auto_remove="force",
        command="python3 /opt/spark/src/gold/common.py",
        docker_url="unix:///var/run/docker.sock",
        network_mode="sao_paulo_planes_default",
        mounts=SPARK_MOUNTS,
        mount_tmp_dir=False,
        environment=SPARK_ENV,
        user="0:0",
    )

    check_bronze >> silver_step >> gold_step