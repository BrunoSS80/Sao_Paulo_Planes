from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

with DAG(
    "planes",
    start_date = datetime(2026,8,24),
    schedule_interval = '30 * * * *',
    catchup = False
):
    start = EmptyOperator(task_id="start")

    def printss():
        print("teste")

    test = BashOperator(
        task_id = "test",
        bash_command = "python /opt/airflow/src/te.py"
    )

    start >> test