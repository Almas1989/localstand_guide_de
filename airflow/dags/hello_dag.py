from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def say_hello():
    print("Hello, Airflow!")


with DAG(
    dag_id="hello_dag",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(hours=6),   # schedule_interval устарел с 2.4
    catchup=True,
    tags=["test"],
) as dag:

    start = EmptyOperator(task_id="start")

    task1 = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )

    task2 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    start >> task1 >> task2