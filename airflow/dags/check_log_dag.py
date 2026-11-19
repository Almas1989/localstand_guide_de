from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def check_data():
    print("Данные есть")


def log_result():
    print("Завершено успешно")


with DAG(
    dag_id="check_and_log_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["lesson1"],
) as dag:

    check_data_task = PythonOperator(task_id="check_data", python_callable=check_data)

    process_data_task = BashOperator(
        task_id="process_data",
        bash_command='echo "Обрабатываем данные"',
    )

    log_result_task = PythonOperator(task_id="log_result", python_callable=log_result)

    check_data_task >> process_data_task >> log_result_task