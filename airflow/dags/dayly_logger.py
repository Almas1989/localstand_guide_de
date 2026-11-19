from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def log_date():
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"Сегодня: {today}")


def log_done():
    print("Задача завершена!")


with DAG(
    dag_id="dayly_logger",
    start_date=datetime(2026, 1, 1),
    schedule="0 8 * * *",
    catchup=False,
    tags=["lesson_1"],
) as dag:
    log_date_task = PythonOperator(
        task_id="log_date",
        python_callable=log_date,
    )

    log_done_task = PythonOperator(
        task_id="log_done",
        python_callable=log_done,
    )

    log_date_task >> log_done_task