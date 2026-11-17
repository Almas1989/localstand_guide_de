from airflow import DAG
from airflow.operators.python import PythonOperator, BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta



def say_hello():
    print("Hello, Airflow!")



with DAG(
    dag_id="hello_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval=timedelta(hours=6),
    catchup=False,
) as dag:
    
    task1 = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )


    task2 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    start = EmptyOperator(task_id="start")

    task1 >> task2