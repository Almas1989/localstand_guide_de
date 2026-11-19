from airflow.decorators import task, dag
from datetime import datetime

@dag(start_date=datetime(2024, 1, 1), schedule_interval=None, catchup=False, tags=["xcom"])

def greet_user_dag():

    @task
    def get_user():
        return "Alice"

    @task
    def greet(name):
        print(f"Hello, {name}!")


    name = get_user()
    greet(name)

greet_user_dag()

