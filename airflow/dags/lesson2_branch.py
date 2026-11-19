from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator


@dag(start_date=datetime(2026, 1, 1), schedule="@daily", catchup=False)
def branch_demo():

    @task.branch
    def choose(logical_date=None):
        if logical_date.weekday() == 0:  # понедельник
            return "weekly_report"
        return "daily_report"

    daily = EmptyOperator(task_id="daily_report")
    weekly = EmptyOperator(task_id="weekly_report")
    done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")
    cleanup = EmptyOperator(task_id="cleanup", trigger_rule="all_done")

    choose() >> [daily, weekly] >> done >> cleanup


branch_demo()