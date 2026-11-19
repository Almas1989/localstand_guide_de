from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="multi_task_xcom_pull",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lesson2"],
)
def multi_task_xcom_pull():

    # task_id по умолчанию = имя функции, поэтому задачи будут push_1, push_2, push_3
    @task
    def push_1():
        return 1

    @task
    def push_2():
        return 2

    @task
    def push_3():
        return 3

    @task
    def sum_all(a, b, c):
        # Каждое значение пришло из XCom своей задачи
        total = a + b + c
        print(f"Значения: {[a, b, c]}, сумма: {total}")
        return total  # сумма тоже окажется в XCom задачи sum_all

    # Три задачи выполняются параллельно и сходятся в sum_all.
    # Graph: push_1, push_2, push_3 >> sum_all
    sum_all(push_1(), push_2(), push_3())


multi_task_xcom_pull()