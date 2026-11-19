from datetime import datetime

from airflow.decorators import dag, task


# @dag превращает функцию в DAG; dag_id = имя функции
@dag(
    dag_id="xcom_sum_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # без расписания, запуск только вручную (Trigger DAG)
    catchup=False,          # не догонять прошедшие даты
    tags=["lesson2"],
)
def xcom_sum_dag():

    @task
    def push_numbers():
        # return автоматически кладёт значение в XCom с ключом return_value
        return [1, 2, 3]

    @task
    def sum_numbers(numbers):
        # numbers Airflow сам достал из XCom задачи push_numbers
        return sum(numbers)

    @task
    def log_numbers(total):
        # total — результат sum_numbers, тоже пришёл через XCom
        print(f"Результат загружен: {total}")

    # Передача результата аргументом = зависимость + передача через XCom.
    # Граф: push_numbers >> sum_numbers >> log_numbers
    numbers = push_numbers()
    total = sum_numbers(numbers)
    log_numbers(total)


# Вызов функции регистрирует DAG — без этой строки Airflow его не увидит
xcom_sum_dag()