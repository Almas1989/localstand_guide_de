from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="xcom_merge_lists",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lesson2"],
)
def xcom_merge_lists():

    @task
    def get_ids_from_source_a():
        # Имитация чтения id из источника A
        return [1, 2, 3]

    @task
    def get_ids_from_source_b():
        # Имитация чтения id из источника B
        return [4, 5]

    @task
    def merge_ids(ids_a, ids_b):
        # Объединяем вручную, как требует задание
        merged = []
        for x in ids_a:
            merged.append(x)
        for x in ids_b:
            merged.append(x)

        print(f"Объединённый список: {merged}")
        # return → XCom задачи merge_ids: [1, 2, 3, 4, 5]
        return merged

    # Источники A и B работают параллельно, merge_ids ждёт оба.
    merge_ids(get_ids_from_source_a(), get_ids_from_source_b())


xcom_merge_lists()