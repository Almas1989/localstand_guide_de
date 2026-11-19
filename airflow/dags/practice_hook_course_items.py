from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "postgres_academy"


@dag(
    dag_id="practice_hook_course_items",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lesson3"],
)
def practice_hook_course_items():

    @task
    def build_paid_course_items():
        # Хук создаём внутри функции: так подключение открывается только при выполнении задачи,
        # а не каждый раз, когда scheduler парсит файл
        hook = PostgresHook(postgres_conn_id=CONN_ID)

        # hook.run() выполняет SQL без возврата результата — подходит для DDL и REFRESH.
        # Список запросов выполняется по порядку.
        hook.run([
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS academy.mv_paid_course_items AS
            SELECT
                c.course_id,
                c.title                   AS course_title,
                count(oi.order_item_id)   AS paid_items      -- считаем позиции, не заказы
            FROM academy.order_items oi
            JOIN academy.orders  o ON o.order_id  = oi.order_id
            JOIN academy.courses c ON c.course_id = oi.course_id
            WHERE lower(trim(o.status)) = 'paid'             -- статус чистим перед сравнением
            GROUP BY c.course_id, c.title;
            """,
            # CREATE ... IF NOT EXISTS при повторном запуске ничего не делает,
            # поэтому данные пересчитываем отдельным REFRESH
            "REFRESH MATERIALIZED VIEW academy.mv_paid_course_items;",
        ])

        # hook.get_first() возвращает первую строку результата как кортеж
        courses_cnt, items_sum = hook.get_first("""
            SELECT count(*), coalesce(sum(paid_items), 0)
            FROM academy.mv_paid_course_items;
        """)

        print(f"Курсов в отчёте: {courses_cnt}")
        print(f"Сумма paid_items: {items_sum}")

    build_paid_course_items()


practice_hook_course_items()