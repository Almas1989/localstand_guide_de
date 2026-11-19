from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Имя подключения из Admin → Connections (подставь своё)
CONN_ID = "postgres_academy"


@dag(
    dag_id="practice_sql_orders",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # запуск вручную
    catchup=False,
    tags=["lesson3"],
)
def practice_sql_orders():

    # 1. Создаём витрину. IF NOT EXISTS — чтобы повторный запуск не падал.
    #    Если объект уже есть, определение не меняется и данные НЕ пересчитываются.
    create_mv = SQLExecuteQueryOperator(
        task_id="create_mv",
        conn_id=CONN_ID,
        sql="""
            CREATE MATERIALIZED VIEW IF NOT EXISTS academy.mv_orders_by_status AS
            SELECT
                lower(trim(status))              AS status,        -- сначала нормализуем
                count(*)                         AS orders_count,  -- потом считаем
                CURRENT_TIMESTAMP::timestamptz   AS refreshed_at   -- время расчёта
            FROM academy.orders
            GROUP BY lower(trim(status));                          -- группируем по нормализованному
        """,
    )

    # 2. Пересчитываем данные. CURRENT_TIMESTAMP вычисляется заново —
    #    поэтому refreshed_at обновляется при каждом запуске.
    refresh_mv = SQLExecuteQueryOperator(
        task_id="refresh_mv",
        conn_id=CONN_ID,
        sql="REFRESH MATERIALIZED VIEW academy.mv_orders_by_status;",
    )

    # 3. Читаем результат через хук и возвращаем — return уходит в XCom.
    @task
    def read_result():
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        rows = hook.get_records("""
            SELECT status, orders_count, refreshed_at
            FROM academy.mv_orders_by_status
            ORDER BY orders_count DESC;
        """)

        # Приводим к списку словарей; дату — в строку, чтобы XCom читался в UI
        result = [
            {"status": s, "orders_count": c, "refreshed_at": str(r)}
            for s, c, r in rows
        ]

        total = sum(r["orders_count"] for r in result)
        for r in result:
            print(f"{r['status']}: {r['orders_count']}")
        print(f"Всего: {total}")

        return result

    create_mv >> refresh_mv >> read_result()


practice_sql_orders()