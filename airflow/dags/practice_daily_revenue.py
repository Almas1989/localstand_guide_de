from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

CONN_ID = "postgres_academy"


with DAG(
    dag_id="practice_daily_revenue",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,       # не больше одного запуска одновременно — дни идут строго по очереди
    tags=["lesson4"],
) as dag:

    # 1. Таблица-приёмник. IF NOT EXISTS — повторный запуск не падает.
    #    PRIMARY KEY на report_date — страховка: дубль за день физически невозможен.
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS academy.daily_revenue (
                report_date     date           PRIMARY KEY,
                payments_count  integer        NOT NULL,
                revenue_rub     numeric(14, 2) NOT NULL,
                loaded_at       timestamptz    NOT NULL
            );
        """,
    )

    # 2. Считаем СВОЙ день: {{ ds }} — логическая дата запуска (YYYY-MM-DD),
    #    а не datetime.now(). Поэтому backfill за 3 января посчитает именно 3 января.
    #
    #    Идемпотентность: DELETE своего дня + INSERT. Оба запроса идут в одной задаче
    #    и одной транзакции (коммит в конце) — либо всё, либо ничего.
    load_day = SQLExecuteQueryOperator(
        task_id="load_day",
        conn_id=CONN_ID,
        sql=[
            # Удаляем прошлый результат за этот день, если он был
            "DELETE FROM academy.daily_revenue WHERE report_date = '{{ ds }}'::date;",

            # Агрегат без GROUP BY всегда даёт ровно одну строку,
            # даже если оплат не было: count(*) = 0, sum = NULL → coalesce в 0
            """
            INSERT INTO academy.daily_revenue (report_date, payments_count, revenue_rub, loaded_at)
            SELECT
                '{{ ds }}'::date                                  AS report_date,
                count(*)                                          AS payments_count,
                coalesce(round(sum(amount_kopecks) / 100.0, 2), 0) AS revenue_rub,  -- копейки → рубли
                now()                                             AS loaded_at
            FROM academy.payments
            WHERE lower(trim(status)) = 'succeeded'               -- чистим статус
              AND paid_at >= '{{ ds }}'                           -- начало дня (включительно)
              AND paid_at <  '{{ macros.ds_add(ds, 1) }}';        -- начало следующего (не включительно)
            """,
        ],
    )

    create_table >> load_day