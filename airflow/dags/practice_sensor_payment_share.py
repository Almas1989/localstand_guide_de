from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "postgres_academy"


@dag(
    dag_id="practice_sensor_payment_share",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lesson3"],
)
def practice_sensor_payment_share():

    # 1. Сенсор ждёт, пока первый отчёт пересчитают ПОСЛЕ начала этого запуска.
    #    SqlSensor смотрит на первую ячейку: false — проверить ещё раз, true — дальше.
    wait_orders_refreshed = SqlSensor(
        task_id="wait_orders_refreshed",
        conn_id=CONN_ID,
        sql="""
            SELECT coalesce(
                max(refreshed_at) >= %(run_start)s::timestamptz,  -- свежий ли отчёт
                false                                              -- пустой отчёт → max = NULL → false
            )
            FROM academy.mv_orders_by_status;
        """,
        # parameters — шаблонизируемое поле: Jinja подставит фактическое начало запуска,
        # а драйвер передаст значение как параметр (без склейки строк в SQL)
        parameters={"run_start": "{{ dag_run.start_date.isoformat() }}"},
        mode="reschedule",   # между проверками слот освобождается, другой DAG успевает отработать
        poke_interval=10,    # проверка каждые 10 секунд
        timeout=180,         # через 3 минуты ожидания — ошибка
        retries=0,           # после таймаута не перезапускаться
    )

    # 2. Создаём второй отчёт, если его ещё нет.
    #    Агрегат без GROUP BY всегда возвращает ровно одну строку.
    create_share_mv = SQLExecuteQueryOperator(
        task_id="create_share_mv",
        conn_id=CONN_ID,
        sql="""
            CREATE MATERIALIZED VIEW IF NOT EXISTS academy.mv_order_payment_share AS
            SELECT
                sum(orders_count)                                   AS total_orders,
                coalesce(sum(orders_count) FILTER (WHERE status = 'paid'), 0)
                                                                    AS paid_orders,
                round(
                    100.0 * sum(orders_count) FILTER (WHERE status = 'paid')  -- 100.0 → numeric
                    / nullif(sum(orders_count), 0),                           -- 0 заказов → NULL
                    2
                )                                                   AS paid_share_pct,
                max(refreshed_at)                                   AS source_refreshed_at
            FROM academy.mv_orders_by_status;
        """,
    )

    # 3. Пересчитываем второй отчёт на свежих данных первого.
    refresh_share_mv = SQLExecuteQueryOperator(
        task_id="refresh_share_mv",
        conn_id=CONN_ID,
        sql="REFRESH MATERIALIZED VIEW academy.mv_order_payment_share;",
    )

    # 4. Читаем результат и кладём в XCom для проверки.
    @task
    def read_share():
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        total, paid, pct, src_ts = hook.get_first("""
            SELECT total_orders, paid_orders, paid_share_pct, source_refreshed_at
            FROM academy.mv_order_payment_share;
        """)

        # Decimal и datetime приводим к простым типам, чтобы XCom нормально отображался
        result = {
            "total_orders": int(total),
            "paid_orders": int(paid),
            "paid_share_pct": str(pct),
            "source_refreshed_at": str(src_ts),
        }
        print(result)
        return result

    wait_orders_refreshed >> create_share_mv >> refresh_share_mv >> read_share()


practice_sensor_payment_share()