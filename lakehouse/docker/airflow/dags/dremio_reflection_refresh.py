from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow_contrib.dremio_client import refresh_reflections

REFLECTIONS = [
    "@lakehouse.payments.orders",
    "@lakehouse.payments.merchant_events",
]


def create_dag():
    with DAG(
        dag_id="dremio_reflection_refresh",
        start_date=datetime(2024, 1, 1),
        schedule_interval="0 * * * *",
        catchup=False,
        tags=["dremio", "reflections"],
    ) as dag:
        refresh = PythonOperator(
            task_id="refresh",
            python_callable=refresh_reflections,
            op_kwargs={"datasets": REFLECTIONS},
        )

    return dag


globals()["dag"] = create_dag()
