from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow_contrib.quality_checks import run_quality_suite


def create_dag():
    with DAG(
        dag_id="validation_quality_checks",
        start_date=datetime(2024, 1, 1),
        schedule_interval="@daily",
        catchup=False,
        tags=["quality", "governance"],
    ) as dag:
        quality = PythonOperator(
            task_id="run_checks",
            python_callable=run_quality_suite,
        )

    return dag


globals()["dag"] = create_dag()
