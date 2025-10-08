from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow_contrib.quality_checks import check_minio_prefix

RAW_BUCKET = "raw"
ICEBERG_NAMESPACE = Variable.get("iceberg_namespace", default_var="payments")
SPARK_APP = "/opt/spark/jobs/batch_mongo_to_iceberg.py"


def trigger_nifi_batch(**context):
    """Hit NiFi REST API to start the batch template."""
    import os
    import requests

    nifi_host = os.environ.get("NIFI_HOST", "nifi")
    template_id = os.environ.get("NIFI_BATCH_TEMPLATE_ID", "batch-mongo-to-minio")
    url = f"http://{nifi_host}:9090/nifi-api/flow/process-groups/root"
    payload = {
        "id": "root",
        "templateId": template_id,
        "originX": 0,
        "originY": 0,
        "component": {"name": f"batch-trigger-{datetime.utcnow().isoformat()}"},
    }
    response = requests.post(f"{url}/template-instance", json=payload, timeout=30)
    response.raise_for_status()


def ensure_raw_partition(execution_date: str, collection: str = "orders"):
    dt = datetime.strptime(execution_date, "%Y-%m-%d")
    partition = dt.strftime("dt=%Y-%m-%d")
    prefix = f"{collection}/{partition}"
    check_minio_prefix(bucket=RAW_BUCKET, prefix=prefix)


def get_default_args():
    return {
        "owner": "data-eng",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }


def create_dag():
    with DAG(
        dag_id="ingest_batch_mongo_to_iceberg",
        description="Snapshot Mongo collections and ingest into Iceberg",
        default_args=get_default_args(),
        start_date=datetime(2024, 1, 1),
        schedule_interval="@daily",
        catchup=False,
        max_active_runs=1,
        tags=["batch", "mongo", "iceberg"],
    ) as dag:
        trigger_nifi = PythonOperator(
            task_id="trigger_nifi_batch",
            python_callable=trigger_nifi_batch,
        )

        wait_for_raw = PythonOperator(
            task_id="wait_for_raw_partition",
            python_callable=ensure_raw_partition,
            op_kwargs={"execution_date": "{{ ds }}"},
        )

        spark_submit = SparkSubmitOperator(
            task_id="spark_batch_ingest",
            application=SPARK_APP,
            conn_id="spark_default",
            application_args=[
                "--namespace",
                ICEBERG_NAMESPACE,
                "--execution-date",
                "{{ ds }}",
            ],
            jars="/opt/spark/jars/*",
            conf={
                "spark.kubernetes.container.image": "lakehouse/spark:3.5.1",
            },
        )

        trigger_nifi >> wait_for_raw >> spark_submit

    return dag


globals()["dag"] = create_dag()
