from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow_contrib.iceberg_operators import NessieBranchOperator

SPARK_APP = "/opt/spark/jobs/stream_kafka_to_iceberg.py"

def create_dag():
    with DAG(
        dag_id="cdc_stream_mongo_to_iceberg",
        description="Continuous CDC ingestion from MongoDB into Iceberg",
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False,
        tags=["cdc", "streaming"],
    ) as dag:
        ensure_branch = NessieBranchOperator(
            task_id="ensure_branch",
        )

        spark_stream = SparkSubmitOperator(
            task_id="spark_stream",
            application=SPARK_APP,
            conn_id="spark_default",
            application_args=["--namespace", "{{ var.value.iceberg_namespace or 'payments' }}"],
            name="MongoCDCToIceberg",
            jars="/opt/spark/jars/*",
            conf={
                "spark.sql.shuffle.partitions": "200",
            },
        )

        ensure_branch >> spark_stream

    return dag


globals()["dag"] = create_dag()
