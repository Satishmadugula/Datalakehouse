from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow_contrib.iceberg_operators import (
    IcebergSnapshotExpireOperator,
    IcebergRewriteManifestsOperator,
    NessieBranchOperator,
)
from airflow_contrib.quality_checks import load_retention_policy

SPARK_APP = "/opt/spark/jobs/compaction_rewrite_data_files.py"


def create_dag():
    with DAG(
        dag_id="maintenance_compaction_snapshot_policies",
        description="Apply Iceberg compaction and snapshot retention policies",
        start_date=datetime(2024, 1, 1),
        schedule_interval="0 2 * * *",
        catchup=False,
        tags=["maintenance", "iceberg"],
    ) as dag:
        policy = PythonOperator(
            task_id="load_policy",
            python_callable=load_retention_policy,
        )

        rewrite_data = SparkSubmitOperator(
            task_id="rewrite_data_files",
            application=SPARK_APP,
            conn_id="spark_default",
            jars="/opt/spark/jars/*",
        )

        rewrite_manifests = IcebergRewriteManifestsOperator(
            task_id="rewrite_manifests",
        )

        expire = IcebergSnapshotExpireOperator(
            task_id="expire_snapshots",
        )

        merge_branch = NessieBranchOperator(
            task_id="merge_branch",
            action="merge",
        )

        policy >> rewrite_data >> rewrite_manifests >> expire >> merge_branch

    return dag


globals()["dag"] = create_dag()
