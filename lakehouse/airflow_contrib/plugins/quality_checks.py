import os
from pathlib import Path

import boto3
from airflow.exceptions import AirflowException

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
RAW_BUCKET = "raw"
RETENTION_FILE = Path("/opt/config/iceberg/retention_policies.yml")


def check_minio_prefix(bucket: str, prefix: str):
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "admin"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "password"),
    )
    resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    if resp.get("KeyCount", 0) == 0:
        raise AirflowException(f"No objects found for s3://{bucket}/{prefix}")


def load_retention_policy():
    import yaml

    return yaml.safe_load(RETENTION_FILE.read_text())


def run_quality_suite(**context):
    # Placeholder example checks using boto3 for counts; extend as needed
    check_minio_prefix(RAW_BUCKET, "orders/")
    # Additional checks could query Iceberg via PyIceberg or Spark REST

