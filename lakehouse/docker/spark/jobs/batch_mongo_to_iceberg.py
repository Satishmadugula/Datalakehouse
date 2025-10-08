import argparse
import json
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array,
    col,
    current_timestamp,
    lit,
    map_from_arrays,
    to_json,
    to_timestamp,
)
from pyspark.sql.types import StructType

TABLE_PROPERTIES_PATH = "/opt/config/iceberg/table_properties.yml"
RETENTION_CONFIG = "/opt/config/iceberg/retention_policies.yml"
EXPECTED_COLUMNS = {
    "order_id",
    "merchant_id",
    "status",
    "amount",
    "currency",
    "event_ts",
    "ingestion_ts",
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", default="payments")
    parser.add_argument("--execution-date", dest="execution_date", default=None)
    parser.add_argument("--collection", default="orders")
    return parser.parse_args()


def load_table_properties():
    path = Path(TABLE_PROPERTIES_PATH)
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text()).get("orders", {})


def flatten(df):
    """Flatten nested structs into top-level columns."""
    complex_fields = True
    while complex_fields:
        complex_fields = False
        fields = []
        for field in df.schema.fields:
            data_type = field.dataType
            name = field.name
            if isinstance(data_type, StructType):
                complex_fields = True
                for nested in data_type.fields:
                    fields.append(col(f"{name}.{nested.name}").alias(f"{name}_{nested.name}"))
            else:
                fields.append(col(name))
        df = df.select(*fields)
    return df


def apply_rest_column(df):
    rest_cols = [c for c in df.columns if c not in EXPECTED_COLUMNS]
    if not rest_cols:
        return df.withColumn("_rest", map_from_arrays(array([]), array([])))
    keys = array(*[lit(c) for c in rest_cols])
    values = array(*[to_json(col(c)) for c in rest_cols])
    df = df.withColumn("_rest", map_from_arrays(keys, values))
    return df.drop(*rest_cols)


def create_orders_table(spark: SparkSession, namespace: str, properties: dict):
    table = f"lakehouse.{namespace}.orders"
    props = ",\n".join([f"'{k}'='{v}'" for k, v in properties.items()])
    tbl_properties_clause = f"TBLPROPERTIES ({props})" if props else ""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            order_id STRING,
            merchant_id STRING,
            status STRING,
            amount DOUBLE,
            currency STRING,
            event_ts TIMESTAMP,
            ingestion_ts TIMESTAMP,
            _rest MAP<STRING, STRING>
        )
        USING ICEBERG
        PARTITIONED BY (days(event_ts))
        {tbl_properties_clause}
        """
    )
    return table


def merge_orders(spark: SparkSession, table: str, df):
    df.createOrReplaceTempView("incoming_orders")
    spark.sql(
        f"""
        MERGE INTO {table} AS target
        USING incoming_orders AS source
        ON target.order_id = source.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    execution_date = args.execution_date
    if execution_date is None:
        from datetime import datetime

        execution_date = datetime.utcnow().strftime("%Y-%m-%d")
    raw_path = f"s3a://raw/{args.collection}/dt={execution_date}/*"
    df = spark.read.json(raw_path)
    df = flatten(df)
    if "event_ts" in df.columns:
        df = df.withColumn("event_ts", to_timestamp(col("event_ts")))
    df = df.withColumn("ingestion_ts", current_timestamp())
    df = apply_rest_column(df)

    properties = load_table_properties()
    table = create_orders_table(spark, args.namespace, properties)
    merge_orders(spark, table, df)
    spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY (event_ts)")


if __name__ == "__main__":
    main()
