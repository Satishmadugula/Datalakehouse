from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

NAMESPACE = "payments"
TABLE = f"lakehouse.{NAMESPACE}.clickstream"


def run_version_1(spark: SparkSession):
    df = spark.createDataFrame(
        [
            ("sess-1", "user-1", {"page": "home", "action": "view"}),
            ("sess-2", "user-2", {"page": "search", "action": "click"}),
        ],
        ["session_id", "user_id", "attributes"],
    )
    df = df.withColumn("event_ts", current_timestamp())
    df = df.withColumn("_rest", lit(None))
    (
        df.writeTo(TABLE)
        .using("iceberg")
        .tableProperty("write.target-file-size-bytes", "268435456")
        .createOrReplace()
    )


def promote_new_fields(spark: SparkSession):
    spark.sql(
        f"""
        ALTER TABLE {TABLE}
        ADD COLUMN browser STRING
        """
    )
    spark.sql(
        f"""
        UPDATE {TABLE}
        SET browser = attributes['browser']
        WHERE attributes['browser'] IS NOT NULL
        """
    )


def run_version_2(spark: SparkSession):
    df = spark.createDataFrame(
        [
            ("sess-3", "user-3", {"page": "checkout", "browser": "safari"}),
        ],
        ["session_id", "user_id", "attributes"],
    )
    df = df.withColumn("event_ts", current_timestamp())
    df = df.withColumn("_rest", lit(None))
    df.writeTo(TABLE).append()


def rename_field(spark: SparkSession):
    spark.sql(
        f"""
        ALTER TABLE {TABLE}
        RENAME COLUMN attributes TO metadata
        """
    )


def time_travel_demo(spark: SparkSession):
    history = spark.sql(f"SELECT snapshot_id FROM {TABLE}$snapshots ORDER BY committed_at")
    first_snapshot = history.first()[0]
    old = spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF {first_snapshot}")
    old.show(truncate=False)


def main():
    spark = SparkSession.builder.appName("SchemaEvolutionDemo").getOrCreate()
    run_version_1(spark)
    promote_new_fields(spark)
    run_version_2(spark)
    rename_field(spark)
    time_travel_demo(spark)


if __name__ == "__main__":
    main()
