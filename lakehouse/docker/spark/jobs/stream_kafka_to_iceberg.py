import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructField, StructType, StringType, DoubleType

SCHEMA = StructType([
    StructField("operationType", StringType()),
    StructField(
        "fullDocument",
        StructType([
            StructField("_id", StringType()),
            StructField("order_id", StringType()),
            StructField("merchant_id", StringType()),
            StructField("status", StringType()),
            StructField("amount", DoubleType()),
            StructField("currency", StringType()),
            StructField("event_ts", StringType()),
        ]),
    ),
    StructField(
        "documentKey",
        StructType([StructField("_id", StringType())]),
    ),
    StructField("clusterTime", StringType()),
])


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", default="payments")
    parser.add_argument("--topic", default="mongo.cdc.orders")
    parser.add_argument("--enable-kafka", action="store_true")
    return parser.parse_args()


def upsert_batch(table):
    def _fn(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        batch_df.createOrReplaceTempView("cdc_batch")
        spark = batch_df.sparkSession
        spark.sql(
            f"""
            MERGE INTO {table} AS target
            USING (
                SELECT
                    coalesce(fullDocument.order_id, documentKey._id) AS order_id,
                    fullDocument.merchant_id AS merchant_id,
                    fullDocument.status AS status,
                    fullDocument.amount AS amount,
                    fullDocument.currency AS currency,
                    coalesce(fullDocument.event_ts, clusterTime) AS event_ts,
                    operationType
                FROM cdc_batch
            ) AS source
            ON target.order_id = source.order_id
            WHEN MATCHED AND source.operationType = 'delete' THEN DELETE
            WHEN MATCHED AND source.operationType != 'delete' THEN UPDATE SET
                merchant_id = COALESCE(source.merchant_id, target.merchant_id),
                status = COALESCE(source.status, target.status),
                amount = COALESCE(source.amount, target.amount),
                currency = COALESCE(source.currency, target.currency),
                event_ts = COALESCE(to_timestamp(source.event_ts), target.event_ts)
            WHEN NOT MATCHED AND source.operationType != 'delete' THEN INSERT (
                order_id, merchant_id, status, amount, currency, event_ts
            ) VALUES (
                source.order_id,
                source.merchant_id,
                source.status,
                source.amount,
                source.currency,
                to_timestamp(source.event_ts)
            )
            """
        )

    return _fn


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("MongoCDCToIceberg").getOrCreate()
    table = f"lakehouse.{args.namespace}.merchant_events"

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            order_id STRING,
            merchant_id STRING,
            status STRING,
            amount DOUBLE,
            currency STRING,
            event_ts TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (days(event_ts), bucket(16, merchant_id))
        TBLPROPERTIES ('write.merge.mode'='merge-on-read')
        """
    )

    enable_kafka = args.enable_kafka or os.getenv("ENABLE_KAFKA", "true").lower() == "true"
    if enable_kafka:
        stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", os.getenv("KAFKA_BROKERS", "kafka:9092"))
            .option("subscribe", args.topic)
            .option("startingOffsets", "latest")
            .load()
        )
        payload = stream.select(from_json(col("value").cast("string"), SCHEMA).alias("data"))
    else:
        stream = (
            spark.readStream.format("mongodb")
            .option("spark.mongodb.connection.uri", os.getenv("MONGO_URI"))
            .option("spark.mongodb.database", os.getenv("MONGO_DATABASE", "payments"))
            .option("spark.mongodb.collection", "orders")
            .option("change.stream.publish.full.document.only", "true")
            .load()
        )
        payload = stream.select(from_json(col("fullDocument"), SCHEMA).alias("data"))

    parsed = payload.select("data.*")

    query = (
        parsed.writeStream.outputMode("update")
        .foreachBatch(upsert_batch(table))
        .option("checkpointLocation", f"s3a://{os.getenv('SPARK_CHECKPOINT_BUCKET', 'bronze')}/checkpoints/merchant_events")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
