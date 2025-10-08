import yaml
from pyspark.sql import SparkSession

CONFIG_PATH = "/opt/config/iceberg/retention_policies.yml"

def load_config():
    with open(CONFIG_PATH) as fh:
        return yaml.safe_load(fh)


def main():
    spark = SparkSession.builder.appName("IcebergCompaction").getOrCreate()
    config = load_config() or {}
    tables = config.get("tables", {})
    for table, settings in tables.items():
        target = settings.get("name", table)
        spark.sql(f"CALL lakehouse.system.rewrite_data_files(table => '{target}')")
        spark.sql(f"CALL lakehouse.system.rewrite_manifests(table => '{target}')")
        spark.sql(
            "CALL lakehouse.system.expire_snapshots(\n"
            f"  table => '{target}',\n"
            f"  older_than => now() - interval '{settings.get('snapshot_retention_days', 7)}' day\n"
            ")"
        )


if __name__ == "__main__":
    main()
