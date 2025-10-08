import yaml
import yaml
from pyspark.sql import SparkSession

CONFIG_PATH = "/opt/config/iceberg/retention_policies.yml"

def main():
    spark = SparkSession.builder.appName("SnapshotTagger").getOrCreate()
    with open(CONFIG_PATH) as fh:
        config = yaml.safe_load(fh)
    for table, settings in config.get("tables", {}).items():
        target = settings.get("name", table)
        tags = settings.get("tags", [])
        for tag in tags:
            snapshot = tag["snapshot"]
            if snapshot == "latest":
                latest = spark.sql(f"SELECT snapshot_id FROM {target}$snapshots ORDER BY committed_at DESC LIMIT 1").first()
                if latest:
                    snapshot = latest[0]
            spark.sql(
                "CALL lakehouse.system.create_tag(\n"
                f"  table => '{target}',\n"
                f"  tag => '{tag['name']}',\n"
                f"  snapshot_id => '{snapshot}'\n"
                ")"
            )
        retention_days = settings.get("snapshot_retention_days", 30)
        spark.sql(
            "CALL lakehouse.system.expire_snapshots(\n"
            f"  table => '{target}',\n"
            f"  older_than => now() - interval '{retention_days}' day,\n"
            "  clean_orphan_files => true\n"
            ")"
        )


if __name__ == "__main__":
    main()
