import os
from datetime import datetime, timedelta
from pathlib import Path

import requests
import yaml
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pyiceberg.actions import ExpireSnapshots, RewriteManifests
from pyiceberg.catalog import load_catalog

RETENTION_CONFIG = Path("/opt/config/iceberg/retention_policies.yml")
CATALOG_NAME = "nessie"


def load_catalog_conf():
    return {
        "uri": os.getenv("NESSIE_URI", "http://nessie:19120/api/v2"),
        "warehouse": "s3a://gold",
        "credential": {
            "type": "basic",
            "username": os.getenv("MINIO_ROOT_USER", "admin"),
            "password": os.getenv("MINIO_ROOT_PASSWORD", "password"),
        },
    }


class NessieBranchOperator(BaseOperator):
    @apply_defaults
    def __init__(self, action: str = "create", branch: str = None, parent: str = "main", **kwargs):
        super().__init__(**kwargs)
        self.action = action
        self.branch = branch or os.getenv("NESSIE_BRANCH", "main")
        self.parent = parent
        self.base_url = os.getenv("NESSIE_URI", "http://nessie:19120/api/v2").rstrip("/")

    def execute(self, context):
        if self.action == "merge":
            self.merge_branch()
        else:
            self.create_branch()

    def create_branch(self):
        payload = {"name": self.branch, "hash": self.get_hash(self.parent)}
        requests.post(f"{self.base_url}/trees/tree", json=payload, timeout=10)

    def merge_branch(self):
        payload = {
            "fromRefName": self.branch,
            "fromHash": self.get_hash(self.branch),
            "toHash": self.get_hash(self.parent),
        }
        requests.post(
            f"{self.base_url}/trees/branch/{self.parent}/merge", json=payload, timeout=10
        ).raise_for_status()

    def get_hash(self, ref: str) -> str:
        resp = requests.get(f"{self.base_url}/trees/tree/{ref}", timeout=10)
        resp.raise_for_status()
        return resp.json()["hash"]


class IcebergRewriteManifestsOperator(BaseOperator):
    def execute(self, context):
        config = yaml.safe_load(RETENTION_CONFIG.read_text())
        catalog = load_catalog("nessie", **load_catalog_conf())
        for table in config.get("tables", {}):
            tbl = catalog.load_table(table)
            RewriteManifests(tbl).with_target_size_in_bytes(268435456).commit()


class IcebergSnapshotExpireOperator(BaseOperator):
    @apply_defaults
    def __init__(self, retention_days: int = None, **kwargs):
        super().__init__(**kwargs)
        self.retention_days = retention_days

    def execute(self, context):
        config = yaml.safe_load(RETENTION_CONFIG.read_text())
        catalog = load_catalog("nessie", **load_catalog_conf())
        for table, settings in config.get("tables", {}).items():
            retention_days = self.retention_days or settings.get("snapshot_retention_days", 7)
            cutoff = datetime.utcnow() - timedelta(days=retention_days)
            tbl = catalog.load_table(table)
            ExpireSnapshots(tbl).expire_snapshots(cutoff).clean_orphan_files(True).commit()

