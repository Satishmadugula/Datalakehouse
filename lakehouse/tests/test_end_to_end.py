import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_docker_compose_services():
    compose = (ROOT / "docker" / "docker-compose.yml").read_text()
    for service in ["minio", "nessie", "airflow-webserver", "spark", "mongo"]:
        assert service in compose


def test_kafka_topics_config():
    topics = json.loads((ROOT / "config" / "kafka" / "topics.json").read_text())
    names = {t["name"] for t in topics["topics"]}
    assert "mongo.cdc.orders" in names


def test_retention_policy_tables():
    content = (ROOT / "config" / "iceberg" / "retention_policies.yml").read_text()
    assert "tables:" in content
    assert "lakehouse.payments.orders" in content
