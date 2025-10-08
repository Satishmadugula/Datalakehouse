from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_compaction_job_exists():
    path = ROOT / "docker" / "spark" / "jobs" / "compaction_rewrite_data_files.py"
    assert path.exists()


def test_retention_snapshot_days_positive():
    content = (ROOT / "config" / "iceberg" / "retention_policies.yml").read_text().splitlines()
    days = [
        int(line.split(":")[1].strip())
        for line in content
        if "snapshot_retention_days" in line
    ]
    assert all(value > 0 for value in days)
