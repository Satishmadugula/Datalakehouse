from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_docs_describe_time_travel():
    doc = (ROOT / "docs" / "architecture.md").read_text()
    assert "time travel" in doc.lower()
    readme = (ROOT / "README.md").read_text().lower()
    assert "time travel" in readme
