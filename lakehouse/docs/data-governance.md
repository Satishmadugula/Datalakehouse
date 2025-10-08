# Data Governance Strategy

The lakehouse leverages Project Nessie and Apache Iceberg to enforce governance, lineage, and retention controls.

## Branching model

* **main**: production-grade tables consumed by downstream systems.
* **dev/<team>**: isolated branches for experimentation. Spark jobs target branches by exporting `NESSIE_REF`.
* **feature/<ticket>**: short-lived branches for schema evolution or data backfills.

Merges into `main` occur via Airflow maintenance DAG once quality checks succeed. The DAG uses the `NessieBranchOperator` to perform merges and tag releases.

## Retention policies

Defined in `config/iceberg/retention_policies.yml`:

* Snapshot expiration after 7 days for bronze tables, 30 days for silver, 90 days for gold.
* Snapshot tagging before expiration ensures reproducibility of historical analytics when required.
* Compaction thresholds tune file sizes to ~512 MB for bronze and ~256 MB for gold tables.

## Data quality

`airflow_contrib/quality_checks.py` implements lightweight validations:

* Row count delta thresholds
* Null-rate monitoring for key attributes
* Duplicate detection on primary business keys

Failed checks block merges to `main` and emit Airflow alerts.

## Lineage

* NiFi records provenance for ingestion flows.
* Spark jobs attach Iceberg snapshot summaries containing source paths and job metadata.
* Dremio captures dataset dependencies for reflections.

## Access patterns

* Raw bucket: restricted to ingestion services.
* Bronze/Silver: accessible to data engineering.
* Gold: curated datasets for analysts, exposed via Dremio semantic layer.

## Compliance

* Implement GDPR/CCPA deletion via Iceberg position deletes (Spark job template provided).
* Use Nessie tags to freeze regulatory reporting datasets at quarter end.
* Document data classification in `config/iceberg/table_properties.yml` under `governance.tags`.

