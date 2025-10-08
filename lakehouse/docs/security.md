# Security Guidelines

While the local deployment prioritizes developer productivity, production environments must follow strict security practices.

## Secrets management

* Never commit real credentials. The repository ships with `.env.example` defaults only.
* Inject secrets via environment variables (`docker compose --env-file`) or Kubernetes secrets.
* Rotate credentials quarterly and automate rotation for MinIO, PostgreSQL, and MongoDB.

## Authentication & authorization

* **MinIO**: Enable OIDC or AD/LDAP integration. Configure per-bucket IAM policies; examples are provided in `docker/minio/policy.json`.
* **Nessie**: Enable authentication by setting `nessie.authentication.enabled=true` and configuring OAuth2 providers in `docker/nessie/application.properties`.
* **Airflow**: Configure RBAC roles and integrate with company SSO using OAuth (`airflow_config.cfg`).
* **Dremio**: Integrate LDAP/AD and enforce strong passwords. Limit administrative rights.
* **NiFi**: Enable secure HTTPS with client certificates and configure Ranger/NiFi Registry for fine-grained policies.

## Network segmentation

* Place data plane services (MinIO, MongoDB, Kafka) on private subnets accessible only to processing engines.
* Expose control-plane UIs (Airflow, NiFi, Dremio) via bastion hosts or VPN-only access.
* Enforce firewall rules restricting ingress to trusted CIDR ranges.

## Data protection

* Enable TLS in transit using ingress controllers or service meshes. Documented options in [operations.md](operations.md).
* Use server-side encryption for MinIO buckets (SSE-S3 or SSE-KMS). Store encryption keys in an HSM or cloud KMS.
* Implement object versioning and WORM policies for compliance workloads.

## Auditing & logging

* Ship logs to a centralized SIEM (e.g., Splunk, ELK). Ensure access logs for MinIO, Airflow, NiFi, and Dremio are retained for 365 days.
* Enable Nessie audit logging by setting `nessie.logging.audit.enabled=true`.
* Capture Airflow DAG run history and NiFi provenance data regularly.

## Vulnerability management

* Rebuild images monthly with security updates (`docker compose build --pull`).
* Use Trivy or Grype to scan images and fix high/critical vulnerabilities.
* Subscribe to security advisories for MongoDB, Apache projects, and Dremio.

