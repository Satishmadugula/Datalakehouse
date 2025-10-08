FROM dremio/dremio-oss:24.3.0

USER root
RUN microdnf install -y jq && microdnf clean all
COPY docker/dremio/bootstrap /opt/dremio/bootstrap
RUN chmod +x /opt/dremio/bootstrap/bootstrap.sh
USER dremio
ENTRYPOINT ["/opt/dremio/bootstrap/bootstrap.sh"]
