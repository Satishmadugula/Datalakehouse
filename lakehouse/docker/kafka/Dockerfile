FROM bitnami/kafka:3.6.1
COPY docker/kafka/entrypoint.sh /opt/bitnami/scripts/kafka/entrypoint.sh
RUN chmod +x /opt/bitnami/scripts/kafka/entrypoint.sh
ENTRYPOINT ["/opt/bitnami/scripts/kafka/entrypoint.sh"]
