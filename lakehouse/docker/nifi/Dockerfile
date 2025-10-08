FROM apache/nifi:1.26.0

USER root
COPY docker/nifi/templates /opt/nifi/nifi-current/conf/templates
COPY docker/nifi/scripts/nifi-bootstrap.sh /opt/nifi/scripts/nifi-bootstrap.sh
RUN chmod +x /opt/nifi/scripts/nifi-bootstrap.sh
USER nifi
ENTRYPOINT ["/opt/nifi/scripts/nifi-bootstrap.sh"]
