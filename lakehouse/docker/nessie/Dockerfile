FROM projectnessie/nessie:0.80.1
COPY docker/nessie/application.properties /nessie/application.properties
ENV QUARKUS_PROFILE=prod
ENTRYPOINT ["/opt/nessie/bin/nessie-server"]
