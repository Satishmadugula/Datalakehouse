FROM bitnami/spark:3.5.1

USER root
RUN install_packages curl unzip && \
    mkdir -p /opt/spark/jars && \
    curl -L -o /tmp/iceberg-spark-runtime.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar && \
    curl -L -o /tmp/nessie-spark-extensions.jar https://repo1.maven.org/maven2/org/projectnessie/nessie-spark-extensions-3.5_2.12/0.80.0/nessie-spark-extensions-3.5_2.12-0.80.0.jar && \
    curl -L -o /tmp/nessie-client.jar https://repo1.maven.org/maven2/org/projectnessie/nessie-client/0.80.0/nessie-client-0.80.0.jar && \
    curl -L -o /tmp/aws-bundle.jar https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.25.32/bundle-2.25.32.jar && \
    curl -L -o /tmp/hadoop-aws.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar && \
    curl -L -o /tmp/mongo-spark-connector.jar https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.3.1/mongo-spark-connector_2.12-10.3.1.jar && \
    mv /tmp/*.jar /opt/spark/jars/ && \
    pip install --no-cache-dir pyyaml==6.0.1 pymongo==4.6.2 boto3==1.34.82

COPY docker/spark/conf /opt/spark/conf
COPY docker/spark/jobs /opt/spark/jobs
ENV SPARK_CLASSPATH="/opt/spark/jars/*"
USER 1001
