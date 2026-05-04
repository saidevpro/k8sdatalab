FROM flink:2.2.0-scala_2.12-java11

USER root

ARG ICEBERG_VERSION=1.10.1
ARG FLINK_VERSION=2.0
ARG KAFKA_CONNECTOR_VERSION=4.0.1-2.0

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        python3 \
        python3-pip \
        python-is-python3 \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --no-cache-dir apache-flink==2.2.0 --break-system-packages

RUN mkdir -p /opt/flink/plugins/s3-fs-presto \
    && cp /opt/flink/opt/flink-s3-fs-presto-*.jar /opt/flink/plugins/s3-fs-presto/

RUN curl -L -o /opt/flink/lib/iceberg-flink-runtime-${FLINK_VERSION}-${ICEBERG_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-${FLINK_VERSION}/${ICEBERG_VERSION}/iceberg-flink-runtime-${FLINK_VERSION}-${ICEBERG_VERSION}.jar

RUN curl -L -o /opt/flink/lib/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

RUN curl -L -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar

COPY src/flink/jobs /opt/flink/usrlib

RUN chown -R flink:flink /opt/flink/usrlib

USER flink
