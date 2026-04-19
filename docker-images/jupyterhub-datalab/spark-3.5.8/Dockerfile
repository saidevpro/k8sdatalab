FROM apache/spark:3.5.8-scala2.12-java17-python3-ubuntu

USER root

ARG NB_USER=jovyan
ARG NB_UID=1000
ENV NB_USER=${NB_USER}
ENV NB_UID=${NB_UID}
ENV HOME=/home/${NB_USER}
ENV JUPYTER_RUNTIME_DIR=${HOME}/.local/share/jupyter/runtime
ENV JUPYTER_DATA_DIR=${HOME}/.local/share/jupyter

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
 && curl -fsSL -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar \
 && curl -fsSL -o /opt/spark/jars/nessie-spark-extensions-3.5_2.12-0.104.5.jar \
      https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.104.5/nessie-spark-extensions-3.5_2.12-0.104.5.jar \
 && curl -fsSL -o /opt/spark/jars/trino-jdbc-469.jar \
      https://repo1.maven.org/maven2/io/trino/trino-jdbc/469/trino-jdbc-469.jar

RUN pip install --no-cache-dir \
     pyspark=3.5.8 \
     jupyterlab==4.3.4 \
     jupyterhub \
     jupyterlab-git \
     jupyterlab-night \
     theme-darcula \
     pandas

RUN useradd -m -s /bin/bash -u ${NB_UID} ${NB_USER} \
    && mkdir -p ${HOME}/.local/share/jupyter/runtime \
    && mkdir -p ${HOME}/.jupyter \
    && chown -R ${NB_USER}:${NB_USER} ${HOME} \
    && chown -R ${NB_USER}:${NB_USER} /opt/spark

USER ${NB_USER}
WORKDIR ${HOME}
