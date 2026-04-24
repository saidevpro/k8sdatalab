FROM saidsow/spark-base:3.5.8

USER root

COPY .confs/spark /app/spark/confs
COPY src/spark/jobs /opt/spark/jobs

RUN chown -R spark:root /app/spark/confs /opt/spark/jobs \
    && chmod -R g=u /app/spark/confs /opt/spark/jobs

WORKDIR /opt/spark/work-dir

USER spark