FROM saidsow/spark:3.5.8

ARG COMPONENT=docker-images/spark/3.5.8-jobs

COPY src/spark/jobs /opt/spark/jobs
COPY ${COMPONENT}/confs /app/spark/confs

RUN pip install --no-cache-dir \
    requests