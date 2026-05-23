from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id="iceberg_retention_maintenance",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["maintenance", "iceberg", "nessie"],
) as dag:

    iceberg_maintenance = SparkSubmitOperator(
        task_id="iceberg_maintenance_all_layers",
        application="local:///opt/spark/jobs/maintenance/iceberg_retention_cleanup.py",
        name="iceberg-daily-maintenance",
        conn_id="spark_local",
        properties_file="/app/spark/confs/spark-small.conf",
        conf={
            "spark.kubernetes.container.image": "saidsow/spark:3.5.8",
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.hadoop.fs.s3a.access.key": "{{ var.value.MINIO_ACCESS_KEY }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ var.value.MINIO_SECRET_KEY }}",
            "spark.sql.catalog.nessie.ref": "main",
            "spark.sql.catalog.nessie.warehouse": "s3a://datalake/warehouse/"
        },
    )
