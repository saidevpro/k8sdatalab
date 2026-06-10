from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id="backfill_clean_next_stop_ids",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["maintenance", "backfill", "next_stop"],
) as dag:

    backfill_clean_next_stop_ids = SparkSubmitOperator(
        task_id="backfill_clean_next_stop_ids",
        application="local:///opt/spark/jobs/maintenance/backfill_clean_next_stop_ids.py",
        name="backfill-clean-next-stop-ids",
        conn_id="spark_local",
        properties_file="/app/spark/confs/spark-small.conf",
        env_vars={
            "NESSIE_CATALOG": "nessie",
            "SILVER_NAMESPACE": "silver",
            "GOLD_NAMESPACE": "gold",
            "DELAY_THRESHOLD_SEC": "60",
        },
        conf={
            "spark.kubernetes.container.image": "saidsow/spark:3.5.8",
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.hadoop.fs.s3a.access.key": "{{ var.value.MINIO_ACCESS_KEY }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ var.value.MINIO_SECRET_KEY }}",
            "spark.sql.catalog.nessie.ref": "dev",
            "spark.sql.catalog.nessie.warehouse": "s3a://datalake/warehouse/",
        },
    )
