from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id="maintenance_nessie_merge_dev_to_main",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["maintenance", "nessie", "promotion"],
) as dag:

    merge_dev_to_main = SparkSubmitOperator(
        task_id="merge_nessie_dev_to_main",
        application="local:///opt/spark/jobs/maintenance/merge_nessie_dev_to_main.py",
        name="nessie-merge-dev-to-main",
        conn_id="spark_local",
        properties_file="/app/spark/confs/spark-small.conf",
        env_vars={
            "NESSIE_CATALOG": "nessie",
            "SOURCE_BRANCH": "dev",
            "TARGET_BRANCH": "main",
        },
        conf={
            "spark.kubernetes.container.image": "saidsow/spark:3.5.8",
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.hadoop.fs.s3a.access.key": "{{ var.value.MINIO_ACCESS_KEY }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ var.value.MINIO_SECRET_KEY }}",
            "spark.sql.catalog.nessie.ref": "main",
            "spark.sql.catalog.nessie.warehouse": "s3a://datalake/warehouse/",
        },
    )
