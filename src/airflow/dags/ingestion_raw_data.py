from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="ingestion_raw_data",
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=False,
) as dag:
    ingest_task = SparkSubmitOperator(
        task_id="ingest_prim_idfm_dataset",
        conn_id="spark_default",
        application="local:///opt/spark/jobs/bronze/ingestion_prim_idfm_dataset.py",
        name="ingestion-prim-idfm",
        deploy_mode="cluster",
        properties_file="/app/spark/confs/spark-small.conf",
        env_vars={
            "NESSIE_NAMESPACE": "bronze",
            "PRIM_DATASET_URI": "{{ var.value.PRIM_DATASET_URI }}",
            "DESTINATION_TABLE": "nessie.bronze.accessibility_gares",
        },
        conf={
            "spark.kubernetes.container.image": "saidsow/spark:3.5.8",
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.sql.catalog.nessie.uri": "http://nessie.k8sdatalab.com/api/v1",
            "spark.hadoop.fs.s3a.access.key": "{{ var.value.MINIO_ACCESS_KEY }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ var.value.MINIO_SECRET_KEY }}",
            "spark.sql.catalog.nessie.ref": "dev",
            "spark.sql.catalog.nessie.warehouse": "s3a://datalake/warehouse/"
        },
        verbose=True,
    )