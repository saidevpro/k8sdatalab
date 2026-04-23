from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import shared_lib.helpers as h

dag_domain = "accessibilites_gares"

with DAG(
    dag_id=h.format_etl_dag_id(dag_domain),
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    ingest_task = SparkSubmitOperator(
        task_id=h.format_etl_bronze_dag_task_id(dag_domain),
        name=h.format_etl_bronze_dag_task_name(dag_domain),
        tags=h.generate_etl_dag_tags(dag_domain),
        conn_id="spark_default",
        application="local:///opt/spark/jobs/bronze/ingestion_prim_idfm_dataset.py",
        deploy_mode="cluster",
        properties_file="/app/spark/confs/spark-small.conf",
        env_vars={
            "NESSIE_NAMESPACE": "bronze",
            "PRIM_DATASET_URI": "{{ var.value.PRIM_DATASET_URI }}",
            "DESTINATION_TABLE": "nessie.bronze.accessibility_gares",
            "DATASET_ENDPOINT": "/accessibilite-en-gare/exports/csv"
        },
        conf={
            "spark.kubernetes.container.image": "saidsow/spark:3.5.8",
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.hadoop.fs.s3a.access.key": "{{ var.value.MINIO_ACCESS_KEY }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ var.value.MINIO_SECRET_KEY }}",
            "spark.sql.catalog.nessie.ref": "dev",
            "spark.sql.catalog.nessie.warehouse": "s3a://datalake/warehouse/",
            "spark.openlineage.namespace": "bronze_ingestion",
            "spark.openlineage.appName": h.format_etl_bronze_dag_task_name(dag_domain)
        },
        verbose=True,
    )