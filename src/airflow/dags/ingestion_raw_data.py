from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime

with DAG(
    dag_id="ingestion_raw_data",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:
    ingest_task = SparkKubernetesOperator(
        task_id="ingest_prim_idfm_dataset",
        namespace="spark-jobs",
        application_file="/app/spark/confs/spark-small.conf",
        image="saidevpro/spark:3.5.8-jobs",
        app_name="ingestion-prim-idfm",
        main_application_file="local:///opt/spark/jobs/bronze/ingestion_prim_idfm_dataset.py",
        spark_conf={
            "spark.kubernetes.container.image": "saidevpro/spark:3.5.8-jobs",
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.master": "k8s://https://kubernetes.default.svc:443",
            "spark.submit.deployMode": "cluster",
            "spark.sql.catalog.nessie.ref": "dev",
            "spark.sql.catalog.nessie.uri": "http://nessie.k8sdatalab.com/api/v1",
            "spark.hadoop.fs.s3a.access.key": "{{ var.value.MINIO_ACCESS_KEY }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ var.value.MINIO_SECRET_KEY }}"
        },
        env_vars={
            "NESSIE_NAMESPACE": "bronze",
            "PRIM_DATASET_URI": "{{ var.value.PRIM_DATASET_URI }}",
            "DESTINATION_TABLE": "nessie.bronze.accessibility_gares"
        }
    )