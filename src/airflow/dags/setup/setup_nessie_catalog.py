from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id="setup_nessie_catalog",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["setup", "nessie", "iceberg"]
) as dag:
    spark_test = SparkSubmitOperator(
        task_id="create_nessie_branch_namespace",
        application="local:///opt/spark/jobs/setup/setup_nessie_catalog.py",
        name="create-nessie-branch-namespace",
        conn_id="spark_local",
        properties_file="/app/spark/confs/spark-small.conf",
        conf={
            "spark.kubernetes.container.image": "saidsow/spark:3.5.8",
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.hadoop.fs.s3a.access.key": "{{ var.value.MINIO_ACCESS_KEY }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ var.value.MINIO_SECRET_KEY }}",
            "spark.sql.catalog.nessie.ref": "dev",
            "spark.sql.catalog.nessie.warehouse": "s3a://datalake/warehouse/",
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": "s3a://spark-logs/eventlogs/",
        }
    )
