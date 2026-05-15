from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import shared_lib.helpers as h


dag_domain = "stream_disruptions"

SPARK_VERSION = "3.5.8"

with DAG(
    dag_id=f"silver_{h.format_etl_dag_id(dag_domain)}",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=[*h.generate_etl_dag_tags(dag_domain), "streaming", "silver"],
) as dag:
    silver_stream_task = SparkSubmitOperator(
        task_id=h.format_etl_silver_dag_task_id(dag_domain),
        name=h.format_etl_silver_dag_task_name(dag_domain),
        conn_id="spark_cluster",
        deploy_mode="cluster",
        application="local:///opt/spark/jobs/silver/disruptions/silver_disruptions.py",
        properties_file="/app/spark/confs/spark-small.conf",
        env_vars={
            "SOURCE_TABLE": "nessie.bronze.disruptions",
            "NESSIE_CATALOG": "nessie",
            "SILVER_NAMESPACE": "silver",
            "CHECKPOINT_LOCATION": "s3a://spark-checkpoints/silver/disruptions/",
            "TRIGGER_INTERVAL": "60 seconds",
            "MAX_FILES_PER_MICRO_BATCH": "100",
        },
        conf={
            "spark.kubernetes.container.image": f"saidsow/spark:{SPARK_VERSION}",
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.hadoop.fs.s3a.access.key": "{{ var.value.MINIO_ACCESS_KEY }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ var.value.MINIO_SECRET_KEY }}",
            "spark.sql.catalog.nessie.ref": "dev",
            "spark.sql.catalog.nessie.warehouse": "s3a://datalake/warehouse/",
            "spark.openlineage.namespace": "silver_transformation",
            "spark.openlineage.appName": h.format_etl_silver_dag_task_name(dag_domain),
        },
        retries=0,
        verbose=True,
    )
