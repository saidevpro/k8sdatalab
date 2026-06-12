from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id="ml_next_stop",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ml", "mlflow", "next_stop"],
) as dag:

    train_delay_models = SparkSubmitOperator(
        task_id="train_next_stop_delay_models",
        name="train-next-stop-delay-models",
        conn_id="spark_cluster",
        deploy_mode="cluster",
        application="local:///opt/spark/jobs/ml/next_stop/train_delay_models.py",
        properties_file="/app/spark/confs/spark-large.conf",
        env_vars={
            "NESSIE_CATALOG": "nessie",
            "GOLD_NAMESPACE": "gold",
            "FEATURES_TABLE": "nessie.gold.next_stop_features",
            "MLFLOW_EXPERIMENT": "next_stop_delay",
            "MLFLOW_REGISTERED_MODEL": "next_stop_delay_classifier",
            "TRAIN_FRACTION": "0.8",
            "MLFLOW_TRACKING_URI": "http://mlflow.mlops.svc.cluster.local:5000",
            "MLFLOW_S3_ENDPOINT_URL": "http://minio.data-platform.svc.cluster.local:9000",
            "MLFLOW_TRACKING_USERNAME": "{{ var.value.MLFLOW_TRACKING_USERNAME }}",
            "MLFLOW_TRACKING_PASSWORD": "{{ var.value.MLFLOW_TRACKING_PASSWORD }}",
            "AWS_ACCESS_KEY_ID": "{{ var.value.MINIO_ACCESS_KEY }}",
            "AWS_SECRET_ACCESS_KEY": "{{ var.value.MINIO_SECRET_KEY }}",
        },
        conf={
            "spark.kubernetes.container.image": "saidsow/spark:3.5.8",
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.hadoop.fs.s3a.access.key": "{{ var.value.MINIO_ACCESS_KEY }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ var.value.MINIO_SECRET_KEY }}",
            "spark.sql.catalog.nessie.ref": "dev",
            "spark.sql.catalog.nessie.warehouse": "s3a://datalake/warehouse/",
            "spark.openlineage.namespace": "ml_training",
            "spark.openlineage.appName": "train-next-stop-delay-models",
        },
        retries=1,
        retry_delay=timedelta(minutes=5),
        verbose=True,
    )
