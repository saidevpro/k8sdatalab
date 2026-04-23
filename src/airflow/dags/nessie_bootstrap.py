from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="bootstrap_nessie",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bootstrap", "nessie"],
) as dag:
    ensure_nessie_namespaces = BashOperator(
        task_id="ensure_nessie_namespaces_main_and_dev",
        bash_command="""
        set -e

        nessie sql <<'SQL'
        CONNECT TO http://nessie.data-platform.svc.cluster.local:19120/api/v2 ON main;
        CREATE NAMESPACE IF NOT EXISTS bronze;
        CREATE NAMESPACE IF NOT EXISTS silver;
        CREATE NAMESPACE IF NOT EXISTS gold;

        CONNECT TO http://nessie.data-platform.svc.cluster.local:19120/api/v2 ON dev;
        CREATE NAMESPACE IF NOT EXISTS bronze;
        CREATE NAMESPACE IF NOT EXISTS silver;
        CREATE NAMESPACE IF NOT EXISTS gold;
        SQL
        """,
    )