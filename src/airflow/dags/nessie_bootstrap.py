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
    ensure_nessie_namespaces_main_and_dev = BashOperator(
        task_id="ensure_nessie_namespaces_main_and_dev",
        bash_command=r"""
    set -e

    nessie -q \
    -u http://nessie.data-platform.svc.cluster.local:19120/api/v2 \
    -r main \
    -c "CREATE NAMESPACE IF NOT EXISTS bronze" \
    -c "CREATE NAMESPACE IF NOT EXISTS silver" \
    -c "CREATE NAMESPACE IF NOT EXISTS gold"

    nessie -q \
    -u http://nessie.data-platform.svc.cluster.local:19120/api/v2 \
    -r dev \
    -c "CREATE NAMESPACE IF NOT EXISTS bronze" \
    -c "CREATE NAMESPACE IF NOT EXISTS silver" \
    -c "CREATE NAMESPACE IF NOT EXISTS gold"
    """,
    )