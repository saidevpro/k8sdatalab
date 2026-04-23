def format_etl_dag_id(domain: str) -> str:
    domain = domain.replace("-", "_").replace(" ", "_")
    return f"etl_{domain}"

def format_etl_bronze_dag_task_id(domain: str) -> str:
    domain = domain.replace("-", "_").replace(" ", "_")
    return f"ingest_{domain}__bronze"

def format_etl_bronze_dag_task_name(domain: str) -> str:
    domain = domain.replace("_", "-").replace(" ", "-")
    return f"ingestion-{domain}--bronze"

def format_etl_silver_dag_task_id(domain: str) -> str:
    domain = domain.replace("-", "_").replace(" ", "_")
    return f"transform_{domain}__silver"

def format_etl_silver_dag_task_name(domain: str) -> str:
    domain = domain.replace("_", "-").replace(" ", "-")
    return f"transformation-{domain}--silver"

def format_etl_gold_dag_task_id(domain: str) -> str:
    domain = domain.replace("-", "_").replace(" ", "_")
    return f"publish_{domain}__gold"

def format_etl_gold_dag_task_name(domain: str) -> str:
    domain = domain.replace("_", "-").replace(" ", "-")
    return f"publishing-{domain}--gold"
    
def generate_etl_dag_tags(domain_name: str) -> list[str]:
    normalized = domain_name.replace("_", " ").replace("-", " ").strip().lower()
    domain_tag = normalized.replace(" ", "-")
    parts = [part for part in normalized.split() if part]

    tags = ["etl", "spark", *parts]
    return list(dict.fromkeys(tags))