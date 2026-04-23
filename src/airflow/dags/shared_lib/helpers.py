def formatDagIdForPipeline(domain: str) -> str: 
    domain = domain.replace("-", "_").replace(" ", "_")
    return f"{domain}_pipeline"

def formatDagBronzeTaskId(domain: str) -> str: 
    domain = domain.replace("-", "_").replace(" ", "_")
    return f"ingest_{domain}__bronze"

def formatDagBronzeTaskName(domain: str) -> str: 
    domain =  domain.replace("_", "-").replace(" ", "-")
    return f"ingestion-{domain}--bronze"

def formatDagSilverTaskId(domain: str) -> str: 
    domain = domain.replace("-", "_").replace(" ", "_")
    return f"transform_{domain}__silver"

def formatDagSilverTaskName(domain: str) -> str: 
    domain =  domain.replace("_", "-").replace(" ", "-")
    return f"transformation-{domain}--silver"

def formatDagGoldTaskId(domain: str) -> str: 
    domain = domain.replace("-", "_").replace(" ", "_")
    return f"publish_{domain}__gold"

def formatDagGoldTaskName(domain: str) -> str: 
    domain =  domain.replace("_", "-").replace(" ", "-")
    return f"publishing-{domain}--gold"
    