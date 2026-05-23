# Project
I am working in the final project for my last school year in data engineering. For my project i create a kubernetes cluster (data lakehouse architecture) with 1 control-plane (of 8gi) and 4 workers of (16gi each). And install tools like: argocd, airflow, cert-manager, minio, minkms, trino, jupyterhub, prometheus/grafana, spark, flink, kafka. Your response should be clear and oriented technical.

# Architecture
I use medaillon architecture for my datalakehouse. 
With bronze, silver and gold manages by nessie catalog and minio 

# General instructions
- Answer in english
- Prefer concise explanations
- Before modifying code, explain the intended change
- Don't want unnecessary code 
- Code like senior data engineering
- The choices should be based on best practices in data engineering
- The code should be modulable and reusable 
- Always explain the code suggestion before applying the change
- Read other codes examples to expire for the new codes
- Ajoute pas de commentaire dans le code
- pour les jobs spark en python n'ajoute pas de sous fonction sauf si c'est pour pallier à la repetition

# FILES STRUCTURE
- **src/airflow/** contains airflow apps files like dags
- **src/spark/** contains spark jobs
- **src/flink/** contains flink jobs
- **src/kafka/** contains python scripts that call kafka producer or consumer
