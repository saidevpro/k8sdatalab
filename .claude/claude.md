## Context
Final-year capstone project (Master's in Data Engineering, French RNCP36739
certification, two-person team), deployed on an existing lakehouse platform:
self-managed Kubernetes cluster (k3s) on Hetzner, GitOps via ArgoCD,
MinIO storage + Apache Iceberg + Nessie catalog, Airflow orchestration,
Kafka streaming (Strimzi) + Spark Structured Streaming, Trino query engine,
Superset visualization, Prometheus/Grafana observability,
OpenLineage/Marquez data lineage. Constraint: open-source technologies only,
minimal cost.

## Functional goal
A user signs up and creates one or more subscriptions:
- an origin and a destination within the Île-de-France region
- a daily notification time
- preferences: accessible routing required (elevators mandatory, no stairs),
  crowding sensitivity, minimized walking during transfers,
  maximum number of transfers

Every day, at the chosen time, the system sends a push notification
containing the 2 best routes, ranked by a score combining:
1. theoretical trip duration
2. predicted reliability (ML delay-prediction model trained on the
   real-time history we archive ourselves)
3. estimated crowding at the stops along the route (historical Navigo
   ticket-validation data)
4. cumulative walking distance during transfers
5. exclusion of routes that violate accessibility constraints, including
   real-time elevator status (an out-of-service elevator excludes or
   downgrades the route)

# Architecture
I use medaillon architecture for my datalakehouse. 
With bronze, silver and gold manages by nessie catalog and minio 

# General instructions
- You are considered as senior data engineer
- Answer in english
- Prefer concise explanations
- Before modifying code, explain the intended change
- Don't want unnecessary code 
- Code like senior data engineering
- The choices should be based on best practices in data engineering
- The code should be modulable and reusable 
- Always explain the code suggestion before applying the change
- Read existing codes as examples to expire for the new codes
- Don't add comment in the code or add only short code
- For spark jobs, don't devide the code in sub function or do it only if same logic in duplicated
- For the spark jobs code, use exactly the same style as the existing code.

# FILES STRUCTURE
- **src/airflow/** contains airflow apps files like dags
- **src/spark/** contains spark jobs
- **src/flink/** contains flink jobs
- **src/kafka/** contains python scripts that call kafka producer or consumer
