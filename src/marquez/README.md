# Marquez — Data Lineage (OpenLineage)

Marquez est le **backend de data lineage** du projet. Il collecte les événements
**OpenLineage** émis automatiquement par Airflow et Spark, et reconstruit le graphe
de bout en bout : quels jobs lisent/écrivent quels datasets, avec l'historique des
exécutions. Il répond à l'exigence de **traçabilité de la donnée** de la plateforme.

> Marquez ≠ OpenMetadata. Marquez capture le lineage **opérationnel au niveau des
> runs** (ce qui s'est réellement exécuté, quand, avec quels jeux de données).
> OpenMetadata est le **catalogue / la gouvernance** (descriptions, tags, tiers,
> qualité, domaines). Voir « Lien avec OpenMetadata » plus bas.

---

## 1. Déploiement

| Élément | Valeur |
|---|---|
| Chart ArgoCD | `argocd/charts/marquez` (values : [`argocd/values/marquez-values.yaml`](../../argocd/values/marquez-values.yaml)) |
| Namespace | `observability` |
| Backend | PostgreSQL `postgresql-data-platform.data-platform…:5432`, base `marquez` |
| API (in-cluster) | `http://marquez.observability.svc.cluster.local` (port 80, endpoint OpenLineage `…/api/v1/lineage`) |
| UI (in-cluster) | service `marquez-web` |
| API (externe) | `https://marquez-api.k8sdatalab.com` |
| UI (externe) | `https://marquez.k8sdatalab.com` |

Routes Traefik : [`argocd/routes/marquez-route.yaml`](../../argocd/routes/marquez-route.yaml).

---

## 2. Ce que Marquez fait dans ce projet

1. **Trace les pipelines Airflow** — chaque DAG et chacune de ses tâches devient un
   job Marquez (namespace `k8sdatalab`).
2. **Trace les jobs Spark** — chaque application Spark (bronze/silver/gold/ML) émet
   son lineage, jusqu'au niveau des sous-opérations Spark (lecture/écriture/SQL).
3. **Catalogue les datasets et leurs relations** — tables Iceberg (via Nessie),
   objets MinIO/S3, topics Kafka, et relie producteurs ↔ consommateurs.
4. **Conserve l'historique des exécutions** — statut, durée, et versions de datasets
   à chaque run → observabilité et débogage d'incidents (ex. « quel run a produit
   cette version de `gold.dim_stops` ? »).

---

## 3. Comment le lineage est produit (OpenLineage)

### Airflow
Configuré dans [`argocd/values/airflow-values.yaml`](../../argocd/values/airflow-values.yaml) :
- package `apache-airflow-providers-openlineage` ;
- `AIRFLOW__OPENLINEAGE__NAMESPACE = k8sdatalab` ;
- `AIRFLOW__OPENLINEAGE__TRANSPORT` = HTTP vers `http://marquez.observability.svc.cluster.local`, endpoint `api/v1/lineage`.

Airflow émet donc automatiquement un événement par run de DAG/tâche.

### Spark
Configuré par job dans les DAGs (`conf` du `SparkSubmitOperator`) :
- `spark.openlineage.namespace` — la couche médaillon du job (voir §4) ;
- `spark.openlineage.appName` — le nom du job (ex. `validations_silver_validations_daily`).

Le listener OpenLineage de Spark envoie le lineage à Marquez à chaque exécution,
y compris les **datasets d'entrée/sortie** (tables Iceberg, S3, Kafka).

---

## 4. Namespaces (réels, observés via l'API)

| Namespace | Origine | Contenu |
|---|---|---|
| `k8sdatalab` | Airflow | DAGs + tâches (≈ 41 jobs) |
| `bronze_ingestion` | Spark | jobs d'ingestion bronze (≈ 35 jobs, sous-spans inclus) |
| `silver_ingestion` / `silver_transformation` | Spark | transformations silver (≈ 44 / 21 jobs) |
| `gold_publication` | Spark | publication gold (≈ 27 jobs) |
| `ml_training` | Spark | entraînement des modèles ML (≈ 43 jobs) |
| `hive://nessie…:19120` | datasets | tables Iceberg avec lineage capturé (**8** à ce jour) |
| `s3://datalake` | datasets | objets MinIO du lakehouse (warehouse) |
| `s3://mlflow-artifacts`, `s3://tmp-spark` | datasets | artefacts ML, fichiers temporaires Spark |
| `kafka://…:9092` | datasets | topics Kafka (flux temps réel) |
| `default`, `file` | datasets | divers / fichiers locaux |

> Note : deux namespaces silver coexistent (`silver_ingestion` et
> `silver_transformation`) car la convention de nommage a évolué entre DAGs ;
> il serait propre de les unifier sur un seul (`silver_transformation`).

---

## 5. Accès et requêtes API

```bash
# Port-forward local vers l'API Marquez
kubectl port-forward -n observability svc/marquez 5000:80

# Lister les namespaces
curl -s localhost:5000/api/v1/namespaces

# Jobs d'un namespace (ex. les DAGs Airflow)
curl -s "localhost:5000/api/v1/namespaces/k8sdatalab/jobs?limit=100"

# Datasets d'un namespace (ex. tables Iceberg)
curl -s "localhost:5000/api/v1/namespaces/hive%3A%2F%2Fnessie.data-platform.svc.cluster.local%3A19120/datasets"

# Lineage d'un nœud (job ou dataset)
curl -s "localhost:5000/api/v1/lineage?nodeId=job:k8sdatalab:etl_gtfs"
```

UI : `https://marquez.k8sdatalab.com` — exploration visuelle du graphe, recherche,
historique des runs.

---

## 6. Lien avec OpenMetadata

- **Marquez** : lineage opérationnel automatique (run-level), alimenté par OpenLineage.
- **OpenMetadata** (`src/openmetadata`) : catalogue + gouvernance (qualité, glossaire,
  tags, tiers, certifications, domaines, data products, KPI).

OpenMetadata ne reconstruit pas automatiquement le lineage des transformations Spark
(elles ne passent pas par Trino). Pour relier les deux, l'approche recommandée est de
**router les mêmes événements OpenLineage vers OpenMetadata** (connecteur OpenLineage
via Kafka), afin d'obtenir le lineage colonne-à-colonne dans le catalogue. Marquez
reste alors la vue d'observabilité des exécutions.

---

## 7. Pistes d'amélioration

- **Couverture Iceberg partielle** : seules ~8 tables Iceberg ont aujourd'hui un
  lineage de dataset capturé (surtout les flux temps réel `next_stop`, `disruptions`,
  `delays`, `transfers`). Les jobs Spark batch GTFS écrivent leurs tables mais leur
  lineage dataset n'apparaît pas encore — à investiguer (intégration OpenLineage
  Iceberg / nommage des datasets) pour couvrir tout le médaillon.
- Unifier les namespaces silver (`silver_ingestion` → `silver_transformation`).
- Activer une **rétention** des runs Marquez (purge périodique) pour limiter la
  croissance de la base `marquez`.
- Brancher les événements OpenLineage **aussi** vers OpenMetadata (lineage dans le catalogue).
- Sécuriser l'UI/API (authentification) si exposées hors cluster.
