# Gouvernance des données — OpenMetadata

Ce dossier configure **OpenMetadata** comme couche de gouvernance du lakehouse
(architecture médaillon bronze/silver/gold sur Iceberg + Nessie + MinIO, requêtée
via Trino) ainsi que de la base applicative PostgreSQL `itineo`.

Tout est **déclaratif et idempotent** : les définitions vivent dans `configs/*.yaml`
et sont appliquées par [`register.py`](register.py) via l'API REST d'OpenMetadata.
Aucune action manuelle dans l'UI n'est nécessaire.

---

## 1. Déploiement (GitOps)

| Élément | Rôle |
|---|---|
| [`register.py`](register.py) | Script idempotent qui pilote l'API OpenMetadata (auth par JWT du bot d'ingestion) |
| [`configs/`](configs/) | Définitions déclaratives (qualité, glossaire, domaines, KPI, org, base applicative) |
| [`kustomization.yaml`](kustomization.yaml) | Génère les ConfigMaps (script + configs) |
| [`k8s/register-job.yaml`](k8s/register-job.yaml) | Job Kubernetes (hook ArgoCD `PostSync`) qui exécute `register.py` via l'image d'ingestion OpenMetadata |
| `argocd/apps/openmetadata-governance.yaml` | Application ArgoCD pointant sur ce dossier |
| `argocd/vault-secrets/openmetadata-secrets.yaml` | Secrets (JWT du bot, mot de passe de la base applicative) issus de Vault |

À chaque synchronisation ArgoCD, le Job rejoue `register.py` ; toutes les
opérations sont en *create-or-update*, donc réexécuter est sans effet de bord.

**Pré-requis (secrets Vault)**
- `prod/openmetadata/config_sync_bot` → propriété `token` (JWT du bot admin) → secret `openmetadata-bot`
- `prod/postgresql-app/admin` → propriété `password` → secret `itineo-app-db`
- `prod/airflow/metadata-db` → propriété `password` → secret `airflow-db`

---

## 2. Sources de données connectées

| Service OpenMetadata | Type | Contenu |
|---|---|---|
| `Trino` | Trino | Catalogue `lakehouse` → schémas `bronze` / `silver` / `gold` (Iceberg/Nessie). Connecté manuellement, métadonnées ingérées. |
| `itineo_app` | PostgreSQL | Base `itineo` (schéma `public`) : `users`, `subscriptions`, `notifications`. Connectée et ingérée par `register.py`. |

FQN type : `Trino.lakehouse.<schema>.<table>` et `itineo_app.itineo.public.<table>`.

Les **57 tables** (bronze/silver/gold + base applicative) ont une **description métier
en français** : config [`configs/descriptions.yaml`](configs/descriptions.yaml),
fonction `register_descriptions`.

**Service de pipelines** : `airflow` (type Airflow) — OpenMetadata lit la base de
métadonnées Airflow (backend PostgreSQL) et ingère les **DAGs comme pipelines**
(tâches + statuts d'exécution). Config [`configs/airflow.yaml`](configs/airflow.yaml),
fonction `register_airflow`. ~23 DAGs ingérés (etl_*, bootstrap_*, ml, maintenance…).

---

## 3. Profiling

Deux profilers alimentent les insights / l'observabilité (métriques colonnes :
taux de nuls, valeurs distinctes, min/max/moyenne) :

| Pipeline | Service | Planning |
|---|---|---|
| `Trino_medallion_profiler` | Trino (bronze/silver/gold) | 03:00 |
| `itineo_app_profiler` | PostgreSQL `itineo_app` | 06:00 |

Configurations : [`configs/profiler.yaml`](configs/profiler.yaml) (Trino) et bloc
profiler de [`register.py`](register.py) (`register_app_database`, PostgreSQL).

**Tuning Trino (cluster peu performant)** : par défaut le profiler lance 5 requêtes
en parallèle, ce qui saturait les workers Trino (`503 PAGE_TRANSPORT_ERROR` → aucun
profil calculé). Le profiler est donc réglé pour être doux :
- `threadCount: 1` — les requêtes de métriques s'enchaînent **en cascade** (séquentiel),
  jamais en parallèle ;
- `profileSample: 10` (`PERCENTAGE`) — échantillonnage `TABLESAMPLE`, bien moins de données scannées ;
- `timeoutSeconds: 10800`.

Conséquence : plus aucune erreur Trino, mais un run **plus lent** (séquentiel, 37 tables).
Le profiler PostgreSQL ne génère pas d'échantillons (pas de copie de PII).

---

## 4. Qualité des données (Data Quality)

Objectif : **chaque table silver et gold** dispose de tests de qualité, exécutés
en **un seul job** pour limiter la charge sur Trino.

- **Suite logique `medallion_quality`** regroupant tous les cas de test.
- **Pipeline `medallion_quality_run`** (`TestSuite`, tous les jours à 04:00) → 1 seul CronJob.
- **78 cas de test** sur les **37 tables** silver + gold.

Modèle de couverture (cf. [`configs/quality_tests.yaml`](configs/quality_tests.yaml)) :

- **Baseline automatique** pour toute table silver/gold : `row_count ≥ 1`
  + `not-null` sur la colonne clé. Les nouvelles tables sont couvertes
  automatiquement, sans modifier la config.
- **Tests curatés** (surchargent la baseline) sur 5 tables critiques du produit :
  - `silver.stop_areas` : coordonnées dans la bbox Île-de-France, `station_id` unique/non nul
  - `gold.station_accessibility` : `pct_elevators_available` ∈ [0,100]
  - `gold.elevators_availability` : intégrité `nb_available ≤ nb_elevators` (SQL custom)
  - `gold.dim_stops` : `stop_id` unique, `wheelchair_boarding ∈ {0,1,2}`
  - `gold.transfer_walking` : clés de transfert non nulles

> **Constat réel détecté** : `gold.crowding_features.id_refa_lda` comporte
> ~10 211 valeurs nulles → clé station manquante sur une partie des lignes de
> crowding (jointure Navigo à corriger côté job Spark).

La base applicative `itineo_app` dispose en plus d'un pipeline
**`auto-classification`** (voir §6) qui contrôle la détection PII.

---

## 5. Glossaire, classifications et tags

Configuration : [`configs/governance.yaml`](configs/governance.yaml).

### Glossaire métier `IDFMobilite`
7 termes métier du domaine transport IDF : **Station, Route, Transfer,
Accessibility, Elevator, Crowding, Reliability**. Propriétaire : la CDO ;
relecteur (reviewer) : le Data Steward.

### Classifications & tags
| Classification | Tags | Application |
|---|---|---|
| `Tier` | Bronze / Silver / Gold | Appliqué aux schémas `bronze`, `silver`, `gold` (niveau médaillon) |
| `PII` | Sensitive / NonSensitive | Appliqué aux colonnes de la base applicative (voir §6) |
| `Certification` | Bronze / Silver / Gold | **Certification d'actif** sur les 54 tables Trino, par couche : bronze→Bronze, silver→Silver, gold→Gold (niveau de confiance = maturité). Bloc `certifications` de [`configs/governance.yaml`](configs/governance.yaml), fonction `register_certifications`. |

---

## 6. Données personnelles (PII)

Double approche (déterministe + automatisée) sur `itineo_app` —
config [`configs/app_database.yaml`](configs/app_database.yaml) :

- **Tags explicites** sur les colonnes personnelles connues :
  - `users` : `email`, `password_hash`, `first_name`, `last_name`, `phone` → **PII.Sensitive**
  - `subscriptions` : `origin_station`, `destination_station` → **PII.NonSensitive** (lieux personnels)
- **Pipeline `itineo_app_autoclassification`** (`autoClassification`, tous les jours
  à 05:00) : échantillonne les données et détecte/étiquette automatiquement les PII
  pour les colonnes futures.

> Recommandation sécurité : désactiver le stockage d'échantillons
> (`storeSampleData: false`) pour ce service afin de ne pas recopier emails /
> hashs de mots de passe dans OpenMetadata.

**Gouvernance au niveau table** (bloc `governance` de
[`configs/app_database.yaml`](configs/app_database.yaml)) : les tables `itineo`
sont rattachées au domaine `user_subscriptions`, possédées par la CDO
(`marie.leroy`, expert : steward), étiquetées **PII.Sensitive** et classées par
criticité — `users`/`subscriptions` en **Tier.Tier1**, `notifications` en **Tier.Tier2**.

---

## 7. Domaines (Data Mesh)

Configuration : [`configs/governance_domains.yaml`](configs/governance_domains.yaml).
Les domaines représentent des **périmètres métier** (et non les couches techniques),
alignés sur les critères de scoring du produit. Les 37 tables silver+gold y sont rattachées.

| Domaine | Type | Périmètre |
|---|---|---|
| `network_schedules` | Source-aligned | Réseau GTFS & horaires (durée théorique, marche) |
| `accessibility` | Consumer-aligned | Accès sans marche & ascenseurs |
| `service_reliability` | Consumer-aligned | Perturbations & prédiction de retards |
| `crowding_ridership` | Consumer-aligned | Validations Navigo & affluence |
| `user_subscriptions` | Consumer-aligned | Comptes utilisateurs, abonnements, notifications (base applicative `itineo`, défini dans [`configs/app_database.yaml`](configs/app_database.yaml)) |

---

## 8. Data Products

5 produits de données = bundles **gold consommables** (16 tables), possédés par
leur domaine — config [`configs/governance_domains.yaml`](configs/governance_domains.yaml).

| Data Product | Domaine | Tables gold |
|---|---|---|
| Trip Schedules | network_schedules | dim_stops, trip_schedule, service_calendar, transfer_walking |
| Station Accessibility | accessibility | station_accessibility, elevators_availability, elevators_downtime |
| Disruptions | service_reliability | disruptions_active, disruptions_by_line |
| Delay Predictions | service_reliability | next_stop_delays, next_stop_features, next_stop_schedule, delays_by_stop |
| Crowding Insights | crowding_ridership | crowding_features, validations_by_category, validations_daily_by_stop |

---

## 9. KPI (entités Metric)

7 indicateurs, chacun avec une **définition métier, une unité et une formule SQL**
sur le gold, rattachés à leur domaine — config
[`configs/governance_domains.yaml`](configs/governance_domains.yaml).

| KPI | Domaine | Définition |
|---|---|---|
| `network_on_time_rate` | service_reliability | % d'arrivées à l'heure (pondéré par passages) |
| `avg_arrival_delay_seconds` | service_reliability | Retard moyen d'arrivée (s) |
| `active_disruptions` | service_reliability | Nombre de perturbations actives |
| `step_free_station_rate` | accessibility | % de stations avec ≥1 ascenseur en service |
| `elevator_availability_rate` | accessibility | % d'ascenseurs opérationnels |
| `avg_transfer_walking_distance_m` | network_schedules | Distance de marche moyenne aux correspondances (m) |
| `daily_validation_volume` | crowding_ridership | Total des validations Navigo (jour le plus récent) |

> OpenMetadata **stocke** la définition + le SQL mais ne l'exécute pas. Pour
> obtenir des valeurs dans le temps, exécuter le SQL via Trino sur planning et
> reposter la valeur (ou exposer les KPI dans Superset).

### KPI de gouvernance (Data Insights)
Deux KPI natifs Data Insights mesurent la **maturité de la gouvernance** dans le
temps (config [`configs/governance.yaml`](configs/governance.yaml) → `dataInsightKpis`) :

| KPI | Cible | Mesure |
|---|---|---|
| `completed_description` | 80 % (fin 2026) | Couverture des descriptions sur les actifs |
| `completed_ownership` | 90 % (fin 2026) | Couverture des propriétaires (owners) sur les actifs |

> Ces KPI nécessitent l'exécution de l'application **Data Insights** d'OpenMetadata
> pour calculer la couverture réelle et tracer la progression vers la cible.

---

## 10. Rôles, utilisateurs et propriété (RBAC)

Configuration : [`configs/governance_org.yaml`](configs/governance_org.yaml).

### Rôles & politiques
| Rôle | Origine | Droits |
|---|---|---|
| `DataSteward` | natif OM | Curation glossaire, tags, descriptions, qualité |
| `DataConsumer` | natif OM | Lecture & requêtes |
| `DataOwner` | personnalisé (`DataOwnerPolicy`) | Édite owners, tags, tier, description, termes de glossaire |
| `DataEngineer` | personnalisé (`DataEngineerPolicy`) | Crée/édite pipelines, lineage, schémas, qualité |

### Organisation (Data Mesh)
`Organization` → BusinessUnit **`idf_mobility_data`** → 4 squads (Groups), un par domaine.

### Utilisateurs (exemples)
| Utilisateur | Rôle | Équipe(s) |
|---|---|---|
| marie.leroy | DataOwner (CDO) | idf_mobility_data |
| paul.moreau | DataSteward | idf_mobility_data |
| lucie.bernard | DataEngineer | idf_mobility_data |
| thomas.dubois | DataOwner | network_schedules + accessibility squads |
| sophie.martin | DataOwner | service_reliability + crowding squads |
| julien.robin | DataConsumer | idf_mobility_data |

### Attribution de la propriété (ownership)
- **Domaines** : possédés par leur squad ; *experts* = data owner du domaine + steward
- **37 tables** : possédées par la squad de leur domaine
- **Data products** : possédés par le data owner du domaine
- **KPI** : possédés par le data owner du domaine
- **Glossaire** : possédé par la CDO ; *reviewer* = steward
- **Pipelines** (profiler + qualité) : possédés par le Data Engineer

> Remarque : ce sont des **identités de gouvernance** (responsabilité/ownership),
> pas des comptes de connexion. La connexion interactive reste sur `admin` tant
> qu'un fournisseur d'authentification (basic-auth/SSO) n'est pas configuré.

---

## 10bis. Data Contracts

Contrats de données sur les actifs consommés, config
[`configs/data_contracts.yaml`](configs/data_contracts.yaml) (fonction
`register_data_contracts`). Chaque contrat lie pour une table : le **schéma**
garanti, des **règles métier** (semantics), les **tests qualité réels** qui les
appliquent (`qualityExpectations` → cas de test existants), un **SLA** et un **propriétaire**.

| Contrat | Table | Statut | SLA |
|---|---|---|---|
| Station Accessibility | gold.station_accessibility | Approved | refresh 1j, dispo 06:00 |
| Elevator Availability | gold.elevators_availability | Approved | refresh 1h, latence max 2h |
| Stops Dimension | gold.dim_stops | Approved | refresh 1j |
| Stop Reliability | gold.delays_by_stop | Approved | refresh 1j |
| Crowding Features | gold.crowding_features | **Draft** | refresh 1j |
| User Accounts | itineo_app…users | Approved | refresh 1h, rétention 36 mois |

> `crowding_features` est en **Draft** délibérément : le contrat documente l'attente
> `id_refa_lda IS NOT NULL`, non encore satisfaite (cf. §4) — à approuver après correction.

## 11. Data Lineage (à configurer)

Le lineage n'est pas encore activé. Particularité : les transformations
bronze→silver→gold sont faites en **Spark** (donc invisibles au parsing de
requêtes Trino). Options recommandées :

1. **OpenLineage → OpenMetadata** (recommandé) : router vers Kafka les événements
   OpenLineage déjà émis par Spark/Airflow → connecteur OpenLineage OM
   (lineage automatique, niveau colonne).
2. **Workflows Trino `usage` + `lineage`** : pour la couche consommation
   (Superset → gold) uniquement.
3. **Lineage explicite** via `PUT /api/v1/lineage` : arêtes médaillon déclarées.

---

## 12. Récapitulatif des plannings

| Pipeline | Type | Planning |
|---|---|---|
| `Trino_medallion_profiler` | profiler | 03:00 |
| `medallion_quality_run` | TestSuite | 04:00 |
| `itineo_app_metadata` | metadata | 02:00 |
| `itineo_app_autoclassification` | autoClassification | 05:00 |
| `itineo_app_profiler` | profiler | 06:00 |
| `airflow_metadata` | metadata (DAGs) | 01:00 |

---

## 13. Exécution manuelle (hors GitOps)

```bash
export OPENMETADATA_HOST_PORT="http://<om-host>:8585/api"
export OPENMETADATA_JWT_TOKEN="<jwt-du-bot>"
export APP_DB_PASSWORD="<mot-de-passe-admin-itineo>"   # optionnel
python src/openmetadata/register.py
```

Dépendances : `requests`, `pyyaml` (présents dans l'image d'ingestion OpenMetadata).
