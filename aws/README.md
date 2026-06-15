# Datalab on AWS — EMR Serverless batch platform

CloudFormation recreation of the on-prem **batch** Airflow DAGs on AWS EMR
Serverless. Same Spark jobs run both on-prem (k3s/MinIO/Nessie) and on AWS
(EMR Serverless/S3/Glue) — only the catalog name and a few env vars change.

## What it provisions

| Area | Resource |
|------|----------|
| Network | None — EMR Serverless runs on the AWS-managed network (internet egress, no NAT cost) |
| Encryption | KMS key `alias/datalab-warehouse` (warehouse data SSE-KMS) |
| Storage | `datalab-{warehouse,logs,tmp-spark}-<account-id>` buckets (account id suffix keeps the global S3 names unique) |
| Catalog | Glue databases `bronze`, `silver`, `gold` — created by the jobs (`CREATE NAMESPACE IF NOT EXISTS`), not by this stack |
| Config | SSM `/datalab/prim/dataset-uri` (value), Secrets Manager `datalab/prim/token` (secret) |
| Image | ECR repo `datalab-spark-emr` + IAM push user (access key/secret in stack outputs) |
| Compute | EMR Serverless app `datalab-spark` with **shared** Spark config (Iceberg + Glue + S3FileIO) |
| Logs | CloudWatch log groups `/datalab/emr/{bronze,silver,gold}`, stream prefix = domain/dataset |
| Orchestration | 6 Step Functions state machines (one per DAG) + EventBridge schedules |

## Batch DAGs recreated

`accessibilites_gares` (daily), `calendar` (monthly), `gtfs` (every 4h),
`elevators` (every 4h), `relations` (daily), `validations` (yearly).

Excluded as requested: streaming (`disruptions`, `next_stop`), `ml_*`, `setup`,
and Nessie-specific `maintenances` (Nessie branches/merge have no Glue equivalent).

## Why these choices

- **Step Functions (Standard) + EventBridge Scheduler.** Step Functions models
  the `bronze >> silver >> gold` dependencies (and the parallel silver/gold task
  groups, via `Map`). EMR Serverless integrates natively via
  `startJobRun.sync`. At these schedules the state-transition cost is a few
  cents/month. Express would be more expensive here (billed per duration while
  blocking on the Spark run). EventBridge Scheduler is the cron trigger.
- **Shared config on the application.** Common Spark properties (Iceberg
  extensions, Glue catalog, S3FileIO, warehouse, KMS, `ICEBERG_CATALOG_NAME`)
  live in the app `RuntimeConfiguration`, so no run repeats them. Each job only
  passes its size profile (`--properties-file`) and per-job env vars.
- **Profiles** `.confs/spark/aws/spark-{small,medium,large}.conf` hold only the
  sizing (executor/driver cores & memory), tuned to EMR Serverless minimums.

## Catalog config that makes the jobs work on Glue

Set application-wide (no per-job change needed):

```
spark.sql.extensions               = org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.glue_catalog               = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.glue_catalog.catalog-impl  = org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.glue_catalog.io-impl       = org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.glue_catalog.warehouse     = s3://datalab-warehouse-<account-id>/warehouse/
spark.sql.catalog.glue_catalog.s3.sse.type   = kms
spark.sql.catalog.glue_catalog.s3.sse.key    = <warehouse KMS key arn>
spark.emr-serverless.driverEnv.ICEBERG_CATALOG_NAME = glue_catalog
```

The jobs read `catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")` and
`CREATE NAMESPACE IF NOT EXISTS {catalog_name}.<layer>`, which maps cleanly to
Glue databases.

## Deploy

Requires: AWS CLI v2, an authenticated AWS profile. The `aws-spark-emr` image is
built by the **CI image pipeline** (`.github/workflows/docker-build.yml`), which
pushes it to the ECR repo of the same name — this stack references that repo
(`SparkRepoName`), it does not create or build it.

```bash
# 1. infra (no EMR app yet — empty SparkImageUri / HasImage false)
AWS_REGION=eu-west-3 ./aws/deploy.sh phase1

# 2. build the image in CI (skip if aws-spark-emr:3.5.8 already exists):
#    push a change under src/spark/jobs or .confs/spark, OR run the
#    "Build & publish Docker images" workflow with force_build=aws-spark-emr:3.5.8

# 3. EMR app + Step Functions + schedules (reads the repo URI from the outputs)
PRIM_TOKEN='your-prim-api-token' AWS_REGION=eu-west-3 ./aws/deploy.sh phase2
```

The local `./aws/deploy.sh build` step still works if you have Docker with buildx,
but CI is the normal path. The stack-managed push user (`EcrPushAccessKeyId` /
`EcrPushSecretAccessKey` outputs) is scoped to push/pull on `aws-spark-emr` for
that manual flow.

## Run a pipeline on demand

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:eu-west-3:<acct>:stateMachine:datalab-etl-gtfs
```

## Networking & internet access

The EMR Serverless application has **no `NetworkConfiguration`**, so it runs on
the AWS-managed network which **has internet egress by default** — it reaches
`data.iledefrance-mobilites.fr` and all AWS service endpoints with **no NAT
gateway** (and therefore no NAT cost). The trade-off: jobs cannot reach private
VPC resources. None of these batch jobs need that. If you later need in-VPC
access (e.g. a private RDS), add a VPC with private subnets + a NAT gateway and
set the app's `NetworkConfiguration` to those subnets/SG.

## Cost notes (student / low budget)

- No fixed networking cost (no NAT).
- EMR Serverless bills only while jobs run; `MaximumCapacity` caps it and
  auto-stop is 15 min.
- Log retention is 14 days.

## Caveats to validate

- **Iceberg jars in the image.** EMR provides AWS SDK v2; the Apache
  `iceberg-spark-runtime` baked in the image supplies `GlueCatalog`/`S3FileIO`.
  If you hit class conflicts, switch to EMR's bundled Iceberg jars.
- **`s3a://` tmp reads.** `gtfs` and `validations` bronze stage extracts to the
  `datalab-tmp-spark` bucket (now via the `SPARK_TMP_BUCKET` env var) and read
  them with `s3a://`. They rely on the job role through the default credential
  chain; verify on first run.
- **Secret injection.** The PRIM token is fetched by Step Functions
  (`secretsmanager:getSecretValue`) and passed as a driver env var, so it
  appears in the execution history. Acceptable for a POC; for production fetch
  it inside the job instead.
- **Global bucket names.** Bucket names carry the `-<account-id>` suffix to stay
  globally unique, so `Prefix=datalab` is fine. Glue databases are not stack-managed,
  so a pre-existing `bronze`/`silver`/`gold` no longer blocks the deploy.
