import os
import requests
import zipfile
import boto3
import io
import secrets
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


def createOrOverwritePartitions(sparkSession, df, dest_table):
    if not sparkSession.catalog.tableExists(dest_table):
        (
            df
            .writeTo(dest_table)
            .partitionedBy("ingestion_date")
            .option("merge-schema", "true")
            .create()
        )
    else:
        (
            df
            .writeTo(dest_table)
            .option("merge-schema", "true")
            .overwritePartitions()
        )


nessie_catalog_namespace = "nessie.bronze"
PRIM_DATASET_URI = os.getenv("PRIM_DATASET_URI")
ENDPOINT = "/offre-horaires-tc-gtfs-idfm/exports/json"
PRIM_TOKEN = os.getenv("PRIM_TOKEN")

headers = {
    "Authorization": f"apikey {PRIM_TOKEN}",
    "Accept": "text/csv",
}

r = requests.get(f"{PRIM_DATASET_URI}{ENDPOINT}", headers=headers, timeout=30)
r.raise_for_status()

res = r.json()

if not len(res):
    raise ValueError("GTFS IDFM response is empty", res)

file = res[0]
zip_path = file.get("url").get("url")
zip_local_dir = "/tmp/gtfs_idfm.zip"

resp = requests.get(zip_path, headers=headers, timeout=60, stream=True)
resp.raise_for_status()
zip_bytes = io.BytesIO(resp.content)

tmp_bucket = "tmp-spark"
run_id = f"{datetime.utcnow():%Y-%m-%dT%H%M%S}-{secrets.token_hex(8)}"
s3_prefix = f"ingestions/gtfs/{run_id}"

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_URL"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
)

uploaded = []
with zipfile.ZipFile(zip_bytes) as zf:
    for info in zf.infolist():
        if info.is_dir():
            continue
        if not info.filename.endswith(".txt"):
            continue

        key = f"{s3_prefix}/{info.filename}"
        data = zf.read(info.filename)

        s3.put_object(
            Bucket=tmp_bucket,
            Key=key,
            Body=data,
            ContentLength=len(data),
        )
        uploaded.append(f"s3a://{tmp_bucket}/{key}")

print(f"Uploaded {len(uploaded)} files")

spark = SparkSession.builder.getOrCreate()

########################### TRANSFERT ##########################
transfers_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .csv(f"s3a://{tmp_bucket}/{s3_prefix}/transfers.txt")
    .withColumn("ingestion_date", current_timestamp())
)

createOrOverwritePartitions(
    sparkSession=spark,
    df=transfers_df,
    dest_table=f"{nessie_catalog_namespace}.transfers"
)

########################### CALENDARS ##########################
calendar_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .csv(f"s3a://{tmp_bucket}/{s3_prefix}/calendar.txt")
    .withColumn("ingestion_date", current_timestamp())
)

createOrOverwritePartitions(
    sparkSession=spark,
    df=calendar_df,
    dest_table=f"{nessie_catalog_namespace}.calendars"
)

########################### CALENDAR DATES ##########################
calendar_dates_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .csv(f"s3a://{tmp_bucket}/{s3_prefix}/calendar_dates.txt")
    .withColumn("ingestion_date", current_timestamp())
)

createOrOverwritePartitions(
    sparkSession=spark,
    df=calendar_dates_df,
    dest_table=f"{nessie_catalog_namespace}.calendar_dates"
)

########################### STOPS ##########################
stops_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .csv(f"s3a://{tmp_bucket}/{s3_prefix}/stops.txt")
    .withColumn("ingestion_date", current_timestamp())
)

createOrOverwritePartitions(
    sparkSession=spark,
    df=stops_df,
    dest_table=f"{nessie_catalog_namespace}.stops"
)

########################### STOPS TIMES ##########################
stop_times_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .csv(f"s3a://{tmp_bucket}/{s3_prefix}/stop_times.txt")
    .withColumn("ingestion_date", current_timestamp())
)

createOrOverwritePartitions(
    sparkSession=spark,
    df=stop_times_df,
    dest_table=f"{nessie_catalog_namespace}.stop_times"
)

########################### TRIPS ##########################
trips_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .csv(f"s3a://{tmp_bucket}/{s3_prefix}/trips.txt")
    .withColumn("ingestion_date", current_timestamp())
)

createOrOverwritePartitions(
    sparkSession=spark,
    df=trips_df,
    dest_table=f"{nessie_catalog_namespace}.trips"
)

########################### ROUTES ##########################
routes_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .csv(f"s3a://{tmp_bucket}/{s3_prefix}/routes.txt")
    .withColumn("ingestion_date", current_timestamp())
)

createOrOverwritePartitions(
    sparkSession=spark,
    df=routes_df,
    dest_table=f"{nessie_catalog_namespace}.routes"
)

########################### PATHWAYS ##########################
pathways_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .csv(f"s3a://{tmp_bucket}/{s3_prefix}/pathways.txt")
    .withColumn("ingestion_date", current_timestamp())
)

createOrOverwritePartitions(
    sparkSession=spark,
    df=pathways_df,
    dest_table=f"{nessie_catalog_namespace}.pathways"
)


spark.stop()
