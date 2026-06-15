from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import requests as rq


spark = SparkSession.builder.getOrCreate()

catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")
namespace = os.getenv("NESSIE_NAMESPACE", "bronze")

DEST_NAMESPACE = f"{catalog_name}.{namespace}"

PRIM_DATASET_URI = os.getenv("PRIM_DATASET_URI")
PRIM_DATASET_TOKEN = os.getenv("PRIM_DATASET_TOKEN")
DATASET = os.getenv("DATASET")
DEST_TABLE = os.getenv("DESTINATION_TABLE")

if not DEST_TABLE:
    DEST_TABLE = f"{DEST_NAMESPACE}.{DATASET.replace('-', '_')}"

DATASET_URL = f"{PRIM_DATASET_URI}/{DATASET}/exports/csv"

headers = {
    "Authorization": f"apikey {PRIM_DATASET_TOKEN}"
}

r = rq.get(DATASET_URL, headers=headers, timeout=120)
r.raise_for_status()

lines = r.content.decode("utf-8").splitlines()

rdd = spark.sparkContext.parallelize(lines)

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ";")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", True)
    .csv(rdd)
)

df.show()

df = df.withColumn("ingestion_date", F.current_date())

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {DEST_NAMESPACE}")

if not spark.catalog.tableExists(DEST_TABLE):
    (
        df
        .writeTo(DEST_TABLE)
        .partitionedBy("ingestion_date")
        .option("merge-schema", "true")
        .create()
    )
else:
    (
        df
        .writeTo(DEST_TABLE)
        .option("merge-schema", "true")
        .overwritePartitions()
    )

spark.stop()