from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import requests as rq

spark = SparkSession.builder.getOrCreate()

nessie_catalog_name = 'nessie'
nessie_ref = spark.conf.get("spark.sql.catalog.nessie.ref")
nessie_namespace = os.getenv("NESSIE_NAMESPACE")

DEST_NAMESPACE = f"{nessie_catalog_name}.{nessie_namespace}"
PRIM_DATASET_URI = os.getenv("PRIM_DATASET_URI")
PRIM_DATASET_TOKEN = os.getenv("PRIM_DATASET_TOKEN")
DATASET = os.getenv("DATASET")
DEST_TABLE = os.getenv("DESTINATION_TABLE")

DATASET_URL = f"{PRIM_DATASET_URI}/{DATASET}/exports/csv"

headers = {
    "Authorization": f"apikey {PRIM_DATASET_TOKEN}"
}

r = rq.get(f"{DATASET_URL}", headers=headers, timeout=30)
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

spark.sql(
    f"CREATE BRANCH IF NOT EXISTS {nessie_ref} IN {nessie_catalog_name} FROM main")
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
