from ..utils import createOrOverwritePartitions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import requests as rq

spark = SparkSession.builder.getOrCreate()

nessie_ref = spark.conf.get("spark.sql.catalog.nessie.ref", "")
nessie_namespace = os.getenv("NESSIE_NAMESPACE")

PRIM_DATASET_URI = os.getenv("PRIM_DATASET_URI")
DEST_TABLE = os.getenv("DESTINATION_TABLE")
DEST_NAMESPACE = f"{nessie_ref}.{nessie_namespace}"

r = rq.get(PRIM_DATASET_URI, timeout=30)
r.raise_for_status()

tmp_file = "/tmp/accessibility-gares.csv"
open(tmp_file, "wb").write(r.content)

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ";")
    .csv(tmp_file)
)

df.show()

df = df.withColumn("ingestion_date", F.current_date())

spark.sql(f"CREATE BRANCH IF NOT EXISTS {nessie_ref} IN nessie FROM main")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {DEST_NAMESPACE}")

createOrOverwritePartitions(
    spark=spark,
    df=df,
    table=DEST_TABLE,
    partitionKey="ingestion_date"
)

spark.stop()
