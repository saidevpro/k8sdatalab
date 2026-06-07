import os

from pyspark.sql import SparkSession


NESSIE_CATALOG = os.getenv("NESSIE_CATALOG", "nessie")
SOURCE_BRANCH = os.getenv("SOURCE_BRANCH", "dev")
TARGET_BRANCH = os.getenv("TARGET_BRANCH", "main")

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

spark.sql(
    f"CREATE BRANCH IF NOT EXISTS {TARGET_BRANCH} IN {NESSIE_CATALOG} FROM main"
)

spark.sql(
    f"MERGE BRANCH {SOURCE_BRANCH} INTO {TARGET_BRANCH} IN {NESSIE_CATALOG}"
)

print(f"merged branch {SOURCE_BRANCH} into {TARGET_BRANCH}")

spark.stop()
