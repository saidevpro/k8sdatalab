from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

spark = SparkSession.builder.getOrCreate()

catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.silver")

transfers_df = spark.sql(f"""
SELECT * FROM {catalog_name}.bronze.transfers
WHERE ingestion_date = (
    SELECT MAX(ingestion_date) FROM {catalog_name}.bronze.transfers
)
""")

transfers_df = (
    transfers_df.withColumn("from_stop_id", F.regexp_extract(
        F.col("from_stop_id"), r"(\d+)", 1).cast("int"))
    .withColumn("to_stop_id", F.regexp_extract(F.col("to_stop_id"), r"(\d+)", 1).cast("int"))
    .withColumn("transfer_type", F.col("transfer_type").cast("int"))
    .withColumn("min_transfer_time", F.col("min_transfer_time").cast("int"))
    .select("from_stop_id", "to_stop_id", "transfer_type", "min_transfer_time")
    .dropDuplicates(["from_stop_id", "to_stop_id"])
)

transfers_df.show(5)

transfers_df.createOrReplaceTempView("transfers_staging")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.silver.transfers (
    from_stop_id      INTEGER,
    to_stop_id        INTEGER,
    transfer_type     INTEGER,
    min_transfer_time INTEGER
)
USING iceberg
""")

spark.sql(f"""
MERGE INTO {catalog_name}.silver.transfers AS target
USING transfers_staging AS source
ON target.from_stop_id = source.from_stop_id AND target.to_stop_id = source.to_stop_id
WHEN MATCHED THEN UPDATE SET
    target.transfer_type     = source.transfer_type,
    target.min_transfer_time = source.min_transfer_time
WHEN NOT MATCHED THEN INSERT (
    from_stop_id, to_stop_id, transfer_type, min_transfer_time
) VALUES (
    source.from_stop_id, source.to_stop_id, source.transfer_type, source.min_transfer_time
)
""")

spark.stop()
