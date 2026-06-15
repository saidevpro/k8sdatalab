from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

spark = SparkSession.builder.getOrCreate()

catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.silver")

dfo = spark.sql(f"""
SELECT * FROM {catalog_name}.bronze.stops
WHERE ingestion_date = (
    SELECT MAX(ingestion_date) FROM {catalog_name}.bronze.stops
)
""")

df = (
    dfo
    .withColumn("stop_id", F.trim(F.col("stop_id")))
    .withColumn("stop_code", F.nullif(F.trim(F.col("stop_code")), F.lit("")))
    .withColumn("stop_name", F.nullif(F.trim(F.regexp_replace(F.col("stop_name"), r"\s+", " ")), F.lit("")))
    .withColumn("parent_station", F.nullif(F.trim(F.col("parent_station")), F.lit("")))
    .withColumn("stop_timezone", F.nullif(F.trim(F.col("stop_timezone")), F.lit("")))
    .withColumn("level_id", F.nullif(F.trim(F.col("level_id")), F.lit("")))
    .withColumn("platform_code", F.nullif(F.trim(F.col("platform_code")), F.lit("")))
    .withColumn("location_type", F.coalesce(F.col("location_type").cast("int"), F.lit(0)))
    .withColumn("wheelchair_boarding", F.coalesce(F.col("wheelchair_boarding").cast("int"), F.lit(0)))
    .withColumn("zone_id", F.col("zone_id").cast("int"))
)

wheelchairs_df = df.filter(F.col("location_type").isin([0]))

wheelchairs_df = wheelchairs_df.withColumn("stop_id", F.regexp_extract(F.col("stop_id"), r"(\d+)", 1).cast("int")) \
    .select("stop_id", "stop_name", "location_type", "wheelchair_boarding")

wheelchairs_df.show(5)

wheelchairs_df.createOrReplaceTempView("wheelchairs_staging")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.silver.wheelchairs (
    stop_id              INTEGER,
    stop_name            STRING,
    location_type        INTEGER,
    wheelchair_boarding  INTEGER
)
USING iceberg
""")

spark.sql(f"""
MERGE INTO {catalog_name}.silver.wheelchairs AS target
USING wheelchairs_staging AS source
ON target.stop_id = source.stop_id
WHEN MATCHED THEN UPDATE SET
    target.stop_name           = source.stop_name,
    target.location_type       = source.location_type,
    target.wheelchair_boarding = source.wheelchair_boarding
WHEN NOT MATCHED THEN INSERT (
    stop_id, stop_name, location_type, wheelchair_boarding
) VALUES (
    source.stop_id, source.stop_name, source.location_type, source.wheelchair_boarding
)
""")

spark.stop()
