from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

spark = SparkSession.builder.getOrCreate()

catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")

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

stop_entrances_df = df.filter(F.col("location_type") == 2)
stop_entrances_df = stop_entrances_df.dropDuplicates()

stop_entrances_df = stop_entrances_df.withColumn("entrance_id", F.regexp_extract(F.col("stop_id"), r"(\d+)", 1).cast("int")) \
    .withColumn("parent_station", F.regexp_extract(F.col("parent_station"), r"(\d+)", 1).cast("int")) \
    .withColumn("entrance_name", F.col("stop_name")) \
    .withColumn("entrance_lon", F.col("stop_lon")) \
    .withColumn("entrance_lat", F.col("stop_lat"))

stop_entrances_df = stop_entrances_df.select(
    "entrance_id", "entrance_name", "parent_station", "entrance_lon", "entrance_lat")

stop_entrances_df.show(5)

stop_entrances_df.createOrReplaceTempView("stop_entrances_staging")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.silver.stop_entrances (
    entrance_id     INTEGER,
    entrance_name   STRING,
    parent_station  INTEGER,
    entrance_lon    DOUBLE,
    entrance_lat    DOUBLE
)
USING iceberg
""")

spark.sql(f"""
MERGE INTO {catalog_name}.silver.stop_entrances AS target
USING stop_entrances_staging AS source
ON target.entrance_id = source.entrance_id
WHEN MATCHED THEN UPDATE SET
    target.entrance_name  = source.entrance_name,
    target.parent_station = source.parent_station,
    target.entrance_lon   = source.entrance_lon,
    target.entrance_lat   = source.entrance_lat
WHEN NOT MATCHED THEN INSERT (
    entrance_id, entrance_name, parent_station,
    entrance_lon, entrance_lat
) VALUES (
    source.entrance_id, source.entrance_name, source.parent_station,
    source.entrance_lon, source.entrance_lat
)
""")

spark.stop()
