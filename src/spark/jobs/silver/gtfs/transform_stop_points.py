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

stop_points_df = df.filter(F.col("location_type") == 0)

stop_points_df = (
    stop_points_df.withColumn("stop_id", F.regexp_extract(
        F.col("stop_id"), r"(\d+)", 1).cast("int"))
    .withColumn("parent_station", F.regexp_extract(F.col("parent_station"), r"(\d+)", 1).cast("int"))
    .select("stop_id", "stop_name", "zone_id", "parent_station", "stop_lon", "stop_lat")
)

stop_points_df.show(truncate=False)

stop_points_df.createOrReplaceTempView("stop_points_staging")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.silver.stop_points (
    stop_id         INTEGER,
    stop_name       STRING,
    zone_id         INTEGER,
    parent_station  INTEGER,
    stop_lon        DOUBLE,
    stop_lat        DOUBLE
)
USING iceberg
""")

spark.sql(f"""
MERGE INTO {catalog_name}.silver.stop_points AS target
USING stop_points_staging AS source
ON target.stop_id = source.stop_id
WHEN MATCHED THEN UPDATE SET
    target.stop_name      = source.stop_name,
    target.zone_id        = source.zone_id,
    target.parent_station = source.parent_station,
    target.stop_lon       = source.stop_lon,
    target.stop_lat       = source.stop_lat
WHEN NOT MATCHED THEN INSERT (
    stop_id, stop_name, zone_id,
    parent_station, stop_lon, stop_lat
) VALUES (
    source.stop_id, source.stop_name, source.zone_id,
    source.parent_station, source.stop_lon, source.stop_lat
)
""")

spark.stop()
