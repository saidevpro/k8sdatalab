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

stations_df = df.filter(F.col("location_type") == 1)
stations_df = stations_df.dropDuplicates()

stations_df = stations_df.withColumn("station_id", F.regexp_extract(F.col("stop_id"), r"(\d+)", 1).cast("int")) \
    .withColumn("station_name", F.col("stop_name")) \
    .withColumn("station_lon", F.col("stop_lon")) \
    .withColumn("station_lat", F.col("stop_lat"))

stations_df = stations_df.select(
    "station_id", "station_name", "station_lon", "station_lat")

stations_df.show(5)

stations_df.createOrReplaceTempView("stop_areas_staging")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.silver.stop_areas (
    station_id    INTEGER,
    station_name  STRING,
    station_lon   DOUBLE,
    station_lat   DOUBLE
)
USING iceberg
""")

spark.sql(f"""
MERGE INTO {catalog_name}.silver.stop_areas AS target
USING stop_areas_staging AS source
ON target.station_id = source.station_id
WHEN MATCHED THEN UPDATE SET
    target.station_name = source.station_name,
    target.station_lon  = source.station_lon,
    target.station_lat  = source.station_lat
WHEN NOT MATCHED THEN INSERT (
    station_id, station_name, station_lon, station_lat
) VALUES (
    source.station_id, source.station_name, source.station_lon, source.station_lat
)
""")

spark.stop()
