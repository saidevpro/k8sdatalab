from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

spark = SparkSession.builder.getOrCreate()

table = "nessie.bronze.accessibility_gares"

dfo = spark.sql("SELECT * FROM {table}")

df = (
    dfo
    .drop("commentaire", "accessibility_level_name", "ingestion_date")
    .withColumn("latitude", F.split(F.col("stop_point_geopoint"), ",").getItem(0).cast("double"))
    .withColumn("longitude", F.split(F.col("stop_point_geopoint"), ",").getItem(1).cast("double"))
)

df.show(5)

df.createOrReplaceTempView("accessibility_gares_staging")

spark.sql("""
CREATE TABLE IF NOT EXISTS nessie.silver.accessibility_gares (
    stop_point_id STRING,
    accessibility_level_id INT,
    stop_name STRING,
    stop_point_latitude DOUBLE,
    stop_point_longitude DOUBLE
)
USING iceberg
""")

spark.sql("""
MERGE INTO nessie.silver.accessibility_gares AS target
USING accessibility_gares_staging AS source
ON target.stop_point_id = source.stop_point_id
WHEN MATCHED THEN UPDATE SET
    target.accessibility_level_id = source.accessibility_level_id,
    target.stop_name = source.stop_name,
    target.stop_point_latitude = source.stop_point_latitude,
    target.stop_point_longitude = source.stop_point_longitude
WHEN NOT MATCHED THEN INSERT (
    stop_point_id,
    accessibility_level_id,
    stop_name,
    stop_point_latitude,
    stop_point_longitude
) VALUES (
    source.stop_point_id,
    source.accessibility_level_id,
    source.stop_name,
    source.stop_point_latitude,
    source.stop_point_longitude
)
""")

spark.stop()