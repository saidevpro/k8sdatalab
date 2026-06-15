from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

spark = SparkSession.builder.getOrCreate()

catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.silver")

dfo = spark.sql(f"""
    SELECT *
    FROM {catalog_name}.bronze.accessibility_gares
    WHERE ingestion_date = (
        SELECT MAX(ingestion_date)
        FROM {catalog_name}.bronze.accessibility_gares
    )
""")

df = (
    dfo
    .drop("commentaire", "accessibility_level_name", "ingestion_date")
    .withColumn("stop_point_latitude", F.split(F.col("stop_point_geopoint"), ",").getItem(0).cast("double"))
    .withColumn("stop_point_longitude", F.split(F.col("stop_point_geopoint"), ",").getItem(1).cast("double"))
    .drop("stop_point_geopoint")
)

df.show(5)

df.createOrReplaceTempView("accessibility_gares_staging")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.silver.accessibility_gares (
    stop_point_id STRING,
    accessibility_level_id INT,
    stop_name STRING,
    stop_point_latitude DOUBLE,
    stop_point_longitude DOUBLE
)
USING iceberg
""")

spark.sql(f"""
MERGE INTO {catalog_name}.silver.accessibility_gares AS target
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
