from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

spark = SparkSession.builder.getOrCreate()

catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.silver")

trips_df = spark.sql(f"""
SELECT * FROM {catalog_name}.bronze.trips
WHERE ingestion_date = (
    SELECT MAX(ingestion_date) FROM {catalog_name}.bronze.trips
)
""")

trips_df = (
    trips_df.withColumn("route_id", F.split(F.col("route_id"), ":").getItem(1))
            .withColumn("service_id", F.substring_index(F.col("service_id"), ":", -1))
            .withColumn("shape_id", F.split(F.col("shape_id"), ":").getItem(1))
)

trips_df = trips_df.select('route_id', 'service_id',
                           'trip_id', 'trip_headsign', 'direction_id', 'shape_id')

trips_df.show(5)

trips_df.createOrReplaceTempView("trips_staging")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.silver.trips (
    route_id        STRING,
    service_id      STRING,
    trip_id         STRING,
    trip_headsign   STRING,
    direction_id    INTEGER,
    shape_id        STRING
)
USING iceberg
""")

spark.sql(f"""
MERGE INTO {catalog_name}.silver.trips AS target
USING trips_staging AS source
ON target.trip_id = source.trip_id
WHEN MATCHED THEN UPDATE SET
    target.route_id      = source.route_id,
    target.service_id    = source.service_id,
    target.trip_headsign = source.trip_headsign,
    target.direction_id  = source.direction_id,
    target.shape_id      = source.shape_id
WHEN NOT MATCHED THEN INSERT (
    route_id, service_id, trip_id,
    trip_headsign, direction_id, shape_id
) VALUES (
    source.route_id, source.service_id, source.trip_id,
    source.trip_headsign, source.direction_id, source.shape_id
)
""")

spark.stop()
