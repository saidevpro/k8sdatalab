from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

spark = SparkSession.builder.getOrCreate()

catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.silver")


def gtfs_time_to_seconds(col_name):
    parts = F.split(F.col(col_name), ":")
    return (
        parts.getItem(0).cast("int") * 3600
        + parts.getItem(1).cast("int") * 60
        + parts.getItem(2).cast("int")
    )


stop_times = spark.sql(f"""
SELECT * FROM {catalog_name}.bronze.stop_times
WHERE ingestion_date = (
    SELECT MAX(ingestion_date) FROM {catalog_name}.bronze.stop_times
)
""")

stop_times = stop_times.select("trip_id", "arrival_time", "departure_time",
                               "stop_id", "stop_sequence", "pickup_type", "drop_off_type", "timepoint")

stop_times = (
    stop_times
    .withColumn("stop_id", F.regexp_extract(F.col("stop_id"), r"(\d+)", 1).cast("int"))
    .withColumn("arrival_seconds", gtfs_time_to_seconds("arrival_time"))
    .withColumn("departure_seconds", gtfs_time_to_seconds("departure_time"))
)

stop_times.show(5)

stop_times.createOrReplaceTempView("stop_times_staging")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.silver.stop_times (
    trip_id            STRING,
    arrival_time       STRING,
    departure_time     STRING,
    stop_id            INTEGER,
    stop_sequence      INTEGER,
    pickup_type        INTEGER,
    drop_off_type      INTEGER,
    timepoint          INTEGER,
    arrival_seconds    INTEGER,
    departure_seconds  INTEGER
)
USING iceberg
""")

spark.sql(f"""
MERGE INTO {catalog_name}.silver.stop_times AS target
USING stop_times_staging AS source
ON target.trip_id = source.trip_id
AND target.stop_sequence = source.stop_sequence
WHEN MATCHED THEN UPDATE SET
    target.arrival_time      = source.arrival_time,
    target.departure_time    = source.departure_time,
    target.stop_id           = source.stop_id,
    target.pickup_type       = source.pickup_type,
    target.drop_off_type     = source.drop_off_type,
    target.timepoint         = source.timepoint,
    target.arrival_seconds   = source.arrival_seconds,
    target.departure_seconds = source.departure_seconds
WHEN NOT MATCHED THEN INSERT (
    trip_id, arrival_time, departure_time, stop_id,
    stop_sequence, pickup_type, drop_off_type,
    timepoint, arrival_seconds, departure_seconds
) VALUES (
    source.trip_id, source.arrival_time, source.departure_time, source.stop_id,
    source.stop_sequence, source.pickup_type, source.drop_off_type,
    source.timepoint, source.arrival_seconds, source.departure_seconds
)
""")

spark.stop()
