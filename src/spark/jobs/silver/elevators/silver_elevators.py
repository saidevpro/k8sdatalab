from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os

spark = SparkSession.builder.getOrCreate()

catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")

BRONZE_TABLE = f"{catalog_name}.bronze.elevators"
CURRENT_TABLE = f"{catalog_name}.silver.elevators_current"
HISTORY_TABLE = f"{catalog_name}.silver.elevators_history"

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.silver")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CURRENT_TABLE} (
    elevator_key STRING,
    elevator_id BIGINT,
    elevator_external_id STRING,

    station_area_id BIGINT,
    station_name STRING,
    station_area_x_epsg2154 DOUBLE,
    station_area_y_epsg2154 DOUBLE,
    station_centroid_raw STRING,
    station_latitude DOUBLE,
    station_longitude DOUBLE,

    transport_mode STRING,
    elevator_location STRING,
    elevator_direction STRING,

    status STRING,
    status_reason STRING,
    state_code INTEGER,
    severity_code INTEGER,
    is_available BOOLEAN,
    status_updated_at TIMESTAMP,

    is_active BOOLEAN,
    missing_since TIMESTAMP,

    processed_at TIMESTAMP
)
USING iceberg
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
    elevator_key STRING,
    elevator_id BIGINT,
    elevator_external_id STRING,

    station_area_id BIGINT,
    station_name STRING,
    station_area_x_epsg2154 DOUBLE,
    station_area_y_epsg2154 DOUBLE,
    station_centroid_raw STRING,
    station_latitude DOUBLE,
    station_longitude DOUBLE,

    transport_mode STRING,
    elevator_location STRING,
    elevator_direction STRING,

    status STRING,
    status_reason STRING,
    state_code INTEGER,
    severity_code INTEGER,
    is_available BOOLEAN,
    status_updated_at TIMESTAMP,

    processed_at TIMESTAMP
)
USING iceberg
""")

bronze_df = spark.sql(f"""
SELECT *
FROM {BRONZE_TABLE}
WHERE ingestion_date = (
    SELECT max(ingestion_date)
    FROM {BRONZE_TABLE}
)
""")

def clean_string(column_name):
    return F.when(
        F.trim(F.col(column_name).cast("string")) == "",
        None
    ).otherwise(F.trim(F.col(column_name).cast("string")))

df = (
    bronze_df
    .withColumn("station_area_id", F.col("zdcid").cast("bigint"))
    .withColumn("station_area_x_epsg2154", F.col("zdcxepsg2154").cast("double"))
    .withColumn("station_area_y_epsg2154", F.col("zdcyepsg2154").cast("double"))
    .withColumn("station_name", clean_string("zdcname"))
    .withColumn("station_centroid_raw", clean_string("centroidzdc"))

    .withColumn("elevator_id", F.col("liftid").cast("bigint"))
    .withColumn("elevator_external_id", clean_string("privateelevatorid"))

    .withColumn("status_reason", clean_string("liftreason"))
    .withColumn("status", F.lower(clean_string("liftstatus")))
    .withColumn("state_code", F.col("liftstate").cast("int"))
    .withColumn("severity_code", F.col("severity").cast("int"))

    .withColumn("transport_mode", clean_string("liftmode"))
    .withColumn("elevator_location", clean_string("liftsituation"))
    .withColumn("elevator_direction", clean_string("liftdirection"))

    .withColumn(
        "status_updated_at",
        F.to_timestamp(F.col("liftstateupdate"), "yyyy-MM-dd HH:mm:ss")
    )

    .withColumn(
        "station_latitude",
        F.regexp_extract(
            F.col("station_centroid_raw"),
            r"^\s*([-+]?\d+(?:\.\d+)?)",
            1
        ).cast("double")
    )
    .withColumn(
        "station_longitude",
        F.regexp_extract(
            F.col("station_centroid_raw"),
            r",\s*([-+]?\d+(?:\.\d+)?)",
            1
        ).cast("double")
    )

    .withColumn(
        "elevator_key",
        F.when(
            F.col("elevator_id").isNotNull(),
            F.concat(F.lit("lift:"), F.col("elevator_id").cast("string"))
        )
        .when(
            F.col("elevator_external_id").isNotNull(),
            F.concat(F.lit("external:"), F.col("elevator_external_id"))
        )
        .otherwise(
            F.concat_ws(
                ":",
                F.lit("station"),
                F.col("station_area_id").cast("string"),
                F.col("station_name")
            )
        )
    )

    .withColumn(
        "is_available",
        F.when(F.col("status") == "available", F.lit(True))
         .when(F.col("status").isNull(), F.lit(None).cast("boolean"))
         .otherwise(F.lit(False))
    )

    .withColumn("processed_at", F.current_timestamp())
)

silver_cols = [
    "elevator_key",
    "elevator_id",
    "elevator_external_id",
    "station_area_id",
    "station_name",
    "station_area_x_epsg2154",
    "station_area_y_epsg2154",
    "station_centroid_raw",
    "station_latitude",
    "station_longitude",
    "transport_mode",
    "elevator_location",
    "elevator_direction",
    "status",
    "status_reason",
    "state_code",
    "severity_code",
    "is_available",
    "status_updated_at",
    "processed_at",
]

silver_df = (
    df
    .select(*silver_cols)
    .filter(F.col("elevator_key").isNotNull())
)

current_window = Window.partitionBy("elevator_key").orderBy(
    F.col("status_updated_at").desc_nulls_last(),
    F.col("processed_at").desc_nulls_last()
)

current_staging_df = (
    silver_df
    .withColumn("rn", F.row_number().over(current_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

history_staging_df = silver_df.dropDuplicates([
    "elevator_key",
    "status_updated_at",
    "status",
    "state_code",
    "severity_code",
    "is_available"
])

if current_staging_df.limit(1).count() == 0:
    spark.stop()
    raise SystemExit("No data to process")

current_staging_df.createOrReplaceTempView("elevator_status_current_staging")
history_staging_df.createOrReplaceTempView("elevator_status_history_staging")

mutable_cols = [
    "elevator_id",
    "elevator_external_id",
    "station_area_id",
    "station_name",
    "station_area_x_epsg2154",
    "station_area_y_epsg2154",
    "station_centroid_raw",
    "station_latitude",
    "station_longitude",
    "transport_mode",
    "elevator_location",
    "elevator_direction",
    "status",
    "status_reason",
    "state_code",
    "severity_code",
    "is_available",
    "status_updated_at",
    "processed_at",
]

update_set = ",\n    ".join([
    f"target.{col} = source.{col}"
    for col in mutable_cols
] + [
    "target.is_active = true",
    "target.missing_since = NULL"
])

current_insert_cols = [
    "elevator_key",
    "elevator_id",
    "elevator_external_id",
    "station_area_id",
    "station_name",
    "station_area_x_epsg2154",
    "station_area_y_epsg2154",
    "station_centroid_raw",
    "station_latitude",
    "station_longitude",
    "transport_mode",
    "elevator_location",
    "elevator_direction",
    "status",
    "status_reason",
    "state_code",
    "severity_code",
    "is_available",
    "status_updated_at",
    "is_active",
    "missing_since",
    "processed_at",
]

current_insert_values = [
    "source.elevator_key",
    "source.elevator_id",
    "source.elevator_external_id",
    "source.station_area_id",
    "source.station_name",
    "source.station_area_x_epsg2154",
    "source.station_area_y_epsg2154",
    "source.station_centroid_raw",
    "source.station_latitude",
    "source.station_longitude",
    "source.transport_mode",
    "source.elevator_location",
    "source.elevator_direction",
    "source.status",
    "source.status_reason",
    "source.state_code",
    "source.severity_code",
    "source.is_available",
    "source.status_updated_at",
    "true",
    "NULL",
    "source.processed_at",
]

spark.sql(f"""
MERGE INTO {CURRENT_TABLE} AS target
USING elevator_status_current_staging AS source
ON target.elevator_key = source.elevator_key

WHEN MATCHED THEN UPDATE SET
    {update_set}

WHEN NOT MATCHED THEN INSERT (
    {", ".join(current_insert_cols)}
) VALUES (
    {", ".join(current_insert_values)}
)

WHEN NOT MATCHED BY SOURCE AND target.is_active = true THEN UPDATE SET
    target.is_active = false,
    target.missing_since = current_timestamp(),
    target.processed_at = current_timestamp()
""")

history_insert_cols = [
    "elevator_key",
    "elevator_id",
    "elevator_external_id",
    "station_area_id",
    "station_name",
    "station_area_x_epsg2154",
    "station_area_y_epsg2154",
    "station_centroid_raw",
    "station_latitude",
    "station_longitude",
    "transport_mode",
    "elevator_location",
    "elevator_direction",
    "status",
    "status_reason",
    "state_code",
    "severity_code",
    "is_available",
    "status_updated_at",
    "processed_at",
]

history_insert_values = [
    f"source.{col}"
    for col in history_insert_cols
]

spark.sql(f"""
MERGE INTO {HISTORY_TABLE} AS target
USING elevator_status_history_staging AS source
ON target.elevator_key = source.elevator_key
AND target.status_updated_at <=> source.status_updated_at
AND target.status <=> source.status
AND target.state_code <=> source.state_code
AND target.severity_code <=> source.severity_code
AND target.is_available <=> source.is_available

WHEN NOT MATCHED THEN INSERT (
    {", ".join(history_insert_cols)}
) VALUES (
    {", ".join(history_insert_values)}
)
""")

spark.stop()