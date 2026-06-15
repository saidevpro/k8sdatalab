from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

spark = SparkSession.builder.getOrCreate()

catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")

calendar_df = spark.sql(f"""
SELECT * FROM {catalog_name}.bronze.calendars
WHERE ingestion_date = (
    SELECT MAX(ingestion_date) FROM {catalog_name}.bronze.calendars
)
""")

calendar_df = (
    calendar_df
    .withColumn("service_id", F.substring_index(F.col("service_id"), ":", -1))
    .withColumn("start_date", F.to_date(F.col("start_date").cast("string"), "yyyyMMdd"))
    .withColumn("end_date", F.to_date(F.col("end_date").cast("string"), "yyyyMMdd"))
    .drop('ingestion_date')
)

calendar_df.show(5)

calendar_df.createOrReplaceTempView("calendars_staging")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.silver.calendars (
    service_id  STRING,
    monday      INTEGER,
    tuesday     INTEGER,
    wednesday   INTEGER,
    thursday    INTEGER,
    friday      INTEGER,
    saturday    INTEGER,
    sunday      INTEGER,
    start_date  DATE,
    end_date    DATE
)
USING iceberg
""")

spark.sql(f"""
MERGE INTO {catalog_name}.silver.calendars AS target
USING calendars_staging AS source
ON target.service_id = source.service_id
WHEN MATCHED THEN UPDATE SET
    target.monday     = source.monday,
    target.tuesday    = source.tuesday,
    target.wednesday  = source.wednesday,
    target.thursday   = source.thursday,
    target.friday     = source.friday,
    target.saturday   = source.saturday,
    target.sunday     = source.sunday,
    target.start_date = source.start_date,
    target.end_date   = source.end_date
WHEN NOT MATCHED THEN INSERT (
    service_id, monday, tuesday, wednesday,
    thursday, friday, saturday, sunday,
    start_date, end_date
) VALUES (
    source.service_id, source.monday, source.tuesday, source.wednesday,
    source.thursday, source.friday, source.saturday, source.sunday,
    source.start_date, source.end_date
)
""")

spark.stop()
