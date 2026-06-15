from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

spark = SparkSession.builder.getOrCreate()

catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")

calendar_dates_df = spark.sql(f"""
SELECT * FROM {catalog_name}.bronze.calendar_dates
WHERE ingestion_date = (
    SELECT MAX(ingestion_date) FROM {catalog_name}.bronze.calendar_dates
)
""")

calendar_dates_df = (
    calendar_dates_df
    .withColumn("service_id", F.substring_index(F.col("service_id"), ":", -1))
    .withColumn("date", F.to_date(F.col("date").cast("string"), "yyyyMMdd"))
    .drop('ingestion_date')
)

calendar_dates_df.show(5)

calendar_dates_df.createOrReplaceTempView("calendar_dates_staging")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.silver.calendar_dates (
    service_id      STRING,
    date            DATE,
    exception_type  INTEGER
)
USING iceberg
""")

spark.sql(f"""
MERGE INTO {catalog_name}.silver.calendar_dates AS target
USING calendar_dates_staging AS source
ON target.service_id = source.service_id
AND target.date = source.date
WHEN MATCHED THEN UPDATE SET
    target.exception_type = source.exception_type
WHEN NOT MATCHED THEN INSERT (
    service_id,
    date,
    exception_type
) VALUES (
    source.service_id,
    source.date,
    source.exception_type
)
""")

spark.stop()
