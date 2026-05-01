from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

calendar_dates_df = spark.sql("""
SELECT * FROM nessie.bronze.calendar_dates
WHERE ingestion_date = (
    SELECT MAX(ingestion_date) FROM nessie.bronze.calendar_dates
)
""")

calendar_dates_df = (
    calendar_dates_df.withColumn('service_id', F.regexp_extract(
        F.col('service_id'), r"(\d+)", 1).cast('int'))
    .withColumn("date", F.to_date(F.col("date").cast("string"), "yyyyMMdd"))
    .drop('ingestion_date')
)

calendar_dates_df.show(5)

calendar_dates_df.createOrReplaceTempView("calendar_dates_staging")

spark.sql("""
CREATE TABLE IF NOT EXISTS nessie.silver.calendar_dates (
    service_id      INTEGER,
    date            DATE,
    exception_type  INTEGER
)
USING iceberg
""")

spark.sql("""
MERGE INTO nessie.silver.calendar_dates AS target
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
