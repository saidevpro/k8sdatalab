from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

routes_df = spark.sql("""
SELECT * FROM nessie.bronze.routes
WHERE ingestion_date = (
    SELECT MAX(ingestion_date) FROM nessie.bronze.routes
)
""")
routes_df = routes_df.withColumn("route_id", F.split(F.col("route_id"), ":").getItem(1)) \
    .withColumn("agency_id", F.split(F.col("agency_id"), ":").getItem(1)) \
    .drop("route_desc", "route_url", "route_sort_order", "ingestion_date")

routes_df.show(5)

routes_df.createOrReplaceTempView("routes_staging")

spark.sql("""
CREATE TABLE IF NOT EXISTS nessie.silver.routes (
    route_id          STRING,
    agency_id         STRING,
    route_short_name  STRING,
    route_long_name   STRING,
    route_type        INTEGER,
    route_color       STRING,
    route_text_color  STRING
)
USING iceberg
""")

spark.sql("""
MERGE INTO nessie.silver.routes AS target
USING routes_staging AS source
ON target.route_id = source.route_id
WHEN MATCHED THEN UPDATE SET
    target.agency_id        = source.agency_id,
    target.route_short_name = source.route_short_name,
    target.route_long_name  = source.route_long_name,
    target.route_type       = source.route_type,
    target.route_color      = source.route_color,
    target.route_text_color = source.route_text_color
WHEN NOT MATCHED THEN INSERT (
    route_id, agency_id, route_short_name,
    route_long_name, route_type, route_color, route_text_color
) VALUES (
    source.route_id, source.agency_id, source.route_short_name,
    source.route_long_name, source.route_type, source.route_color, source.route_text_color
)
""")

spark.stop()
