from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

pathways_df = spark.sql("""
SELECT * FROM nessie.bronze.pathways
WHERE ingestion_date = (
    SELECT MAX(ingestion_date) FROM nessie.bronze.pathways
)
""")

pathways_df = (
    pathways_df.withColumn("from_stop_id", F.regexp_extract(
        F.col("from_stop_id"), r"(\d+)", 1).cast("int"))
    .withColumn("to_stop_id", F.regexp_extract(F.col("to_stop_id"), r"(\d+)", 1).cast("int"))
    .select("pathway_id", "from_stop_id", "to_stop_id", "pathway_mode", "is_bidirectional", "length", "traversal_time")
)

pathways_df.show(5)

pathways_df.createOrReplaceTempView("pathways_staging")

spark.sql("""
CREATE TABLE IF NOT EXISTS nessie.silver.pathways (
    pathway_id        STRING,
    from_stop_id      INTEGER,
    to_stop_id        INTEGER,
    pathway_mode      INTEGER,
    is_bidirectional  INTEGER,
    length            DOUBLE,
    traversal_time    INTEGER
)
USING iceberg
""")

spark.sql("""
MERGE INTO nessie.silver.pathways AS target
USING pathways_staging AS source
ON target.pathway_id = source.pathway_id
WHEN MATCHED THEN UPDATE SET
    target.from_stop_id     = source.from_stop_id,
    target.to_stop_id       = source.to_stop_id,
    target.pathway_mode     = source.pathway_mode,
    target.is_bidirectional = source.is_bidirectional,
    target.length           = source.length,
    target.traversal_time   = source.traversal_time
WHEN NOT MATCHED THEN INSERT (
    pathway_id, from_stop_id, to_stop_id,
    pathway_mode, is_bidirectional, length, traversal_time
) VALUES (
    source.pathway_id, source.from_stop_id, source.to_stop_id,
    source.pathway_mode, source.is_bidirectional, source.length, source.traversal_time
)
""")

spark.stop()
