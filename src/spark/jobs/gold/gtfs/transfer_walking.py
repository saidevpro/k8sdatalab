import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


NESSIE_CATALOG = os.getenv("NESSIE_CATALOG", "nessie")
SILVER_NAMESPACE = os.getenv("SILVER_NAMESPACE", "silver")
GOLD_NAMESPACE = os.getenv("GOLD_NAMESPACE", "gold")

TRANSFERS_TABLE = f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.transfers"
PATHWAYS_TABLE = f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.pathways"

TRANSFER_WALKING_TABLE = f"{NESSIE_CATALOG}.{GOLD_NAMESPACE}.transfer_walking"


def ensure_namespace(spark: SparkSession) -> None:
    nessie_ref = spark.conf.get(f"spark.sql.catalog.{NESSIE_CATALOG}.ref")
    # spark.sql(
        # f"CREATE BRANCH IF NOT EXISTS {nessie_ref} IN {NESSIE_CATALOG} FROM main"
    # )
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {NESSIE_CATALOG}.{GOLD_NAMESPACE}"
    )


def build_transfer_walking(spark: SparkSession):
    transfers = spark.read.table(TRANSFERS_TABLE)

    pathways = (
        spark.read.table(PATHWAYS_TABLE)
        .groupBy("from_stop_id", "to_stop_id")
        .agg(
            F.round(F.sum("length"), 1).alias("pathway_length_m"),
            F.sum("traversal_time").alias("pathway_traversal_time"),
            F.max((F.col("pathway_mode") == 2).cast("int")).cast("boolean").alias("has_stairs"),
            F.max((F.col("pathway_mode") == 4).cast("int")).cast("boolean").alias("has_escalator"),
            F.max((F.col("pathway_mode") == 5).cast("int")).cast("boolean").alias("has_elevator"),
        )
    )

    return (
        transfers.alias("t")
        .join(pathways.alias("p"), ["from_stop_id", "to_stop_id"], "left")
        .select(
            F.col("from_stop_id"),
            F.col("to_stop_id"),
            F.col("t.transfer_type").alias("transfer_type"),
            F.col("t.min_transfer_time").alias("min_transfer_time"),
            F.col("p.pathway_length_m").alias("pathway_length_m"),
            F.col("p.pathway_traversal_time").alias("pathway_traversal_time"),
            F.coalesce(F.col("p.has_stairs"), F.lit(False)).alias("has_stairs"),
            F.coalesce(F.col("p.has_escalator"), F.lit(False)).alias("has_escalator"),
            F.coalesce(F.col("p.has_elevator"), F.lit(False)).alias("has_elevator"),
        )
    )


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ensure_namespace(spark)

    build_transfer_walking(spark).writeTo(TRANSFER_WALKING_TABLE).using("iceberg").createOrReplace()

    print("gold transfer_walking refreshed")


if __name__ == "__main__":
    main()
