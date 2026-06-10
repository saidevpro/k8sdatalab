import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


NESSIE_CATALOG = os.getenv("NESSIE_CATALOG", "nessie")
SILVER_NAMESPACE = os.getenv("SILVER_NAMESPACE", "silver")
GOLD_NAMESPACE = os.getenv("GOLD_NAMESPACE", "gold")
DELAY_THRESHOLD_SEC = int(os.getenv("DELAY_THRESHOLD_SEC", "60"))

NEXT_STOP_TABLE = f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.next_stop"
SCHEDULE_TABLE = f"{NESSIE_CATALOG}.{GOLD_NAMESPACE}.next_stop_schedule"
DELAYS_TABLE = f"{NESSIE_CATALOG}.{GOLD_NAMESPACE}.next_stop_delays"
DELAYS_BY_STOP_TABLE = f"{NESSIE_CATALOG}.{GOLD_NAMESPACE}.delays_by_stop"

NUMERIC_REGEX = r"(\\d+):?$"
ALNUM_REGEX = r"([A-Za-z0-9]+):?$"


def backfill_ids(spark: SparkSession, table: str, regex_map: dict) -> None:
    set_clause = ", ".join(
        f"{col} = regexp_extract({col}, '{regex}', 1)"
        for col, regex in regex_map.items()
    )
    where_clause = " OR ".join(f"{col} RLIKE '^STIF:'" for col in regex_map)
    spark.sql(f"UPDATE {table} SET {set_clause} WHERE {where_clause}")
    print(f"backfilled ids in {table}")


def dedup_latest(spark: SparkSession, table: str, key_cols: list) -> None:
    latest = Window.partitionBy(*key_cols).orderBy(F.col("batch_time").desc())
    deduped = (
        spark.read.table(table)
        .withColumn("__rn", F.row_number().over(latest))
        .filter(F.col("__rn") == 1)
        .drop("__rn")
    )
    deduped.persist()
    deduped.count()
    deduped.writeTo(table).overwritePartitions()
    deduped.unpersist()
    print(f"deduplicated {table} on ({', '.join(key_cols)})")


def rebuild_delays_by_stop(spark: SparkSession) -> None:
    spark.sql(
        f"""
        INSERT OVERWRITE {DELAYS_BY_STOP_TABLE}
        SELECT
          stop_point_ref,
          line_ref,
          published_line,
          COUNT(*) AS nb_passages,
          SUM(CAST(arrival_delay_sec >= {DELAY_THRESHOLD_SEC} AS INT)) AS nb_delayed,
          ROUND(AVG(arrival_delay_sec), 1) AS avg_arrival_delay_sec,
          MAX(arrival_delay_sec) AS max_arrival_delay_sec,
          ROUND(100 * (1 - SUM(CAST(arrival_delay_sec >= {DELAY_THRESHOLD_SEC} AS INT)) / COUNT(*)), 1) AS pct_on_time
        FROM {NEXT_STOP_TABLE}
        WHERE arrival_delay_sec IS NOT NULL
        GROUP BY stop_point_ref, line_ref, published_line
        """
    )
    print(f"rebuilt {DELAYS_BY_STOP_TABLE}")


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    backfill_ids(
        spark,
        NEXT_STOP_TABLE,
        {"line_ref": ALNUM_REGEX, "destination_ref": NUMERIC_REGEX, "stop_point_ref": NUMERIC_REGEX},
    )
    dedup_latest(spark, NEXT_STOP_TABLE, ["journey_ref", "stop_point_ref"])

    backfill_ids(
        spark,
        SCHEDULE_TABLE,
        {"line_ref": ALNUM_REGEX, "stop_point_ref": NUMERIC_REGEX},
    )
    dedup_latest(spark, SCHEDULE_TABLE, ["journey_ref", "stop_point_ref"])

    backfill_ids(
        spark,
        DELAYS_TABLE,
        {"line_ref": ALNUM_REGEX, "stop_point_ref": NUMERIC_REGEX},
    )

    rebuild_delays_by_stop(spark)

    spark.stop()


if __name__ == "__main__":
    main()
