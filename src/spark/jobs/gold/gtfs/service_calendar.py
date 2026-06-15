import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")
SILVER_NAMESPACE = os.getenv("SILVER_NAMESPACE", "silver")
GOLD_NAMESPACE = os.getenv("GOLD_NAMESPACE", "gold")

CALENDARS_TABLE = f"{catalog_name}.{SILVER_NAMESPACE}.calendars"
CALENDAR_DATES_TABLE = f"{catalog_name}.{SILVER_NAMESPACE}.calendar_dates"

SERVICE_CALENDAR_TABLE = f"{catalog_name}.{GOLD_NAMESPACE}.service_calendar"

WEEKDAY_FLAGS = {
    1: "sunday",
    2: "monday",
    3: "tuesday",
    4: "wednesday",
    5: "thursday",
    6: "friday",
    7: "saturday",
}


def ensure_namespace(spark: SparkSession) -> None:
    # nessie_ref = spark.conf.get(f"spark.sql.catalog.{catalog_name}.ref")
    # spark.sql(
        # f"CREATE BRANCH IF NOT EXISTS {nessie_ref} IN {catalog_name} FROM main"
    # )
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{GOLD_NAMESPACE}"
    )


def build_service_calendar(spark: SparkSession):
    calendars = spark.read.table(CALENDARS_TABLE)
    calendar_dates = spark.read.table(CALENDAR_DATES_TABLE)

    active_flag = F.lit(0)
    for dow, day_col in WEEKDAY_FLAGS.items():
        active_flag = F.when(
            F.dayofweek(F.col("date")) == dow, F.col(day_col)
        ).otherwise(active_flag)

    regular = (
        calendars.withColumn(
            "date",
            F.explode(
                F.expr("sequence(start_date, end_date, interval 1 day)")
            ),
        )
        .withColumn("active", active_flag)
        .where(F.col("active") == 1)
        .select("service_id", "date")
    )

    added = calendar_dates.where(F.col("exception_type") == 1).select(
        "service_id", "date"
    )
    removed = calendar_dates.where(F.col("exception_type") == 2).select(
        "service_id", "date"
    )

    return (
        regular.unionByName(added)
        .distinct()
        .join(removed, ["service_id", "date"], "left_anti")
        .withColumn("day_of_week", F.date_format(F.col("date"), "EEEE"))
        .withColumn(
            "is_weekend",
            F.dayofweek(F.col("date")).isin([1, 7]),
        )
    )


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ensure_namespace(spark)

    (
        build_service_calendar(spark)
        .writeTo(SERVICE_CALENDAR_TABLE)
        .using("iceberg")
        .partitionedBy(F.months("date"))
        .createOrReplace()
    )

    print("gold service_calendar refreshed")


if __name__ == "__main__":
    main()
