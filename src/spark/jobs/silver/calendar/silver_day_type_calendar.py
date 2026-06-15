from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

spark = SparkSession.builder.getOrCreate()

catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.silver")

feries = (
    spark.sql(f"""
        SELECT * FROM {catalog_name}.bronze.jours_feries
        WHERE ingestion_date = (
            SELECT MAX(ingestion_date) FROM {catalog_name}.bronze.jours_feries
        )
    """)
    .select(F.to_date(F.col("date")).alias("holiday_date"))
    .where(F.col("holiday_date").isNotNull())
    .distinct()
)

vacances = (
    spark.sql(f"""
        SELECT * FROM {catalog_name}.bronze.vacances_scolaires
        WHERE ingestion_date = (
            SELECT MAX(ingestion_date) FROM {catalog_name}.bronze.vacances_scolaires
        )
    """)
    .where(F.col("zones") == "Zone C")
    .select(
        F.to_date(F.substring(F.col("start_date"), 1, 10)).alias("start_d"),
        F.to_date(F.substring(F.col("end_date"), 1, 10)).alias("end_d"),
    )
    .where(F.col("start_d").isNotNull() & F.col("end_d").isNotNull())
    .distinct()
)

bounds = (
    feries.select(F.col("holiday_date").alias("d"))
    .union(vacances.select(F.col("start_d").alias("d")))
    .union(vacances.select(F.col("end_d").alias("d")))
    .agg(F.min("d").alias("lo"), F.max("d").alias("hi"))
    .collect()[0]
)

if bounds["lo"] is None:
    spark.stop()
    raise SystemExit("No calendar reference data to build day_type_calendar")

spine = spark.sql(
    f"SELECT explode(sequence(to_date('{bounds['lo']}'), "
    f"to_date('{bounds['hi']}'), interval 1 day)) AS date"
)

school_dates = (
    spine.alias("s")
    .join(
        vacances.alias("v"),
        (F.col("s.date") >= F.col("v.start_d")) & (F.col("s.date") < F.col("v.end_d")),
        "inner",
    )
    .select("s.date")
    .distinct()
    .withColumn("is_school_holiday", F.lit(True))
)

df = (
    spine
    .join(feries, spine["date"] == feries["holiday_date"], "left")
    .withColumn("is_holiday", F.col("holiday_date").isNotNull())
    .drop("holiday_date")
    .join(school_dates, "date", "left")
    .withColumn("is_school_holiday", F.coalesce(F.col("is_school_holiday"), F.lit(False)))
    .withColumn("day_of_week", F.expr("(dayofweek(date) + 5) % 7 + 1"))
    .withColumn("is_weekend", F.col("day_of_week").isin(6, 7))
    .withColumn(
        "cat_jour",
        F.when((F.dayofweek("date") == 1) | F.col("is_holiday"), F.lit("DIJFP"))
        .when(
            F.dayofweek("date") == 7,
            F.when(F.col("is_school_holiday"), F.lit("SAVS")).otherwise(F.lit("SAHV")),
        )
        .otherwise(
            F.when(F.col("is_school_holiday"), F.lit("JOVS")).otherwise(F.lit("JOHV"))
        ),
    )
    .select("date", "cat_jour", "is_holiday", "is_school_holiday", "day_of_week", "is_weekend")
)

df.show(5)

df.createOrReplaceTempView("day_type_calendar_staging")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.silver.day_type_calendar (
    date              DATE,
    cat_jour          STRING,
    is_holiday        BOOLEAN,
    is_school_holiday BOOLEAN,
    day_of_week       INTEGER,
    is_weekend        BOOLEAN
)
USING iceberg
PARTITIONED BY (months(date))
""")

spark.sql(f"""
MERGE INTO {catalog_name}.silver.day_type_calendar AS target
USING day_type_calendar_staging AS source
ON target.date = source.date
WHEN MATCHED THEN UPDATE SET
    target.cat_jour          = source.cat_jour,
    target.is_holiday        = source.is_holiday,
    target.is_school_holiday = source.is_school_holiday,
    target.day_of_week       = source.day_of_week,
    target.is_weekend        = source.is_weekend
WHEN NOT MATCHED THEN INSERT (
    date, cat_jour, is_holiday, is_school_holiday, day_of_week, is_weekend
) VALUES (
    source.date, source.cat_jour, source.is_holiday, source.is_school_holiday,
    source.day_of_week, source.is_weekend
)
""")

spark.stop()
