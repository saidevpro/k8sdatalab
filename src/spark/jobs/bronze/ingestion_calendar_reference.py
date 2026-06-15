import os

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


JOURS_FERIES_URL = os.getenv(
    "JOURS_FERIES_URL",
    "https://calendrier.api.gouv.fr/jours-feries/metropole.json",
)

VACANCES_SCOLAIRES_URL = os.getenv(
    "VACANCES_SCOLAIRES_URL",
    "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/"
    "fr-en-calendrier-scolaire/exports/json?timezone=Europe%2FParis",
)

VACANCES_FIELDS = [
    "description",
    "population",
    "start_date",
    "end_date",
    "location",
    "zones",
    "annee_scolaire",
]


def createTableIfNotExists(sparkSession, df, dest_table):
    if not sparkSession.catalog.tableExists(dest_table):
        (
            df
            .writeTo(dest_table)
            .partitionedBy("ingestion_date")
            .option("merge-schema", "true")
            .create()
        )


def appendToBronze(sparkSession, df, dest_table):
    createTableIfNotExists(sparkSession, df, dest_table)

    (
        df
        .writeTo(dest_table)
        .option("merge-schema", "true")
        .append()
    )


spark = SparkSession.builder.getOrCreate()

catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")

ingestion_date = current_timestamp()

feries_resp = requests.get(JOURS_FERIES_URL, timeout=30)
feries_resp.raise_for_status()
feries = feries_resp.json()

if not feries:
    raise ValueError("Jours feries response is empty")

feries_rows = [(day, str(libelle)) for day, libelle in feries.items()]

feries_df = (
    spark.createDataFrame(feries_rows, ["date", "libelle"])
    .withColumn("ingestion_date", ingestion_date)
)

feries_df.show(5, truncate=False)

appendToBronze(spark, feries_df, f"{catalog_name}.bronze.jours_feries")

vacances_resp = requests.get(VACANCES_SCOLAIRES_URL, timeout=60)
vacances_resp.raise_for_status()
vacances = vacances_resp.json()

if not vacances:
    raise ValueError("Vacances scolaires response is empty")

vacances_rows = [
    tuple(
        None if record.get(field) is None else str(record.get(field))
        for field in VACANCES_FIELDS
    )
    for record in vacances
]

vacances_df = (
    spark.createDataFrame(vacances_rows, VACANCES_FIELDS)
    .withColumn("ingestion_date", ingestion_date)
)

vacances_df.show(5, truncate=False)

appendToBronze(spark, vacances_df, f"{catalog_name}.bronze.vacances_scolaires")

spark.stop()
