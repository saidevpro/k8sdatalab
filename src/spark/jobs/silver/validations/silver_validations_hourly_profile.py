from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

spark = SparkSession.builder.getOrCreate()

catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.silver")

dfo = spark.sql(f"""
SELECT * FROM {catalog_name}.bronze.validations_profil
WHERE ingestion_date = (
    SELECT MAX(ingestion_date) FROM {catalog_name}.bronze.validations_profil
)
""")

df = (
    dfo
    .withColumn("stif_trns", F.col("CODE_STIF_TRNS").cast("int"))
    .withColumn("stif_res", F.col("CODE_STIF_RES").cast("int"))
    .withColumn("stif_arret", F.col("CODE_STIF_ARRET").cast("int"))
    .withColumn("id_refa_lda", F.col("ID_REFA_LDA").cast("double").cast("long"))
    .withColumn("libelle_arret", F.nullif(F.trim(F.regexp_replace(F.col("LIBELLE_ARRET"), r"\s+", " ")), F.lit("")))
    .withColumn("cat_jour", F.upper(F.trim(F.col("CAT_JOUR"))))
    .withColumn("hour_start", F.regexp_extract(F.col("TRNC_HORR_60"), r"^(\d+)H", 1).cast("int"))
    .withColumn("hour_end", F.regexp_extract(F.col("TRNC_HORR_60"), r"-(\d+)H", 1).cast("int"))
    .withColumn("pct_validations", F.regexp_replace(F.col("pourc_validations"), ",", ".").cast("double"))
    .select("stif_trns", "stif_res", "stif_arret", "id_refa_lda", "libelle_arret",
            "cat_jour", "hour_start", "hour_end", "pct_validations")
    .where(F.col("hour_start").isNotNull())
)

df.show(5)

df.createOrReplaceTempView("validations_hourly_profile_staging")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.silver.validations_hourly_profile (
    stif_trns       INTEGER,
    stif_res        INTEGER,
    stif_arret      INTEGER,
    id_refa_lda     BIGINT,
    libelle_arret   STRING,
    cat_jour        STRING,
    hour_start      INTEGER,
    hour_end        INTEGER,
    pct_validations DOUBLE
)
USING iceberg
PARTITIONED BY (cat_jour)
""")

spark.sql(f"""
MERGE INTO {catalog_name}.silver.validations_hourly_profile AS target
USING validations_hourly_profile_staging AS source
ON target.stif_trns = source.stif_trns
AND target.stif_res = source.stif_res
AND target.stif_arret = source.stif_arret
AND target.cat_jour = source.cat_jour
AND target.hour_start = source.hour_start
WHEN MATCHED THEN UPDATE SET
    target.id_refa_lda     = source.id_refa_lda,
    target.libelle_arret   = source.libelle_arret,
    target.hour_end        = source.hour_end,
    target.pct_validations = source.pct_validations
WHEN NOT MATCHED THEN INSERT (
    stif_trns, stif_res, stif_arret, id_refa_lda, libelle_arret,
    cat_jour, hour_start, hour_end, pct_validations
) VALUES (
    source.stif_trns, source.stif_res, source.stif_arret, source.id_refa_lda, source.libelle_arret,
    source.cat_jour, source.hour_start, source.hour_end, source.pct_validations
)
""")

spark.stop()
