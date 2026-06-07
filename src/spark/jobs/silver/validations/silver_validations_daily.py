from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

dfo = spark.sql("""
SELECT * FROM nessie.bronze.validations_nb
WHERE ingestion_date = (
    SELECT MAX(ingestion_date) FROM nessie.bronze.validations_nb
)
""")

df = (
    dfo
    .withColumn("jour", F.to_date(F.col("JOUR")))
    .withColumn("stif_trns", F.col("CODE_STIF_TRNS").cast("int"))
    .withColumn("stif_res", F.col("CODE_STIF_RES").cast("int"))
    .withColumn("stif_arret", F.col("CODE_STIF_ARRET").cast("int"))
    .withColumn("id_refa_lda", F.col("ID_REFA_LDA").cast("double").cast("long"))
    .withColumn("libelle_arret", F.nullif(F.trim(F.regexp_replace(F.col("LIBELLE_ARRET"), r"\s+", " ")), F.lit("")))
    .withColumn("categorie_titre", F.upper(F.trim(F.col("CATEGORIE_TITRE"))))
    .withColumn("nb_vald", F.when(F.col("NB_VALD").rlike("^[0-9]+$"), F.col("NB_VALD").cast("int")))
    .withColumn("nb_vald_masked", ~F.col("NB_VALD").rlike("^[0-9]+$"))
    .select("jour", "stif_trns", "stif_res", "stif_arret", "id_refa_lda",
            "libelle_arret", "categorie_titre", "nb_vald", "nb_vald_masked")
    .where(F.col("jour").isNotNull())
)

df.show(5)

df.createOrReplaceTempView("validations_daily_staging")

spark.sql("""
CREATE TABLE IF NOT EXISTS nessie.silver.validations_daily (
    jour            DATE,
    stif_trns       INTEGER,
    stif_res        INTEGER,
    stif_arret      INTEGER,
    id_refa_lda     BIGINT,
    libelle_arret   STRING,
    categorie_titre STRING,
    nb_vald         INTEGER,
    nb_vald_masked  BOOLEAN
)
USING iceberg
PARTITIONED BY (months(jour))
""")

spark.sql("""
MERGE INTO nessie.silver.validations_daily AS target
USING validations_daily_staging AS source
ON target.jour = source.jour
AND target.stif_trns = source.stif_trns
AND target.stif_res = source.stif_res
AND target.stif_arret = source.stif_arret
AND target.categorie_titre = source.categorie_titre
WHEN MATCHED THEN UPDATE SET
    target.id_refa_lda    = source.id_refa_lda,
    target.libelle_arret  = source.libelle_arret,
    target.nb_vald        = source.nb_vald,
    target.nb_vald_masked = source.nb_vald_masked
WHEN NOT MATCHED THEN INSERT (
    jour, stif_trns, stif_res, stif_arret, id_refa_lda,
    libelle_arret, categorie_titre, nb_vald, nb_vald_masked
) VALUES (
    source.jour, source.stif_trns, source.stif_res, source.stif_arret, source.id_refa_lda,
    source.libelle_arret, source.categorie_titre, source.nb_vald, source.nb_vald_masked
)
""")

spark.stop()
