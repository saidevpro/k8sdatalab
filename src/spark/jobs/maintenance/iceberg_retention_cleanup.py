import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────
# Configuration par layer
# ─────────────────────────────────────────────────────────────────
LAYER_CONFIG = {
    "bronze": {
        "snapshot_retention_days": 30,  # Long historique pour le reprocessing
        "retain_last_snapshots":   10,
        "orphan_retention_days":   7,
        "target_file_size_mb":     256,
        "min_input_files":         5,
    },
    "silver": {
        "snapshot_retention_days": 10,
        "retain_last_snapshots":   5,
        "orphan_retention_days":   5,
        "target_file_size_mb":     256,
        "min_input_files":         5,
    },
    "gold": {
        "snapshot_retention_days": 7,  # Court : Gold est très queryé
        "retain_last_snapshots":   3,
        "orphan_retention_days":   3,   # Minimum absolu
        "target_file_size_mb":     512, # Gros fichiers → meilleures perfs analytiques
        "min_input_files":         3,
    },
}


# ─────────────────────────────────────────────────────────────────
# SparkSession — les confs nessie sont déjà présentes dans spark-defaults
# ─────────────────────────────────────────────────────────────────
def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("iceberg-daily-maintenance")
        .getOrCreate()
    )


def read_spark_conf(spark: SparkSession, key: str) -> str:
    """Lit une conf Spark et lève une erreur explicite si absente."""
    value = spark.conf.get(key, None)
    if not value:
        raise RuntimeError(
            f"Configuration Spark manquante : '{key}'. "
            f"Vérifier spark-defaults.conf ou les options de soumission du job."
        )
    log.info(f"  {key} = {value}")
    return value


# ─────────────────────────────────────────────────────────────────
# Opérations de maintenance
# ─────────────────────────────────────────────────────────────────
def expire_snapshots(spark: SparkSession, table: str, cfg: dict) -> None:
    cutoff = datetime.utcnow() - timedelta(days=cfg["snapshot_retention_days"])
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    log.info(f"    expire_snapshots → older_than={cutoff_str}, retain_last={cfg['retain_last_snapshots']}")
    spark.sql(f"""
        CALL nessie.system.expire_snapshots(
            table       => '{table}',
            older_than  => TIMESTAMP '{cutoff_str}',
            retain_last => {cfg['retain_last_snapshots']}
        )
    """).show(truncate=False)


def rewrite_manifests(spark: SparkSession, table: str) -> None:
    log.info(f"    rewrite_manifests")
    spark.sql(f"CALL nessie.system.rewrite_manifests(table => '{table}')").show(truncate=False)


def rewrite_data_files(spark: SparkSession, table: str, cfg: dict) -> None:
    target_bytes = cfg["target_file_size_mb"] * 1024 * 1024
    log.info(f"    rewrite_data_files → target={cfg['target_file_size_mb']}MB, min_input_files={cfg['min_input_files']}")
    spark.sql(f"""
        CALL nessie.system.rewrite_data_files(
            table   => '{table}',
            options => map(
                'target-file-size-bytes', '{target_bytes}',
                'min-input-files',        '{cfg["min_input_files"]}'
            )
        )
    """).show(truncate=False)


def remove_orphan_files(spark: SparkSession, table: str, cfg: dict) -> None:
    # Jamais moins de 3 jours : risque de corrompre des écrits en cours
    retention_days = max(cfg["orphan_retention_days"], 3)
    cutoff = datetime.utcnow() - timedelta(days=retention_days)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    log.info(f"    remove_orphan_files → older_than={cutoff_str}")
    spark.sql(f"""
        CALL nessie.system.remove_orphan_files(
            table      => '{table}',
            older_than => TIMESTAMP '{cutoff_str}'
        )
    """).show(truncate=False)


# ─────────────────────────────────────────────────────────────────
# Maintenance d'un layer complet
# ─────────────────────────────────────────────────────────────────
def maintain_layer(spark: SparkSession, layer: str, run_orphan_cleanup: bool) -> None:
    namespace = f"nessie.{layer}"
    cfg = LAYER_CONFIG[layer]

    log.info(f"{'═'*60}")
    log.info(f"  LAYER : {layer.upper()}  ({namespace})")
    log.info(f"{'═'*60}")

    try:
        tables = [
            f"{namespace}.{row['tableName']}"
            for row in spark.sql(f"SHOW TABLES IN {namespace}").collect()
        ]
    except Exception as e:
        log.error(f"Impossible de lister les tables de {namespace} : {e}")
        return

    if not tables:
        log.warning(f"Aucune table dans {namespace}, layer ignoré.")
        return

    log.info(f"  {len(tables)} table(s) trouvée(s) : {tables}")

    errors = []
    for table in tables:
        log.info(f"  ── {table}")
        for op_name, op_fn, op_args in [
            ("expire_snapshots",  expire_snapshots,  (spark, table, cfg)),
            ("rewrite_manifests", rewrite_manifests, (spark, table)),
            ("rewrite_data_files",rewrite_data_files,(spark, table, cfg)),
        ]:
            try:
                op_fn(*op_args)
            except Exception as e:
                log.error(f"    ✗ {op_name} sur {table} : {e}", exc_info=True)
                errors.append((table, op_name, str(e)))

        if run_orphan_cleanup:
            try:
                remove_orphan_files(spark, table, cfg)
            except Exception as e:
                log.error(f"    ✗ remove_orphan_files sur {table} : {e}", exc_info=True)
                errors.append((table, "remove_orphan_files", str(e)))

    if errors:
        log.warning(f"  {len(errors)} erreur(s) sur le layer {layer} :")
        for table, op, msg in errors:
            log.warning(f"    - {table} / {op} : {msg}")


# ─────────────────────────────────────────────────────────────────
# Point d'entrée principal
# ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    spark = get_spark()

    log.info("Lecture des confs Spark...")
    nessie_uri = read_spark_conf(spark, "spark.sql.catalog.nessie.uri")
    warehouse  = read_spark_conf(spark, "spark.sql.catalog.nessie.warehouse")
    log.info(f"Nessie URI : {nessie_uri}")
    log.info(f"Warehouse  : {warehouse}")

    # Orphan cleanup seulement le dimanche (weekday() == 6)
    is_sunday          = datetime.utcnow().weekday() == 6
    run_orphan_cleanup = is_sunday
    log.info(f"Orphan cleanup : {'OUI (dimanche)' if run_orphan_cleanup else 'NON'}")

    # ── Boucle sur tous les layers
    for layer in LAYER_CONFIG:
        maintain_layer(spark, layer, run_orphan_cleanup)

    log.info("Maintenance quotidienne terminée.")
    spark.stop()
