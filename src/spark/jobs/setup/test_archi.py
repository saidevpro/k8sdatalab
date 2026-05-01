import argparse
import os
import traceback
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--catalog", default=os.getenv("ICEBERG_CATALOG", "nessie"))
    parser.add_argument(
        "--namespace", default=os.getenv("ICEBERG_TEST_NAMESPACE", "smoke_test"))
    parser.add_argument(
        "--table", default=os.getenv("ICEBERG_TEST_TABLE", "airflow_smoke_test"))
    parser.add_argument("--rows", type=int,
                        default=int(os.getenv("ICEBERG_TEST_ROWS", "10")))
    parser.add_argument("--drop-at-end", action="store_true")
    return parser.parse_args()


def hadoop_path_exists(spark, path):
    jvm = spark.sparkContext._jvm
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    fs_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = fs_path.getFileSystem(hconf)
    return fs.exists(fs_path)


def list_hadoop_path(spark, path):
    jvm = spark.sparkContext._jvm
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    fs_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = fs_path.getFileSystem(hconf)

    if not fs.exists(fs_path):
        return []

    statuses = fs.listStatus(fs_path)
    return [str(status.getPath()) for status in statuses]


def print_spark_conf(spark, catalog):
    conf = dict(spark.sparkContext.getConf().getAll())

    keys = [
        "spark.app.name",
        "spark.master",
        "spark.eventLog.enabled",
        "spark.eventLog.dir",
        "spark.history.fs.logDirectory",
        "spark.sql.extensions",
        f"spark.sql.catalog.{catalog}",
        f"spark.sql.catalog.{catalog}.catalog-impl",
        f"spark.sql.catalog.{catalog}.uri",
        f"spark.sql.catalog.{catalog}.ref",
        f"spark.sql.catalog.{catalog}.warehouse",
        f"spark.sql.catalog.{catalog}.authentication.type",
        "spark.hadoop.fs.s3a.endpoint",
        "spark.hadoop.fs.s3a.path.style.access",
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        "spark.hadoop.fs.s3a.impl",
    ]

    print("=== Effective Spark configuration ===")
    for key in keys:
        print(f"{key}={conf.get(key)}")


def main():
    args = parse_args()

    run_id = uuid.uuid4().hex[:8]
    full_namespace = f"{args.catalog}.{args.namespace}"
    full_table = f"{full_namespace}.{args.table}"

    spark = SparkSession.builder.getOrCreate()

    try:
        print("=== Spark session started ===")
        print(f"spark.version={spark.version}")
        print(f"spark.applicationId={spark.sparkContext.applicationId}")
        print(f"test.table={full_table}")
        print(f"run_id={run_id}")

        print_spark_conf(spark, args.catalog)

        event_log_enabled = spark.conf.get("spark.eventLog.enabled", "false")
        event_log_dir = spark.conf.get("spark.eventLog.dir", None)
        history_log_dir = spark.conf.get("spark.history.fs.logDirectory", None)
        warehouse = spark.conf.get(
            f"spark.sql.catalog.{args.catalog}.warehouse", None)

        print("=== Checking event log configuration ===")
        print(f"spark.eventLog.enabled={event_log_enabled}")
        print(f"spark.eventLog.dir={event_log_dir}")
        print(f"spark.history.fs.logDirectory={history_log_dir}")

        if event_log_enabled.lower() != "true":
            raise RuntimeError("spark.eventLog.enabled is not true")

        if not event_log_dir:
            raise RuntimeError("spark.eventLog.dir is missing")

        if not event_log_dir.startswith("s3a://"):
            raise RuntimeError(
                f"spark.eventLog.dir must start with s3a://, got: {event_log_dir}")

        print("=== Listing event log directory before actions ===")
        print(list_hadoop_path(spark, event_log_dir))

        if warehouse:
            print("=== Checking Iceberg warehouse path ===")
            print(f"warehouse={warehouse}")
            print(f"warehouse.exists={hadoop_path_exists(spark, warehouse)}")

        print("=== Creating Iceberg namespace through Nessie catalog ===")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {full_namespace}")

        print("=== Creating Iceberg table if not exists through Nessie catalog ===")
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {full_table} (
                id BIGINT,
                label STRING,
                run_id STRING,
                created_at TIMESTAMP
            )
            USING iceberg
            """
        )

        print("=== Writing DataFrame to Iceberg table ===")
        df = (
            spark.range(0, args.rows)
            .withColumn("label", lit("iceberg_nessie_eventlog_test"))
            .withColumn("run_id", lit(run_id))
            .withColumn("created_at", current_timestamp())
        )

        df.writeTo(full_table).append()

        print("=== Reading Iceberg table for current run ===")
        read_df = spark.table(full_table).where(f"run_id = '{run_id}'")
        read_df.show(truncate=False)

        count_after_append = read_df.count()
        print(f"count.after.append.for.run={count_after_append}")

        if count_after_append != args.rows:
            raise RuntimeError(
                f"Expected {args.rows} rows for run_id={run_id}, got {count_after_append}")

        print("=== Writing SQL row to Iceberg table ===")
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        spark.sql(
            f"""
            INSERT INTO {full_table}
            VALUES (
                999999,
                'sql_insert_test',
                '{run_id}',
                TIMESTAMP '{now_utc}'
            )
            """
        )

        final_count = spark.table(full_table).where(
            f"run_id = '{run_id}'").count()
        print(f"count.final.for.run={final_count}")

        if final_count != args.rows + 1:
            raise RuntimeError(
                f"Expected {args.rows + 1} rows for run_id={run_id}, got {final_count}")

        print("=== Iceberg history ===")
        spark.sql(f"SELECT * FROM {full_table}.history").show(truncate=False)

        print("=== Iceberg snapshots ===")
        spark.sql(f"SELECT * FROM {full_table}.snapshots").show(truncate=False)

        print("=== Listing event log directory after actions ===")
        print(list_hadoop_path(spark, event_log_dir))

        if args.drop_at_end:
            print("=== Dropping test table ===")
            spark.sql(f"DROP TABLE IF EXISTS {full_table}")

        print("=== SUCCESS ===")
        print("S3A, Spark event logs, Nessie catalog, and Iceberg read/write are working.")

    except Exception:
        print("=== FAILED ===")
        traceback.print_exc()
        raise

    finally:
        print("=== Stopping Spark session ===")
        spark.stop()


if __name__ == "__main__":
    main()
