import os
import re
import requests
import zipfile
import boto3
import io
import secrets
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from botocore.exceptions import ClientError


NB_COLUMNS = [
    "JOUR",
    "CODE_STIF_TRNS",
    "CODE_STIF_RES",
    "CODE_STIF_ARRET",
    "LIBELLE_ARRET",
    "ID_REFA_LDA",
    "CATEGORIE_TITRE",
    "NB_VALD",
]

PROFIL_COLUMNS = [
    "CODE_STIF_TRNS",
    "CODE_STIF_RES",
    "CODE_STIF_ARRET",
    "LIBELLE_ARRET",
    "ID_REFA_LDA",
    "CAT_JOUR",
    "TRNC_HORR_60",
    "pourc_validations",
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


def dbg(name, *args):
    print("*" * 10, f"Start debug {name}", "*" * 10, flush=True)
    print(*args, flush=True)
    print("*" * 10, "Debug end", "*" * 10, flush=True)


def detect_delimiter_from_s3_key(s3_client, bucket, key):
    obj = s3_client.get_object(
        Bucket=bucket,
        Key=key,
        Range="bytes=0-4096"
    )

    raw = obj["Body"].read()

    try:
        sample = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None

    lines = sample.splitlines()

    if not lines:
        return None

    first_line = lines[0]

    counts = {
        "\t": first_line.count("\t"),
        ";": first_line.count(";"),
        ",": first_line.count(","),
    }

    delimiter = max(counts, key=counts.get)

    if counts[delimiter] == 0:
        return None

    return delimiter


def clean_column_names(df):
    cleaned_cols = []

    for c in df.columns:
        clean_name = c.replace("\ufeff", "")
        clean_name = clean_name.replace("�", "")
        clean_name = clean_name.strip()
        clean_name = re.sub(r"\s+", "_", clean_name)

        cleaned_cols.append(col(f"`{c}`").alias(clean_name))

    return df.select(cleaned_cols)


def normalize_common_columns(df):
    rename_map = {
        "lda": "ID_REFA_LDA",
        "ID_ZDC": "ID_REFA_LDA",
        "Pourcentage_validations": "pourc_validations",
    }

    for old_name, new_name in rename_map.items():
        if old_name in df.columns and new_name not in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    return df


def is_malformed_single_column(df):
    return len(df.columns) == 1 and (
        ";" in df.columns[0]
        or "," in df.columns[0]
        or "\t" in df.columns[0]
    )


def normalize_to_schema(df, expected_columns):
    df = clean_column_names(df)
    df = normalize_common_columns(df)

    if is_malformed_single_column(df):
        return None

    missing_columns = [
        c for c in expected_columns
        if c not in df.columns
    ]

    if missing_columns:
        return None

    return df.select([
        col(c).cast("string").alias(c)
        for c in expected_columns
    ])


spark = SparkSession.builder.getOrCreate()

catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.bronze")

PRIM_DATASET_URL = os.getenv("PRIM_DATASET_URI")
ENDPOINT = "/histo-validations-reseau-ferre/exports/json"
DATASET_API_KEY = os.getenv("PRIM_DATASET_TOKEN")

headers = {
    "Authorization": f"apikey {DATASET_API_KEY}",
}

r = requests.get(
    f"{PRIM_DATASET_URL}{ENDPOINT}",
    headers=headers,
    timeout=30
)
r.raise_for_status()

data = r.json()

if not len(data):
    raise ValueError("Historique validation reseau ferre vide")

dbg("PRIM VALIDATIONS RESPONSE", data)

tmp_bucket = os.getenv("SPARK_TMP_BUCKET", "tmp-spark")
run_id = f"{datetime.utcnow():%Y-%m-%dT%H%M%S}-{secrets.token_hex(8)}"
s3_prefix = f"validations/{run_id}"

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_URL"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
)


def load_zip_to_s3(zip_url):
    resp = requests.get(
        zip_url,
        headers=headers,
        timeout=60,
        stream=True
    )
    resp.raise_for_status()

    zip_bytes = io.BytesIO(resp.content)
    uploaded = []

    with zipfile.ZipFile(zip_bytes) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue

            if not info.filename.endswith(".txt"):
                continue

            key = f"{s3_prefix}/{info.filename}"
            file_data = zf.read(info.filename)

            s3.put_object(
                Bucket=tmp_bucket,
                Key=key,
                Body=file_data,
                ContentLength=len(file_data),
            )

            uploaded.append(key)

    return uploaded


list_files = []

for item in data:
    file = item.get("reseau_ferre") or {}

    if not file:
        continue

    year = item.get("annee")
    zip_url = file.get("url")

    dbg("Loading validation zip", year, zip_url)

    if zip_url:
        files_path = load_zip_to_s3(zip_url)
        list_files.extend(files_path)


ingestion_date = current_timestamp()

nb_keys = [
    key
    for key in list_files
    if "NB_FER" in key and key.endswith(".txt")
]

profil_keys = [
    key
    for key in list_files
    if "PROFIL_FER" in key and key.endswith(".txt")
]


for key in nb_keys:
    try:
        path = f"s3a://{tmp_bucket}/{key}"
        delimiter = detect_delimiter_from_s3_key(s3, tmp_bucket, key)

        if delimiter is None:
            dbg("Skipping NB file because encoding or delimiter is invalid", key)
            continue

        dbg("Reading NB file", path, "delimiter", delimiter)

        df_nb = (
            spark.read
            .option("header", "true")
            .option("sep", delimiter)
            .csv(path)
        )

        df_nb = normalize_to_schema(df_nb, NB_COLUMNS)

        if df_nb is None:
            dbg("Skipping malformed NB file", path)
            continue

        df_nb = df_nb.withColumn("ingestion_date", ingestion_date)

        df_nb.show(5, truncate=False)

        appendToBronze(
            sparkSession=spark,
            df=df_nb,
            dest_table=f"{catalog_name}.bronze.validations_nb"
        )

    except Exception as e:
        dbg("Skipping NB file because of error", key, str(e))
        continue


for key in profil_keys:
    try:
        path = f"s3a://{tmp_bucket}/{key}"
        delimiter = detect_delimiter_from_s3_key(s3, tmp_bucket, key)

        if delimiter is None:
            dbg("Skipping PROFIL file because encoding or delimiter is invalid", key)
            continue

        dbg("Reading PROFIL file", path, "delimiter", delimiter)

        df_profil = (
            spark.read
            .option("header", "true")
            .option("sep", delimiter)
            .csv(path)
        )

        df_profil = normalize_to_schema(df_profil, PROFIL_COLUMNS)

        if df_profil is None:
            dbg("Skipping malformed PROFIL file", path)
            continue

        df_profil = df_profil.withColumn("ingestion_date", ingestion_date)

        df_profil.show(5, truncate=False)

        appendToBronze(
            sparkSession=spark,
            df=df_profil,
            dest_table=f"{catalog_name}.bronze.validations_profil"
        )

    except Exception as e:
        dbg("Skipping PROFIL file because of error", key, str(e))
        continue


for key in list_files:
    try:
        dbg("Deleting tmp s3 object", tmp_bucket, key)

        s3.delete_object(
            Bucket=tmp_bucket,
            Key=key,
        )

    except ClientError:
        print(f"Warning: deleting s3 object {key} failed", flush=True)


spark.stop()
