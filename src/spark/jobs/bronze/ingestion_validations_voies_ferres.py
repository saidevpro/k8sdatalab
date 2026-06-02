import os
import requests
import zipfile
import boto3
import io
import secrets
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from botocore.exceptions import ClientError

def createOrOverwritePartitions(sparkSession, df, dest_table):
    if not sparkSession.catalog.tableExists(dest_table):
        (
            df
            .writeTo(dest_table)
            .partitionedBy("ingestion_date")
            .option("merge-schema", "true")
            .create()
        )
    else:
        (
            df
            .writeTo(dest_table)
            .option("merge-schema", "true")
            .overwritePartitions()
        )
        
def dbg(name, *args):
    print("*"*10, f"Start debug {name}", "*"*10, flush=True)
    print(*args, flush=True)
    print("*"*10, "Debug end", "*"*10, flush=True)

spark = SparkSession.builder.getOrCreate()

PRIM_DATASET_URL = os.getenv("PRIM_DATASET_URI")
ENDPOINT = "/histo-validations-reseau-ferre/exports/json"
DATASET_API_KEY = os.getenv("PRIM_DATASET_TOKEN")

headers = {
    "Authorization": f"apikey {DATASET_API_KEY}",
}

r = requests.get(f"{PRIM_DATASET_URL}{ENDPOINT}", headers=headers, timeout=30)
r.raise_for_status()

data = r.json()

if not len(data):
    raise ValueError("Historique validation reseau ferre")


dbg("PRIM VALIDATIONS RESPONSE", data)

tmp_bucket = "tmp-spark"
run_id = f"{datetime.utcnow():%Y-%m-%dT%H%M%S}-{secrets.token_hex(8)}"
s3_prefix = f"validations/{run_id}"

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_URL"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
)

def load_zip_to_s3(zip_url):
    resp = requests.get(zip_url, headers=headers, timeout=60, stream=True)
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
            data = zf.read(info.filename)
    
            s3.put_object(
                Bucket=tmp_bucket,
                Key=key,
                Body=data,
                ContentLength=len(data),
            )
            uploaded.append(key)

    return uploaded

list_files = []
for item in data:
    file = item.get("reseau_ferre") or {}
    if not file:
        continue;
        
    year = item.get("annee")
    zip_url = file.get("url")
    dbg("Loading validation zip", zip_url)
    
    if zip_url:
        files_path = load_zip_to_s3(zip_url)
        list_files.extend(files_path)


SPARK_CSV_VALIDATIONS_NB = f"s3a://{tmp_bucket}/{s3_prefix}/*NB_FER*.txt"
SPARK_CSV_VALIDATIONS_PROFIL = f"s3a://{tmp_bucket}/{s3_prefix}/*PROFIL_FER*.txt"

df_nb = (
    spark.read
    .option("header",   "true")
    .option("sep",      "\t")
    .option("encoding", "UTF-16")
    .csv(SPARK_CSV_VALIDATIONS_NB)
)

df_nb.show(5)

createOrOverwritePartitions(
    sparkSession=spark,
    df=df_nb,
    dest_table="nessie.bronze.validations_nb"
)

df_profil = (
    spark.read
    .option("header", "true")
    .option("sep",    "\t")
    .csv(SPARK_CSV_VALIDATIONS_PROFIL)
)

df_profil.show(5)

createOrOverwritePartitions(
    sparkSession=spark,
    df=df_profil,
    dest_table="nessie.bronze.validations_profil"
)


for key in list_files:
    try:
        dbg("Deleting tmp s3 object", tmp_bucket, key)
        
        s3.delete_object(
            Bucket=tmp_bucket,
            Key=key,
        )
    except ClientError as e:
        print(f"Warning: deleting s3 object {key} failed")   
        
spark.stop()
