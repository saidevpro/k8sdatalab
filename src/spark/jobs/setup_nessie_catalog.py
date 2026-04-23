from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

branches = ["main", "dev", "uat"]
namespaces = ["bronze", "silver", "gold"]

for branch in branches:
    spark.conf.set("spark.sql.catalog.nessie.ref", branch)
    for namespace in namespaces:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.{namespace}")
        
        
spark.stop()