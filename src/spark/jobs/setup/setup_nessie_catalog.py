from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

branches = ["main", "dev"]
namespaces = ["bronze", "silver", "gold"]

for branch in branches:
    spark.sql(f"CREATE BRANCH IF NOT EXISTS {branch} IN nessie FROM main")
    spark.sql(f"USE REFERENCE {branch} IN nessie")
    for namespace in namespaces:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.{namespace}")
        
        
spark.stop()