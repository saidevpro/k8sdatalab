def nessieCreateBranchFromMain(spark, branch: str):
    spark.sql("")


def createOrOverwritePartitions(spark, df, table: str, partitionKey: str | list[str]):
    if not spark.catalog.tableExists(table):
        (
            df
            .writeTo(table)
            .partitionedBy("ingestion_date")
            .option("merge-schema", "true")
            .create()
        )
    else:
        (
            df
            .writeTo(table)
            .option("merge-schema", "true")
            .overwritePartitions()
        )
