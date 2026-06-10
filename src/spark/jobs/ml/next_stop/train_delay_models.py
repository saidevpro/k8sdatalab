import os

import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import (
    LogisticRegression,
    DecisionTreeClassifier,
    RandomForestClassifier,
    GBTClassifier,
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)


NESSIE_CATALOG = os.getenv("NESSIE_CATALOG", "nessie")
GOLD_NAMESPACE = os.getenv("GOLD_NAMESPACE", "gold")
FEATURES_TABLE = os.getenv(
    "FEATURES_TABLE", f"{NESSIE_CATALOG}.{GOLD_NAMESPACE}.next_stop_features"
)

EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT", "next_stop_delay")
REGISTERED_MODEL = os.getenv("MLFLOW_REGISTERED_MODEL", "next_stop_delay_classifier")
TRAIN_FRACTION = float(os.getenv("TRAIN_FRACTION", "0.8"))

LABEL_COL = "label"
CATEGORICAL_COLS = ["line_ref", "published_line", "direction", "cat_jour"]
NUMERIC_COLS = [
    "hour",
    "day_of_week",
    "is_weekend",
    "is_holiday",
    "is_school_holiday",
    "hist_avg_arrival_delay_sec",
    "hist_pct_on_time",
]


def load_dataset(spark: SparkSession):
    df = spark.read.table(FEATURES_TABLE).withColumn(
        LABEL_COL, F.col("is_delayed").cast("double")
    )
    for col in NUMERIC_COLS:
        df = df.withColumn(col, F.col(col).cast("double"))
    return (
        df.where(F.col(LABEL_COL).isNotNull() & F.col("service_date").isNotNull())
        .na.fill(0.0, NUMERIC_COLS)
    )


def time_split(df):
    df = df.withColumn("__ts", F.unix_timestamp(F.col("service_date")))
    cutoff = df.approxQuantile("__ts", [TRAIN_FRACTION], 0.01)[0]
    train = df.where(F.col("__ts") <= cutoff).drop("__ts")
    test = df.where(F.col("__ts") > cutoff).drop("__ts")
    return train, test


def build_feature_stages():
    indexers = [
        StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep")
        for col in CATEGORICAL_COLS
    ]
    encoder = OneHotEncoder(
        inputCols=[f"{col}_idx" for col in CATEGORICAL_COLS],
        outputCols=[f"{col}_oh" for col in CATEGORICAL_COLS],
        handleInvalid="keep",
    )
    assembler = VectorAssembler(
        inputCols=[f"{col}_oh" for col in CATEGORICAL_COLS] + NUMERIC_COLS,
        outputCol="features",
    )
    return indexers + [encoder, assembler]


def train_and_log(client, name, model, params, feature_stages, train, test):
    auc_eval = BinaryClassificationEvaluator(
        labelCol=LABEL_COL, metricName="areaUnderROC"
    )
    f1_eval = MulticlassClassificationEvaluator(labelCol=LABEL_COL, metricName="f1")

    with mlflow.start_run(run_name=name) as run:
        fitted = Pipeline(stages=feature_stages + [model]).fit(train)
        predictions = fitted.transform(test)

        roc_auc = auc_eval.evaluate(predictions)
        f1 = f1_eval.evaluate(predictions)

        mlflow.set_tag("algorithm", name)
        mlflow.log_params(params)
        mlflow.log_metric("roc_auc", roc_auc)
        mlflow.log_metric("f1", f1)
        mlflow.spark.log_model(fitted, artifact_path="model")

        version = mlflow.register_model(
            f"runs:/{run.info.run_id}/model", REGISTERED_MODEL
        ).version
        client.set_model_version_tag(REGISTERED_MODEL, version, "algorithm", name)
        client.set_model_version_tag(REGISTERED_MODEL, version, "roc_auc", f"{roc_auc:.4f}")

        print(f"{name}: roc_auc={roc_auc:.4f} f1={f1:.4f} version={version}")
        return name, roc_auc, version


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    mlflow.set_experiment(EXPERIMENT_NAME)
    client = MlflowClient()

    train, test = time_split(load_dataset(spark))
    train.cache()
    test.cache()
    print(f"train rows={train.count()} test rows={test.count()}")

    feature_stages = build_feature_stages()

    models = [
        (
            "logreg",
            LogisticRegression(labelCol=LABEL_COL, featuresCol="features", maxIter=50),
            {"model": "logistic_regression", "maxIter": 50},
        ),
        (
            "dtree",
            DecisionTreeClassifier(labelCol=LABEL_COL, featuresCol="features", maxDepth=8),
            {"model": "decision_tree", "maxDepth": 8},
        ),
        (
            "rforest",
            RandomForestClassifier(
                labelCol=LABEL_COL, featuresCol="features", numTrees=200, maxDepth=10
            ),
            {"model": "random_forest", "numTrees": 200, "maxDepth": 10},
        ),
        (
            "gbt",
            GBTClassifier(labelCol=LABEL_COL, featuresCol="features", maxIter=100, maxDepth=5),
            {"model": "gbt", "maxIter": 100, "maxDepth": 5},
        ),
    ]

    results = [
        train_and_log(client, name, model, params, feature_stages, train, test)
        for name, model, params in models
    ]

    best_name, best_auc, best_version = max(results, key=lambda r: r[1])
    client.set_model_version_tag(REGISTERED_MODEL, best_version, "best", "true")
    client.set_registered_model_alias(REGISTERED_MODEL, "champion", best_version)
    print(f"best model: {best_name} (roc_auc={best_auc:.4f}) version={best_version} -> alias 'champion'")

    spark.stop()


if __name__ == "__main__":
    main()
