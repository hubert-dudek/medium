# Databricks notebook source
# Databricks Runtime ML recommended
import mlflow
import mlflow.spark

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# ---------- UC objects ----------
CATALOG = "hub"
SCHEMA  = "default"
TABLE   = f"{CATALOG}.{SCHEMA}.re_prices"      # UC feature table with PRIMARY KEY (id)
UC_MODEL= f"{CATALOG}.{SCHEMA}.re_price_lr"    # UC (3-part) model name

# ---------- columns ----------
key_col   = "id"
label_col = "price"
features  = ["bedrooms", "bathrooms", "sqft", "year_built", "dist_km"]

fe = FeatureEngineeringClient()

# 1) Build a TrainingSet (single-table lookup for lineage)
#    df should contain keys + label; features are looked up from the same table.
label_df = fe.read_table(name=TABLE).select(key_col, label_col)

training_set = fe.create_training_set(
    df=label_df,
    feature_lookups=[
        FeatureLookup(
            table_name=TABLE,
            feature_names=features,   # explicit to avoid pulling the label as a feature
            lookup_key=key_col
        )
    ],
    label=label_col,
    exclude_columns=[key_col]        # keep label from df, exclude key from assembled features
)

train_df = training_set.load_df()

# Spark ML requires numeric types; cast BIGINT -> DOUBLE for features + label.
cast_cols = features + [label_col]
train_df = train_df.select(
    *[col(c).cast("double").alias(c) if c in cast_cols else col(c) for c in train_df.columns]
)

# 2) Train a simple Spark ML regression model
assembler = VectorAssembler(inputCols=features, outputCol="features")
lr        = LinearRegression(featuresCol="features", labelCol=label_col)
pipeline  = Pipeline(stages=[assembler, lr])

train_df, test_df = train_df.randomSplit([0.75, 0.25], seed=42)

with mlflow.start_run(run_name="re_price_train_sparkml"):
    model = pipeline.fit(train_df)

    # quick metrics (Spark-side) so you see something in the run
    preds = model.transform(test_df)
    rmse = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="rmse").evaluate(preds)
    r2   = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="r2").evaluate(preds)
    mlflow.log_metrics({"rmse": rmse, "r2": r2})

    # 3) Log & register with lineage (Feature Eng client + Spark flavor)
    fe.log_model(
        model=model,
        artifact_path="model",
        flavor=mlflow.spark,
        training_set=training_set,           # <-- attaches feature spec / lineage
        registered_model_name=UC_MODEL,      # UC registry (3-part name)
        infer_input_example=True             # logs input example from training_set
    )

print(f"✅ Registered: {UC_MODEL}")

