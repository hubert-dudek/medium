# Databricks notebook source
# Widgets expected by the deployment job:
dbutils.widgets.text("model_name", "")
dbutils.widgets.text("model_version", "")
model_name    = dbutils.widgets.get("model_name")
model_version = dbutils.widgets.get("model_version")

CATALOG = "hub"
SCHEMA  = "default"
TABLE   = f"{CATALOG}.{SCHEMA}.re_prices"      # UC feature table with PRIMARY KEY (id)
UC_MODEL= f"{CATALOG}.{SCHEMA}.re_price_lr"    # UC (3-part) model name

import mlflow
from pyspark.sql.functions import col
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

# --- Columns / table inferred from the same schema as the model ---
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
    exclude_columns=[]        # keep label from df, exclude key from assembled features
)

train_df = training_set.load_df()

# Spark ML requires numeric types; cast BIGINT -> DOUBLE for features + label.
cast_cols = features + [label_col]
train_df = train_df.select(
    *[col(c).cast("bigint").alias(c) if c in cast_cols else col(c) for c in train_df.columns]
)

train_df = train_df.withColumn("dist_km", col("dist_km").cast("double"))

# Use a 25% split for evaluation (seed aligned with training)
train_df, test_df = train_df.randomSplit([0.9, 0.1], seed=42)

with mlflow.start_run(run_name="re_price_eval"):
    result = mlflow.models.evaluate(
        model=f"models:/{model_name}/{model_version}",
        data=test_df,                 # Spark DataFrame
        targets=label_col,
        model_type="regressor",
        evaluators="default",
    )

# Print a compact dict of metrics
print({k: round(float(v), 4) for k, v in result.metrics.items()})

