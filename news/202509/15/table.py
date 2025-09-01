# Databricks notebook source
from delta.tables import DeltaTable, IdentityGenerator
from pyspark.sql.types import LongType, DoubleType
import pandas as pd
import numpy as np

CATALOG = "hub"
SCHEMA  = "default"
TABLE   = f"{CATALOG}.{SCHEMA}.re_prices"
UC_MODEL= f"{CATALOG}.{SCHEMA}.re_price_lr"

spark.sql(f"DROP TABLE IF EXISTS {TABLE}")

# Create schema/table with a tiny synthetic dataset
rng = np.random.default_rng(42)
n   = 300

pdf = pd.DataFrame({
    "bedrooms":   rng.integers(1, 5, n),
    "bathrooms":  rng.integers(1, 4, n),
    "sqft":       rng.integers(400, 2200, n),
    "year_built": rng.integers(1950, 2021, n),
    "dist_km":    rng.uniform(0, 20, n),  # distance to downtown
})
# Linear-ish price with noise
price = (50_000
         + pdf["bedrooms"] * 60_000
         + pdf["bathrooms"] * 40_000
         + pdf["sqft"] * 300
         - (2021 - pdf["year_built"]) * 1_000
         - pdf["dist_km"] * 5_000
         + rng.normal(0, 25_000, n))
pdf["price"] = price.round(0).astype(np.int64)  # Ensure price is of type int64

# Create Delta table with identity column
spark.sql(f"""
CREATE TABLE {TABLE} (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    bedrooms BIGINT,
    bathrooms BIGINT,
    sqft BIGINT,
    year_built BIGINT,
    dist_km DOUBLE,
    price BIGINT,
    PRIMARY KEY (id)
)
""")

# Save data to the Delta table
spark.createDataFrame(pdf).write.mode("append").saveAsTable(TABLE)
