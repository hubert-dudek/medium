# Databricks notebook source
data = [
    (1, 120,  80, 200),
    (2, 150,  90, 220),
    (3, 170, 100, 240),
    (4, 160, 130, 250),
    (5, 180, 120, 260),
    (6, 200, 140, 280),
]
cols = ["month", "electronics", "furniture", "clothing"]
df = spark.createDataFrame(data, cols)

# COMMAND ----------

# ────────────────────────────────────────────────────────────────────────────────
# Line chart – monthly trend by category
# ────────────────────────────────────────────────────────────────────────────────
df.plot.line()
    x="month",
    y=["electronics", "furniture", "clothing"],
    title="Monthly Sales by Category",
    markers=True
)

# COMMAND ----------

# ────────────────────────────────────────────────────────────────────────────────
# Stacked bar chart – same data in bar form
# ────────────────────────────────────────────────────────────────────────────────
df.plot.bar()
    x="month",
    y=["electronics", "furniture", "clothing"],
    title="Monthly Sales – Stacked Bar",
    barmode='group',
    text_auto=True
)

# COMMAND ----------

# ────────────────────────────────────────────────────────────────────────────────
# Pie Lake :-)
# ────────────────────────────────────────────────────────────────────────────────
df.plot.pie()
    x="month",
    y="electronics",
    title="Pie Chart",
    hole=.3
)

# COMMAND ----------

# ────────────────────────────────────────────────────────────────────────────────
# Histogram – distribution of electronics sales
# ────────────────────────────────────────────────────────────────────────────────
df.select("electronics").plot(
    kind="hist",
    bins=4,
    title="Electronics Sales Distribution")
