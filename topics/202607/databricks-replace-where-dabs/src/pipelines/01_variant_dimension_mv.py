from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col


@dp.materialized_view(
    name="sales_product_spec_mv",
    replace_where=col("order_date") >= F.date_sub(F.current_date(), 90),
)
def sales_by_product_spec():
    orders = spark.read.table(
        f"main.replace_where_demo_src_dev.orders_fact"
    )
    products = spark.read.table(
        f"main.replace_where_demo_src_dev.product_spec_dim"
    )
    return orders.join(products, "product_id").select(
        "product_id", "order_id", "order_date", "product_spec"
    )
