# Databricks notebook source
target_table = "main.default.transactions"
database = "free-sql-db-1216115"
connection = "azure_sql"

# Get last imported primary key
if spark.catalog.tableExists(target_table):
    last_id = spark.sql(f"""
        SELECT COALESCE(MAX(transaction_id), -1)
        FROM {target_table}
    """).first()[0]
else:
    last_id = -1

print(f"Last imported transaction_id: {last_id}")


# Read only new rows using the UC connection
df = spark.sql(f"""
    SELECT *
    FROM remote_query(
        '{connection}',
        database => '{database}',
        query => '
            SELECT
                transaction_id,
                customer_id,
                transaction_date,
                amount
            FROM dbo.transactions
            WHERE transaction_id > {last_id}
        '
    )
""")


# Append to Delta
(df.write
    .format("delta")
    .mode("append")
    .saveAsTable(target_table))

print(f"Imported {df.count()} rows")
