# JDBC connections in Unity Catalog

**On camera in AWS-DEV** open **Catalog → Connect → Connections → Create connection**. Select **JDBC**, enter `news_202621_jdbc` as the name and click **Next**. Show the JDBC URL, **Add Volume JAR**, and the options allowlist. Explain how the connection owner configures the driver and credentials once, then callers use only the connection name. Close the wizard after showing the fields. [Databricks JDBC guide](https://docs.databricks.com/aws/en/connect/jdbc-connection)

**For a live query** use an existing connection or prepare [create_connection.sql](create_connection.sql) before filming. Replace the angle-bracket fields with the driver's actual UC volume path, a reachable JDBC URL, and the scope and key names for the stored username and password. Run the creation cell as a principal with `CREATE CONNECTION`. The caller needs `USE CONNECTION` and access to read the driver volume. Serverless SQL warehouses require the isolated-workload networking preview. [Databricks connection options](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-connection)

Open [demo.sql](demo.sql), enter the connection name for `connection_name`, and run the second cell. The source database should return one row with `connected = 1` when it supports `SELECT 1`. The query reads no application tables.
