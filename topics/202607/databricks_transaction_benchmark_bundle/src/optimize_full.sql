-- Force a full rewrite/reclustering after data creation.

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

OPTIMIZE customers FULL;
OPTIMIZE transactions FULL;
