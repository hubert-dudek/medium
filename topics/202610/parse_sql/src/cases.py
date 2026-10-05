CASES = [
    ("literal_select", "select", "SELECT 1 AS value"),
    ("inline_values", "select", "SELECT * FROM VALUES (1, 'alpha'), (2, 'beta') AS items(id, label)"),
    ("unresolved_table", "relations", "SELECT id FROM demo_catalog.sandbox.table_that_does_not_exist"),
    ("unresolved_function", "relations", "SELECT demo_catalog.sandbox.function_that_does_not_exist(42) AS result"),
    (
        "join",
        "relations",
        "SELECT o.id, c.name FROM demo_catalog.sandbox.orders o "
        "LEFT JOIN demo_catalog.sandbox.customers c ON o.customer_id = c.id",
    ),
    (
        "nested_ctes",
        "relations",
        "WITH outer_cte AS (WITH inner_cte AS (SELECT 1 AS id) "
        "SELECT id FROM inner_cte) SELECT id FROM outer_cte",
    ),
    (
        "correlated_subquery",
        "relations",
        "SELECT c.id FROM demo_catalog.sandbox.customers c WHERE EXISTS "
        "(SELECT 1 FROM demo_catalog.sandbox.orders o WHERE o.customer_id = c.id)",
    ),
    ("union", "relations", "SELECT 1 AS id UNION ALL SELECT 2 AS id"),
    (
        "quoted_identifier_with_dot",
        "relations",
        "SELECT `order.id` FROM `demo_catalog`.`sandbox`.`orders.archive`",
    ),
    ("projection_aliases", "expressions", "SELECT 1 AS explicit_alias, 2 implicit_alias, 3 + 4, upper('hello')"),
    ("projection_star", "expressions", "SELECT t.*, t.id AS copied_id FROM VALUES (1, 'alpha') AS t(id, label)"),
    (
        "window_expression",
        "expressions",
        "SELECT id, row_number() OVER (ORDER BY id DESC) AS position "
        "FROM VALUES (1), (2) AS items(id)",
    ),
    ("named_parameter", "parameters", "SELECT :country AS country, :minimum_amount + 1 AS threshold"),
    ("positional_parameters", "parameters", "SELECT ? AS country, ? + 1 AS threshold"),
    (
        "merge",
        "dml",
        "MERGE INTO demo_catalog.sandbox.target t "
        "USING (SELECT 1 AS id, 'updated' AS label) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET label = s.label "
        "WHEN NOT MATCHED THEN INSERT (id, label) VALUES (s.id, s.label)",
    ),
    ("delete", "dml", "DELETE FROM demo_catalog.sandbox.orders WHERE status = 'cancelled'"),
    ("update", "dml", "UPDATE demo_catalog.sandbox.orders SET status = 'review' WHERE amount > 1000"),
    (
        "insert_select",
        "dml",
        "INSERT INTO demo_catalog.sandbox.archived_orders "
        "SELECT * FROM demo_catalog.sandbox.orders WHERE status = 'closed'",
    ),
    (
        "create_view",
        "ddl",
        "CREATE OR REPLACE VIEW demo_catalog.sandbox.open_orders AS "
        "SELECT id, amount FROM demo_catalog.sandbox.orders WHERE status = 'open'",
    ),
    ("create_table", "ddl", "CREATE TABLE demo_catalog.sandbox.items (id BIGINT, label STRING) USING DELTA"),
    ("explain_select", "utility", "EXPLAIN FORMATTED SELECT 1 AS value"),
    (
        "comments_and_string_semicolons",
        "boundaries",
        "-- A comment with a semicolon ;\nSELECT 'first;second' AS text /* another ; */;",
    ),
    ("keywords_as_identifiers", "expressions", "SELECT FROM WHERE"),
    ("malformed_select", "invalid", "SELEC 1"),
    ("empty_input", "invalid", ""),
    ("null_input", "invalid", None),
    ("multiple_valid_statements", "multiple", "SELECT 1 AS first_value; SELECT 2 AS second_value; SELECT 3 AS third_value;"),
    ("valid_invalid_valid", "multiple", "SELECT 1 AS first_value; SELEC 2; SELECT 3 AS third_value;"),
    (
        "sql_script",
        "scripting",
        "BEGIN\n  DECLARE counter INT DEFAULT 1;\n  SET counter = counter + 1;\n  SELECT counter AS value;\nEND",
    ),
]

CASES.extend([
    ("comments_only", "boundaries", "/* only a comment ; */ -- and another\n"),
    ("unicode_offsets", "boundaries", "SELECT '😀' AS emoji; SELECT 2 AS next_value"),
])
