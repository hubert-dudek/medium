-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![image1.png](./image1.png "image1.png")

-- COMMAND ----------

WITH RECURSIVE recursive_cte(col) MAX RECURSION LEVEL 15 -- recursive limit, kind of timeout as if more than 10 it will fail (VIDEO show fail)
AS (
  SELECT
    'a'
  UNION ALL
    -- get the previous result of recursive CTE and add another character
  SELECT
    col || CHAR(ASCII(SUBSTR(col, -1)) + 1)
  FROM
    recursive_cte -- it is recursive cte not table, it is self-referencing
  WHERE
    LENGTH(col) < 10
)
SELECT
  col
FROM
  recursive_cte;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![image2.png](./image2.png "image2.png")

-- COMMAND ----------

-- Example data ---------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW routes (origin, destination) AS
VALUES
  ('New York', 'Washington'),
  ('New York', 'Boston'),
  ('Boston', 'New York'),
  ('Washington', 'Boston'),
  ('Washington', 'Raleigh');

  SELECT * FROM routes;

-- COMMAND ----------

-- Recursive CTE --------------------------------------------------------------
WITH RECURSIVE recursive_cte AS (
  /* anchor = New York = starting city */
  SELECT
    'New York' AS current_city,
    ARRAY('New York') AS path,
    0 AS hops -- start 0 hops
  UNION ALL
    /* step = follow every outgoing edge that does not revisit a city */
  SELECT
    r.destination AS current_city,
    concat(recursive_cte.path, array(r.destination)),
    recursive_cte.hops + 1
  FROM
    routes r
    JOIN recursive_cte  -- it is recursive cte not a table, it is self-referencing
        ON r.origin = recursive_cte.current_city -- we join with next city which we can reach from city
    AND NOT array_contains(recursive_cte.path, r.destination) -- we exclude already visited cities
)
SELECT
  *
FROM
  recursive_cte
ORDER BY
  hops,
  current_city;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![image3.png](./image3.png "image3.png")

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW department_hierarchy (department, parent) AS
VALUES
  ('Company', NULL),
  ('Sales', 'Company'),
  ('North America Sales', 'Sales'),
  ('California Sales', 'North America Sales'),
  ('Florida Sales', 'North America Sales'),
  ('EMEA Sales', 'Sales'),
  ('France Sales', 'EMEA Sales');

  SELECT * FROM department_hierarchy;

-- COMMAND ----------



-- Recursive CTE --------------------------------------------------------------
WITH RECURSIVE recursive_cte AS (
  -- anchor row(s): the department we start from
  SELECT
    1 AS level,
    department,
    parent
  FROM
    department_hierarchy
  WHERE
    department = 'Sales'
  UNION ALL
    -- recursive step: get direct children of the previous level
  SELECT
    recursive_cte.level + 1 AS level,
    d.department,
    d.parent
  FROM
    department_hierarchy d
    JOIN recursive_cte -- it is recursive cte not table, it is self-referencing
      ON d.parent = recursive_cte.department
)
SELECT
  level,
  department,
  parent
FROM
  recursive_cte
ORDER BY
  level,
  department;
