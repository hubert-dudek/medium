-- Databricks notebook source
-- MAGIC %md
-- MAGIC https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-scripting

-- COMMAND ----------

BEGIN
  SELECT 1;
END;

-- COMMAND ----------

-- DBTITLE 1,BEGIN END
-- sum up all odd numbers from 1 through 10
BEGIN
  DECLARE sum INT DEFAULT 0;
  DECLARE num INT DEFAULT 0;
  sumNumbers: WHILE num < 10 DO
    SET num = num + 1;
    IF num % 2 = 0 THEN
      ITERATE sumNumbers;
    END IF;
    SET sum = sum + num;
  END WHILE sumNumbers;
  VALUES (sum);
END;

-- Compare with the much more efficient relational computation:
SELECT
  sum(num)
FROM
  range(1, 10) AS t (num)
WHERE
  num % 2 = 1;

-- COMMAND ----------

-- DBTITLE 1,CASE
-- a simple case statement
BEGIN
  DECLARE choice INT DEFAULT 3;
  DECLARE result STRING;
  CASE choice
    WHEN 1 THEN
      VALUES ('one fish');
    WHEN 2 THEN
      VALUES ('two fish');
    WHEN 3 THEN
      VALUES ('red fish');
    WHEN 4 THEN
      VALUES ('blue fish');
    ELSE
      VALUES ('no fish');
  END CASE;
END;

-- COMMAND ----------

-- DBTITLE 1,BEGIN END
-- A compound statement with local variables, and exit hanlder and a nested compound.
BEGIN
    DECLARE a INT DEFAULT 1;
    DECLARE b INT DEFAULT 5;
    DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO
      div0: BEGIN
        VALUES (15);
      END div0;
    SET a = 10;
    SET a = b / 0;
    VALUES (a);
END;


-- COMMAND ----------

-- DBTITLE 1,FOR
-- sum up all odd numbers from 1 through 10
BEGIN
    DECLARE sum INT DEFAULT 0;
    sumNumbers: FOR row AS SELECT num FROM range(1, 20) AS t(num) DO
      IF num > 10 THEN
         LEAVE sumNumbers;
      ELSEIF num % 2 = 0 THEN
        ITERATE sumNumbers;
      END IF;
      SET sum = sum + row.num;
    END FOR sumNumbers;
    VALUES (sum);
  END;


-- Compare with the much more efficient relational computation:
SELECT sum(num) FROM range(1, 10) AS t(num) WHERE num % 2 = 1;

-- COMMAND ----------

-- DBTITLE 1,GET DIAGNOSTICS
-- Retrieve the number of rows inserted by an INSERt statement
CREATE OR REPLACE TABLE emp(name STRING, salary DECIMAL(10, 2));

BEGIN
    DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO
      BEGIN
        DECLARE cond STRING;
        DECLARE message STRING;
        DECLARE state STRING;
        DECLARE args MAP<STRING, STRING>;
        DECLARE line BIGINT;
        DECLARE argstr STRING;
        DECLARE log STRING;
        GET DIAGNOSTICS CONDITION 1
           cond    = CONDITION_IDENTIFIER,
           message = MESSAGE_TEXT,
           state   = RETURNED_SQLSTATE,
           args    = MESSAGE_ARGUMENTS,
           line    = LINE_NUMBER;
        SET argstr =
          (SELECT aggregate(array_agg('Parm:' || key || ' Val: value '),
                            '', (acc, x)->(acc || ' ' || x))
             FROM explode(args) AS args(key, val));
        SET log = 'Condition: ' || cond ||
                  ' Message: ' || message ||
                  ' SQLSTATE: ' || state ||
                  ' Args: ' || argstr ||
                  ' Line: ' || line;
        VALUES (log);
      END;
    SELECT 10/0;
  END;

-- COMMAND ----------

-- DBTITLE 1,IF THEN ELSE statement
BEGIN
  DECLARE choice DOUBLE DEFAULT 3.9;
  DECLARE result STRING;
  IF choice < 2 THEN
    VALUES ('one fish');
  ELSEIF choice < 3 THEN
    VALUES ('two fish');
  ELSEIF choice < 4 THEN
    VALUES ('red fish');
  ELSEIF choice < 5 OR choice IS NULL THEN
    VALUES ('blue fish');
  ELSE
    VALUES ('no fish');
  END IF;
END;

-- COMMAND ----------

-- DBTITLE 1,ITERATE
-- sum up all odd numbers from 1 through 10
BEGIN
    DECLARE sum INT DEFAULT 0;
    DECLARE num INT DEFAULT 0;
    sumNumbers: LOOP
      SET num = num + 1;
      IF num > 10 THEN
        LEAVE sumNumbers;
      END IF;
      IF num % 2 = 0 THEN
        ITERATE sumNumbers;
      END IF;
      SET sum = sum + num;
    END LOOP sumNumbers;
    VALUES (sum);
  END;

-- COMMAND ----------

-- DBTITLE 1,LEAVE
-- sum up all odd numbers from 1 through 10
-- Iterate over even numbers and leave the loop after 10 has been reached.
BEGIN
    DECLARE sum INT DEFAULT 0;
    DECLARE num INT DEFAULT 0;
    sumNumbers: LOOP
      SET num = num + 1;
      IF num > 10 THEN
        LEAVE sumNumbers;
      END IF;
      IF num % 2 = 0 THEN
        ITERATE sumNumbers;
      END IF;
      SET sum = sum + num;
    END LOOP sumNumbers;
    VALUES (sum);
  END;

-- COMMAND ----------

-- DBTITLE 1,LOOP
-- sum up all odd numbers from 1 through 10
BEGIN
    DECLARE sum INT DEFAULT 0;
    DECLARE num INT DEFAULT 0;
    sumNumbers: LOOP
      SET num = num + 1;
      IF num > 10 THEN
        LEAVE sumNumbers;
      END IF;
      IF num % 2 = 0 THEN
        ITERATE sumNumbers;
      END IF;
      SET sum = sum + num;
    END LOOP sumNumbers;
    VALUES (sum);
  END;

-- COMMAND ----------

-- DBTITLE 1,REPEAT
-- sum up all odd numbers from 1 through 10
BEGIN
    DECLARE sum INT DEFAULT 0;
    DECLARE num INT DEFAULT 0;
    sumNumbers: REPEAT
      SET num = num + 1;
      IF num % 2 = 0 THEN
        ITERATE sumNumbers;
      END IF;
      SET sum = sum + num;
    UNTIL num = 10
    END REPEAT sumNumbers;
    VALUES (sum);
  END;

-- COMMAND ----------

-- DBTITLE 1,RESIGNAL
CREATE TABLE log(eventtime TIMESTAMP, log STRING);

BEGIN
    DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO
      BEGIN
        DECLARE cond STRING;
        DECLARE message STRING;
        DECLARE state STRING;
        DECLARE args MAP<STRING, STRING>;
        DECLARE line BIGINT;
        DECLARE argstr STRING;
        DECLARE log STRING;
        GET DIAGNOSTICS CONDITION 1
           cond = CONDITION_IDENTIFIER,
           message = MESSAGE_TEXT,
           state = RETURNED_SQLSTATE,
           args = MESSAGE_ARGUMENTS,
           line = LINE_NUMBER;
        SET argstr =
          (SELECT aggregate(array_agg('Parm:' || key || ' Val: value '),
                            '', (acc, x)->(acc || ' ' || x))
             FROM explode(args) AS args(key, val));
        SET log = 'Condition: ' || cond ||
                  ' Message: ' || message ||
                  ' SQLSTATE: ' || state ||
                  ' Args: ' || argstr ||
                  ' Line: ' || line;
        INSERT INTO log VALUES(current_timestamp(), log);
        RESIGNAL;
      END;
    SELECT 10/0;
END;

-- COMMAND ----------

-- DBTITLE 1,SIGNAL
DECLARE OR REPLACE input INT DEFAULT 5;

BEGIN
    DECLARE arg_map MAP<STRING, STRING>;
    IF input > 4 THEN
      SET arg_map = map('errorMessage',
                        'Input must be <= 4.');
      SIGNAL USER_RAISED_EXCEPTION
        SET MESSAGE_ARGUMENTS = arg_map;
    END IF;
  END;
