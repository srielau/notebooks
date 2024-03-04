-- Databricks notebook source
-- DBTITLE 1,Some sample Data
CREATE OR REPLACE TABLE residents(first_name STRING, last_name STRING, address STRUCT<street STRING, city STRING, zip INT>);
INSERT INTO residents VALUES 
  ('Jason', 'Jones'  , struct('100 Jewel St.' , 'Jasper'        , 12345)),
  ('Jane' ,  'Jones' , struct('12 Jello Ct.'  , 'Jericho'       , 54321)),
  ('Frank', 'Francis', struct('Wormser Str. 4', 'Kaiserslautern', 67657));


-- COMMAND ----------

-- DBTITLE 1,A regular "hardcoded" query
SELECT first_name FROM residents WHERE last_name = 'Jones';

-- COMMAND ----------

-- DBTITLE 1,Templated query using parameter marker
-- MAGIC %python
-- MAGIC spark.sql("SELECT first_name, last_name, address.city"
-- MAGIC           "  FROM residents WHERE last_name = ?", args = [ "Jones" ] ).show()

-- COMMAND ----------

-- DBTITLE 1,Query using a variable
DECLARE OR REPLACE last_name = 'Jones';
SELECT first_name FROM residents WHERE last_name = session.last_name;

-- COMMAND ----------

-- DBTITLE 1,Dynamic Resident Query Builder
SET VAR last_name = 'Jones';
DECLARE OR REPLACE first_name STRING;

DECLARE OR REPLACE stmt_head STRING DEFAULT 'SELECT address.city FROM residents ';
DECLARE OR REPLACE stmt STRING;
SET VAR stmt = stmt_head || CASE WHEN  last_name IS NOT NULL AND first_name IS NOT NULL 
                                  THEN 'WHERE last_name = session.last_name AND first_name = session.first_name'
                                 WHEN last_name IS NOT NULL
                                  THEN 'WHERE last_name = session.last_name'
                                 WHEN first_name IS NOT NULL
                                  THEN 'WHERE first_name = session.first_name'
                            ELSE '' END;
SELECT stmt;


-- COMMAND ----------

-- DBTITLE 1,Run the statement (DBR 14.3 and later)
EXECUTE IMMEDIATE stmt;

-- COMMAND ----------

-- DBTITLE 1,Templated table name
DECLARE OR REPLACE my_table = 'residents';
SELECT first_name FROM IDENTIFIER(my_table);


-- COMMAND ----------

-- DBTITLE 1,Tenmplated table name using a constant expression
SET VAR my_table = 'dents';
SELECT first_name FROM IDENTIFIER('`resi' || my_table || '`');

-- COMMAND ----------

-- DBTITLE 1,Templated schema qualifier
DECLARE OR REPLACE my_schema = current_schema();
SELECT first_name FROM IDENTIFIER(my_schema || '.residents');

-- COMMAND ----------

-- DBTITLE 1,Tenplated schema qualifier and table name
SET VAR my_table = 'residents';
SELECT first_name FROM IDENTIFIER(my_schema || '.' || my_table);

-- COMMAND ----------

-- DBTITLE 1,Templated column names
DECLARE OR REPLACE col_name = 'first_name';
SELECT IDENTIFIER(col_name) FROM residents WHERE IDENTIFIER(col_name) LIKE 'F%';

-- COMMAND ----------

-- DBTITLE 1,Termplated field name
DECLARE OR REPLACE field_name = 'street';
SELECT IDENTIFIER('address. ' || field_name) FROM residents; 

-- COMMAND ----------

-- DBTITLE 1,Templated function
DECLARE OR REPLACE agg_name = 'min';
SELECT IDENTIFIER(agg_name)(first_name) FROM residents;


-- COMMAND ----------

-- DBTITLE 1,Dynamic Table Creation Script
DECLARE OR REPLACE tab_name = 'tmp_' || translate(uuid(), '-', '_');
CREATE OR REPLACE TABLE IDENTIFIER(tab_name)(c1 INT);
INSERT INTO IDENTIFIER(tab_name) VALUES(1);
SELECT * FROM IDENTIFIER(tab_name);


-- COMMAND ----------

-- DBTITLE 1,Templated DROP statement

DROP TABLE IDENTIFIER(tab_name);   
