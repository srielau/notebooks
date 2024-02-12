-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Building SQL with SQL: An introduction to EXECUTE IMMEDIATE
-- MAGIC
-- MAGIC In Databricks you have many means to compose and execute queries. You can:
-- MAGIC
-- MAGIC Incrementally build a query and execute it using the dataframe API
-- MAGIC Use Python, Scala, or some supported other language, to glue together a SQL string and use spark.sql() to compile and execute the SQL
-- MAGIC In a variation of the above, you can also protect against SQL injection by using spark.sql() to pass different values to a parameterized SQL statement string.
-- MAGIC All these solutions, however, require you to use a language outside of SQL to build the query.
-- MAGIC
-- MAGIC If you prefer Python or Scala that may be fine, but if you are into SQL you’re probably looking for a native solution. EXECUTE IMMEDIATE allows you to do just that.

-- COMMAND ----------

-- DBTITLE 1,Clean up from a previous run
USE CATALOG main;
USE SCHEMA srielau;

DROP TABLE IF EXISTS persons;
DROP TEMPORARY VARIABLE IF EXISTS tableId;
DROP TEMPORARY VARIABLE IF EXISTS pkColumns;
DROP TEMPORARY VARIABLE IF EXISTS queryStr;
DROP TEMPORARY VARIABLE IF EXISTS last;
DROP TEMPORARY VARIABLE IF EXISTS first;
DROP TEMPORARY VARIABLE IF EXISTS location;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##  Find duplicate primary key entries in a table
-- MAGIC
-- MAGIC Given a set of Delta tables in Unity Catalog with primary keys, and given only the name of the table as input: Generate and execute a query that returns all the duplicate keys.
-- MAGIC
-- MAGIC What do we need to pull this of?
-- MAGIC
-- MAGIC We need to run queries against the Information Schema to find the list of columns composing the primary key
-- MAGIC We need to collect the list of columns, so we can use it in a GROUP BY
-- MAGIC We need to compose a query that then groups by the key columns and selects only those with a count greater one.
-- MAGIC We finally need to execute this query.
-- MAGIC But can we do all this without leaving SQL?
-- MAGIC Using session variables as glue we certainly can!
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,CodeSnippetPrimaryKeyDuplicateChecking
-- Create and fill a sample table
CREATE TABLE persons(firstname STRING NOT NULL, lastname STRING NOT NULL, location STRING);
ALTER TABLE persons ADD CONSTRAINT persons_pk PRIMARY KEY (firstname, lastname);
INSERT INTO persons VALUES
  ('Tom'      , 'Sawyer'      , 'St. Petersburg'       ),
  ('Benjamin' , 'Bluemchen'   , 'Neustadt'             ),
  ('Benjamin' , 'Bluemchen'   , 'Neustaedter Zoo'      ),
  ('Emil'     , 'Svensson'    , 'Loenneberga'          ),
  ('Pippi'    , 'Longstocking', 'Villa Villekulla'     ),
  ('Pippi'    , 'Longstocking', 'Kurrekurredutt Island');

-- Declare a variable to gold the qualified table name 
DECLARE tableId STRUCT<catalog STRING, schema STRING, name STRING>;
SET VAR tableId = named_struct('catalog', current_catalog(),
                               'schema' , current_schema() ,
                               'name'   , 'persons'        );

-- A variable to hold the primary ley columns of the table
DECLARE pkColumns ARRAY<STRING>;

-- Compute the primary key columns from the information schema
SET VAR pkColumns =
  (SELECT array_agg(ccu.column_name)
     FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
     NATURAL JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS ccu
     WHERE tableId.catalog = tc.table_catalog
       AND tableId.schema = tc.table_schema
       AND tableId.name = tc.table_name
       AND tc.constraint_type = 'PRIMARY KEY'); 

-- Declare a variable to hold the query to analyze the primary key constraint.
DECLARE queryStr STRING;

-- Build the query string based on the table name and teh primary key columns.
SET VAR queryStr =
   'SELECT ' || aggregate(pkColumns, '', (list, col) -> list || col || ', ') || ' count(1) AS num_dups '
   '  FROM `' || tableId.catalog || '`. `' || tableId.schema || '`. `' || tableId.name || '` '
   '  GROUP BY ALL HAVING COUNT(1) > 1';

-- COMMAND ----------

-- DBTITLE 1,Print the query for educational purposes (and debugging)
SELECT queryStr;

-- COMMAND ----------

-- DBTITLE 1,Check that we have a primary key
SELECT CASE WHEN array_size(pkColumns) == 0
            THEN raise_error('No primary key found for: `' || tableId.catalog || '`.`' || tableId.schema || '`.`' || tableId.name || '`')
       END;

-- COMMAND ----------

-- DBTITLE 1,Find duplicte PRIMARTY KEYS now
EXECUTE IMMEDIATE queryStr;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # EXECUTE IMMEDIATE tear down
-- MAGIC
-- MAGIC Having hopefully sufficiently motivated the use of EXECUTE IMMEDIATE let's dive deeper into what it can do.
-- MAGIC
-- MAGIC The syntax is pretty straight forward:
-- MAGIC
-- MAGIC ```
-- MAGIC EXECUTE IMMEDIATE sql_string
-- MAGIC   [ INTO var_name [, ...] ]
-- MAGIC   [ USING { arg_expr [ AS ] [alias] } [, ...] ]
-- MAGIC ```

-- COMMAND ----------

-- DBTITLE 1,Other statements work with EXECUTE IMMEDIATE
SET VAR queryStr = 'INSERT INTO persons'
                   '  VALUES (\'Josefine\', \'Mausekind\', \'Sprotten vor dem Wind\')';
EXECUTE IMMEDIATE queryStr;

SET VAR queryStr = 'UPDATE persons SET location = \'Leuchtturm Josefine\''
                  ' WHERE firstname =\'Josefine\' AND lastname =\'Mausekind\'';
EXECUTE IMMEDIATE queryStr;

EXECUTE IMMEDIATE 'DELETE FROM persons WHERE location = \'Leuchtturm Josefine\'';

EXECUTE IMMEDIATE 'ALTER TABLE persons ADD COLUMN dob DATE';

-- COMMAND ----------

-- DBTITLE 1,Run GRANTs (fix principal)
--EXECUTE IMMEDIATE 'GRANT MODIFY ON TABLE persons TO `alf@melmak.et`';

-- COMMAND ----------

-- DBTITLE 1,Using unnamed parameter markers
EXECUTE IMMEDIATE 'SELECT * FROM persons WHERE firstname = ? AND lastname = ?'
  USING 'Tom', 'Sawyer';

-- COMMAND ----------

-- DBTITLE 1,Using named parameter markers
EXECUTE IMMEDIATE 'SELECT * FROM persons WHERE firstname = :first AND lastname = :last'
  USING 'Tom' AS first, 'Sawyer' AS last; 

-- COMMAND ----------

-- DBTITLE 1,Using named parameter markers
EXECUTE IMMEDIATE 'SELECT * FROM persons WHERE firstname = :first AND lastname = :last'
  USING 'Tom' AS first, 'Sawyer' AS last; 

-- COMMAND ----------

-- DBTITLE 1,Using unnamed parameter markers and session variables to bind query and values
SET VAR queryStr = 'SELECT * FROM persons WHERE firstname = ? AND lastname = ?';
DECLARE first = 'Tom';
DECLARE last = 'Sawyer';
EXECUTE IMMEDIATE queryStr USING first as first, last as last;

-- COMMAND ----------

-- DBTITLE 1,Using named parameter markers and session variables to bind query and values
SET VAR queryStr = 'SELECT * FROM persons WHERE firstname = :first AND lastname = :last';
EXECUTE IMMEDIATE queryStr USING last as last, first as first;

-- COMMAND ----------

-- DBTITLE 1,Using named parameter markers and session variables to bind query and values
SET VAR queryStr = 'SELECT * FROM persons WHERE firstname = :first AND lastname = :last';
EXECUTE IMMEDIATE queryStr USING last as last, first as first;

-- COMMAND ----------

-- DBTITLE 1,Using session variables inside the query string
-- Using session variables inside the query string
SET VAR queryStr = 'SELECT * FROM persons WHERE firstname = first AND lastname = last';
EXECUTE IMMEDIATE queryStr; 

-- COMMAND ----------

-- DBTITLE 1,EXECUTE IMMEDIATE returns the "right" result for teh statement type
-- Using an INSERT statement with unnamed parameters. No results are returned
EXECUTE IMMEDIATE 'INSERT INTO persons (firstname, lastname, location) VALUES (?, ?, ?)'
  USING 'Bibi', 'Blocksberg', 'Neustadt';

-- COMMAND ----------

-- DBTITLE 1,Single Row Query Results with INTO Clause
-- Using the INTO clause to return the results of a single row query 
DECLARE location STRING;
SET VAR first = 'Emil';
EXECUTE IMMEDIATE 'SELECT lastname, location FROM persons WHERE firstname = ?'
   INTO last, location
   USING first;
SELECT last, location;

-- COMMAND ----------

-- DBTITLE 1,NULL on empty semantic
EXECUTE IMMEDIATE 'SELECT lastname, location FROM persons WHERE firstname = ?'
   INTO last, location
   USING 'Huckleberry';

SELECT last, location;

-- COMMAND ----------

-- DBTITLE 1,To use INTO a query must not return more than one row
EXECUTE IMMEDIATE 'SELECT lastname, location FROM persons WHERE firstname = ?'
   INTO last, location
   USING 'Benjamin';
