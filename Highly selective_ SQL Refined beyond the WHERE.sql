-- Databricks notebook source
-- DBTITLE 1,Creating some sample data
USE SCHEMA srielau;
DROP TABLE IF EXISTS products;
CREATE TABLE products(name STRING, category STRING, price INTEGER, units INTEGER, consumable BOOLEAN, description STRING);
INSERT INTO products VALUES
('colander'                , 'housewares'    ,  10,  1, false, 'Bowl with large holes to drain water from past and vegetables'),
('coffee filter'           , 'housewares'    ,   4, 20, true,  'Holds ground coffee'                                          ),
('skimmer'                 , 'pool supplies' , 100,  1, false, 'Drain filtering leaves from the water surface'                ),
('diatomaceous earth'      , 'pool supplies' ,  30,  0, true,  'Coats pool filter to filter impurities from water'            ),
('strainer'                , 'housewares'    ,  15,  1, false, 'Bowl with a handle and fine holes'                            ),
('tea infuser'             , 'housewares'    ,   5,  3, false, 'Holds loose tea leaves for infusion'                          ),
('electrostatic air filter', 'HVAC hardware' ,  10,  1, false, 'Air filter for HVAC systems'                                  );

-- COMMAND ----------

-- DBTITLE 1,A naive query using WHERE
SELECT name FROM products WHERE category = 'pool supplies';

-- COMMAND ----------

-- DBTITLE 1,The WHERE clause protects against the division by zero in the later SELECT list
SELECT name, price / units AS unitprice  FROM products WHERE category = 'pool supplies' AND units != 0;

-- COMMAND ----------

-- DBTITLE 1,No Lateral Column Aliasing in the WHERE clause
SELECT *, price / nullif(units, 0)  AS unitprice  FROM products WHERE unitprice IS NULL;

-- COMMAND ----------

-- DBTITLE 1,INNER JOINs also filter rows
SELECT name, price * number AS total
  FROM products
  NATURAL JOIN VALUES('skimmer' , 1),
                     ('colander', 2) AS orders(name, number)
WHERE category = 'housewares';

-- COMMAND ----------

-- DBTITLE 1,You. cannot rely on the ON clause executing before thw WHERE clause
SET spark.sql.ansi.enabled=true;
SELECT *
  FROM products
  NATURAL JOIN VALUES('skimmer' , 1),
                     ('colander', 2) AS orders(name, number)
WHERE price / units > 1;

-- COMMAND ----------

-- DBTITLE 1,The order execution in a WHERE clause is indetermined - Beware!
SET spark.sql.ansi.enabled=true;
SELECT *
  FROM products
  NATURAL JOIN VALUES('skimmer' , 1),
                     ('colander', 2) AS orders(name, number)
WHERE units != 0 AND price / units > 1;

-- COMMAND ----------

-- DBTITLE 1,Filtering out groups by nesting queries.
SELECT *
  FROM (SELECT category, count(*) AS num_items FROM products GROUP BY ALL)
  WHERE num_items > 1;

-- COMMAND ----------

-- DBTITLE 1,Filtering out groups using HAVING
SELECT category, count(*) AS num_items FROM products GROUP BY ALL HAVING num_items > 1;

-- COMMAND ----------

-- DBTITLE 1,A naive GROUP BY
SELECT category, count(*) AS num_items FROM products GROUP BY ALL;

-- COMMAND ----------

-- DBTITLE 1,Grouping by a sub group results in more rows.
SELECT category, consumable, count(*) AS num_items FROM products GROUP BY ALL;

-- COMMAND ----------

-- DBTITLE 1,Use CASE expressions you can subgroup using extra columns, not rows.
SELECT category,
       sum(CASE WHEN consumable  THEN 1 ELSE 0 END) AS consumable,
       sum(CASE WHEN !consumable THEN 1 ELSE 0 END) AS non_consumable
  FROM products
  GROUP BY ALL;

-- COMMAND ----------

-- DBTITLE 1,A count_if() is denser than a CASE expression
SELECT category,
       count_if(consumable) AS consumable,
       count_if(!consumable) AS non_consumable
  FROM products
  GROUP BY ALL;

-- COMMAND ----------

-- DBTITLE 1,But there is no sum_if()
SELECT category,
       count_if(consumable) AS consumable,
       sum(CASE WHEN consumable  THEN price ELSE 0 END) AS consumable_price,
       count_if(!consumable) AS noon_consumable,
       sum(CASE WHEN !consumable THEN price ELSE 0 END) AS non_consumable_price
  FROM products
  GROUP BY ALL;

-- COMMAND ----------

-- DBTITLE 1,FILTER provides a generic solution
SELECT category,
       count(1) FILTER(WHERE consumable) AS consumable,
       sum(price) FILTER(WHERE consumable)AS consumable_price,
       count(1) FILTER(WHERE !consumable) AS noon_consumable,
       sum(price) FILTER(WHERE consumable) AS non_consumable_price
  FROM products
  GROUP BY ALL;

-- COMMAND ----------

-- DBTITLE 1,Top Priced Product Query
SELECT * FROM products ORDER BY price DESC LIMIT 1;

-- COMMAND ----------

-- DBTITLE 1,Product Category Price Leaders - the hard way
SELECT p.* FROM products AS p
 WHERE EXISTS (SELECT 1 
                 FROM products m
                WHERE m.category = p.category
                HAVING max(m.price) = p.price);

-- COMMAND ----------

-- DBTITLE 1,Product Category Price Leaders - without the join.
SELECT max_by(name, price) AS name,
       category,
       max(price) AS price,
       max_by(units, price) AS units,
       max_by(consumable, price) AS consumable,
       max_by(description, price) as description
  FROM products
  GROUP BY ALL;

-- COMMAND ----------

-- DBTITLE 1,Product Category Price Leaders - without the join, using window function
SELECT * EXCEPT(rank)
  FROM (SELECT row_number() OVER (PARTITION BY category ORDER BY price DESC) AS rank,
               *
          FROM products)
  WHERE rank = 1;


-- COMMAND ----------

-- DBTITLE 1,roduct Category Price Leaders - using QUALIFY
SELECT * FROM products QUALIFY row_number() OVER(PARTITION BY category ORDER BY price DESC) = 1
