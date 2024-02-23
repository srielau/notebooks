-- Databricks notebook source
-- DBTITLE 1,QuickSort
SELECT array_sort(array(5, 2, 8, 1, 3),
                  (a, b) -> a - b);

-- COMMAND ----------

-- DBTITLE 1,Configurable Quicksort
DECLARE OR REPLACE VARIABLE sortorder = -1;

SELECT array_sort(array(5, 2, 8, 1, 3),
                  (a, b) -> (a - b) * sortorder);

-- COMMAND ----------

-- DBTITLE 1,Lateral correlation
SELECT *
  FROM VALUES(-1), (1) AS t(sortorder),
       LATERAL (SELECT array_sort(array(5, 2, 8, 1, 3),
                                  (a, b) -> (a - b) * sortorder));

-- COMMAND ----------

-- DBTITLE 1,Geospatial Maximum Distance Aggregator
SELECT reduce(array_agg(struct(x, y)),
              named_struct('x', null::integer, 'y', null::integer, 'len', null::integer),
              (acc, point) -> CASE WHEN acc.len IS NULL
                                     OR acc.len < point.x * point.x + point.y * point.y
                                   THEN named_struct('x', point.x, 'y', point.y,
                                                     'len', point.x * point.x + point.y * point.y)
                                   ELSE acc END,
              acc -> struct(acc.x, acc.y))
 FROM VALUES(1, 10), (2, -10), (-10, 3) AS points(x, y);

-- COMMAND ----------

-- DBTITLE 1,Array Aggregation Custom Reducer
CREATE OR REPLACE FUNCTION max_distance(a array<struct<x int, y int>>)
 RETURN reduce(a,
               named_struct('x', null::integer, 'y', null::integer, 'len', null::integer),
               (acc, point) -> CASE WHEN acc.len IS NULL
                                      OR acc.len < point.x * point.x + point.y * point.y
                                    THEN named_struct('x', point.x, 'y', point.y,
                                                      'len', point.x * point.x + point.y * point.y)
                                    ELSE acc END,
               acc -> struct(acc.x, acc.y));

SELECT max_distance(array_agg(struct(x, y)))
  FROM VALUES (1, 10), (2, -10), (-10, 3) AS points(x, y);
