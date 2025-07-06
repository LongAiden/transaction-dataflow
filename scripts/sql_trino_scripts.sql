-- Create schema and tables in Trino
CREATE SCHEMA IF NOT EXISTS lakehouse.project
WITH (LOCATION = 's3://transaction-data-user/demographic');

CREATE TABLE IF NOT EXISTS lakehouse.project.customer (
   user_id VARCHAR,
   age INTEGER,
   gender VARCHAR,
   location VARCHAR,
   occupation VARCHAR,
   day_start DATE
) WITH (
 LOCATION = 's3://transaction-data-user/demographic'
);

CREATE TABLE IF NOT EXISTS lakehouse.project.features (
   user_id VARCHAR,
   num_transactions_l1w INTEGER,
   total_amount_l1w DOUBLE,
   avg_amount_l1w DOUBLE,
   min_amount_l1w DOUBLE,
   max_amount_l1w DOUBLE,
   num_vendors_l1w INTEGER,
   num_sources_l1w INTEGER
) WITH (
   LOCATION = 's3://transaction-data/features'
);

-- Select top n rows form Trino table
SELECT * FROM lakehouse.project.customer
LIMIT 10