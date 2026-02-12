-- 1. Create a Permanent Table
CREATE OR REPLACE TABLE shipping (
    ROW_ID INT,
    Order_ID STRING,
    Order_Date STRING,
    Ship_Date STRING,
    Ship_Mode STRING,
    Customer_ID STRING,
    Customer_Name STRING,
    Segment STRING,
    Postal_Code INT,
    City STRING,
    State STRING,
    Country STRING,
    Region STRING,
    Market STRING,
    Product_ID STRING,
    Category STRING,
    Sub_Category STRING,
    Product_Name STRING,
    Sales STRING,
    Quantity STRING,
    Discount STRING,
    Profit STRING,
    Shipping_Cost STRING,
    Order_Priority STRING
)
DATA_RETENTION_TIME_IN_DAYS = 1;  -- Permanent table default retention

-- 2. Create a Temporary Table (session-scoped, retention = 0)
CREATE OR REPLACE TEMP TABLE shipping_temp LIKE shipping
DATA_RETENTION_TIME_IN_DAYS = 0;

-- 3. Create a Transient Table (retention configurable, often 0 or 1)
CREATE OR REPLACE TRANSIENT TABLE shipping_transient LIKE shipping
DATA_RETENTION_TIME_IN_DAYS = 1;

-- 4. Show Data Retention Periods
SHOW TABLES LIKE 'shipping';
SHOW TABLES LIKE 'shipping_temp';
SHOW TABLES LIKE 'shipping_transient';

-- 5. Load Data using Table Stage
-- First, put a sample file into the table stage
-- (Assume shipping.csv exists locally)
PUT file://shipping.csv @%shipping;

-- Then load data from the stage into the permanent table
COPY INTO shipping
FROM @%shipping/shipping.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

-- 6. Implement Secure Share for the Permanent Table
-- Create a secure share
CREATE SHARE shipping_share SECURE;

-- Add the permanent table to the share
ALTER SHARE shipping_share ADD TABLE shipping;

-- Grant usage on database and schema to the share
GRANT USAGE ON DATABASE <your_database> TO SHARE shipping_share;
GRANT USAGE ON SCHEMA <your_schema> TO SHARE shipping_share;

-- Add a consumer account to the share
ALTER SHARE shipping_share ADD ACCOUNT = '<consumer_account_identifier>';






2nd
-- 1. Create a Snowflake table to hold shipping data
CREATE OR REPLACE TABLE shipping (
    ROW_ID INT,
    Order_ID STRING,
    Order_Date DATE,
    Ship_Date DATE,
    Ship_Mode STRING,
    Customer_ID STRING,
    Customer_Name STRING,
    Segment STRING,
    Postal_Code INT,
    City STRING,
    State STRING,
    Country STRING,
    Region STRING,
    Market STRING,
    Product_ID STRING,
    Category STRING,
    Sub_Category STRING,
    Product_Name STRING,
    Sales NUMBER(10,2),
    Quantity INT,
    Discount NUMBER(5,2),
    Profit NUMBER(10,2),
    Shipping_Cost NUMBER(10,2),
    Order_Priority STRING
);

-- 2. Create an External Stage pointing to AWS S3
CREATE OR REPLACE STAGE shipping_stage
URL = 's3://<your-bucket-name>/shipping_data/'
STORAGE_INTEGRATION = my_s3_integration
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

-- 3. Load data from S3 into the Snowflake table
COPY INTO shipping
FROM @shipping_stage
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

-- 4. Create a Materialized View for performance optimization
-- Example: aggregate sales by category
CREATE OR REPLACE MATERIALIZED VIEW mv_sales_by_category AS
SELECT Category, SUM(Sales) AS total_sales, SUM(Profit) AS total_profit
FROM shipping
GROUP BY Category;

-- 5. Validate data and MV output
-- Check base table
SELECT COUNT(*) AS total_rows, SUM(Sales) AS total_sales, SUM(Profit) AS total_profit
FROM shipping;

-- Check materialized view
SELECT * FROM mv_sales_by_category;

-- 6. Performance Optimization Notes
-- - Materialized views automatically maintain results, reducing query cost for repeated aggregations.
-- - External stage allows parallel bulk loading from S3, improving ingestion speed.
-- - COPY INTO with FILE_FORMAT ensures efficient parsing and avoids row-by-row inserts.
-- - Clustering keys or partitions can be added to the base table if queries filter heavily on columns like Region or Order_Date.
-- - Query caching + materialized views = faster response times for analytical queries.




3rd
-- 1. Create target table
CREATE OR REPLACE TABLE shipping (
    ROW_ID INT,
    Order_ID STRING,
    Order_Date DATE,
    Ship_Date DATE,
    Ship_Mode STRING,
    Customer_ID STRING,
    Customer_Name STRING,
    Segment STRING,
    Postal_Code INT,
    City STRING,
    State STRING,
    Country STRING,
    Region STRING,
    Market STRING,
    Product_ID STRING,
    Category STRING,
    Sub_Category STRING,
    Product_Name STRING,
    Sales NUMBER(10,2),
    Quantity INT,
    Discount NUMBER(5,2),
    Profit NUMBER(10,2),
    Shipping_Cost NUMBER(10,2),
    Order_Priority STRING
);

-- 2. Create External Stage pointing to AWS S3
CREATE OR REPLACE STAGE shipping_stage
URL = 's3://<your-bucket-name>/shipping_data/'
STORAGE_INTEGRATION = my_s3_integration
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

-- 3. Create Snowpipe to load data automatically from S3
CREATE OR REPLACE PIPE shipping_pipe
AUTO_INGEST = TRUE
AS
COPY INTO shipping
FROM @shipping_stage
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

-- 4. Enable automatic ingestion
-- This requires AWS S3 event notifications to be configured to publish to an SNS topic
-- and Snowflake to subscribe to that topic via STORAGE_INTEGRATION.
-- Example (outside Snowflake, in AWS):
--   Configure S3 bucket event -> SNS topic -> Snowflake notification channel

-- 5. Verify data load
-- Check pipe status
SHOW PIPES LIKE 'shipping_pipe';

-- View pipe load history
SELECT * FROM INFORMATION_SCHEMA.COPY_HISTORY
WHERE TABLE_NAME = 'SHIPPING'
ORDER BY START_TIME DESC;

-- Validate data in the table
SELECT COUNT(*) AS total_rows, SUM(Sales) AS total_sales, SUM(Profit) AS total_profit
FROM shipping;


q4

-- 1. Create target table
CREATE OR REPLACE TABLE shipping (
    ROW_ID INT,
    Order_ID STRING,
    Order_Date DATE,
    Ship_Date DATE,
    Ship_Mode STRING,
    Customer_ID STRING,
    Customer_Name STRING,
    Segment STRING,
    Postal_Code INT,
    City STRING,
    State STRING,
    Country STRING,
    Region STRING,
    Market STRING,
    Product_ID STRING,
    Category STRING,
    Sub_Category STRING,
    Product_Name STRING,
    Sales NUMBER(10,2),
    Quantity INT,
    Discount NUMBER(5,2),
    Profit NUMBER(10,2),
    Shipping_Cost NUMBER(10,2),
    Order_Priority STRING
);

-- 2. Create External Stage pointing to AWS S3
CREATE OR REPLACE STAGE shipping_stage
URL = 's3://<your-bucket-name>/shipping_data/'
STORAGE_INTEGRATION = my_s3_integration
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

-- 3. Load data from S3 into the Snowflake table
COPY INTO shipping
FROM @shipping_stage
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

-- 4. Create Roles for RBAC
CREATE ROLE analyst_us;
CREATE ROLE analyst_europe;

-- Grant usage on schema and table to roles
GRANT USAGE ON SCHEMA <your_schema> TO ROLE analyst_us;
GRANT USAGE ON SCHEMA <your_schema> TO ROLE analyst_europe;
GRANT SELECT ON TABLE shipping TO ROLE analyst_us;
GRANT SELECT ON TABLE shipping TO ROLE analyst_europe;

-- 5. Implement Row-Level Security using RBAC
-- Create a secure view that filters rows based on CURRENT_ROLE()
CREATE OR REPLACE SECURE VIEW shipping_rls AS
SELECT *
FROM shipping
WHERE (CURRENT_ROLE() = 'ANALYST_US' AND Region = 'US')
   OR (CURRENT_ROLE() = 'ANALYST_EUROPE' AND Region = 'Europe');

-- Grant access to the secure view
GRANT SELECT ON VIEW shipping_rls TO ROLE analyst_us;
GRANT SELECT ON VIEW shipping_rls TO ROLE analyst_europe;

-- 6. Prove access control using SQL
-- Switch role to US analyst
USE ROLE analyst_us;
SELECT DISTINCT Region, COUNT(*) AS row_count
FROM shipping_rls
GROUP BY Region;

-- Switch role to Europe analyst
USE ROLE analyst_europe;
SELECT DISTINCT Region, COUNT(*) AS row_count
FROM shipping_rls
GROUP BY Region;

-- Expected Results:
-- analyst_us role → only sees rows where Region = 'US'
-- analyst_europe role → only sees rows where Region = 'Europe'