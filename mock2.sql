create or replace database mock2
use database mock2

create or replace schema mock2schema
use schema mock2schema

create or replace table shipping (ROW_ID int, Order_ID string, Order_Date string, Ship_Date string, Ship_Mode string,Customer_ID string,Customer_Name string,
Segment string, Postal_Code int, City string, State string, Country string, Region string,Market string,
Product_ID string, Category string, Sub_Category string, Product_Name string,
Sales string,Quantity string,Discount string,Profit string, Shipping_Cost string, Order_Priority string
)

create or replace storage integration s3_int
type = external_stage
storage_provider = s3
enabled = TRUE
storage_aws_role_arn = 'arn:aws:iam::498504718091:role/raunakroles'
storage_allowed_locations=('s3://raunakjhaa/RAW/assignment/');

desc integration s3_int



create or replace FILE FORMAT csv_format
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER =1
field_optionally_enclosed_by ='"'
  NULL_IF = ('NULL','null')
  EMPTY_FIELD_AS_NULL = TRUE;

  create or replace stage ext_csv_stage
  URL = 's3://raunakjhaa/RAW/assignment/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = csv_format;

  list @ext_csv_stage

  


create or replace pipe my_pipe 
auto_ingest=True
as 
copy into shipping from 
@ext_csv_stage
on_error=CONTINUE;

show pipes

select * from shipping
SELECT SYSTEM$PIPE_STATUS('my_pipe');

----------------------------------

create or replace table shipping (ROW_ID int, Order_ID string, Order_Date string, Ship_Date string, Ship_Mode string,Customer_ID string,Customer_Name string,
Segment string, Postal_Code int, City string, State string, Country string, Region string,Market string,
Product_ID string, Category string, Sub_Category string, Product_Name string,
Sales string,Quantity string,Discount string,Profit string, Shipping_Cost string, Order_Priority string
)
  COPY INTO shipping
FROM @ext_csv_stage
ON_ERROR = CONTINUE

select * from shipping
CREATE OR REPLACE SECURE VIEW sales_secure_vw AS
SELECT
    order_id,
    region,
    sales
FROM shipping;

select * from sales_secure_vw


create or replace share sales_share
GRANT USAGE ON DATABASE mock2 TO SHARE sales_share;
GRANT USAGE ON SCHEMA mock2.mock2schema TO SHARE sales_share;
GRANT SELECT ON VIEW mock2.mock2schema.sales_secure_vw TO SHARE sales_share

alter share sales_share
add accounts = GIXVIHS.HI59839;

show shares;

-----------------------------------------------------------------

select * from shipping

CREATE OR REPLACE DATABASE mock2_clone CLONE mock2;


CREATE OR REPLACE SCHEMA mock2schema_clone CLONE mock2schema;


CREATE OR REPLACE TABLE shipping_clone CLONE shipping;

-   DML operations

INSERT INTO shipping_clone 
VALUES (
  83686, -- ROW_ID
  'CA-2014-AB10015140-41954', -- Order_ID
  '2014-11-11 00:00:00', -- Order_Date
  '2014-11-13 00:00:00', -- Ship_Date
  'First Class', -- Ship_Mode
  'AB-100151402', -- Customer_ID
  'Raunak', -- Customer_Name
  'Consumer', -- Segment
  73120, -- Postal_Code
  'Oklahoma City', -- City
  'Oklahoma', -- State
  'United States', -- Country
  'Central US', -- Region
  'USCA', -- Market
  'TEC-PH-5816', -- Product_ID
  'Technology', -- Category
  'Phones', -- Sub_Category
  'Samsung Convoy 3', -- Product_Name
  '221.98', -- Sales
  '2', -- Quantity
  '0', -- Discount
  '62.1544', -- Profit
  '40.77', -- Shipping_Cost
  'High' -- Order_Priority
);

UPDATE shipping_clone
SET Customer_Name = 'Raunak Sharma'
WHERE Customer_ID = 'AB-100151402';

delete from shipping_clone
where row_id= 40098

select * from shipping_clone

select * from shipping


------------------------------------------

create or replace table shipping_clone1 (ROW_ID int, Order_ID string, Order_Date string, Ship_Date string, Ship_Mode string,Customer_ID string,Customer_Name string,
Segment string, Postal_Code int, City string, State string, Country string, Region string,Market string,
Product_ID string, Category string, Sub_Category string, Product_Name string,
Sales NUMBER(10,2),Quantity string,Discount string,Profit string, Shipping_Cost string, Order_Priority string
)

list @ext_csv_stage
  COPY INTO shipping_clone1
FROM @ext_csv_stage
ON_ERROR = CONTINUE

select * from shipping_clone1


create or replace role hr_role
create or replace role admin_role
create  or replace role sales_role



GRANT ROLE HR_ROLE TO USER RAUNAK108;
GRANT ROLE sales_ROLE TO USER RAUNAK108;
GRANT ROLE ADMIN_ROLE TO USER RAUNAK108;


GRANT USAGE ON DATABASE mock2_clone TO ROLE HR_ROLE;
GRANT USAGE ON DATABASE mock2_clone TO ROLE SALES_ROLE;
GRANT USAGE ON DATABASE mock2_clone TO ROLE ADMIN_ROLE;

GRANT USAGE ON SCHEMA mock2schema_clone TO ROLE HR_ROLE;
GRANT USAGE ON SCHEMA mock2schema_clone TO ROLE SALES_ROLE;
GRANT USAGE ON SCHEMA mock2schema_clone TO ROLE ADMIN_ROLE;

GRANT SELECT ON TABLE shipping_clone1 TO ROLE HR_ROLE;




GRANT SELECT ON TABLE shipping_clone1 TO ROLE SALES_ROLE;
GRANT SELECT ON TABLE shipping_clone1 TO ROLE ADMIN_ROLE;

select * from shipping_clone



CREATE OR REPLACE MASKING POLICY saless_mask AS (val NUMBER) 
RETURNS NUMBER ->
  CASE
    WHEN CURRENT_ROLE() IN ('HR_ROLE','ADMIN_ROLE') THEN val
    ELSE NULL
  END;

  ALTER TABLE shipping_clone1
MODIFY COLUMN SALES 
SET MASKING POLICY saless_mask;

select * from shipping_clone1

CREATE OR REPLACE MASKING POLICY salesss_mask AS (val STRING) 
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('HR_ROLE','ADMIN_ROLE') THEN val
    ELSE '**masked**'
  END;

  ALTER TABLE shipping_clone1
MODIFY COLUMN CUSTOMER_NAME
SET MASKING POLICY salesss_mask;

select * from shipping_clone1



CREATE OR REPLACE ROW ACCESS POLICY shipping_policy
AS (region STRING)
RETURNS BOOLEAN ->
  CASE
     WHEN CURRENT_ROLE() = 'ADMIN_ROLE' THEN TRUE
    WHEN CURRENT_ROLE() = 'HR_ROLE' AND region = 'Oceania' THEN TRUE
    WHEN CURRENT_ROLE() = 'SALES_ROLE' AND region = 'Central US' THEN TRUE
    ELSE FALSE
  END;
ALTER TABLE shipping_clone1
ADD ROW ACCESS POLICY shipping_policy
ON (region);

----------------------------------------------
STREAM+TASK
-- 1. Create a stream on the shipping table
CREATE OR REPLACE STREAM shipping_stream
ON TABLE shipping;

-- 2. Create a consumer table to capture changes
CREATE OR REPLACE TABLE shipping_consume (
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
);

-- 3. Create a task to move stream data into consumer table
CREATE OR REPLACE TASK shipping_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('shipping_stream')
AS
INSERT INTO shipping_consume
SELECT ROW_ID, Order_ID, Order_Date, Ship_Date, Ship_Mode,
       Customer_ID, Customer_Name, Segment, Postal_Code, City,
       State, Country, Region, Market, Product_ID, Category,
       Sub_Category, Product_Name, Sales, Quantity, Discount,
       Profit, Shipping_Cost, Order_Priority
FROM shipping_stream;

-- 4. Resume the task so it starts running
ALTER TASK shipping_task RESUME;

-- 5. Insert sample data into shipping table
INSERT INTO shipping VALUES (
    1, 'ORD1001', '2025-12-25', '2025-12-28', 'Standard Class',
    'CUST001', 'Alice Johnson', 'Consumer', 400001, 'Mumbai',
    'Maharashtra', 'India', 'South Asia', 'PROD001', 'Furniture',
    'Chairs', 'Ergonomic Chair', '250', '2', '0.1', '50',
    '30', 'High'
);

-- 6. Check the stream (shows captured changes)
SELECT * FROM shipping_stream;

-- 7. Check the consumer table (shows applied changes)
SELECT * FROM shipping_consume;



---TIMESTAMP
SELECT *
FROM shipping
AT (TIMESTAMP => '2025-12-25 10:00:00');

SELECT *
FROM shipping
AT (STATEMENT => '01a12345-0600-1234-0000-123456789abc');

SELECT *
FROM shipping
BEFORE (OFFSET => -5*60);
alter session set timezone='UTC';
select current_timestamp();


INSERT INTO shipping
SELECT *
FROM shipping
AT (TIMESTAMP => '2025-12-25 10:00:00');







  




  
  



