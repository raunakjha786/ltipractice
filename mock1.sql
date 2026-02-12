create or replace database mock1
use database mock1

create or replace schema mock1schema
use schema mock1schema


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

  
  COPY INTO shipping
FROM @ext_csv_stage
ON_ERROR = CONTINUE

CREATE OR REPLACE MATERIALIZED VIEW my_view
AS
SELECT
    Customer_Name, Customer_ID,
    SUM(Quantity) AS total_qty
FROM shipping
GROUP BY CUSTOMER_NAME, CUSTOMER_ID;

select * from my_view


create or replace role hr_role
create or replace role admin_role
create or replace role sales_role
;


GRANT ROLE HR_ROLE TO USER RAUNAK108;
GRANT ROLE sales_ROLE TO USER RAUNAK108;
GRANT ROLE ADMIN_ROLE TO USER RAUNAK108;

-- Grant usage on database and schema
GRANT USAGE ON DATABASE mock1 TO ROLE HR_ROLE;
GRANT USAGE ON DATABASE mock1 TO ROLE SALES_ROLE;
GRANT USAGE ON DATABASE mock1 TO ROLE ADMIN_ROLE;

GRANT USAGE ON SCHEMA mock1schema TO ROLE HR_ROLE;
GRANT USAGE ON SCHEMA mock1schema TO ROLE SALES_ROLE;
GRANT USAGE ON SCHEMA mock1schema TO ROLE ADMIN_ROLE;

-- Grant select on employee table
GRANT SELECT ON TABLE shipping1 TO ROLE HR_ROLE;
GRANT SELECT ON TABLE shipping1 TO ROLE SALES_ROLE;
GRANT SELECT ON TABLE shipping1 TO ROLE ADMIN_ROLE;


CREATE OR REPLACE ROW ACCESS POLICY shipping_policy
AS (region STRING)
RETURNS BOOLEAN ->
  CASE
     WHEN CURRENT_ROLE() = 'ADMIN_ROLE' THEN TRUE
    WHEN CURRENT_ROLE() = 'HR_ROLE' AND region = 'Oceania' THEN TRUE
    WHEN CURRENT_ROLE() = 'SALES_ROLE' AND region = 'Central US' THEN TRUE
    ELSE FALSE
  END;
ALTER TABLE shipping1
ADD ROW ACCESS POLICY shipping_policy
ON (region);

use role sales_role
select * from shipping1

use role hr_role
select * from shipping1







--------------------------
--stream + task
create or replace table my_orders(OrderID string,
CustomerID string,Region string,OrderStatus string,Sales string,
LastUpdated string)


CREATE OR REPLACE STREAM shipping_stream 
ON TABLE my_orders;

CREATE OR REPLACE TABLE myorder_audit (
OrderID string,
CustomerID string,Region string,OrderStatus string,Sales string,
LastUpdated string
 
);


CREATE OR REPLACE TASK shipping_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('shipping_stream')
AS
INSERT INTO myorder_audit
SELECT OrderID, CustomerID,Region,OrderStatus,Sales,
LastUpdated
FROM shipping_stream;

select * from my_orders
select * from myorder_audit
select * from shipping_stream
INSERT INTO myorder_audit VALUES
('ORD-1029','CUST-029','East','Shipped','310','2024-01-29'
);

select * from shipping_stream
ALTER TASK shipping_task RESUME;

--------------------------------------
create or replace table shipping1 (ROW_ID int, Order_ID string, Order_Date string, Ship_Date string, Ship_Mode string,Customer_ID string,Customer_Name string,
Segment string, Postal_Code int, City string, State string, Country string, Region string,Market string,
Product_ID string, Category string, Sub_Category string, Product_Name string,
Sales string,Quantity string,Discount string,Profit string, Shipping_Cost string, Order_Priority string

)

CREATE OR REPLACE STAGE shipping_named_stage;


list @shipping_named_stage

COPY INTO shipping1
FROM @shipping_named_stage
FILE_FORMAT = (
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    field_optionally_enclosed_by ='"'
  NULL_IF = ('NULL','null')
  EMPTY_FIELD_AS_NULL = TRUE
)
ON_ERROR = CONTINUE;


select * from shipping1

CREATE OR REPLACE STAGE shipping_s3_ext_stage1;
list @shipping_s3_ext_stage1;

COPY INTO @shipping_s3_ext_stage1
FROM (SELECT * FROM shipping1)
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"')
OVERWRITE = TRUE


select count(*) as toal_count from shipping1;










