create database ms2
use database ms2

create schema ms2schema
use schema ms2schema

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
select count(*) as my_count from shipping

SELECT SYSTEM$PIPE_STATUS('my_pipe');

--------------------------------------

create or replace table shipping1 (ROW_ID int, Order_ID string, Order_Date string, Ship_Date string, Ship_Mode string,Customer_ID string,Customer_Name string,
Segment string, Postal_Code int, City string, State string, Country string, Region string,Market string,
Product_ID string, Category string, Sub_Category string, Product_Name string,
Sales number(10,2),Quantity string,Discount string,Profit string, Shipping_Cost string, Order_Priority string
)

copy into shipping1 from 
@ext_csv_stage
on_error=CONTINUE;


create or replace role REGIONAL_ROLE;
create or replace role ADMIN_ROLE;
create  or replace role ANALYST_ROLE;



GRANT ROLE REGIONAL_ROLE TO USER RAUNAK108;
GRANT ROLE ANALYST_ROLE TO USER RAUNAK108;
GRANT ROLE ADMIN_ROLE TO USER RAUNAK108;


GRANT USAGE ON DATABASE MS2 TO ROLE REGIONAL_ROLE;
GRANT USAGE ON DATABASE MS2 TO ROLE ANALYST_ROLE;
GRANT USAGE ON DATABASE MS2 TO ROLE ADMIN_ROLE;

GRANT USAGE ON SCHEMA MS2SCHEMA TO ROLE REGIONAL_ROLE;
GRANT USAGE ON SCHEMA MS2SCHEMA TO ROLE ANALYST_ROLE;
GRANT USAGE ON SCHEMA MS2SCHEMA TO ROLE ADMIN_ROLE;

GRANT SELECT ON TABLE SHIPPING1 TO ROLE REGIONAL_ROLE;
GRANT SELECT ON TABLE shipping1 TO ROLE ANALYST_ROLE;
GRANT SELECT ON TABLE shipping1 TO ROLE ADMIN_ROLE;

CREATE OR REPLACE MASKING POLICY saless_mask AS (val NUMBER) 
RETURNS NUMBER ->
  CASE
    WHEN CURRENT_ROLE() IN ('REGIONAL_ROLE','ADMIN_ROLE') THEN val
    ELSE NULL
  END;

  ALTER TABLE shipping1
MODIFY COLUMN SALES 
SET MASKING POLICY saless_mask;

SELECT * FROM  SHIPPING1



CREATE OR REPLACE MASKING POLICY salessss_mask AS (val STRING) 
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ADMIN_ROLE') THEN val
    ELSE '**masked**'
  END;

  ALTER TABLE shipping1
MODIFY COLUMN CUSTOMER_NAME
SET MASKING POLICY salessss_mask;

use role regional_role
select * from shipping1


CREATE OR REPLACE ROW ACCESS POLICY shipping_policy
AS (region STRING)
RETURNS BOOLEAN ->
  CASE
     WHEN CURRENT_ROLE() = 'ADMIN_ROLE' THEN TRUE
    WHEN CURRENT_ROLE() = 'REGIONAL_ROLE' AND region = 'Oceania' THEN TRUE
    WHEN CURRENT_ROLE() = 'ANALYST_ROLE' AND region = 'Central US' THEN TRUE
    ELSE FALSE
  END;
ALTER TABLE shipping1
ADD ROW ACCESS POLICY shipping_policy
ON (region);

use role regional_role
select * from shipping1



-----------------------------------------------------------------------




create or replace table shipping2 (ROW_ID int, Order_ID string, Order_Date string, Ship_Date string, Ship_Mode string,Customer_ID string,Customer_Name string,
Segment string, Postal_Code int, City string, State string, Country string, Region string,Market string,
Product_ID string, Category string, Sub_Category string, Product_Name string,
Sales number(10,2),Quantity string,Discount string,Profit string, Shipping_Cost string, Order_Priority string
)


CREATE OR REPLACE STAGE shipping_user_stage;


list @shipping_user_stage

COPY INTO shipping2
FROM @shipping_user_stage
FILE_FORMAT = (
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    field_optionally_enclosed_by ='"'
  NULL_IF = ('NULL','null')
  EMPTY_FIELD_AS_NULL = TRUE
)
ON_ERROR = CONTINUE;


CREATE OR REPLACE MATERIALIZED VIEW my_view
AS
SELECT
    Customer_Name, Customer_ID, region, category
    SUM(SALES) AS total_sales
FROM shipping
GROUP BY CUSTOMER_NAME, CUSTOMER_ID, region, category


select * from shipping2



CREATE OR REPLACE STAGE shipping_s3_ext_stage1;
list @shipping_s3_ext_stage1;

COPY INTO @shipping_s3_ext_stage1
FROM (SELECT * FROM shipping2)
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"')
OVERWRITE = TRUE




CREATE OR REPLACE MATERIALIZED VIEW my_view
AS
SELECT
    Customer_Name, Customer_ID,
    SUM(Quantity) AS total_qty
FROM shipping2
GROUP BY CUSTOMER_NAME, CUSTOMER_ID;

select * from shipping2



select * FROM my_view;



------------------------------------------------











  

CREATE OR REPLACE TABLE employee_perm (
    EmpID INT,
    EmpName STRING,
    Department STRING,
    Salary NUMBER(10,2),
    JoinDate STRING
);


CREATE OR REPLACE TEMPORARY TABLE employee_temp AS
SELECT * FROM employee_perm;


CREATE OR REPLACE TRANSIENT TABLE employee_transient AS
SELECT * FROM employee_perm;


COPY INTO employee_perm
FROM @%employee_perm
FILE_FORMAT = (
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
)
ON_ERROR = CONTINUE
select * from employee_perm

SHOW TABLES IN SCHEMA ms2schema;



CREATE OR REPLACE SHARE employee_share;



GRANT USAGE ON DATABASE ms2 TO SHARE employee_share;
GRANT USAGE ON SCHEMA ms2.ms2schema TO SHARE employee_share;
GRANT SELECT ON TABLE ms2.ms2schema.employee_perm
TO SHARE employee_share;


ALTER SHARE employee_share
ADD ACCOUNTS = GIXVIHS.HI59839;

SHOW SHARES


SHOW SHARES;





