create database mock_practice1
use database mock_practice1

create schema mp1schema
use schema mp1schema

---external stage creation-----------
create or replace table employee (
empno int,
ename varchar(256),
job varchar(256),
mgr varchar(256),
hiredate varchar(256),
sal number(10,2),
comm number(10,2) null,
deptno int
);

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

  LIST @EXT_CSV_STAGE




  
  ------------pipe ---------------

create or replace pipe my_pipe 
auto_ingest=True
as 
copy into employee from 
@ext_csv_stage
on_error=CONTINUE;

show pipes

select * from employee
SELECT SYSTEM$PIPE_STATUS('my_pipe');


-----------------masking--------------------------------------
create or replace role hr_role
create or replace role admin_role
create  or replace role sales_role



GRANT ROLE HR_ROLE TO USER RAUNAK108;
GRANT ROLE sales_ROLE TO USER RAUNAK108;
GRANT ROLE ADMIN_ROLE TO USER RAUNAK108;

-- Grant usage on database and schema
GRANT USAGE ON DATABASE mock_practice1 TO ROLE HR_ROLE;
GRANT USAGE ON DATABASE mock_practice1 TO ROLE SALES_ROLE;
GRANT USAGE ON DATABASE mock_practice1 TO ROLE ADMIN_ROLE;

GRANT USAGE ON SCHEMA mp1schema TO ROLE HR_ROLE;
GRANT USAGE ON SCHEMA mp1schema TO ROLE SALES_ROLE;
GRANT USAGE ON SCHEMA mp1schema TO ROLE ADMIN_ROLE;

-- Grant select on employee table
GRANT SELECT ON TABLE employee TO ROLE HR_ROLE;
GRANT SELECT ON TABLE employee TO ROLE SALES_ROLE;
GRANT SELECT ON TABLE employee TO ROLE ADMIN_ROLE;

----to unset masking policy------
ALTER TABLE employee 
MODIFY COLUMN sal 
UNSET MASKING POLICY;


-- Create masking policy for salary
CREATE OR REPLACE MASKING POLICY sal_mask AS (val NUMBER) 
RETURNS NUMBER ->
  CASE
    WHEN CURRENT_ROLE() IN ('HR_ROLE','ADMIN_ROLE') THEN val
    ELSE NULL
  END;

-- Apply masking policy to the column
ALTER TABLE employee 
MODIFY COLUMN sal 
SET MASKING POLICY sal_mask;

show masking policies

select * from employee


USE ROLE hr_role
use role sales_role

-- Create row access policy
CREATE OR REPLACE ROW ACCESS POLICY dept_policy
AS (emp_deptno int) RETURNS BOOLEAN ->
  CASE
    WHEN CURRENT_ROLE() = 'SALES_ROLE' AND emp_deptno = 30 THEN TRUE
    WHEN CURRENT_ROLE() = 'HR_ROLE' AND emp_deptno = 10 THEN TRUE
    WHEN CURRENT_ROLE() = 'ADMIN_ROLE' THEN TRUE
    ELSE FALSE
  END;

-- Apply policy to the table
ALTER TABLE employee 
ADD ROW ACCESS POLICY dept_policy ON (deptno);

---cloning----

select * from employee
CREATE OR REPLACE TABLE employee_clone CLONE employee;
select * from employee_clone
CREATE OR REPLACE SCHEMA mp1schema_clone CLONE mp1schema;
CREATE OR REPLACE TABLE employee_clone CLONE employee;

----sharing---
CREATE or replace SHARE employee_share;


GRANT USAGE ON DATABASE mock_practice1 TO SHARE employee_share;
GRANT USAGE ON SCHEMA mp1schema TO SHARE employee_share;
GRANT SELECT ON TABLE employee TO SHARE employee_share;

ALTER SHARE employee_share ADD ACCOUNTS = GIXVIHS.HI59839;

SHOW SHARES


-----------------------NOW 2ND QUESTION ----------------------------------------

use database mock_practice1
use schema mp1schema

---named stage data loading
create or replace table employee (
empno int,
ename varchar(256),
job varchar(256),
mgr varchar(256),
hiredate varchar(256),
sal number(10,2),
comm number(10,2) null,
deptno int
);


create or replace stage employee_named_stage;

list @employee_named_stage


COPY INTO employee
FROM @employee_named_stage
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  field_optionally_enclosed_by='"'
  SKIP_HEADER = 1
)
validation_mode = 'return_errors';

select * from employee

----- awss3 data loading external stage ------

create or replace storage integration s3_int
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::498504718091:role/raunakroles'
  storage_allowed_locations = ('s3://raunakjhaa/RAW/assignment/');

   desc integration s3_int

CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  field_optionally_enclosed_by='"'
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE;


   CREATE OR REPLACE STAGE ext_csv_stage
  URL = 's3://raunakjhaa/RAW/assignment/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = csv_format;

  LIST @ext_csv_stage;

  COPY INTO employee
FROM @ext_csv_stage
ON_ERROR = CONTINUE
  
select * from employee

-----time travel ------
INSERT INTO employee VALUES
(201, 'Raunak Bhai', 'Manager', '100', '2025-12-25', 75000, NULL, 10);
UPDATE employee SET sal = sal + 1000 WHERE empno = 301;
DELETE FROM employee WHERE empno = 7369;

SELECT * FROM employee BEFORE (OFFSET => -60*2);


---- STREAM+TASK --------


CREATE OR REPLACE STREAM employee_stream 
ON TABLE employee;


CREATE OR REPLACE TABLE employee_consume (
  empno INT,
  ename VARCHAR(256),
  job VARCHAR(256),
  mgr VARCHAR(256),
  hiredate VARCHAR(256),
  sal NUMBER(10,2),
  comm NUMBER(10,2),
  deptno INT
);

CREATE OR REPLACE TASK employee_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('employee_stream')
AS
INSERT INTO employee_consume
SELECT empno, ename, job, mgr, hiredate, sal, comm, deptno
FROM employee_stream;

ALTER TASK employee_task RESUME;

INSERT INTO employee VALUES (401, 'Bob', 'Engineer', '300', '2025-12-25', 60000, NULL, 40);
a

SELECT * FROM employee_stream;
SELECT * FROM employee_consume;

CREATE OR REPLACE TABLE EMPLOYEE_CLONE CLONE EMPLOYEE

SELECT * FROM EMPLOYEE_CLONE
----------------------------------------Finished------------------------------------------------------------------------------









CREATE OR REPLACE SECURE VIEW sales_secure_vw AS
SELECT
    order_id,
    region,
    revenue
FROM sales_data;
CREATE OR REPLACE SHARE sales_share;


GRANT USAGE ON DATABASE my_db TO SHARE sales_share;
GRANT USAGE ON SCHEMA my_db.public TO SHARE sales_share;
GRANT SELECT ON VIEW my_db.public.sales_secure_vw TO SHARE sales_share




CREATE OR REPLACE TABLE shipping_perm (
    ROW_ID INT,
    Order_ID STRING,
    Customer_Name STRING,
    Sales NUMBER(10,2)
);

--MS2

-- Temporary table
CREATE OR REPLACE TEMPORARY TABLE shipping_temp AS
SELECT * FROM shipping_perm;

-- Transient table
CREATE OR REPLACE TRANSIENT TABLE shipping_transient AS
SELECT * FROM shipping_perm;

SHOW TABLES LIKE 'shipping%';


SELECT table_name, retention_time
FROM information_schema.tables
WHERE table_name LIKE 'SHIPPING%';


-- Put a file into the table stage
PUT file://orders.csv @%shipping_perm;

-- Load data from table stage into permanent table
COPY INTO shipping_perm
FROM @%shipping_perm/orders.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

-- Create a share
CREATE OR REPLACE SHARE shipping_share;

-- Grant usage on database and schema
GRANT USAGE ON DATABASE mydb TO SHARE shipping_share;
GRANT USAGE ON SCHEMA mydb.public TO SHARE shipping_share;

-- Grant select on permanent table
GRANT SELECT ON TABLE shipping_perm TO SHARE shipping_share;

-- Add consumer account to the share

ALTER SHARE shipping_share ADD ACCOUNT = '<consumer_account_name>';

CREATE DATABASE shipping_db FROM SHARE <provider_account>.shipping_share;


