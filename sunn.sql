Q1------------------------------------------------------------------------------------------------------------------------------------------

--Question Requirements (Decoded)

--You must demonstrate:

--Create a Permanent Table

--Convert it into Temporary and Transient Table

--Show Data Retention Period for all tables

--Load data using Table Stage

--Implement Secure Share for the Permanent Table


CREATE OR REPLACE DATABASE milestone_db;
USE DATABASE milestone_db;

CREATE OR REPLACE SCHEMA milestone_schema;
USE SCHEMA milestone_schema;


CREATE OR REPLACE TABLE employee_perm (
    emp_id INT,
    emp_name STRING,
    department STRING,
    salary NUMBER(10,2)
);


CREATE OR REPLACE TEMPORARY TABLE employee_temp AS
SELECT * FROM employee_perm;


CREATE OR REPLACE TRANSIENT TABLE employee_transient AS
SELECT * FROM employee_perm;


SHOW TABLES IN SCHEMA milestone_schema;



PUT file://C:/Users/User/Desktop/EMP.txt @%employee_perm AUTO_COMPRESS=TRUE;



LIST @%employee_perm;



COPY INTO employee_perm
FROM @%employee_perm
FILE_FORMAT = (
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
)
ON_ERROR = CONTINUE;



SELECT * FROM employee_perm;






CREATE OR REPLACE SHARE employee_share;



GRANT USAGE ON DATABASE milestone_db TO SHARE employee_share;


GRANT USAGE ON SCHEMA milestone_db.milestone_schema TO SHARE employee_share;


GRANT SELECT ON TABLE milestone_db.milestone_schema.employee_perm
TO SHARE employee_share;


ALTER SHARE employee_share
ADD ACCOUNTS = ABC12345.AWS;


SHOW SHARES;
------------------------------------------------------------------------------------------------------


--Q2
--Load data from AWS S3

--Use External Stage

--Load data into a Snowflake table

--Create a Materialized View

--Validate data and MV output

--Show understanding of performance optimization



CREATE OR REPLACE DATABASE mv_db;
USE DATABASE mv_db;

CREATE OR REPLACE SCHEMA mv_schema;
USE SCHEMA mv_schema;


CREATE OR REPLACE TABLE employees (
    emp_id INT,
    first_name STRING,
    last_name STRING,
    department STRING,
    salary NUMBER(10,2),
    country STRING
);


CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://mybucket/empdata/');



DESC INTEGRATION s3_int;


CREATE OR REPLACE FILE FORMAT emp_csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('NULL','null');



CREATE OR REPLACE STAGE emp_ext_stage
  URL = 's3://mybucket/empdata/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = emp_csv_format;



LIST @emp_ext_stage;



COPY INTO employees
FROM @emp_ext_stage
ON_ERROR = CONTINUE;


SELECT * FROM employees;



CREATE OR REPLACE MATERIALIZED VIEW mv_dept_salary
AS
SELECT
    department,
    SUM(salary) AS total_salary,
    COUNT(*) AS emp_count
FROM employees
GROUP BY department;



SELECT * FROM mv_dept_salary;



SHOW MATERIALIZED VIEWS IN SCHEMA mv_schema;


-------------------------------------------------------------------------------------------------------------------------------

--Q3

--Load data from AWS S3

--Use External Stage

--Create Snowpipe

--Enable automatic ingestion

--Verify data load



CREATE OR REPLACE DATABASE snowpipe_db;
USE DATABASE snowpipe_db;

CREATE OR REPLACE SCHEMA snowpipe_schema;
USE SCHEMA snowpipe_schema;



CREATE OR REPLACE TABLE employees (
    emp_id INT,
    emp_name STRING,
    department STRING,
    salary NUMBER(10,2)
);


CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://mybucket/snowpipe/');


DESC INTEGRATION s3_int;


CREATE OR REPLACE FILE FORMAT emp_csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1;



CREATE OR REPLACE STAGE emp_ext_stage
  URL = 's3://mybucket/snowpipe/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = emp_csv_format;

LIST @emp_ext_stage;


CREATE OR REPLACE PIPE emp_pipe
AUTO_INGEST = TRUE
AS
COPY INTO employees
FROM @emp_ext_stage
ON_ERROR = CONTINUE;



DESC PIPE emp_pipe;



Upload EMP.txt to:

s3://mybucket/snowpipe/


SELECT * FROM employees;


SELECT *
FROM INFORMATION_SCHEMA.LOAD_HISTORY
WHERE PIPE_NAME = 'EMP_PIPE'
ORDER BY LAST_LOAD_TIME DESC;


-----------------------------------------------------------------------------------------

--Q4

--Load data from AWS S3

--Use External Stage

--Load data into a Snowflake table

--Create Roles

--Implement Row-Level Security using RBAC

--Prove access control using SQL




CREATE OR REPLACE DATABASE rbac_db;
USE DATABASE rbac_db;

CREATE OR REPLACE SCHEMA rbac_schema;
USE SCHEMA rbac_schema;



CREATE OR REPLACE TABLE employees (
    emp_id INT,
    emp_name STRING,
    department STRING,
    salary NUMBER(10,2),
    country STRING
);


CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://mybucket/rbac/');



DESC INTEGRATION s3_int;



CREATE OR REPLACE FILE FORMAT emp_csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1;




CREATE OR REPLACE STAGE emp_ext_stage
  URL = 's3://mybucket/rbac/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = emp_csv_format;


LIST @emp_ext_stage;


COPY INTO employees
FROM @emp_ext_stage
ON_ERROR = CONTINUE;


SELECT * FROM employees;


CREATE OR REPLACE ROLE india_role;
CREATE OR REPLACE ROLE us_role;


GRANT USAGE ON DATABASE rbac_db TO ROLE india_role;
GRANT USAGE ON SCHEMA rbac_db.rbac_schema TO ROLE india_role;
GRANT SELECT ON TABLE employees TO ROLE india_role;

GRANT USAGE ON DATABASE rbac_db TO ROLE us_role;
GRANT USAGE ON SCHEMA rbac_db.rbac_schema TO ROLE us_role;
GRANT SELECT ON TABLE employees TO ROLE us_role;


CREATE OR REPLACE ROW ACCESS POLICY country_policy
AS (country STRING)
RETURNS BOOLEAN ->
CASE
    WHEN CURRENT_ROLE() = 'INDIA_ROLE' AND country = 'India' THEN TRUE
    WHEN CURRENT_ROLE() = 'US_ROLE' AND country = 'USA' THEN TRUE
    ELSE FALSE
END;


ALTER TABLE employees
ADD ROW ACCESS POLICY country_policy
ON (country);


USE ROLE india_role;
SELECT * FROM employees;



USE ROLE us_role;
SELECT * FROM employees;

-------------------------------------------------------------------------------------------------------------------------------Finished--------------------------------------------------------------------------------------------------------------------