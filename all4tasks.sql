--create external stage
--create snowpipe
--row level and column level masking
--database schema table cloning
--Share the access of the table to on aws account

--external stage
use database day7
use schema day7schema

create or replace storage integration s3_int
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::498504718091:role/raunakroles'
  storage_allowed_locations = ('s3://raunakjhaa/RAW/assignment/');

  DESC INTEGRATION s3_int;
CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('NULL','null')
  EMPTY_FIELD_AS_NULL = TRUE;

   CREATE OR REPLACE STAGE ext_csv_stage
  URL = 's3://raunakjhaa/RAW/assignment/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = csv_format;
  
  list @ext_csv_stage

-----------
--now pipe
  
create or replace table customer_raw ( INDEX INT,
Customer_Id STRING,
First_Name STRING,
Last_Name STRING,
Company STRING,
City STRING,
Country STRING,
Phone_1 STRING,
Phone_2 STRING,
Email STRING,
Subscription_Date STRING,
Website STRING
)

CREATE OR REPLACE PIPE pipe_customer_raw
  AUTO_INGEST = TRUE
  AS
  COPY INTO customer_raw
  FROM @ext_csv_stage
  FILE_FORMAT = (FORMAT_NAME = csv_format)
  ON_ERROR = 'CONTINUE';

  DESC PIPE pipe_customer_raw;


LIST @ext_csv_stage;

SELECT * FROM customer_raw;


-- Create a masking policy (column masking)
CREATE OR REPLACE MASKING POLICY mask_email_policy 
AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','ANALYST') THEN val
    ELSE '***MASKED***'
  END;

-- Apply the policy to the Email column
ALTER TABLE customer_raw 
  MODIFY COLUMN Email 
  SET MASKING POLICY mask_email_policy;

show masking policies


USE ROLE ACCOUNTADMIN;
SELECT Email FROM customer_raw;

USE ROLE HR_ROLE
SELECT Email from customer_raw


---row level masking
CREATE OR REPLACE ROW ACCESS POLICY rap_region_policy
AS (region STRING) RETURNS BOOLEAN ->
  CASE
    WHEN CURRENT_ROLE() = 'REGION_EAST' THEN region = 'East'
    WHEN CURRENT_ROLE() = 'REGION_WEST' THEN region = 'West'
    WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN TRUE
    ELSE FALSE
  END;



--cloning
  CREATE OR REPLACE DATABASE mock_practice1_clone CLONE mock_practice1;
  CREATE OR REPLACE SCHEMA mp1schema_clone CLONE mp1schema;
  CREATE OR REPLACE TABLE employee_clone CLONE employee;