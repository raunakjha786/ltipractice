--Q1
CREATE OR REPLACE DATABASE emp_db;
USE DATABASE emp_db;

CREATE OR REPLACE SCHEMA emp_schema;
USE SCHEMA emp_schema;


CREATE OR REPLACE TABLE employees (
    emp_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department VARCHAR(50),
    salary NUMBER(10,2),
    manager_id INT
);

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
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE;


  CREATE OR REPLACE STAGE ext_csv_stage
  URL = 's3://raunakjhaa/RAW/assignment/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = csv_format;


  LIST @ext_csv_stage;


  COPY INTO employees
FROM @ext_csv_stage
ON_ERROR = CONTINUE


select * from employees;



--Q2
CREATE OR REPLACE TABLE employees (
    emp_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department VARCHAR(50),
    salary NUMBER(10,2),
    manager_id INT
);


PUT file://C:/Users/User/Desktop/emp.txt @%employees AUTO_COMPRESS = TRUE;

List @%employees;


SELECT * FROM employees AT (OFFSET => -60);


COPY INTO employees
FROM @%employees
FILE_FORMAT = (
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
)
ON_ERROR = CONTINUE;


select * from employees



SELECT * FROM employees AT (OFFSET => -120);


--Q3
CREATE OR REPLACE STAGE emp_named_stage;

PUT file://C:/Users/User/Desktop/emp.txt @emp_named_stage AUTO_COMPRESS = TRUE;

LIST @emp_named_stage;




CREATE OR REPLACE TABLE employees (
    emp_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department VARCHAR(50),
    salary NUMBER(10,2),
    manager_id INT
);
 

COPY INTO employees
FROM @emp_named_stage
FILE_FORMAT = (
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
)
ON_ERROR = CONTINUE;

select * from employees;

-- now comes the sharing part

CREATE OR REPLACE SHARE emp_secure_share;

GRANT USAGE ON DATABASE emp_db TO SHARE emp_secure_share;


GRANT USAGE ON SCHEMA emp_db.emp_schema TO SHARE emp_secure_share;

GRANT SELECT ON TABLE emp_db.emp_schema.employees TO SHARE emp_secure_share;

ALTER SHARE emp_secure_share
ADD ACCOUNTS = GIXVIHS.HI59839;

SHOW SHARES;



--Q4
CREATE OR REPLACE FILE FORMAT emp_csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE;


CREATE OR REPLACE STAGE emp_s3_ext_stage
  URL = 's3://raunakjhaa/RAW/assignment/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = emp_csv_format;


  list @emp_s3_ext_stage;


CREATE OR REPLACE TABLE employees (
    emp_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department VARCHAR(50),
    salary NUMBER(10,2),
    manager_id INT
);

COPY INTO employees
FROM @emp_s3_ext_stage/emp.txt
ON_ERROR = CONTINUE;


select * from employees

COPY INTO @emp_s3_ext_stage/unload/
FROM employees
FILE_FORMAT = emp_csv_format
OVERWRITE = TRUE;


LIST @emp_s3_ext_stage/unload/;






--Q5(inclass)
CREATE OR REPLACE DATABASE emp_db;
USE DATABASE emp_db;

CREATE OR REPLACE SCHEMA emp_schema;
USE SCHEMA emp_schema;

CREATE OR REPLACE TABLE emp_table(
emp_id       INT,
first_name   VARCHAR(50),
last_name    VARCHAR(50),
department   VARCHAR(50),
salary       NUMBER(10,2),
manager_id   INT
);

CREATE OR REPLACE STAGE stage_nm;

list @stage_nm;

COPY INTO emp_table
FROM @stage_nm
FILE_FORMAT = (
TYPE = CSV
SKIP_HEADER = 1
FIELD_DELIMITER = ','
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
);

SELECT * FROM emp_table;

CREATE OR REPLACE STAGE stage_nm1;

list @stage_nm1;

COPY INTO @stage_nm1
FROM (SELECT * FROM emp_table)
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"')
OVERWRITE = TRUE;

list @stage_nm1;


>put file:///C:\Users\User\Documents\emp.txt @stage_nm;
get @stage_nm1 file://C:\Users\User\Documents\123;































  













