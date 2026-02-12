use database emp_db;
use schema emp_schema;

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

  list @emp_]s3_ext_stage

  COPY INTO employee
FROM @emp_s3_ext_stage/EMP.txt
ON_ERROR = CONTINUE;

select * from employee;


CREATE OR REPLACE STAGE emp_s3_ext_stage1;
list @emp_s3_ext_stage1;

COPY INTO @emp_s3_ext_stage1
FROM (SELECT * FROM employee)
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"')
OVERWRITE = TRUE;

list @emp_s3_ext_stage1


-------------------------------------
--Q2 

CREATE OR REPLACE TABLE EMP(
    empno NUMBER, 
    ename STRING, 
    job STRING, 
    mgr NUMBER, 
    hiredate DATE, 
    sal NUMBER(8,2), 
    comm NUMBER(8,2), 
    deptno NUMBER
);

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = CSV
    FIELD_DELIMITER =','
    SKIP_HEADER = 1
    TRIM_SPACE = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE;



    create or replace storage integration s3_int
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::498504718091:role/raunakroles'
  storage_allowed_locations = ('s3://raunakjhaa/RAW/assignment/');

  desc integration s3_int


  CREATE OR REPLACE STAGE ext_csv_stage
  URL = 's3://raunakjhaa/RAW/assignment/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = csv_format;
  
  list @ext_csv_stage


  COPY INTO EMP
FROM @ext_csv_stage/EMP.txt
FILE_FORMAT = CSV_FORMAT
ON_ERROR = CONTINUE;
-- VALIDATION = 'return_errors';



SELECT * FROM EMP;
INSERT INTO EMP VALUES 
(1234, 'Raunak', 'Jha', 7698, '1981-09-28', 1250.00, 1400.00, 30),
(5678, 'anei', 'ss', 7839, '1981-05-01', 2850.00, NULL, 30);


UPDATE EMP SET SAL = 25000.00 WHERE EMPNO = 5678;


DELETE FROM EMP WHERE EMPNO = 1234;

create or replace share myshare
grant usage on database emp_db to share myshare;
grant usage on schema emp_db.emp_schema to share myshare;
grant select on table emp_db.emp_schema.EMP to share myshare;

alter share myshare
add account = GIXVIHS.HI59839;

show shares






SELECT GET_DDL('DATABASE','DATABASE_NAME');



  










