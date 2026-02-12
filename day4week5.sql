create database raunakbucket
use database raunakbucket
create schema bucket
use schema bucket

CREATE or replace TRANSIENT TABLE HEALTHCARE_CSV(
    AVERAGE_COVERED_CHARGES    NUMBER(38,6)  
   ,AVERAGE_TOTAL_PAYMENTS    NUMBER(38,6)  
   ,TOTAL_DISCHARGES    NUMBER(38,0)  
   ,BACHELORORHIGHER    NUMBER(38,1)  
   ,HSGRADORHIGHER    NUMBER(38,1)  
   ,TOTALPAYMENTS    VARCHAR(128)  
   ,REIMBURSEMENT    VARCHAR(128)  
   ,TOTAL_COVERED_CHARGES    VARCHAR(128) 
   ,REFERRALREGION_PROVIDER_NAME    VARCHAR(256)  
   ,REIMBURSEMENTPERCENTAGE    NUMBER(38,9)  
   ,DRG_DEFINITION    VARCHAR(256)  
   ,REFERRAL_REGION    VARCHAR(26)  
   ,INCOME_PER_CAPITA    NUMBER(38,0)  
   ,MEDIAN_EARNINGSBACHELORS    NUMBER(38,0)  
   ,MEDIAN_EARNINGS_GRADUATE    NUMBER(38,0)  
   ,MEDIAN_EARNINGS_HS_GRAD    NUMBER(38,0)  
   ,MEDIAN_EARNINGSLESS_THAN_HS    NUMBER(38,0)  
   ,MEDIAN_FAMILY_INCOME    NUMBER(38,0)  
   ,NUMBER_OF_RECORDS    NUMBER(38,0)  
   ,POP_25_OVER    NUMBER(38,0)  
   ,PROVIDER_CITY    VARCHAR(128)  
   ,PROVIDER_ID    NUMBER(38,0)  
   ,PROVIDER_NAME    VARCHAR(256)  
   ,PROVIDER_STATE    VARCHAR(128)  
   ,PROVIDER_STREET_ADDRESS    VARCHAR(256)  
   ,PROVIDER_ZIP_CODE    NUMBER(38,0)  
);

create or replace storage integration s3_int
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::498504718091:role/raunakroles'
  storage_allowed_locations = ('s3://raunakjhaa/RAW');

 

desc integration s3_int;

  create or replace file format csv_format
                    type = csv
                   field_delimiter = '|'
                    skip_header = 1
                    null_if = ('NULL', 'null')
                    empty_field_as_null = true;
                    

create or replace stage ext_csv_stage
  URL = 's3://raunakjhaa/RAW/health_pipe.csv'
  STORAGE_INTEGRATION = s3_int
  file_format = csv_format;

list @ext_csv_stage;

copy into HEALTHCARE_CSV
from @ext_csv_stage



show tables
on_error = CONTINUE;


select * from HEALTHCARE_CSV;






