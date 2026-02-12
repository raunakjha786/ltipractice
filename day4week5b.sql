use database raunakbucket
use schema bucket
CREATE OR REPLACE TABLE healthcare_parquet_raw (
    raw_data VARIANT,
    filename STRING,
    file_row_number NUMBER,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

create or replace storage integration s3_int
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::498504718091:role/raunakroles'
  storage_allowed_locations = ('s3://raunakjhaa/RAW/delhi');

desc integration s3_int;

create or replace file format parquet_format
  type = 'parquet';

  create or replace stage ext_parquet_stage
  URL = 's3://raunakjhaa/RAW/delhi/health.parquet'
  STORAGE_INTEGRATION = s3_int
  file_format = parquet_format;

  list @ext_parquet_stage;



select * from healthcare_parquet_raw;

SELECT
    raw_data:" Average Covered Charges "::VARCHAR,
    raw_data:" Average Total Payments "::VARCHAR,
    raw_data:" Total Discharges "::INTEGER,
    raw_data:"% Bachelor's or Higher"::FLOAT,
    raw_data:"% HS Grad or Higher"::VARCHAR,
    raw_data:"Total payments"::VARCHAR,
    raw_data:"% Reimbursement"::VARCHAR,
    raw_data:"Total covered charges"::VARCHAR,
    raw_data:"Referral Region Provider Name"::VARCHAR,
    raw_data:"ReimbursementPercentage"::VARCHAR,
    raw_data:"DRG Definition"::VARCHAR,
    raw_data:"Referral Region"::VARCHAR,
    raw_data:"INCOME_PER_CAPITA"::VARCHAR,
    raw_data:"MEDIAN EARNINGS - BACHELORS"::VARCHAR,
    raw_data:"MEDIAN EARNINGS - GRADUATE"::VARCHAR,
    raw_data:"MEDIAN EARNINGS - HS GRAD"::VARCHAR,
    raw_data:"MEDIAN EARNINGS- LESS THAN HS"::VARCHAR,
    raw_data:"MEDIAN_FAMILY_INCOME"::VARCHAR,
    raw_data:"Number of Records"::VARCHAR,
    raw_data:"POP_25_OVER"::VARCHAR,
    raw_data:"Provider City"::VARCHAR,
    raw_data:"Provider Id"::VARCHAR,
    raw_data:"Provider Name"::VARCHAR,
    raw_data:"Provider State"::VARCHAR,
    raw_data:"Provider Street Address"::VARCHAR,
    raw_data:"Provider Zip Code"::VARCHAR,
    filename,
    file_row_number,
    load_timestamp
FROM healthcare_parquet_raw;


CREATE or replace TABLE HEALTHCARE_PARQUET(
    AVERAGE_COVERED_CHARGES    VARCHAR(150)  
   ,AVERAGE_TOTAL_PAYMENTS    VARCHAR(150)  
   ,TOTAL_DISCHARGES    VARCHAR(150)   
   ,BACHELORORHIGHER    VARCHAR(150)   
   ,HSGRADORHIGHER    VARCHAR(150)   
   ,TOTALPAYMENTS    VARCHAR(128)  
   ,REIMBURSEMENT    VARCHAR(128)  
   ,TOTAL_COVERED_CHARGES    VARCHAR(128) 
   ,REFERRALREGION_PROVIDER_NAME    VARCHAR(256)  
   ,REIMBURSEMENTPERCENTAGE    VARCHAR(150)   
   ,DRG_DEFINITION    VARCHAR(256)  
   ,REFERRAL_REGION    VARCHAR(26)  
   ,INCOME_PER_CAPITA    VARCHAR(150)   
   ,MEDIAN_EARNINGSBACHELORS    VARCHAR(150)   
   ,MEDIAN_EARNINGS_GRADUATE    VARCHAR(150) 
   ,MEDIAN_EARNINGS_HS_GRAD    VARCHAR(150)   
   ,MEDIAN_EARNINGSLESS_THAN_HS    VARCHAR(150) 
   ,MEDIAN_FAMILY_INCOME    VARCHAR(150)   
   ,NUMBER_OF_RECORDS    VARCHAR(150)  
   ,POP_25_OVER    VARCHAR(150)  
   ,PROVIDER_CITY    VARCHAR(128)  
   ,PROVIDER_ID    VARCHAR(150)   
   ,PROVIDER_NAME    VARCHAR(256)  
   ,PROVIDER_STATE    VARCHAR(128)  
   ,PROVIDER_STREET_ADDRESS    VARCHAR(256)  
   ,PROVIDER_ZIP_CODE    VARCHAR(150) 
   ,filename    VARCHAR
   ,file_row_number VARCHAR
   ,load_timestamp timestamp default TO_TIMESTAMP_NTZ(current_timestamp)
);

insert into HEALTHCARE_PARQUET
SELECT
    raw_data:" Average Covered Charges "::VARCHAR,
    raw_data:" Average Total Payments "::VARCHAR,
    raw_data:" Total Discharges "::INTEGER,
    raw_data:"% Bachelor's or Higher"::FLOAT,
    raw_data:"% HS Grad or Higher"::VARCHAR,
    raw_data:"Total payments"::VARCHAR,
    raw_data:"% Reimbursement"::VARCHAR,
    raw_data:"Total covered charges"::VARCHAR,
    raw_data:"Referral Region Provider Name"::VARCHAR,
    raw_data:"ReimbursementPercentage"::VARCHAR,
    raw_data:"DRG Definition"::VARCHAR,
    raw_data:"Referral Region"::VARCHAR,
    raw_data:"INCOME_PER_CAPITA"::VARCHAR,
    raw_data:"MEDIAN EARNINGS - BACHELORS"::VARCHAR,
    raw_data:"MEDIAN EARNINGS - GRADUATE"::VARCHAR,
    raw_data:"MEDIAN EARNINGS - HS GRAD"::VARCHAR,
    raw_data:"MEDIAN EARNINGS- LESS THAN HS"::VARCHAR,
    raw_data:"MEDIAN_FAMILY_INCOME"::VARCHAR,
    raw_data:"Number of Records"::VARCHAR,
    raw_data:"POP_25_OVER"::VARCHAR,
    raw_data:"Provider City"::VARCHAR,
    raw_data:"Provider Id"::VARCHAR,
    raw_data:"Provider Name"::VARCHAR,
    raw_data:"Provider State"::VARCHAR,
    raw_data:"Provider Street Address"::VARCHAR,
    raw_data:"Provider Zip Code"::VARCHAR,
    filename,
    file_row_number,
    load_timestamp
FROM healthcare_parquet_raw;

select * from healthcare_parquet;



