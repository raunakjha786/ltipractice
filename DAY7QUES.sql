create or replace database day7;
use database day7;

create or replace schema day7schema;
use schema day7schema;

create or replace table customer ( INDEX INT,
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

create or replace stage named_stage_customer;
list @named_stage_customer;
PUT file://C:/Users/User/Desktop/Customer_file.csv @named_stage_customer AUTO_COMP
                                     RESS=TRUE;
--here we will put the file using snowsql into named stage

COPY INTO customer
FROM @named_stage_customer
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  field_optionally_enclosed_by='"'
  SKIP_HEADER = 1
)
validation_mode = 'return_errors';

select * from customer;

--next part timetravel

create or replace table customer ( INDEX INT,
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

  COPY INTO customer
FROM @ext_csv_stage
ON_ERROR = CONTINUE
  
select * from customer

select CURRENT_TIMESTAMP();
alter session set timezone='UTC';

SELECT * FROM customer BEFORE (TIMESTAMP => '2025-12-24 07:10:20.458 +0000');
select * from customer


insert into customer values (101, 'dnjdfhi342321', 'raunak','jha','ltimindtree',
'faridabad', 'india','361289','376238','raunak23@gmail.com','2022-04-01','www.raunak.com')

select * from customer;
SELECT * FROM customer AT (OFFSET => -60*2);
update customer set first_name = 'raunakk' where index = 101; 
delete from customer where first_name = 'raunakk'


--stream task part
create or replace stream customer_stream on table customer;

create or replace table  consume_table_cust(Index int,Customer_Id string,First_Name string,Last_Name string,Company string,City string,Country string,Phone_1 string,Phone_2 string,Email string,Subscription_Date string,Website string);


create or replace task stream_task
warehouse=COMPUTE_WH
schedule='1 minute'
when system$stream_has_data('customer_stream')
as
insert into consume_table_cust
select index,customer_id,First_Name,Last_Name,Company,City,Country,Phone_1 ,Phone_2 ,Email,Subscription_Date ,Website from customer_stream;
alter task stream_task resume;



insert into customer values(101,'AAAAAA234','Raunak','Tewari','LTI','Faridbad','INDIA','22222229090','1234567890','raunak@gmail.com','2025-03-26','https://raunakb.com');


select * from customer_stream;
select * from consume_table_cust;

--clone
CREATE OR REPLACE TABLE customer_clone CLONE customer;

SELECT * FROM customer_clone;









  
  
   


  










