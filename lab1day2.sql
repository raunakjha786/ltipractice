-- LAB1 DAY2 

create or replace database lab_sql_dml;

use database lab_sql_dml;

create or replace schema raw;

create or replace schema curated;

create or replace table curated.sales_orders (

order_id NUMBER,
order_date DATE,
customer_id NUMBER,
amount NUMBER(10,2),
status STRING

);



create or replace stage raw.my_csv_stage;
create or replace stage raw.my_json_stage;
create or replace stage raw.my_parquet_stage;




