

create database snowpark_db;


CREATE OR REPLACE TABLE snowpark_db.public.CUSTOMERS (
    CUSTOMER_ID INTEGER,
    CUSTOMER_NAME STRING,
    CITY STRING,
    SEGMENT STRING,
    CREATED_DATE DATE
);

INSERT INTO snowpark_db.public.CUSTOMERS VALUES
(1, 'Ravi Kumar', 'Chennai', 'Retail', '2024-01-10'),
(2, 'Anita Sharma', 'Bangalore', 'Corporate', '2024-01-15'),
(3, 'John Paul', 'Hyderabad', 'Retail', '2024-02-01'),
(4, 'Meena Iyer', 'Mumbai', 'Corporate', '2024-02-05'),
(5, 'Arjun Singh', 'Delhi', 'SME', '2024-02-10');


UPDATE SNOWPARK_DB.PUBLIC.CUSTOMERS SET CUSTOMER_NAME = 'Ravi Kumar     ' where customer_id =1;

CREATE OR REPLACE TABLE snowpark_db.public.ORDERS (
    ORDER_ID INTEGER,
    CUSTOMER_ID INTEGER,
    ORDER_DATE DATE,
    AMOUNT NUMBER(10,2),
    STATUS STRING
);

INSERT INTO snowpark_db.public.ORDERS VALUES
(101, 1, '2024-03-01', 2500.00, 'DELIVERED'),
(102, 2, '2024-03-02', 5500.00, 'DELIVERED'),
(103, 3, '2024-03-05', 1500.00, 'CANCELLED'),
(104, 1, '2024-03-07', 3200.00, 'DELIVERED'),
(105, 2, '2024-03-10', 9800.00, 'DELIVERED'),
(106, 3, '2024-03-12', 4300.00, 'PENDING');


CREATE OR REPLACE TABLE snowpark_db.public.PAYMENTS (
    PAYMENT_ID INTEGER,
    ORDER_ID INTEGER,
    PAYMENT_DATE DATE,
    PAYMENT_MODE STRING,
    PAID_AMOUNT NUMBER(10,2)
);




CREATE OR REPLACE TABLE ORDERS1 (
    ORDER_ID        INT,
    CUSTOMER_ID     INT,
    DEPARTMENT      VARCHAR(100),
    EMPLOYEE        VARCHAR(100),
    SALARY          NUMBER(12,2),
    STATUS          VARCHAR(50),
    AMOUNT          NUMBER(12,2),
    DISCOUNT        NUMBER(12,2),
    ORDER_DATE      DATE
);

INSERT INTO ORDERS1 (
    ORDER_ID,
    CUSTOMER_ID,
    DEPARTMENT,
    EMPLOYEE,
    SALARY,
    STATUS,
    AMOUNT,
    DISCOUNT,
    ORDER_DATE
)
VALUES
    (1, 101, 'HR', 'Alice', 50000, 'Completed', 1200.50, 50.00, '2025-01-01'),
    (2, 102, 'IT', 'Bob', 60000, 'Pending', 800.00, 20.00, '2025-01-02'),
    (3, 103, 'Finance', 'Charlie', 70000, 'Completed', 1500.00, 75.00, '2025-01-03'),
    (4, 104, 'Sales', 'David', 55000, 'Shipped', 2000.00, 100.00, '2025-01-04'),
    (5, 105, 'Marketing', 'Eva', 65000, 'Cancelled', 950.00, 30.00, '2025-01-05'),
    (6, 106, 'HR', 'Frank', 48000, 'Completed', 1100.00, 45.00, '2025-01-06'),
    (7, 107, 'IT', 'Grace', 72000, 'Pending', 1750.00, 60.00, '2025-01-07'),
    (8, 108, 'Finance', 'Helen', 80000, 'Completed', 2200.00, 120.00, '2025-01-08'),
    (9, 109, 'Sales', 'Ian', 53000, 'Shipped', 1300.00, 55.00, '2025-01-09'),
    (10, 110, 'Marketing', 'Jane', 62000, 'Completed', 1450.00, 70.00, '2025-01-10');




    CREATE OR REPLACE TABLE support_ticket (
    ticket_id        NUMBER,
    customer_name    VARCHAR,
    ticket_date      DATE,
    issue_type       VARCHAR,
    priority         VARCHAR,
    ticket_text      VARCHAR
);


INSERT INTO support_ticket VALUES
(1001, 'Ramesh Kumar', '2025-01-05', 'Delivery', 'High',
 'The order was placed on 1st January 2025. It was shipped on 3rd January and delivered on 6th January 2025. The delivery was delayed by one day.'),

(1002, 'Anita Sharma', '2025-01-06', 'Refund', 'Medium',
 'I cancelled my order on 2nd January 2025. The refund amount of ₹2,500 was promised within 7 working days but has not yet been credited.'),

(1003, 'Vikram Rao', '2025-01-07', 'Product', 'Low',
 'The product quality is good and I am satisfied with the purchase. No further issues.'),

(1004, 'Sneha Iyer', '2025-01-08', 'Support', 'High',
 'I contacted customer support on 5th January 2025 regarding a login issue. The issue was resolved on 6th January 2025.'),

(1005, 'Arjun Mehta', '2025-01-09', 'Delivery', 'High',
 'The order was supposed to be delivered on 8th January 2025 but is still pending. Please confirm the expected delivery date.'),

(1006, 'Pooja Nair', '2025-01-10', 'Billing', 'Medium',
 'I was charged twice for the same order on 7th January 2025. The extra amount needs to be refunded.'),

(1007, 'Suresh Patel', '2025-01-11', 'Refund', 'High',
 'The refund of ₹1,200 was initiated on 4th January 2025 and credited on 10th January 2025.'),

(1008, 'Neha Verma', '2025-01-12', 'Delivery', 'Low',
 'The order was delivered on time on 11th January 2025. Very happy with the service.');





 CREATE OR REPLACE TABLE faq_docs (
  id INT,
  content STRING
);

INSERT INTO faq_docs VALUES
(1, 'You can reset your password using the Forgot Password link'),
(2, 'Refunds are processed within 7 business days'),
(3, 'Contact support by emailing support@company.com');










---------------------------------5th jan ------------------------------------------------

Q1----

use snowpark_db.public;

create or replace function get_discount(amount NUMBER)
RETURNS NUMBER
AS
$$
    amount * 0.10
$$;

show functions like 'GET_DISCOUNT';


select get_discount(10000);





q2----
-- Create or replace function with two parameters
CREATE OR REPLACE FUNCTION get_discount(amount NUMBER, discount NUMBER)
RETURNS NUMBER
AS
$$
    amount * discount
$$;

-- Show the function
SHOW FUNCTIONS LIKE 'GET_DISCOUNT';

-- Test the function directly
SELECT get_discount(10000, 0.10);

-- Use the function in a query
SELECT 
    order_id, 
    amount, 
    get_discount(amount, 0.10) AS discount
FROM orders;

select order_id, amount , get_discount(amount) as discount from orders;


q3---

create or replace function calc_discount(amount NUMBER)
RETURNS NUMBER
AS
$$
   CASE 
      WHEN AMOUNT <1000 THEN AMOUNT *0.05
      WHEN AMOUNT<=5000 THEN AMOUNT * 0.10
      ELSE AMOUNT *0.15
  END
$$
show functions like 'calc_discount';
select  calc_discount(1000) + 2000 test;
select order_id , amount , calc_discount(amount) as discount from orders;


q4---
create or replace function total_amount_by_status(p_status string)
returns table (total_amount NUMBER)
AS 
$$
  SELECT SUM(AMOUNT) as total_amount
  from orders
  where status = p_status
$$;
Select * from 
table(total_amount_by_status('PENDING'))



q5---
create or replace function order_summary_by_status(p_status string)
returns table( status string , total_orders number,total_amount number)
AS
$$
 select 
  status ,
  count(*) as total_orders,
  sum(amount) as total_amount
 from orders
 where status = p_status group by status

 $$;
 select * from table(order_summary_by_status('PENDING'));


q6---

 CREATE OR REPLACE FUNCTION discount_py(amount FLOAT)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'calc_discount'
AS
$$
def calc_discount(amount):
    if amount is None:
        return 0.0
    return amount * 0.10
$$;

-- Test the function
SELECT discount_py(10000) AS discount;

-- Apply to table
SELECT 
    order_id,
    amount,
    discount_py(amount) AS discount
FROM orders;


create or replace procedure hello_proc()
returns string
language sql
as 
$$ 
BEGIN 
    RETURN 'Hello from Snowflake Stored Procedure';
END;
$$;
call hello_proc();





CREATE OR REPLACE PROCEDURE hello_proc(p_name STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    RETURN 'Hello, ' || p_name || '! Greetings from Snowflake Stored Procedure';
END;
$$;

-- Call the procedure with your name
CALL hello_proc('Raunak');

-- Show procedures
SHOW PROCEDURES LIKE 'HELLO_PROC';



create or replace procedure total_amount_by_status_sp(p_status STRING)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
DECLARE 
    total_amt NUMBER;
BEGIN 
    SELECT SUM(AMOUNT)
    INTO : total_amt
    WHERE STATUS = :p_status;

    RETURN NVL(total_amt , 0);
END;
$$;






 CREATE OR REPLACE PROCEDURE get_total_amount_sp1(p_status STRING)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
DECLARE
    v_total NUMBER;
BEGIN
    -- If input is NULL, return 0
    IF (p_status IS NULL) THEN
        RETURN 0;
    END IF;

    -- Calculate total amount for given status
    SELECT SUM(amount)
    INTO v_total
    FROM orders
    WHERE status = p_status;

    -- If no rows or sum is NULL, return 0
    IF (v_total IS NULL) THEN
        RETURN 0;
    END IF;

    RETURN v_total;

EXCEPTION
    WHEN STATEMENT_ERROR THEN
        RETURN -1;
    WHEN OTHER THEN
        RETURN -2;
END;
$$;





CREATE OR REPLACE PROCEDURE total_amount_py(p_status STRING)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = ('snowflake-snowpark-python' )
HANDLER = 'run'
AS
$$
def run(session, p_status) :
    try:
        result = session.sql(f"""
            SELECT SUM(AMOUNT ) FROM ORDERS
            WHERE STATUS = '{p_status}'
        """).collect ()  
        return result [0][0] or 0
    except Exception as e:
        return f"ERROR: {e}"
$$;

CALL TOTAL_AMOUNT_PY('PENDING' );





create or replace stage jsonstage
     url = 's3://bucketsnowflake-jsondemo';

list @jsonstage;


create or replace file format JSONFORMAT
TYPE = JSON;


CREATE OR REPLACE table json_raw (
raw_file variant
);

copy into json_raw 
     from @jsonstage
     file_format = JSONFORMAT
     files = ('HR_data.json');

select * from json_raw;


select 
   raw_file:"id"::int Id,
   raw_file:"first_name"::string First_Name,
   raw_file:"last_name"::string Lirst_Name,
   raw_file:"city"::string City,
   raw_file:"gender"::string Gener
From JSON_RAW;


select 
   raw_file:"id"::int Id,
   raw_file:"first_name"::string First_Name,
   raw_file:"last_name"::string Lirst_Name,
   raw_file:"city"::string City,
   raw_file:"gender"::string Gener,
   raw_file:"job"."salary"::float Salary,
   raw_file:"job"."title"::string Job_Title
From JSON_RAW;

select 
    raw_file:"id"::int Id,
    raw_file:"first_name" ::string First_Name,
    raw_file:"prev_company":: array prev_company
from JSON_RAW


select 
   raw_file:"id"::int Id,
   raw_file:"first_name"::string First_Name,
   raw_file:"prev_company"[0]::string as first_company,
   raw_file:"prev_company"[1]::string as second_company,
   raw_file:"prev_company"[2]::string as first_company
from json_raw


select 
     raw_file:"id"::int Id,
     raw_file:"first_name"::string First_Name,
     array_size(RAW_FILE:"prev_copmany") companies
FROM JSON_RAW order by companies desc;



SELECT 
    RAW_FILE:"id"::int AS ID,
    RAW_FILE:"first_name"::string AS FIRST_NAME,
    t.value::string AS COMPANY
FROM json_raw,
     TABLE(FLATTEN(input => RAW_FILE:"prev_company")) AS t;



CREATE OR REPLACE TABLE HR_DATA AS
SELECT 
    raw_file:"id"::int AS ID,
    raw_file:"first_name"::string AS FIRST_NAME,
    raw_file:"prev_company"[0]::string AS FIRST_COMPANY,
    raw_file:"prev_company"[1]::string AS SECOND_COMPANY,
    raw_file:"prev_company"[2]::string AS THIRD_COMPANY,
    t.value::string AS COMPANY
FROM json_raw,
     TABLE(FLATTEN(input => raw_file:"prev_company")) AS t;

SELECT 
    raw_file:"id"::int AS ID,
    raw_file:"first_name"::string AS FIRST_NAME,
    f.value:language::string AS LANGUAGE,
    f.value:level::string AS LEVEL
FROM JSON_RAW,
     TABLE(FLATTEN(input => raw_file:"spoken_language")) AS f;


create or replace stage parquetstage
url = 's3://snowflakeparquetdemo';

create or replace file format parquet_format
    type = 'parquet';

select * from @PARQUETSTAGE
(file_format => 'PARQUET_FORMAT');



SELECT $1:id::string id,
    $1:cat_id::string cat_id,
    $1:dept_id::string dept_id,
    to_timestamp ($1:date)::datetime order_date,
    $1:"item_id"::string,
    $1:state_id::string,
    $1:store_id::string,
    $1:value::int
from @parquetstage
(file_format => 'PARQUET_FORMAT');


create user osama identified by 'binladen123';

create role finance_role;
grant usage on warehouse compute_wh to finance_role;
grant usage on database snowpark_db to finance_role;
grant usage on schema public to finance_role;
grant select on table orders to role finance_role;
grant select on table customers to role finance_role;


show grants;

grant role finance_role to user osama;

-- on incognito------

select * from snowpark_db.public.orders;

use warehouse compute_wh;

use role finance_role;

select current_role();

select * from snowpark_db.public.orders;



-----on main---------
CREATE OR REPLACE ROW ACCESS POLICY pending_orders_role_based
AS (status_col VARCHAR)
RETURNS BOOLEAN ->
    CASE
        WHEN CURRENT_ROLE () = 'ACCOUNTADMIN' THEN TRUE
        ELSE status_col = 'PENDING'
    END;
    
ALTER TABLE ORDERS
ADD ROW ACCESS POLICY pending_orders_role_based
ON (STATUS) ;



CREATE OR REPLACE MASKING POLICY mask_segment
AS (val STRING)
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE ( ) = 'ACCOUNTADMIN' THEN val
        ELSE 'XXXX-XXXX'
    END;
    
ALTER TABLE customers
MODIFY COLUMN segment
SET MASKING POLICY mask_segment;


DROP TABLE IF EXISTS ORDER_SUMMARY;
-- or DROP VIEW IF EXISTS ORDER_SUMMARY;

CREATE OR REPLACE DYNAMIC TABLE ORDER_SUMMARY
TARGET_LAG = '1 MINUTE'
WAREHOUSE = COMPUTE_WH
AS
SELECT STATUS,
       SUM(AMOUNT) AS TOTAL_AMOUNT,
       MAX(AMOUNT) AS MAX_AMOUNT,
       MIN(AMOUNT) AS MIN_AMOUNT,
       COUNT(AMOUNT) AS NUMBER_OF_ORDERS
FROM ORDERS
GROUP BY STATUS;

show dynamic tables

select * from order_summary
select * from orders

insert into orders values(111,110,current_date())

















