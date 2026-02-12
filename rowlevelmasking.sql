use database day7
use schema day7schema

CREATE OR REPLACE TABLE emp (
  emp_id   NUMBER,
  emp_name STRING,
  dept     STRING,
  region   STRING,
  salary   NUMBER,
  status   STRING
);


INSERT INTO emp VALUES
(1, 'Ravi',  'HR', 'IN', 60000, 'ACTIVE'),
(2, 'Anita', 'HR', 'US', 75000, 'ACTIVE'),
(3, 'John',  'IT', 'US', 90000, 'ACTIVE'),
(4, 'Kiran', 'IT', 'IN', 65000, 'INACTIVE'),
(5, 'Meena', 'FIN','IN', 70000, 'ACTIVE'),
(6, 'David', 'FIN','US', 85000, 'ACTIVE');


CREATE OR REPLACE ROLE US_ROLE;
CREATE OR REPLACE ROLE IN_ROLE;
CREATE OR REPLACE ROLE ADMIN_ROLE;


GRANT ROLE US_ROLE TO USER RAUNAK108;
GRANT ROLE IN_ROLE TO USER RAUNAK108;
GRANT ROLE ADMIN_ROLE TO USER RAUNAK108;


GRANT USAGE ON DATABASE DAY7  TO ROLE US_ROLE;
GRANT USAGE ON DATABASE DAY7 TO ROLE IN_ROLE;
GRANT USAGE ON DATABASE DAY7 TO ROLE ADMIN_ROLE;



GRANT USAGE ON SCHEMA DAY7SCHEMA TO ROLE US_ROLE;
GRANT USAGE ON SCHEMA DAY7SCHEMA TO ROLE IN_ROLE;
GRANT USAGE ON SCHEMA DAY7SCHEMA TO ROLE ADMIN_ROLE;


GRANT SELECT ON TABLE EMP TO ROLE US_ROLE;
GRANT SELECT ON TABLE EMP TO ROLE IN_ROLE;
GRANT SELECT ON TABLE EMP TO ROLE ADMIN_ROLE;

CREATE OR REPLACE ROW ACCESS POLICY emp_dept_policy
AS (region STRING)
RETURNS BOOLEAN ->
  CASE
     WHEN CURRENT_ROLE() = 'ADMIN_ROLE' THEN TRUE
    WHEN CURRENT_ROLE() = 'US_ROLE' AND region = 'US' THEN TRUE
    WHEN CURRENT_ROLE() = 'IN_ROLE' AND region = 'IN' THEN TRUE
    ELSE FALSE
  END;
ALTER TABLE emp
ADD ROW ACCESS POLICY emp_dept_policy
ON (region);

SELECT * FROM emp

use role us_role
select*from emp



------------------------------------------------
create or replace table customer(
order_id int,
category string,
sub_category string,
sales number(10,2),
order_date varchar,
region string
);

INSERT INTO customer (order_id, category, sub_category, sales, order_date, region) VALUES
(1001, 'Furniture', 'Chair', 250.75, '2025-01-15', 'East'),
(1002, 'Technology', 'Laptop', 1200.00, '2025-02-10', 'West'),
(1003, 'Office Supplies', 'Paper', 35.50, '2025-02-18', 'South'),
(1004, 'Furniture', 'Table', 540.20, '2025-03-05', 'North'),
(1005, 'Technology', 'Smartphone', 899.99, '2025-03-12', 'East'),
(1006, 'Office Supplies', 'Binder', 12.00, '2025-03-20', 'West'),
(1007, 'Furniture', 'Bookshelf', 320.00, '2025-04-02', 'South'),
(1008, 'Technology', 'Headphones', 150.00, '2025-04-15', 'North'),
(1009, 'Office Supplies', 'Pen', 5.25, '2025-04-20', 'East'),
(1010, 'Furniture', 'Sofa', 1100.00, '2025-05-01', 'West');


CREATE OR REPLACE MATERIALIZED VIEW mv_monthly_sales_by_region_category
AS
SELECT
    DATE_TRUNC('MONTH', TO_DATE(order_date, 'YYYY-MM-DD')) AS sales_month,
    category,
    region,
    SUM(sales) AS total_sales
FROM customer
GROUP BY sales_month, category, region;


SELECT 
    sales_month,
    category,
    region,
    total_sales
FROM mv_monthly_sales_by_region_category
ORDER BY sales_month, category, region;


-------------------------------------
create external stage
create snowpipe
row level and column level masking
database schema table cloning
Share the access of the table to on aws account

