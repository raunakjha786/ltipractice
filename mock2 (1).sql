
use database snowpark_db.public





















CREATE OR REPLACE TABLE student_courses_raw (
  student VARIANT
);







CREATE OR REPLACE STAGE student_courses_stage;






CREATE OR REPLACE FILE FORMAT ff_json_courses
TYPE = JSON
STRIP_OUTER_ARRAY = TRUE;

LIST @student_courses_stage;


COPY INTO student_courses_raw
FROM @student_courses_stage/student_courses.json
FILE_FORMAT = (FORMAT_NAME = ff_json_courses);


SELECT * FROM student_courses_raw;




--inspect raw json
SELECT student
FROM student_courses_raw;


--extract simple fields
SELECT
  student:user_id::STRING            AS user_id,
  student:name::STRING               AS name,
  student:meta.plan::STRING          AS plan,
  student:meta.region::STRING        AS region,
  student:meta.signup_ts::TIMESTAMP  AS signup_ts
FROM student_courses_raw;





SELECT
  student:user_id::STRING          AS user_id,
  student:name::STRING             AS name,
  f.value:session_id::STRING       AS session_id,
  f.value:start_ts::TIMESTAMP      AS session_start_ts,
  f.value:active::BOOLEAN          AS is_active
FROM student_courses_raw,
LATERAL FLATTEN(input => student:sessions) f;



--flatten with filter
SELECT
  student:user_id::STRING AS user_id,
  f.value:session_id::STRING AS session_id,
  f.value:start_ts::TIMESTAMP AS start_ts
FROM student_courses_raw,
LATERAL FLATTEN(input => student:sessions) f
WHERE f.value:active::BOOLEAN = TRUE;


--count session per user aggregation
SELECT
  student:user_id::STRING AS user_id,
  COUNT(*) AS total_sessions
FROM student_courses_raw,
LATERAL FLATTEN(input => student:sessions)
GROUP BY user_id
ORDER BY total_sessions DESC;



-----------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE orders_city (
order_id STRING,
order_date DATE,
city STRING,
fulfillment_type STRING,
order_value NUMBER(12,2)
);


INSERT INTO orders_city (order_id, order_date, city, fulfillment_type, order_value) VALUES
-- January - Chennai
('OC1001','2025-01-02','Chennai','Standard', 12500.00),
('OC1002','2025-01-03','Chennai','Express', 18900.00),
('OC1003','2025-01-04','Chennai','Pickup', 9200.00),

-- January - Bengaluru
('OC1004','2025-01-02','Bengaluru','Standard', 14600.00),
('OC1005','2025-01-05','Bengaluru','Express', 21250.00),
('OC1006','2025-01-07','Bengaluru','Pickup', 7800.00),

-- January - Mumbai
('OC1007','2025-01-03','Mumbai','Standard', 16800.00),
('OC1008','2025-01-06','Mumbai','Express', 24500.00),
('OC1009','2025-01-08','Mumbai','Pickup', 8600.00),

-- February - Chennai
('OC1010','2025-02-01','Chennai','Standard', 13200.00),
('OC1011','2025-02-03','Chennai','Express', 19850.00),
('OC1012','2025-02-05','Chennai','Pickup', 9900.00),

-- February - Bengaluru
('OC1013','2025-02-02','Bengaluru','Standard', 15000.00),
('OC1014','2025-02-04','Bengaluru','Express', 22600.00),
('OC1015','2025-02-07','Bengaluru','Pickup', 8200.00),

-- February - Mumbai
('OC1016','2025-02-02','Mumbai','Standard', 17500.00),
('OC1017','2025-02-06','Mumbai','Express', 25900.00),
('OC1018','2025-02-09','Mumbai','Pickup', 9050.00),

-- March - Chennai
('OC1019','2025-03-01','Chennai','Standard', 13800.00),
('OC1020','2025-03-02','Chennai','Express', 20500.00),
('OC1021','2025-03-04','Chennai','Pickup', 10400.00),

-- March - Bengaluru
('OC1022','2025-03-01','Bengaluru','Standard', 15350.00),
('OC1023','2025-03-03','Bengaluru','Express', 23100.00),
('OC1024','2025-03-06','Bengaluru','Pickup', 8400.00),

-- March - Mumbai
('OC1025','2025-03-02','Mumbai','Standard', 18250.00),
('OC1026','2025-03-05','Mumbai','Express', 26750.00),
('OC1027','2025-03-08','Mumbai','Pickup', 9300.00);



---------------------------------------------------------------------------------------


CREATE OR REPLACE TABLE product_dim (
product_id STRING,
product_name STRING,
category STRING
);



CREATE OR REPLACE TABLE sales_lineitems (
order_id STRING,
order_date DATE,
product_id STRING,
qty NUMBER(10,0),
unit_price NUMBER(10,2)
);




INSERT INTO product_dim (product_id, product_name, category) VALUES
('Q001', 'Smartphone X', 'Electronics'),
('Q002', 'Noise-Cancel Headset','Electronics'),
('Q003', 'Ergo Keyboard', 'Electronics'),
('Q004', 'Mesh Office Chair', 'Furniture'),
('Q005', 'Standing Work Desk', 'Furniture'),
('Q006', 'Air Fryer', 'Home & Kitchen'),
('Q007', 'Espresso Machine', 'Home & Kitchen'),
('Q008', 'LED Desk Lamp', 'Home & Kitchen'),
('Q009', 'Bookshelf 5-Tier', 'Furniture'),
('Q010', 'Portable Blender', 'Home & Kitchen');



INSERT INTO sales_lineitems (order_id, order_date, product_id, qty, unit_price) VALUES
('L1001', '2025-01-05', 'Q001', 3, 35000.00),
('L1002', '2025-01-06', 'Q002', 5, 8000.00),
('L1003', '2025-01-06', 'Q003', 4, 4500.00),
('L1004', '2025-01-07', 'Q004', 2, 12000.00),
('L1005', '2025-01-08', 'Q005', 1, 29000.00),
('L1006', '2025-01-08', 'Q006', 7, 6000.00),
('L1007', '2025-01-09', 'Q007', 2, 26000.00),
('L1008', '2025-01-09', 'Q008', 10, 3500.00),
('L1009', '2025-01-10', 'Q009', 3, 15000.00),
('L1010', '2025-01-10', 'Q010', 6, 3200.00),

('L1011', '2025-02-01', 'Q001', 1, 34000.00),
('L1012', '2025-02-02', 'Q002', 7, 7800.00),
('L1013', '2025-02-03', 'Q003', 8, 4300.00),
('L1014', '2025-02-04', 'Q004', 1, 12500.00),
('L1015', '2025-02-05', 'Q005', 2, 29500.00),
('L1016', '2025-02-06', 'Q006', 12, 5900.00),
('L1017', '2025-02-07', 'Q007', 3, 25500.00),
('L1018', '2025-02-08', 'Q008', 8, 3600.00),
('L1019', '2025-02-09', 'Q009', 2, 14800.00),
('L1020', '2025-02-10', 'Q010', 5, 3100.00),

('L1021', '2025-03-01', 'Q001', 2, 34500.00),
('L1022', '2025-03-02', 'Q002', 4, 7900.00),
('L1023', '2025-03-03', 'Q003', 10, 4400.00),
('L1024', '2025-03-04', 'Q004', 3, 11800.00),
('L1025', '2025-03-05', 'Q005', 1, 30000.00),
('L1026', '2025-03-06', 'Q006', 15, 6100.00),
('L1027', '2025-03-07', 'Q007', 2, 25800.00),
('L1028', '2025-03-08', 'Q008', 6, 3550.00),
('L1029', '2025-03-09', 'Q009', 4, 15200.00),
('L1030', '2025-03-10', 'Q010', 3, 3300.00);



-------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE help_library (
doc_id STRING,
title STRING,
body STRING,
category STRING,
source_uri STRING
);


INSERT INTO help_library (doc_id, title, body, category, source_uri) VALUES
('HL-001', 'Onboarding Procedure',
'Follow the onboarding steps: account setup, MFA enablement, device registration, and policy acknowledgment.',
'Procedure', 'kb/procedures/onboarding.txt'),

('HL-002', 'VPN Setup Guide',
'To set up the VPN: install the client, import the configuration, authenticate with MFA, and verify connectivity.',
'Guide', 'kb/guides/vpn_setup.txt'),

('HL-003', 'Password Reset Policy',
'Passwords must be reset every 90 days. Use complex combinations and do not reuse previous passwords.',
'Policy', 'kb/policies/password_reset.txt'),

('HL-004', 'Remote Access Troubleshooting',
'If remote access fails: check internet, restart the client, re-authenticate, and confirm firewall rules.',
'FAQ', 'kb/faqs/remote_access_troubleshooting.txt'),

('HL-005', 'Email Configuration Steps',
'Open the mail client, add account with IMAP/SMTP settings, enable SSL/TLS, and run a test send/receive.',
'Guide', 'kb/guides/email_configuration.txt');



CREATE OR REPLACE CORTEX SEARCH SERVICE help_search_svc
ON body
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 hour'
AS
SELECT
    doc_id,
    title,
    body,
    category,
    source_uri
FROM help_library;



SHOW CORTEX SEARCH SERVICES;

DESCRIBE CORTEX SEARCH SERVICE help_search_svc;


SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  'help_search_svc',
  '{
    "query": "steps to set up VPN",
    "columns": ["DOC_ID","TITLE","CATEGORY","BODY"],
    "limit": 3
  }'
) AS RAW_JSON;


SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  'help_search_svc',
  '{
    "query": "procedure for new employee onboarding",
    "columns": ["DOC_ID","TITLE","CATEGORY","BODY"],
    "limit": 3
  }'
) AS RAW_JSON;


--parse json (no flatten yet)

WITH j AS (
  SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'help_search_svc',
      '{
        "query": "password reset",
        "columns": ["DOC_ID","TITLE","CATEGORY","BODY"],
        "limit": 3
      }'
    )
  ) AS r
)
SELECT
  r:results[0]:TITLE::STRING       AS title_test,
  r:results[0]:CATEGORY::STRING    AS category_test,
  r:results[0]:DOC_ID::STRING      AS doc_id_test,
  r:results[0]:"@scores":cosine_similarity::FLOAT AS score_test
FROM j;


--USING FLATTEN
WITH j AS (
  SELECT
    'password reset query' AS query,
    PARSE_JSON(
      SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'help_search_svc',
        '{
          "query": "password reset",
          "columns": ["DOC_ID","TITLE","CATEGORY","BODY"],
          "limit": 3
        }'
      )
    ) AS r
)
SELECT
  query,
  f.value:BODY::STRING       AS strip,       
  f.value:TITLE::STRING      AS matched_title,
  f.value:CATEGORY::STRING   AS category,
  f.value:DOC_ID::STRING     AS doc_id,
  f.value:"@scores":cosine_similarity::FLOAT AS score
FROM j,
LATERAL FLATTEN(input => r:results) f
ORDER BY score DESC;

select 
'steps to set up VPN' as query,
value:title::string as matched_title,
value:body::string as snippet,
value:"@scores"."cosine_similarity"::float as cosine_score
from table(flatten(input=>parse_json(
 snowflake.cortex.search_preview(
  'library_search',
  '{
    "query": "steps to set up VPN",
    "columns": ["doc_id", "body","category","title"],
    "limit": 2
  }'
)):results));




-----------dynamic tables

CREATE OR REPLACE DYNAMIC TABLE dt_daily_sales
TARGET_LAG = '1 minutes'
WAREHOUSE = compute_wh
AS
SELECT
  ORDER_DATE,
  SUM(AMOUNT) AS TOTAL_SALES
FROM ORDERS
GROUP BY ORDER_DATE;
SHOW DYNAMIC TABLES;

select * from dt_daily_sales;

describe table orders;

insert into orders VALUES 

(107,1000,'2024-03-01',1000,'DELIVERD'),
(108,1000,current_date(),1000,'PENDING'),
(109,2000,current_date(),1000,'PENDING'),
(110,1000,current_date(),1000,'PENDING'),
(110,1000,current_date(),1000,'PENDING');
;

SELECT * FROM dt_daily_sales;
--bronze
CREATE OR REPLACE DYNAMIC TABLE bronze_orders
TARGET_LAG = '1 minutes'
WAREHOUSE = compute_wh
AS
SELECT * FROM ORDERS;
--silver
CREATE OR REPLACE DYNAMIC TABLE silver_orders
TARGET_LAG = '1 minutes'
WAREHOUSE = compute_wh
AS
SELECT
  ORDER_ID,
  CUSTOMER_ID,
  ORDER_DATE,
  AMOUNT,
  STATUS
FROM bronze_orders
WHERE STATUS IN ('CONFIRMED', 'SHIPPED');
--gold
CREATE OR REPLACE DYNAMIC TABLE gold_sales_summary
TARGET_LAG = '5 minutes'
WAREHOUSE = compute_wh
AS
SELECT
  STATUS,
  COUNT(*) AS TOTAL_ORDERS,
  SUM(AMOUNT) AS TOTAL_AMOUNT
FROM silver_orders
GROUP BY STATUS;
DESCRIBE DYNAMIC TABLE gold_sales_summary;
ALTER DYNAMIC TABLE gold_sales_summary SUSPEND;
ALTER DYNAMIC TABLE gold_sales_summary RESUME;





