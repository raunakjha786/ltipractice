q1-----------
CREATE OR REPLACE TABLE ORDER_SUMMARY (
ORDER_ID INT,
ORDER_TS TIMESTAMP_NTZ,
COUNTRY STRING,
CITY STRING,
SALES_CHANNEL STRING,
PAYMENT_METHOD STRING,
ORDER_VALUE NUMBER(10,2)
);


INSERT INTO ORDER_SUMMARY VALUES
(1001,'2025-11-01 10:05:00','India','Bengaluru','Web','UPI',1499.00),
(1002,'2025-11-01 10:18:00','USA','Austin','Mobile','Card',89.50),
(1003,'2025-11-01 11:02:00','UK','London','Web','Card',320.00),
(1004,'2025-11-02 09:30:00','India','Mumbai','Partner','NetBanking',760.25),
(1005,'2025-11-02 13:12:00','USA','Seattle','Web','Card',1200.00),
(1006,'2025-11-03 16:40:00','UK','Manchester','Mobile','Wallet',49.99),
(1007,'2025-11-03 18:05:00','India','Delhi','Web','Card',999.00),
(1008,'2025-11-04 08:15:00','USA','New York','Mobile','PayPal',540.75),
(1009,'2025-11-04 19:22:00','UK','Bristol','Web','Card',210.00),
(1010,'2025-11-05 14:55:00','India','Chennai','Mobile','UPI',399.00);


q2-------------------
CREATE OR REPLACE TABLE PRODUCT_SALES (
SALE_ID INT,
SALE_DATE DATE,
STORE_ID STRING,
CATEGORY STRING,
PRODUCT_NAME STRING,
QUANTITY INT,
AMOUNT NUMBER(10,2)
);




INSERT INTO PRODUCT_SALES VALUES
(1,'2025-10-01','S-001','Electronics','Wireless Headphones',2,3999.00),
(2,'2025-10-01','S-002','Grocery','Olive Oil 1L',1,699.00),
(3,'2025-10-02','S-001','Clothing','Denim Jacket',1,2499.00),
(4,'2025-10-02','S-003','Electronics','Smartwatch',1,7999.00),
(5,'2025-10-03','S-002','Grocery','Coffee Beans 500g',2,1200.00),
(6,'2025-10-03','S-003','Clothing','Running Shoes',1,4599.00),
(7,'2025-10-04','S-001','Electronics','Bluetooth Speaker',1,2999.00),
(8,'2025-10-04','S-002','Grocery','Protein Bar Pack',3,450.00),
(9,'2025-10-05','S-003','Clothing','Formal Shirt',2,1798.00),
(10,'2025-10-05','S-001','Electronics','Tablet',1,15999.00);




CREATE OR REPLACE FUNCTION classify_amount(x NUMBER)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'classify'
AS
$$
def classify(x):
    if x is None:
        return "UNKNOWN"
    elif x < 500:
        return "LOW"
    elif x < 1000:
        return "MEDIUM"
    else:
        return "HIGH"
$$;


SHOW USER FUNCTIONS LIKE 'CLASSIFY_AMOUNT';

SELECT
    SALE_ID,
    PRODUCT_NAME,
    AMOUNT,
    CLASSIFY_AMOUNT(AMOUNT) AS AMOUNT_CATEGORY
FROM PRODUCT_SALES
ORDER BY AMOUNT DESC
LIMIT 5;



--------------------------------------
CREATE OR REPLACE FUNCTION discount_slab_sql(amount FLOAT)
RETURNS FLOAT
AS
$$
    CASE
        WHEN amount IS NULL THEN 0.0
        WHEN amount < 1000 THEN amount * 0.05
        WHEN amount <= 5000 THEN amount * 0.10
        ELSE amount * 0.15
    END
$$;



SELECT 
    order_id,
    amount,
    discount_slab_sql(amount) AS discount
FROM orders;



CREATE OR REPLACE FUNCTION discount_slab_py(amount FLOAT)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'slab_discount'
AS
$$
def slab_discount (amount):
    if amount is None:
      return 0.0
    if amount < 1000:
      return amount * 0.05
    elif amount <= 5000:
      return amount *0.10
    else:
      return amount * 0.15
$$;


SELECT order_id, amount, discount_slab_py(amount)
FROM orders;

-----------------------------babjee 

--scalarudf

CREATE OR REPLACE FUNCTION FN_TEST(AMT NUMBER)
RETURNS DECIMAL(10,2)
LANGUAGE SQL
AS
$$
  CASE WHEN AMT >=3000 THEN AMT *.05
       WHEN  AMT >2000 THEN AMT *.1
       ELSE AMT * 0.15
  END 
  
$$

SELECT FN_TEST(4000) AS DISCOUNT;

SELECT
  ORDER_ID,
  AMOUNT,
  FN_TEST(AMOUNT) AS DISCOUNT
FROM ORDERS;


--table udf



CREATE OR REPLACE FUNCTION order_summary_by_status(p_status STRING)
RETURNS TABLE (
    STATUS STRING,TOTAL_ORDERS NUMBER,    TOTAL_AMOUNT NUMBER
)
AS
$$
    SELECT
        STATUS,
        COUNT(*) AS TOTAL_ORDERS,
        SUM(AMOUNT) AS TOTAL_AMOUNT
    FROM ORDERS
    WHERE STATUS = p_status     GROUP BY STATUS
$$;



SELECT
    STATUS,
    COUNT(*) AS TOTAL_ORDERS,
    SUM(AMOUNT) AS TOTAL_AMOUNT
FROM ORDERS
WHERE STATUS = 'DELIVERED'
GROUP BY STATUS;



SELECT *
FROM TABLE(order_summary_by_status('DELIVERED'));







q3---
CREATE OR REPLACE TABLE USER_REVIEWS (
REVIEW_ID INT,
USER_ID STRING,
PRODUCT_SKU STRING,
REGION STRING,
RATING INT,
REVIEW_TS TIMESTAMP_NTZ,
REVIEW_TEXT STRING
);



INSERT INTO USER_REVIEWS VALUES
(1,'U-001','SKU-HEADPH','APAC',5,'2025-11-10 09:12:00','Excellent sound quality and quick delivery.'),
(2,'U-002','SKU-TABLET','NA',2,'2025-11-10 10:20:00','Battery drains fast and support was unhelpful.'),
(3,'U-003','SKU-SHOES','EMEA',3,'2025-11-11 08:05:00','Comfort is okay, but size runs small.'),
(4,'U-004','SKU-JACKET','APAC',5,'2025-11-11 12:45:00','Loved the fit and the fabric feels premium.'),
(5,'U-005','SKU-COFFEE','EMEA',4,'2025-11-12 07:30:00','Good flavor, packaging could be better.'),
(6,'U-006','SKU-WATCH','NA',1,'2025-11-12 16:10:00','Stopped working in two days. Very disappointed.'),
(7,'U-007','SKU-SPEAKR','APAC',4,'2025-11-13 11:25:00','Great value for money and easy to pair.'),
(8,'U-008','SKU-SHIRT','EMEA',2,'2025-11-13 19:40:00','Stitching quality is poor for the price.'),
(9,'U-009','SKU-OIL1L','NA',4,'2025-11-14 14:05:00','Arrived on time and tastes fresh.'),
(10,'U-010','SKU-TABLET','APAC',1,'2025-11-14 20:15:00','Terrible experience. Would not recommend.');



SELECT * FROM USER_REVIEWS LIMIT 5;

SELECT
    REVIEW_ID,
    REVIEW_TEXT,
    SNOWFLAKE.CORTEX.SENTIMENT(REVIEW_TEXT) AS SENTIMENT
FROM USER_REVIEWS;


SELECT
    SNOWFLAKE.CORTEX.SENTIMENT(REVIEW_TEXT) AS SENTIMENT,
    COUNT(*) AS REVIEW_COUNT
FROM USER_REVIEWS
GROUP BY SENTIMENT;




q4--
CREATE OR REPLACE TABLE INSURANCE_CLAIMS (
CLAIM_ID INT,
POLICY_ID STRING,
CLAIM_TYPE STRING,
STATE STRING,
CLAIM_STATUS STRING,
CLAIM_DATE DATE,
CLAIM_AMOUNT NUMBER(12,2)
);


INSERT INTO INSURANCE_CLAIMS VALUES
(1,'P-1001','Health','KA','APPROVED','2025-09-01',1200.00),
(2,'P-1002','Auto','TX','PENDING','2025-09-02',800.00),
(3,'P-1003','Health','MH','APPROVED','2025-09-03',1500.00),
(4,'P-1004','Home','CA','REJECTED','2025-09-04',2000.00),
(5,'P-1005','Auto','WA','APPROVED','2025-09-05',700.00),
(6,'P-1006','Health','KA','PENDING','2025-09-06',1100.00),
(7,'P-1007','Home','NY','APPROVED','2025-09-07',1800.00),
(8,'P-1008','Auto','TX','APPROVED','2025-09-08',950.00),
(9,'P-1009','Health','MH','REJECTED','2025-09-09',1300.00),
(10,'P-1010','Home','CA','PENDING','2025-09-10',2100.00);


use database snowpark_db
use schema public
SHOW PROCEDURES LIKE 'PROCESS_INSURANCE_CLAIMS';


CALL PROCESS_INSURANCE_CLAIMS('Health', 1000, 'APPROVED');


q5-----
CREATE OR REPLACE TABLE SUPPORT_TICKETS (
    TICKET_ID INT,
    CUSTOMER_ID STRING,
    CHANNEL STRING,
    PRIORITY STRING,
    CREATED_AT TIMESTAMP_NTZ,
    DESCRIPTION STRING
);

INSERT INTO SUPPORT_TICKETS VALUES
(1,'C-001','Email','High','2025-10-20 09:05:00','Delivery is delayed and I need an update on the shipment.'),
(2,'C-002','Chat','High','2025-10-20 10:15:00','The product arrived defective and stops working after a few minutes.'),
(3,'C-003','Phone','Medium','2025-10-21 08:40:00','Customer support was excellent and resolved my issue quickly.'),
(4,'C-004','Email','Low','2025-10-21 12:30:00','I was charged twice on my card, please help with billing.'),
(5,'C-005','Chat','Medium','2025-10-22 11:10:00','The refund is not processed yet even after 7 days.'),
(6,'C-006','Phone','High','2025-10-22 16:55:00','App crashes during checkout; cannot complete purchase.'),
(7,'C-007','Email','Low','2025-10-23 09:20:00','Packaging was damaged but item is okay. Sharing feedback.'),
(8,'C-008','Chat','High','2025-10-23 18:05:00','Delivery agent did not call and marked order as failed delivery.'),
(9,'C-009','Email','Medium','2025-10-24 14:35:00','I need help changing my address on the order.'),
(10,'C-010','Phone','High','2025-10-24 19:10:00','Terrible service—support kept transferring me and never fixed the issue.');


SELECT TICKET_ID, DESCRIPTION
FROM SUPPORT_TICKETS;


-- Cortex Semantic Classification Categories:
-- 1. Delivery / Shipping Issue
-- 2. Product / Technical Issue
-- 3. Billing / Refund Issue
-- 4. Customer Service Experience


SELECT
    TICKET_ID,
    DESCRIPTION,
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        DESCRIPTION,
        ARRAY_CONSTRUCT(
            'Delivery / Shipping Issue',
            'Product / Technical Issue',
            'Billing / Refund Issue',
            'Customer Service Experience'
        )
    ) AS ASSIGNED_CATEGORY
FROM SUPPORT_TICKETS;




SELECT
    TICKET_ID,
    DESCRIPTION,
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        DESCRIPTION,
        ARRAY_CONSTRUCT(
            'Delivery or shipping delays, failed delivery, logistics problems',
            'Product defects, app crashes, technical or quality issues',
            'Billing problems, refunds, charges, payment or invoice issues',
            'Customer support experience, service quality, communication issues'
        )
    ) AS ASSIGNED_CATEGORY
FROM SUPPORT_TICKETS;


SELECT
    TICKET_ID,
    DESCRIPTION,
    ASSIGNED_CATEGORY
FROM (
    SELECT
        TICKET_ID,
        DESCRIPTION,
        SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
            DESCRIPTION,
            ARRAY_CONSTRUCT(
                'Delivery or shipping delays, failed delivery, logistics problems',
                'Product defects, app crashes, technical or quality issues',
                'Billing problems, refunds, charges, payment or invoice issues',
                'Customer support experience, service quality, communication issues'
            )
        ) AS ASSIGNED_CATEGORY
    FROM SUPPORT_TICKETS
)
WHERE ASSIGNED_CATEGORY LIKE 'Customer support%';


q6------
-- EVENT_LOGS
CREATE OR REPLACE TABLE EVENT_LOGS (
EVENT_ID INT,
EVENT_TS TIMESTAMP_NTZ,
SOURCE_APP STRING,
USER_ID STRING,
SESSION_ID STRING,
EVENT_DATA VARIANT
);

INSERT INTO EVENT_LOGS VALUES
(1,'2025-11-01 10:01:00','web','U1','S1',PARSE_JSON('{"action":"login","device":{"os":"Android","model":"Pixel"},"geo":{"country":"IN","city":"Bengaluru"}}')),
(2,'2025-11-01 10:05:00','web','U2','S2',PARSE_JSON('{"action":"view_item","item":{"sku":"SKU-HEADPH","category":"Electronics"},"geo":{"country":"US","city":"Austin"}}')),
(3,'2025-11-01 10:07:00','mobile','U2','S2',PARSE_JSON('{"action":"add_to_cart","cart":{"items":[{"sku":"SKU-HEADPH","qty":1},{"sku":"SKU-OIL1L","qty":2}]}}')),
(4,'2025-11-01 10:10:00','mobile','U3','S3',PARSE_JSON('{"action":"purchase","order":{"id":"O-9001","amount":15999.00,"currency":"INR"},"geo":{"country":"IN","city":"Mumbai"}}')),
(5,'2025-11-01 10:12:00','web','U4','S4',PARSE_JSON('{"action":"logout","geo":{"country":"UK","city":"London"}}')),
(6,'2025-11-02 09:01:00','web','U1','S5',PARSE_JSON('{"action":"view_item","item":{"sku":"SKU-SHOES","category":"Clothing"},"geo":{"country":"IN","city":"Delhi"}}')),
(7,'2025-11-02 09:05:00','mobile','U1','S5',PARSE_JSON('{"action":"add_to_cart","cart":{"items":[{"sku":"SKU-SHOES","qty":1}]}}')),
(8,'2025-11-02 09:08:00','mobile','U1','S5',PARSE_JSON('{"action":"purchase","order":{"id":"O-9002","amount":4599.00,"currency":"INR"},"geo":{"country":"IN","city":"Delhi"}}')),
(9,'2025-11-03 18:20:00','web','U5','S6',PARSE_JSON('{"action":"view_item","item":{"sku":"SKU-COFFEE","category":"Grocery"},"geo":{"country":"DE","city":"Berlin"}}')),
(10,'2025-11-03 18:25:00','web','U5','S6',PARSE_JSON('{"action":"support_chat","topic":"refund_status","geo":{"country":"DE","city":"Berlin"}}'));

-- USER_ACTIONS
CREATE OR REPLACE TABLE USER_ACTIONS (
ACTION_ID INT,
ACTION_TS TIMESTAMP_NTZ,
USER_ID STRING,
ACTION_TYPE STRING,
CHANNEL STRING,
AMOUNT NUMBER(10,2)
);

INSERT INTO USER_ACTIONS VALUES
(1,'2025-11-01 09:00:00','U1','LOGIN','web',0.00),
(2,'2025-11-01 09:10:00','U2','VIEW_ITEM','web',0.00),
(3,'2025-11-01 09:15:00','U2','ADD_TO_CART','mobile',0.00),
(4,'2025-11-01 09:20:00','U3','PURCHASE','mobile',15999.00),
(5,'2025-11-01 09:30:00','U4','LOGOUT','web',0.00),
(6,'2025-11-02 10:00:00','U1','VIEW_ITEM','web',0.00),
(7,'2025-11-02 10:05:00','U1','ADD_TO_CART','mobile',0.00),
(8,'2025-11-02 10:10:00','U1','PURCHASE','mobile',4599.00),
(9,'2025-11-03 18:00:00','U5','VIEW_ITEM','web',0.00),
(10,'2025-11-03 18:10:00','U5','SUPPORT_CHAT','web',0.00);



------today json questions



CREATE OR REPLACE TABLE JSON_EVENTS (
  EVENT_ID    INT,
  INGEST_TS   TIMESTAMP_NTZ,
  PAYLOAD     VARIANT
);

-- =====================================================
-- Create table
-- =====================================================
CREATE OR REPLACE TABLE JSON_EVENTS (
  EVENT_ID    INT,
  INGEST_TS   TIMESTAMP_NTZ,
  PAYLOAD     VARIANT
);

-- =====================================================
-- Insert data (10 rows, no UNION ALL)
-- =====================================================

INSERT INTO JSON_EVENTS VALUES
(1,'2025-05-01 09:00:00',
 PARSE_JSON('{"user":{"id":"U001","name":"Asha"},"device":{"os":"Android","app_version":"1.2.0"},"event":{"type":"login","success":true},"geo":{"country":"IN","city":"Bengaluru"}}'));

INSERT INTO JSON_EVENTS VALUES
(2,'2025-05-01 09:05:00',
 PARSE_JSON('{"user":{"id":"U002","name":"Ravi"},"device":{"os":"iOS","app_version":"1.3.1"},"event":{"type":"login","success":false},"geo":{"country":"IN","city":"Chennai"}}'));

INSERT INTO JSON_EVENTS VALUES
(3,'2025-05-01 09:10:00',
 PARSE_JSON('{"user":{"id":"U003","name":"Mia"},"device":{"os":"Web","app_version":"2.0.0"},"event":{"type":"view_item","item_id":"SKU-001"},"geo":{"country":"US","city":"Austin"}}'));

INSERT INTO JSON_EVENTS VALUES
(4,'2025-05-01 09:15:00',
 PARSE_JSON('{"user":{"id":"U001","name":"Asha"},"device":{"os":"Android","app_version":"1.2.0"},"event":{"type":"add_to_cart","item_id":"SKU-001","qty":2},"geo":{"country":"IN","city":"Bengaluru"}}'));

INSERT INTO JSON_EVENTS VALUES
(5,'2025-05-01 09:20:00',
 PARSE_JSON('{"user":{"id":"U004","name":"Noah"},"device":{"os":"Web","app_version":"2.0.0"},"event":{"type":"purchase","order_total":120.50},"geo":{"country":"US","city":"Seattle"}}'));

INSERT INTO JSON_EVENTS VALUES
(6,'2025-05-01 09:25:00',
 PARSE_JSON('{"user":{"id":"U005","name":"Emma"},"device":{"os":"Android","app_version":"1.2.2"},"event":{"type":"login","success":true},"geo":{"country":"UK","city":"London"}}'));

INSERT INTO JSON_EVENTS VALUES
(7,'2025-05-01 09:30:00',
 PARSE_JSON('{"user":{"id":"U006","name":"Oliver"},"device":{"os":"iOS","app_version":"1.3.1"},"event":{"type":"view_item","item_id":"SKU-002"},"geo":{"country":"UK","city":"Manchester"}}'));

INSERT INTO JSON_EVENTS VALUES
(8,'2025-05-01 09:35:00',
 PARSE_JSON('{"user":{"id":"U003","name":"Mia"},"device":{"os":"Web","app_version":"2.0.0"},"event":{"type":"purchase","order_total":89.99},"geo":{"country":"US","city":"Austin"}}'));

INSERT INTO JSON_EVENTS VALUES
(9,'2025-05-01 09:40:00',
 PARSE_JSON('{"user":{"id":"U007","name":"Sophia"},"device":{"os":"Android","app_version":"1.4.0"},"event":{"type":"add_to_cart","item_id":"SKU-010","qty":1},"geo":{"country":"FR","city":"Paris"}}'));

INSERT INTO JSON_EVENTS VALUES
(10,'2025-05-01 09:45:00',
 PARSE_JSON('{"user":{"id":"U008","name":"Ethan"},"device":{"os":"Web","app_version":"2.1.0"},"event":{"type":"login","success":true},"geo":{"country":"SG","city":"Singapore"}}'));

-- =====================================================
-- Validation (allowed)
-- =====================================================
SELECT COUNT(*) FROM JSON_EVENTS;


create or replace stage json_stagee
file_format=(type='JSON');

list @json_stagee

create or replace file format jsonformat
type='JSON';




CREATE OR REPLACE TABLE JSON_EVENTS(raw_file variant);
copy into  JSON_EVENTS 
from @json_stagee
file_format=jsonformat;

select * from json_events


SELECT COUNT(*) FROM JSON_EVENTS;





DESC TABLE JSON_EVENTS;


SELECT
  RAW_FILE:event_data:user:id::STRING      AS USER_ID,
  RAW_FILE:event_data:user:name::STRING    AS USER_NAME,
  RAW_FILE:event_data:device:os::STRING    AS DEVICE_OS,
  RAW_FILE:event_data:event:type::STRING   AS EVENT_TYPE,
  RAW_FILE:event_data:geo:country::STRING  AS COUNTRY,
  RAW_FILE:event_data:geo:city::STRING     AS CITY
FROM JSON_EVENTS
LIMIT 10;




select 
raw_file:"id"::int as Id,
raw_file:"event_time"::string as Event_time,
raw_file:"event_data"."user"."id"::string as user_id,
raw_file:"event_data"."user"."name"::string as user_name,
raw_file:"event_data"."device"."os"::string as os_type,
raw_file:"event_data"."device"."app_version"::string as app_version,
raw_file:"event_data"."event"."item_id"::string as event_item_id
from json_events;



SELECT
  RAW_FILE['event_data']['user']['id']::STRING      AS USER_ID,
  RAW_FILE['event_data']['user']['name']::STRING    AS USER_NAME,
  RAW_FILE['event_data']['device']['os']::STRING    AS DEVICE_OS,
  RAW_FILE['event_data']['event']['type']::STRING   AS EVENT_TYPE,
  RAW_FILE['event_data']['geo']['country']::STRING  AS COUNTRY,
  RAW_FILE['event_data']['geo']['city']::STRING     AS CITY
FROM JSON_EVENTS
LIMIT 10;



SELECT
  RAW_FILE:event_data:user:id::STRING      AS USER_ID,
  RAW_FILE:event_data:user:name::STRING    AS USER_NAME,
  RAW_FILE:event_data:device:os::STRING    AS DEVICE_OS,
  RAW_FILE:event_data:event:type::STRING   AS EVENT_TYPE,
  RAW_FILE:event_data:geo:country::STRING  AS COUNTRY,
  RAW_FILE:event_data:geo:city::STRING     AS CITY
FROM JSON_EVENTS
LIMIT 10;

---task2

CREATE OR REPLACE TABLE SALES_CUSTOMERS (
  CUSTOMER_ID  STRING,
  FULL_NAME    STRING,
  SEGMENT      STRING,
  COUNTRY      STRING,
  SIGNUP_DATE  DATE,
  STATUS       STRING
);
INSERT INTO SALES_CUSTOMERS VALUES
('C001','Asha Mehta','Consumer','IN','2025-01-05','ACTIVE'),
('C002','Ravi Nair','SMB','IN','2025-02-12','ACTIVE'),
('C003','Mia Johnson','Consumer','US','2025-01-18','ACTIVE'),
('C004','Noah Williams','Enterprise','US','2024-12-10','ACTIVE'),
('C005','Emma Brown','Consumer','UK','2025-03-02','ACTIVE'),
('C006','Oliver Smith','SMB','UK','2025-03-12','INACTIVE'),
('C007','Liam Garcia','Consumer','US','2025-04-01','ACTIVE'),
('C008','Sophia Martin','SMB','FR','2025-02-21','ACTIVE'),
('C009','Ethan Lee','Enterprise','SG','2025-01-28','ACTIVE'),
('C010','Isabella Kim','Consumer','SG','2025-03-20','ACTIVE');
CREATE OR REPLACE TABLE SALES_ORDERS (
  ORDER_ID      STRING,
  CUSTOMER_ID   STRING,
  ORDER_DATE    DATE,
  CHANNEL       STRING,
  ORDER_TOTAL   NUMBER(10,2),
  TAX_AMOUNT    NUMBER(10,2),
  ORDER_STATUS  STRING
);
INSERT INTO SALES_ORDERS VALUES
('O1001','C001','2025-04-01','Web',1499.00, 90.00,'DELIVERED'),
('O1002','C002','2025-04-02','Mobile', 799.00, 48.00,'DELIVERED'),
('O1003','C003','2025-04-02','Web', 120.00,  7.20,'CANCELLED'),
('O1004','C004','2025-04-03','Partner',5200.00,312.00,'DELIVERED'),
('O1005','C005','2025-04-03','Web', 980.00, 58.80,'DELIVERED'),
('O1006','C006','2025-04-04','Mobile',450.00, 27.00,'RETURNED'),
('O1007','C007','2025-04-05','Web',2100.00,126.00,'DELIVERED'),
('O1008','C008','2025-04-05','Web',1750.00,105.00,'DELIVERED'),
('O1009','C009','2025-04-06','Partner',12500.00,750.00,'DELIVERED'),
('O1010','C010','2025-04-06','Mobile',1300.00,78.00,'DELIVERED');


-----------------------------------------------------------------------


----ganesh question 2 whatsapp
CREATE OR REPLACE TABLE demo_users (
  user_id STRING,
  user_name STRING,
  signup_date DATE
);

INSERT INTO demo_users VALUES
('U001','Amit','2025-01-01'),
('U002','Neha','2025-01-03'),
('U003','Ravi','2025-01-05');


SELECT * FROM demo_users;


SELECT *
FROM demo_users
ORDER BY signup_date;


----------------------------------------------------------------------------------------------
-- 9th jan 2025
















