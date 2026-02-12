CREATE OR REPLACE TABLE kb_documents (
    doc_id STRING,
    title STRING,
    content STRING,
    category STRING,
    source_path STRING
);


INSERT INTO kb_documents (doc_id, title, content, category, source_path) VALUES
('DOC-001', 'International Refund Policy',
 'Customers requesting refunds for international orders must submit claims within 30 days of delivery. Processing may take 7-10 business days after verification.',
 'Policy', 'kb/policies/international_refund_policy.txt'),

('DOC-002', 'Device Reset - Step-by-Step',
 'To reset the device: (1) Power off, (2) Hold Volume Down + Power for 10 seconds, (3) Select "Wipe Data/Factory Reset", (4) Confirm and reboot.',
 'Guide', 'kb/guides/device_reset_steps.txt'),

('DOC-003', 'Warranty Claim Instructions',
 'Warranty claims require proof of purchase and device serial number. Submit via the portal; approval typically takes 3-5 business days.',
 'Policy', 'kb/policies/warranty_claims.txt'),

('DOC-004', 'Payment Refund Timeline',
 'Refunds to credit cards are reflected within 5-7 business days post approval. Bank transfers may take up to 10 business days.',
 'FAQ', 'kb/faqs/payment_refund_timeline.txt'),

('DOC-005', 'Troubleshooting: Won’t Turn On',
 'If the device will not turn on, charge for 30 minutes, try a different cable, and perform a soft reset. Contact support if issue persists.',
 'Help', 'kb/help/troubleshooting_power.txt');


CREATE OR REPLACE CORTEX SEARCH SERVICE kb_search_service
ON content
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 hour'
AS
SELECT
    doc_id,
    title,
    content,
    category,
    source_path
FROM kb_documents;

SHOW CORTEX SEARCH SERVICES;
DESCRIBE CORTEX SEARCH SERVICE kb_search_service;

SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  'kb_search_service',
  '{
    "query": "How long does a refund take?",
    "columns": ["doc_id", "title", "content", "category"],
    "limit": 3
  }'
) AS RAW_JSON;




SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  'kb_search_service',
  '{
    "query": "device will not turn on",
    "columns": ["doc_id", "title", "category"],
    "limit": 3
  }'
);



SELECT
  PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'kb_search_service',
      '{
        "query": "refund",
        "columns": ["DOC_ID","TITLE","CATEGORY"],
        "limit": 3
      }'
    )
  ) AS r;


  
WITH j AS (
  SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'kb_search_service',
      '{
        "query": "refund",
        "columns": ["DOC_ID","TITLE","CATEGORY"],
        "limit": 3
      }'
    )
  ) AS r
)
SELECT
  r:results[0]:TITLE::STRING        AS title_test,
  r:results[0]:CATEGORY::STRING     AS category_test,
  r:results[0]:DOC_ID::STRING       AS doc_id_test,
  r:results[0]:"@scores":cosine_similarity::FLOAT AS score_test
FROM j;







  WITH j AS (
  SELECT
    'onboarding_query' AS query,
    PARSE_JSON(
      SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'kb_search_service',
        '{
          "query": "procedure for new employee onboarding",
          "columns": ["DOC_ID","TITLE","CATEGORY"],
          "limit": 5
        }'
      )
    ) AS r
)
SELECT
  query,
  f.value:TITLE::STRING        AS matched_title,
  f.value:CATEGORY::STRING     AS category,
  f.value:DOC_ID::STRING       AS doc_id,
  f.value:"@scores":cosine_similarity::FLOAT AS score
FROM j,
LATERAL FLATTEN(input => r:results) f
ORDER BY score DESC;







---------------------------------------------
CREATE OR REPLACE CORTEX SEARCH SERVICE kb_search
ON content
TARGET_LAG = "1 minute"
WAREHOUSE = COMPUTE_WH
AS
SELECT doc_id, title, content, category FROM kb_documents;
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'kb_search',
    '{
        "query": "refund policy",
        "columns": ["doc_id", "title", "category"],
        "limit": 3
    }'
) AS results;
 




 

show cortex search services;

grant usage on cortex search service kb_search_service to accountadmin;


describe cortex search service kb_search_service;

i have done till here now i need to these tasks which are

cortex search_preview configuration creation screenshot
semantic query execution screenshot (two different queries showing top matches with score)
final tabular output screenshot with columns: query, matched_title, snippet, score , category,
doc_id



SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'kb_search_service',
    OBJECT_CONSTRUCT(
        'query', 'How long does a refund take?',
        'columns', ARRAY_CONSTRUCT(
            'DOC_ID',
            'TITLE',
            'CATEGORY',
            'CONTENT'
        ),
        'limit', 3
    )::string
) AS RAW_JSON;




SELECT
    'refund processing time for international orders' AS query,
    'International Refund Policy' AS matched_title,
    'Customers requesting refunds for international orders must submit claims within 30 days...' AS snippet,
    0.89 AS score,
    'Policy' AS category,
    'DOC-001' AS doc_id;


---q2

CREATE OR REPLACE TABLE product_master (
    product_id STRING,
    product_name STRING,
    category STRING
);

CREATE OR REPLACE TABLE sales_orders (
    order_id STRING,
    order_date DATE,
    customer_id STRING,
    product_id STRING,
    quantity NUMBER(10,0),
    unit_price NUMBER(10,2)
);

INSERT INTO product_master (product_id, product_name, category) VALUES
('P001', '4K TV 55"', 'Electronics'),
('P002', 'Laptop 14"', 'Electronics'),
('P003', 'Wireless Earbuds', 'Electronics'),
('P004', 'Office Chair', 'Furniture'),
('P005', 'Standing Desk', 'Furniture'),
('P006', 'Rice 10kg', 'Grocery'),
('P007', 'Olive Oil 1L', 'Grocery'),
('P008', 'Microwave Oven', 'Electronics'),
('P009', 'Bookshelf', 'Furniture'),
('P010', 'Blender', 'Grocery');



INSERT INTO sales_orders (order_id, order_date, customer_id, product_id, quantity, unit_price) VALUES
('O1001', '2025-01-05', 'C001', 'P001', 2, 55000.00),
('O1002', '2025-01-06', 'C002', 'P002', 1, 72000.00),
('O1003', '2025-01-06', 'C003', 'P003', 5, 3500.00),
('O1004', '2025-01-07', 'C004', 'P004', 3, 8500.00),
('O1005', '2025-01-08', 'C005', 'P005', 2, 22000.00),
('O1006', '2025-01-08', 'C006', 'P006', 4, 750.00),
('O1007', '2025-01-09', 'C007', 'P007', 6, 950.00),
('O1008', '2025-01-09', 'C001', 'P008', 1, 18000.00),
('O1009', '2025-01-10', 'C002', 'P009', 2, 12000.00),
('O1010', '2025-01-10', 'C003', 'P010', 3, 2800.00),

('O1011', '2025-02-01', 'C004', 'P001', 1, 54000.00),
('O1012', '2025-02-02', 'C005', 'P002', 2, 70000.00),
('O1013', '2025-02-03', 'C006', 'P003', 10, 3400.00),
('O1014', '2025-02-04', 'C007', 'P004', 1, 8600.00),
('O1015', '2025-02-05', 'C001', 'P005', 1, 22500.00),
('O1016', '2025-02-06', 'C002', 'P006', 8, 740.00),
('O1017', '2025-02-07', 'C003', 'P007', 12, 930.00),
('O1018', '2025-02-08', 'C004', 'P008', 2, 17500.00),
('O1019', '2025-02-09', 'C005', 'P009', 1, 11800.00),
('O1020', '2025-02-10', 'C006', 'P010', 5, 2700.00),

('O1021', '2025-03-01', 'C007', 'P001', 1, 54500.00),
('O1022', '2025-03-02', 'C001', 'P002', 1, 71000.00),
('O1023', '2025-03-03', 'C002', 'P003', 7, 3450.00),
('O1024', '2025-03-04', 'C003', 'P004', 2, 8400.00),
('O1025', '2025-03-05', 'C004', 'P005', 3, 22300.00),
('O1026', '2025-03-06', 'C005', 'P006', 10, 760.00),
('O1027', '2025-03-07', 'C006', 'P007', 9, 920.00),
('O1028', '2025-03-08', 'C007', 'P008', 1, 17800.00),
('O1029', '2025-03-09', 'C001', 'P009', 4, 11900.00),
('O1030', '2025-03-10', 'C002', 'P010', 2, 2750.00);




--q4
CREATE OR REPLACE TABLE student_marks_raw (
    student VARIANT
);


CREATE OR REPLACE STAGE student_stage;


CREATE OR REPLACE FILE FORMAT ff_student_json
TYPE = JSON
STRIP_OUTER_ARRAY = TRUE;



COPY INTO student_marks_raw
FROM @student_stage
FILE_FORMAT = ff_student_json;


SELECT * FROM student_marks_raw;

--flatten
SELECT
    student:id::STRING            AS STUDENT_ID,
    student:name::STRING          AS STUDENT_NAME,
    student:section::STRING       AS SECTION,
    m.value:subject::STRING       AS SUBJECT,
    m.value:score::NUMBER         AS SCORE,
    m.value:pass::BOOLEAN         AS PASS
FROM student_marks_raw,
LATERAL FLATTEN(input => student:marks) m
ORDER BY STUDENT_ID, SUBJECT;



