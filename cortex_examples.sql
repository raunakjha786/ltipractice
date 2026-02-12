select snowflake.cortex.complete(
'snowflake-arctic',
'Explain what is primary key in in simple terms'
) as explanation;
--complete

select snowflake.cortex.complete(
'snowflake-arctic',
'Summarize monthly sales trend in simple business language'
) as summary;


  CREATE OR REPLACE TABLE feedback (
  id INT,
  raw_text STRING
);

INSERT INTO feedback VALUES
(1, 'The builder is delaying work unnecessarily'),
(2, 'Too many irrelevant questions were raised in the meeting'),
(3, 'Finish this immediately');


SELECT id, raw_text,SNOWFLAKE.CORTEX.COMPLETE(
'snowflake-arctic',
'Rewrite the following text in a polite and professional tone.' || raw_text
) AS rewritten_text
FROM feedback;


select snowflake.cortex.complete(
'snowflake-arctic',
'Simplify the following sentence so that a  non-technical person can understand it:
the discrepencies observed during the audit require immediate retifications'
) as simple_text;



SELECT
  SNOWFLAKE.CORTEX.COMPLETE(
    'snowflake-arctic',
    'Act as a senior corporate communication expert.
     Rewrite the text below to be:
     - Professional
     - Neutral
     - Suitable for audit or board-level communication

     Text:
     The contractor is careless and not cooperating.'
  ) AS board_ready_text;

--classification
select snowflake.cortex.classify_text(
'Booked flight to Delhi for client meeting',
['Travel','Meals','Office']
)['label']::string as  simple_classification;

select snowflake.cortex.classify_text(
'Lunch with vendor',
['Travel','Meals','Professional Fees']
)['label']::string as  accounting_expenses;

select snowflake.cortex.classify_text(
'I want to apply for earned leave next Friday',
['Leave','Payroll','Recruitment','Policy']
)['label']::string as  hr_request;


select snowflake.cortex.classify_text(
'Please cancel y subscription and refund the amount',
['Cancellation','Refund','Enquiry','Complaint']
)['label']::string as  cancelation;

  
--translate
select 
snowflake.cortex.translate(
'welcome to snowflake Cortex',
'en',
'hi'
) as translate_text;




select 
snowflake.cortex.translate(
'स्नोफ्लेक कॉर्टेक्स में आपका स्वागत है',
'hi',
'en'
) as translate_text;


CREATE OR REPLACE TABLE feedback (
  id INT,
  comment STRING
);

INSERT INTO feedback VALUES
(1, 'The service was excellent'),
(2, 'Delivery was delayed'),
(3, 'Customer support is helpful');


SELECT id, comment, SNOWFLAKE.CORTEX.TRANSLATE(comment, 'en', 'fr') AS comment_french,
SNOWFLAKE.CORTEX.TRANSLATE(comment, 'en', 'hi') AS comment_hindi,
SNOWFLAKE.CORTEX.TRANSLATE(comment, 'en', 'mar') AS comment_marathi

  FROM feedback;

--Auto Language Detection (Advanced)

SELECT SNOWFLAKE.CORTEX.TRANSLATE(
  'La qualité du produit est excellente',
  NULL,
  'en'
) AS TEXT;

--Multi-Language Reporting

SELECT SNOWFLAKE.CORTEX.TRANSLATE( COMMENT,'en', 'hi') Hindi,
       SNOWFLAKE.CORTEX.TRANSLATE( COMMENT,'en', 'ta') Tamil,
       SNOWFLAKE.CORTEX.TRANSLATE( COMMENT,'en', 'fr') french
from feedback;


--sentiment_score
SELECT 
    SNOWFLAKE.CORTEX.SENTIMENT('The service was excellent') AS sentiment_score,
    CASE 
        WHEN SNOWFLAKE.CORTEX.SENTIMENT('The service was excellent') > 0.2 THEN 'Positive'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT('The service was excellent') < -0.2 THEN 'Negative'
        ELSE 'Neutral'
    END AS sentiment_label
FROM FEEDBACK;

----------------
select snowflake.cortex.extract_answer(
'The order was shipped on 12th june and delivered on 15th june.',
'when was the order delivered?'
)as answer;

-----------------------------------------------
--ques
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




  select ticket_id,snowflake.cortex.extract_answer(
 ticket_text,
 'when was the order delivered'
 )as delivery_date
 from support_ticket;
-----------------------------------------------------------------

  CREATE OR REPLACE TABLE faq_docs (
  id INT,
  content STRING
);

INSERT INTO faq_docs VALUES
(1, 'You can reset your password using the Forgot Password link'),
(2, 'Refunds are processed within 7 business days'),
(3, 'Contact support by emailing support@company.com');


create or replace cortex search service faq_search
on content
warehouse=compute_wh
target_lag='1 hour'
as
select id,content from faq_docs;


show cortex search services;


grant usage on cortex search service faq_search to accountadmin;


describe cortex search service faq_search;



select snowflake.cortex.search_preview(
  'faq_search',
  '{
    "query": "refunds are processed in how many days?",
    "columns": ["id", "content"],
    "limit": 2
  }'
) as output;



SELECT 
    value:id::string AS id,
    value:content::string AS content,
    value:"@scores".cosine_similarity::float AS semantic_score
FROM TABLE(FLATTEN(INPUT => PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'faq_search',
        '{"query": "refunds are processed in how many days?", "columns": ["id", "content"], "limit": 2}'
    )
):results));


SELECT 
    value:id::string AS id,
    value:content::string AS content,
    value:"@scores".cosine_similarity::float AS semantic_score
FROM TABLE(FLATTEN(INPUT => PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'faq_search',
        '{"query": "How do i repair my laptop", "columns": ["id", "content"], "limit": 2}'
    )
):results));

--------------------------------------
CREATE OR REPLACE TABLE company_docs (
    id INT,
    category STRING,
    search_text STRING
);

ALTER TABLE company_docs SET CHANGE_TRACKING = TRUE;

-- Insert sample data
INSERT INTO company_docs (id, category, search_text) VALUES
(1, 'Security', 'To enable Multi-Factor Authentication (MFA), go to settings and select Security.'),
(2, 'Payroll', 'Direct deposit changes take one full pay cycle to update in the system.'),
(3, 'IT', 'VPN access requires the GlobalProtect client installed on your company laptop.');


CREATE OR REPLACE CORTEX SEARCH SERVICE doc_search_service
  ON search_text               
  ATTRIBUTES id, category      
  WAREHOUSE = compute_wh
  TARGET_LAG = '1 minute'
  AS 
  SELECT id, category, search_text FROM company_docs;


SET user_query = 'How do I set up MFA?';

SET retrieved_context = (
    SELECT LISTAGG(value:search_text::string, '\n\n') 
    FROM TABLE(FLATTEN(INPUT => PARSE_JSON(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            'doc_search_service', 
            '{"query": "' || $user_query || '", "columns": ["search_text"], "limit": 2}'
        )
    ):results))
);

select $retrieved_context;

select snowflake.cortex.complete(
'llama3-70b',
concat(
'answer the question using only the following facts:',
'\n\nFacts:', $retrieved_context,
'\n\nQuestion:', $user_query
)
) as ai_response;




 









---today mock1
cortex code 
--verify the data 
SELECT * FROM kb_documents;



-- create cortex srearch service 

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



--basic semantic search (raw json output)

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




--Parse SEARCH_PREVIEW JSON into rows (IMPORTANT)

WITH search_result AS (
    SELECT PARSE_JSON(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
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
        )
    ) AS result
)
SELECT
    f.value:document:DOC_ID::STRING     AS DOC_ID,
    f.value:document:TITLE::STRING      AS TITLE,
    f.value:document:CATEGORY::STRING   AS CATEGORY,
    f.value:document:CONTENT::STRING    AS CONTENT,
    f.value:score::FLOAT                AS SCORE
FROM search_result,
LATERAL FLATTEN(input => result:results) f
ORDER BY SCORE DESC;


 Validate semantic correctness (recommended)

SELECT
    DOC_ID,
    TITLE,
    CATEGORY
FROM (
    SELECT
        f.value:document:DOC_ID::STRING AS DOC_ID,
        f.value:document:TITLE::STRING  AS TITLE,
        f.value:document:CATEGORY::STRING AS CATEGORY,
        f.value:score::FLOAT AS SCORE
    FROM (
        SELECT PARSE_JSON(
            SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                'kb_search_service',
                OBJECT_CONSTRUCT(
                    'query', 'refund processing time',
                    'columns', ARRAY_CONSTRUCT('DOC_ID','TITLE','CATEGORY'),
                    'limit', 5
                )::string
            )
        ) r
    ),
    LATERAL FLATTEN(input => r:results) f
)
WHERE CATEGORY IN ('Policy','FAQ');

 