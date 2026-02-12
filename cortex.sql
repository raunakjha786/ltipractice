alter account set cortex_enabled_cross_region = 'AWS_US';

select snowflake.cortex.complete(
'snowflake-arctic',
'Explain what a primary key is in simple terms.'
) AS explanation;


select snowflake.cortex.complete(
'snowflake-arctic',
'summarize monthly sales trend in simple business language.'
) AS explanation;


create schema snowpark_db.cortext;

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

SELECT SNOWFLAKE.CORTEX.COMPLETE(
'snowflake-arctic',
'Simplify the following sentence so that a non technical person can understand it : The discripences observed during the audit require imediate rectification.'
) AS simple_text;


SELECT SNOWFLAKE.CORTEX.COMPLETE(
'snowflake-arctic',
'Rewrite thesentence in a fprmal and professional tone suitable for offical email :please finish this work fast and update me .'
) AS formal_text;


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






select snowflake.cortex.complete(
'snowflake-arctic',
'Simplify the following sentence so that a  non-technical person can understand it:
the discrepencies observed during the audit require immediate retifications'
) as simple_text;

select snowflake.cortex.complete(
'snowflake-arctic',
'Rewrite this sentence in a formal and professional tone suitable for official email:
please finish the work fast and update me 
'
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



select snowflake.cortex.classify_text(
'Vendor invoice does not have GST registration number',
['GST Non-Compliance','Documentation Issue','Pricing Issue']
)['label']::string as audit_finding;


select 
snowflake.cortex.translate(
'cow is our mother',
'en',
'hi'
) as translate_text;


SELECT id, raw_text, SNOWFLAKE.CORTEX.TRANSLATE(raw_text, 'en', 'fr') AS comment_french,
SNOWFLAKE.CORTEX.TRANSLATE(raw_text, 'en', 'hi') AS comment_hindi,
SNOWFLAKE.CORTEX.TRANSLATE(raw_text, 'en', 'ben') AS comment_marathi

  FROM feedback;



select snowflake.cortex.extract_answer(
'The order was shipped on 12th june and delivered on 15th june.',
'when was the order delivered?'
)as answer;


select 
ticket_text,
snowflake.cortex.extract_answer(
ticket_text,
'when was the order delivered'
) as delivery_date
FROM support_ticket;



SHOW TABLES IN SCHEMA SNOWPARK_DB.CORTEXT;
SHOW VIEWS IN SCHEMA SNOWPARK_DB.CORTEXT;

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
    "query": "How do I get my money back!",
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
        '{"query": "How do I get my money back?", "columns": ["id", "content"], "limit": 2}'
    )
):results));





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








import streamlit as st
from snowflake.snowpark.context import get_active_session
session = get_active_session()
st.title("Cortex Chat")
if "result" not in st.session_state:
    st.session_state.result = ""

prompt = st.text_input("Ask something")

if prompt:
    sql = "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', ?)"

    with st.spinner("Thinking..."):
        response = session.sql(sql, params=[prompt]).collect()
        st.session_state.result = response[0][0]
if st.session_state.result:
    st.write(st.session_state.result)


import streamlit as st
from snowflake.snowpark.context import get_active_session


session = get_active_session()

st.title("Cortex Chat")


if "result" not in st.session_state:
    st.session_state.result = ""

prompt = st.text_input("Ask something")

if prompt:
    sql = "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', ?)"
    
    with st.spinner("Thinking..."):
      
        response = session.sql(sql, params=[prompt]).collect()
        st.session_state.result = response[0][0]

if st.session_state.result:
    st.write(st.session_state.result)





