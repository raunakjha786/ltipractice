--json_practice flatten and without flatten everythinh

CREATE OR REPLACE TABLE customer_events_raw (
    event VARIANT
);

CREATE OR REPLACE STAGE customer_events_stage;


CREATE OR REPLACE FILE FORMAT ff_json_events
TYPE = JSON
STRIP_OUTER_ARRAY = TRUE;


LIST @customer_events_stage;


COPY INTO customer_events_raw
FROM @customer_events_stage/customers.json
FILE_FORMAT = (FORMAT_NAME = ff_json_events);


select * from customer_events_raw

DESC TABLE customer_events_raw;

--dotnotation (extract nested fields)
SELECT
  event:event_id::INT        AS event_id,
  event:user_id::STRING     AS user_id,
  event:event_type::STRING  AS event_type,
  event:device::STRING      AS device
FROM customer_events_raw;

--bracket_notation
SELECT
  event['event_id']::INT        AS event_id,
  event['user_id']::STRING     AS user_id,
  event['event_type']::STRING  AS event_type
FROM customer_events_raw;


--handle missing fields
SELECT
  event:user_id::STRING AS user_id,
  event:amount::NUMBER AS amount
FROM customer_events_raw;



--filtering
SELECT *
FROM customer_events_raw
WHERE event:event_type::STRING = 'purchase';

---total purchase amount per user
SELECT
  event:user_id::STRING AS user_id,
  SUM(event:amount::NUMBER) AS total_spent
FROM customer_events_raw
WHERE event:event_type::STRING = 'purchase'
GROUP BY user_id;

--- filtering with two fields
SELECT
  event:user_id::STRING       AS user_id,
  event:event_type::STRING    AS event_type,
  event:device::STRING        AS device,
  event:amount::NUMBER        AS amount
FROM customer_events_raw
WHERE event:event_type::STRING IN ('login','purchase');




-------------------------------------------------------



--flatten 

CREATE OR REPLACE TABLE orders_complex (
  data VARIANT
);


CREATE OR REPLACE STAGE orders_stage;


CREATE OR REPLACE FILE FORMAT ff_orders_json
TYPE = JSON
STRIP_OUTER_ARRAY = TRUE;

COPY INTO orders_complex
FROM @orders_stage/customer_json.json
FILE_FORMAT = (FORMAT_NAME = ff_orders_json);

select * from orders_complex


----extract no array fields no flatten
SELECT
  data:order_id::STRING                 AS order_id,
  data:customer:id::STRING              AS customer_id,
  data:customer:name::STRING            AS customer_name,
  data:shipping:address:city::STRING    AS city,
  data:shipping:address:country::STRING AS country
FROM orders_complex;


--flatten the item array (core concept)
SELECT
  data:order_id::STRING        AS order_id,
  f.value:sku::STRING          AS sku,
  f.value:product::STRING      AS product,
  f.value:qty::INT             AS quantity,
  f.value:price::NUMBER        AS price
FROM orders_complex,
LATERAL FLATTEN(input => data:items) f;

------------calculate item level total
SELECT
  data:order_id::STRING AS order_id,
  f.value:product::STRING AS product,
  f.value:qty::INT * f.value:price::NUMBER AS item_total
FROM orders_complex,
LATERAL FLATTEN(input => data:items) f;

--flatten the payments array (second array)
SELECT
  data:order_id::STRING      AS order_id,
  p.value:method::STRING     AS payment_method,
  p.value:amount::NUMBER     AS amount
FROM orders_complex,
LATERAL FLATTEN(input => data:payments) p;

--combine flatten parent +child
SELECT
  data:order_id::STRING AS order_id,
  data:customer:name::STRING AS customer_name,
  f.value:product::STRING AS product,
  f.value:qty::INT AS quantity
FROM orders_complex,
LATERAL FLATTEN(input => data:items) f;

-----------------------------------------------------------------------

--json_mock1

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


SELECT COUNT(*) FROM student_marks_raw;

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

--dotnotation
SELECT
    student:id::STRING       AS STUDENT_ID,
    student:name::STRING     AS STUDENT_NAME,
    student:class::STRING    AS CLASS,
    student:section::STRING  AS SECTION
FROM student_marks_raw;



--flatten



--mostimp query
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

--avergae score per student
SELECT
    student:id::STRING AS STUDENT_ID,
    AVG(m.value:score::NUMBER) AS AVG_SCORE
FROM student_marks_raw,
LATERAL FLATTEN(input => student:marks) m
GROUP BY STUDENT_ID
ORDER BY AVG_SCORE DESC;

--count failes=d subjects 
SELECT
    student:id::STRING AS STUDENT_ID,
    COUNT(*) AS FAILED_SUBJECTS
FROM student_marks_raw,
LATERAL FLATTEN(input => student:marks) m
WHERE m.value:pass::BOOLEAN = FALSE
GROUP BY STUDENT_ID;


--toppers per subject
SELECT
    SUBJECT,
    STUDENT_NAME,
    SCORE
FROM (
    SELECT
        m.value:subject::STRING AS SUBJECT,
        student:name::STRING AS STUDENT_NAME,
        m.value:score::NUMBER AS SCORE,
        ROW_NUMBER() OVER (
            PARTITION BY m.value:subject::STRING
            ORDER BY m.value:score::NUMBER DESC
        ) AS rn
    FROM student_marks_raw,
    LATERAL FLATTEN(input => student:marks) m
)
WHERE rn = 1;

-----------------------------------------------------
CREATE OR REPLACE stage JSONSTAGE
     url='s3://bucketsnowflake-jsondemo';




SELECT
    RAW_FILE:"id"::int Id,
    f.value:language::STRING AS language,
    f.value:level::STRING    AS level
FROM 
JSON_RAW,table(flatten(RAW_FILE:"spoken_languages")) f ;

SELECT
    RAW_FILE:"id"::int Id,
   t.value::string prv_company
FROM 
JSON_RAW,table(flatten(RAW_FILE:"prev_company")) t ;




SELECT
    RAW_FILE:"id"::int Id,
    f.value:language::STRING AS language,
    f.value:level::STRING    AS level,
    t.value::string prv_company
FROM 
JSON_RAW,table(flatten(RAW_FILE:"spoken_languages")) f ,table(flatten(RAW_FILE:"prev_company")) t;




