PRACTISE--create base table
--on top base table creat stream
--create consume table
--create task

-- create or replace task stream_task
-- warehouse = COMPUTE_WH
-- SCHEDULE = '1 MINUTE'
-- WHEN SYSTEM$STREAM_HAS_DATA('employ_stream')
-- as 
-- insert into employee_consume
-- select 
-- employee_id,
-- salary,
-- manager_id,
-- from employee_stream;





use database raunakbucket

use schema bucket

create or replace table employee_base(employee_id INT, salary number, manager_id INT)

CREATE OR REPLACE STREAM employee_stream 
ON TABLE employee_base;

CREATE OR REPLACE TABLE employee_consume (
    employee_id INT,
    salary NUMBER,
    manager_id INT,
    action STRING,          -- from METADATA$ACTION
    is_update BOOLEAN,      -- from METADATA$ISUPDATE
    row_id STRING           -- from METADATA$ROW_ID

);


CREATE OR REPLACE TASK stream_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('employee_stream')
AS
INSERT INTO employee_consume (employee_id, salary, manager_id, action, is_update, row_id)
SELECT 
    employee_id,
    salary,
    manager_id,
    METADATA$ACTION,
    METADATA$ISUPDATE,
    METADATA$ROW_ID
FROM employee_stream;

alter task stream_task resume;

INSERT INTO employee_base (employee_id, salary, manager_id)
VALUES (1, 50000, 101),
       (2, 60000, 102);


select * from employee_stream
select * from employee_base
select * from employee_consume



















