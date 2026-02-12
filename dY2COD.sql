CREATE DATABASE SQL_TRAINING_DB;
CREATE SCHEMA SQL_TRAINING_DB_OPERATIONS;

use database SQL_TRAINING_DB;
USE SCHEMA SQL_TRAINING_DB_OPERATIONS

CREATE TABLE emp_data (
    emp_id INT,
    emp_name STRING,
    joining_date DATE,
    salary NUMBER(10,2),
    is_active BOOLEAN
);


INSERT INTO emp_data VALUES
(1, 'Aamir', '2022-01-15', 50000, TRUE),
(2, 'Rahul', '2021-06-10', 60000, TRUE),
(3, 'Sneha', '2020-03-20', 55000, FALSE);

UPDATE emp_data
SET salary = 55000
WHERE emp_id = 1;


DELETE FROM emp_data
WHERE emp_id = 3;




SELECT emp_name, salary
FROM emp_data;



