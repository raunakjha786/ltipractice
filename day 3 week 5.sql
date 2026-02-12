create database practise
use practise;
create schema test
use schema test;
create table employ (employee_id integer autoincrement start=1 increment=1,
employee_name varchar default 'SNOW',
load_time timestamp);

create or replace task employ_task
 warehouse=compute_wh
 schedule='1 minute'
 as
   insert into employ(load_time) values(current_timestamp);
show tasks;

alter task employ_task resume;
alter task employ_task suspend;
select * from employ;
create table employ_parent(employee_id integer autoincrement start=1 increment=1,employ_name varchar default 'SNOW',load_time timestamp );
create or replace task employ_parent_task
  warehouse=compute_wh
  schedule='1 minute'
  as
  insert into employ_parent(load_time) values(current_timestamp);
show tasks;
alter task employ_parent_task resume;
alter task employ_parent_task suspend;
create table employ_child1(employ_id integer,
employ_name varchar, load_time timestamp);

create or replace task employ_child1_task
  warehouse=compute_wh
after employ_parent_task
as 
  insert into employ_child1(employ_id,employ_name,load_time)


select * from employ_parent;
select * from employ_child1;

alter task employ_parent_task resume;
alter task employ_child1_task resume;

alter task employ_parent_task suspend;
alter task employ_child1_task suspend;

create or replace task my_crontask
 warehouse=compute_wh
 scehdule='using cron * * * * * utc'
 as
 insert into employees values(employee_sequence.nextval,'F_NAME','L_NAME','101');


 select * from table(information_scehma.task_history())
 order by scheduled_time;


 create temporary table emp_tt
 (eid int,ename string,esal number);

 create database cloned_db clone practise;
 select * from practise.test.emp_tt;
 select * from cloned_db.student.emp_tt;

show tables;
create table cloned_employee clone employee;
select * from cloned_employee;


--------------------------------------------------
create or replace role analyst;
create or replace role business;
create or replace role developer;


create or replace table employee_info(employee_id number,
empl_join_date date, dept varchar(10), salary number, manager_id number);



insert into employee_info values(1,'2014-10-01', 'HR',40000,4),
(2,'2014-09-01', 'Tech',50000,9),
(3,'2018-09-01', 'Marketing',30000,5),
(4,'2017-09-01', 'HR',10000,5),
(5,'2019-09-01', 'HR',35000,9),
(6,'2015-09-01', 'Tech',90000,4),
(7,'2016-09-01', 'Marketing',20000,1);



grant select on table employee_info to role analyst;
grant select on table employee_info to role business;
grant select on table employee_info to role developer;

grant role business to user RAUNAK108
grant role ANALYST to user RAUNAK108
grant role developer to user RAUNAK108


grant usage on warehouse compute_wh to role business;
grant usage on warehouse compute_wh to role analyst;
grant usage on warehouse compute_wh to role developer;


grant usage on database practise to role business;
grant usage on database practise to role analyst;
grant usage on database practise to role developer;



grant usage on schema test to role business;
grant usage on schema test to role analyst;
grant usage on schema test to role developer;




create or replace masking policy masking_number as (val NUMBER) returns number ->
 case 
   when current_role() in ('DEVELOPER','ACCOUNTADMIN') then val
   else '999999999999'
 end;

create or replace masking policy masking_strings as (val STRING) returns STRING ->
 case 
   when current_role() in ('Business','Developer','Accountadmin') then val
   else '**************'
 end;


 alter table if exists employee_info modify column salary set masking policy masking_number;

 alter table if exists employee_info modify column dept set masking policy masking_strings;

 select * from employee_info;
 
 
  
select * from employee_info;

 


select * from "mypriority"

select * from "MYMOCKQ4"


CREATE OR REPLACE TABLE EMPLOYEE_PERFORMANCE(
        EmpID INT,
        ReviewYear VARCHAR(30),
        PerformanceScore VARCHAR(30)
        )







create or replace table product_sales_data (
ProductID INT,	 
ProductName	VARCHAR(30), 
Region VARCHAR(30),	 
SalesAmount INT

)

SELECT * FROM "mock2q4"  where "order_risk_level"='High Risk';


select * from "mymockq1"
select * from "mymock2q1a"





CREATE OR REPLACE TABLE SALES_DATA (
    ORDER_ID        NUMBER,
    PRODUCT_NAME    STRING,
    CATEGORY        STRING,
    REGION          STRING,
    ORDER_DATE      DATE,
    QUANTITY        NUMBER,
    SALES_AMOUNT    NUMBER
);


INSERT INTO SALES_DATA VALUES
(1001, 'Laptop',  'Electronics', 'East',  '2023-01-15', 2, 60000),
(1002, 'Mobile',  'Electronics', 'South', '2023-02-10', 3, 45000),
(1003, 'Tablet',  'Electronics', 'North', '2023-03-05', 1, 18000),
(1004, 'Printer', 'Electronics', 'East',  '2023-04-12', 1, 22000),
(1005, 'Monitor', 'Electronics', 'South', '2023-05-18', 2, 32000);



SELECT * FROM "ms4q1"

SELECT * FROM "myms4q4"


SELECT * FROM "myms4q2"