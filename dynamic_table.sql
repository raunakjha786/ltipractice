create or replace dynamic table SNOWPARK_DB.PUBLIC.ORDERS_JOIN(
	ORDER_ID,
	ORDER_DATE,
	CUSTOMER_ID,
	CUSTOMER_NAME,
	AMOUNT,
	STATUS
) target_lag = '1 minute' refresh_mode = AUTO initialize = ON_CREATE warehouse = COMPUTE_WH
 as
        select ORDER_ID,ORDER_DATE,C.CUSTOMER_ID,CUSTOMER_NAME,AMOUNT,STATUS
        from ORDERS O JOIN CUSTOMERS C ON O.CUSTOMER_ID=C.CUSTOMER_ID;


ALTER DYNAMIC TABLE ORDERS_JOIN SUSPEND;

create or replace dynamic table ORDERS_BRONZE
target_lag = '1 Minute'
warehouse = 'COMPUTE_WH'
as
select *
from orders;

-- SILVER
create or replace dynamic table ORDERS_SILVER
target_lag = '1 Minute'
warehouse = 'COMPUTE_WH'
as
select ORDER_ID,
        ORDER_DATE,
        YEAR(ORDER_DATE) AS ORDER_YEAR,
        MONTH(ORDER_DATE) AS ORDER_MONTH,
        CUSTOMER_ID,
        AMOUNT,
        INITCAP(STATUS) AS STATUS
FROM ORDERS_BRONZE WHERE STATUS IS NOT NULL;

-- GOLD
create or replace dynamic table ORDERS_GOLD
target_lag = '1 Minute'
warehouse = 'COMPUTE_WH'
as
select STATUS,
        ORDER_YEAR,
        ORDER_MONTH,
        SUM(AMOUNT) AS TOTAL_AMOUNT
FROM ORDERS_SILVER
GROUP BY STATUS, ORDER_YEAR, ORDER_MONTH;
















