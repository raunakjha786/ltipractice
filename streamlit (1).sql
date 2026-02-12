create database mypy

use database mypy
create schema mypys
use schema mypys


CREATE OR REPLACE TABLE DIM_CUSTOMER (
    SK_CUSTOMER     NUMBER AUTOINCREMENT PRIMARY KEY,
    CUSTOMER_ID     STRING,
    FIRST_NAME      STRING,
    LAST_NAME       STRING,
    EMAIL           STRING,
    PHONE           STRING,
    SEGMENT         STRING,
    CITY            STRING,
    STATE           STRING,
    COUNTRY         STRING,
    EFFECTIVE_START DATE,
    EFFECTIVE_END   DATE,
    IS_CURRENT      BOOLEAN
);

CREATE OR REPLACE TABLE DIM_ROUTE (
    SK_ROUTE        NUMBER AUTOINCREMENT PRIMARY KEY,
    ROUTE_ID        STRING,
    SOURCE_CITY     STRING,
    DEST_CITY       STRING,
    DISTANCE_KM     NUMBER,
    SOURCE_STATE    STRING,
    DEST_STATE      STRING,
    REGION          STRING,
    STATUS          STRING
);



CREATE OR REPLACE TABLE DIM_BUS (
    SK_BUS          NUMBER AUTOINCREMENT PRIMARY KEY,
    BUS_ID          STRING,
    OPERATOR_NAME   STRING,
    BUS_TYPE        STRING,
    SEAT_CAPACITY   NUMBER,
    BUS_MODEL       STRING,
    REGISTRATION_NO STRING,
    STATUS          STRING,
    EFFECTIVE_START DATE,
    EFFECTIVE_END   DATE,
    IS_CURRENT      BOOLEAN
);




CREATE OR REPLACE TABLE DIM_DATE (
    SK_DATE     NUMBER AUTOINCREMENT PRIMARY KEY,
    DATE_VALUE  DATE,
    YEAR        NUMBER,
    MONTH       NUMBER,
    DAY         NUMBER,
    DAY_NAME    STRING,
    IS_WEEKEND  BOOLEAN
);

CREATE OR REPLACE TABLE FACT_BOOKING (
    BOOKING_ID      STRING,
    BOOKING_LINE_ID STRING,
    SK_DATE         NUMBER,
    SK_CUSTOMER     NUMBER,
    SK_ROUTE        NUMBER,
    SK_BUS          NUMBER,
    PASSENGER_COUNT NUMBER,
    FARE_AMOUNT     NUMBER,
    DISCOUNT_PCT    NUMBER,
    NET_AMOUNT      NUMBER,
    PAYMENT_MODE    STRING,
    BOOKING_STATUS  STRING
);



INSERT INTO DIM_CUSTOMER VALUES
(1,'C001','Rahul','Sharma','rahul@gmail.com','9876543210','Regular','Bangalore','Karnataka','India','2025-01-01','9999-12-31',TRUE),
(2,'C002','Amit','Verma','amit@gmail.com','9876543211','Premium','Delhi','Delhi','India','2025-01-01','9999-12-31',TRUE),
(3,'C003','Sneha','Patil','sneha@gmail.com','9876543212','Regular','Pune','Maharashtra','India','2025-01-01','9999-12-31',TRUE),
(4,'C004','Priya','Singh','priya@gmail.com','9876543213','Premium','Mumbai','Maharashtra','India','2025-01-01','9999-12-31',TRUE),
(5,'C005','Rohit','Mehta','rohit@gmail.com','9876543214','Regular','Ahmedabad','Gujarat','India','2025-01-01','9999-12-31',TRUE),
(6,'C006','Anjali','Iyer','anjali@gmail.com','9876543215','Premium','Chennai','Tamil Nadu','India','2025-01-01','9999-12-31',TRUE),
(7,'C007','Vikas','Gupta','vikas@gmail.com','9876543216','Regular','Jaipur','Rajasthan','India','2025-01-01','9999-12-31',TRUE),
(8,'C008','Neha','Kumar','neha@gmail.com','9876543217','Regular','Patna','Bihar','India','2025-01-01','9999-12-31',TRUE),
(9,'C009','Suresh','Reddy','suresh@gmail.com','9876543218','Premium','Hyderabad','Telangana','India','2025-01-01','9999-12-31',TRUE),
(10,'C010','Kiran','Joshi','kiran@gmail.com','9876543219','Regular','Indore','MP','India','2025-01-01','9999-12-31',TRUE),
(11,'C011','Deepak','Nair','deepak@gmail.com','9876543220','Regular','Kochi','Kerala','India','2025-01-01','9999-12-31',TRUE),
(12,'C012','Rina','Das','rina@gmail.com','9876543221','Premium','Kolkata','WB','India','2025-01-01','9999-12-31',TRUE),
(13,'C013','Manoj','Yadav','manoj@gmail.com','9876543222','Regular','Lucknow','UP','India','2025-01-01','9999-12-31',TRUE),
(14,'C014','Pooja','Mishra','pooja@gmail.com','9876543223','Premium','Bhopal','MP','India','2025-01-01','9999-12-31',TRUE),
(15,'C015','Sanjay','Chopra','sanjay@gmail.com','9876543224','Regular','Chandigarh','Punjab','India','2025-01-01','9999-12-31',TRUE),
(16,'C016','Mehul','Shah','mehul@gmail.com','9876543225','Premium','Surat','Gujarat','India','2025-01-01','9999-12-31',TRUE),
(17,'C017','Nitin','Agarwal','nitin@gmail.com','9876543226','Regular','Agra','UP','India','2025-01-01','9999-12-31',TRUE),
(18,'C018','Swati','Kulkarni','swati@gmail.com','9876543227','Premium','Nagpur','Maharashtra','India','2025-01-01','9999-12-31',TRUE),
(19,'C019','Arjun','Kapoor','arjun@gmail.com','9876543228','Regular','Amritsar','Punjab','India','2025-01-01','9999-12-31',TRUE),
(20,'C020','Kavita','Malhotra','kavita@gmail.com','9876543229','Premium','Noida','UP','India','2025-01-01','9999-12-31',TRUE);


INSERT INTO DIM_ROUTE VALUES
(1,'R001','Bangalore','Chennai',350,'Karnataka','Tamil Nadu','South','ACTIVE'),
(2,'R002','Bangalore','Hyderabad',570,'Karnataka','Telangana','South','ACTIVE'),
(3,'R003','Mumbai','Pune',150,'Maharashtra','Maharashtra','West','ACTIVE'),
(4,'R004','Delhi','Jaipur',280,'Delhi','Rajasthan','North','ACTIVE'),
(5,'R005','Chennai','Coimbatore',500,'Tamil Nadu','Tamil Nadu','South','ACTIVE'),
(6,'R006','Kolkata','Durgapur',170,'WB','WB','East','ACTIVE'),
(7,'R007','Hyderabad','Vijayawada',275,'Telangana','AP','South','ACTIVE'),
(8,'R008','Ahmedabad','Surat',265,'Gujarat','Gujarat','West','ACTIVE'),
(9,'R009','Indore','Bhopal',190,'MP','MP','Central','ACTIVE'),
(10,'R010','Lucknow','Kanpur',90,'UP','UP','North','ACTIVE'),
(11,'R011','Patna','Gaya',100,'Bihar','Bihar','East','ACTIVE'),
(12,'R012','Jaipur','Udaipur',400,'Rajasthan','Rajasthan','North','ACTIVE'),
(13,'R013','Nagpur','Pune',720,'MH','MH','West','ACTIVE'),
(14,'R014','Kochi','Trivandrum',220,'Kerala','Kerala','South','ACTIVE'),
(15,'R015','Noida','Agra',210,'UP','UP','North','ACTIVE'),
(16,'R016','Surat','Vadodara',150,'Gujarat','Gujarat','West','ACTIVE'),
(17,'R017','Amritsar','Ludhiana',140,'Punjab','Punjab','North','ACTIVE'),
(18,'R018','Chandigarh','Shimla',110,'HP','HP','North','ACTIVE'),
(19,'R019','Bhopal','Jabalpur',330,'MP','MP','Central','ACTIVE'),
(20,'R020','Coimbatore','Madurai',215,'TN','TN','South','ACTIVE');


INSERT INTO DIM_BUS VALUES
(1,'B001','VRL','AC Sleeper',40,'Volvo','KA01AA1111','ACTIVE','2025-01-01','9999-12-31',TRUE),
(2,'B002','SRS','Non-AC',45,'Ashok Leyland','KA01BB2222','ACTIVE','2025-01-01','9999-12-31',TRUE),
(3,'B003','Orange','AC Seater',42,'Scania','MH01CC3333','ACTIVE','2025-01-01','9999-12-31',TRUE),
(4,'B004','KSRTC','Non-AC',50,'Tata','KA01DD4444','ACTIVE','2025-01-01','9999-12-31',TRUE),
(5,'B005','TNSTC','AC Sleeper',38,'Volvo','TN01EE5555','ACTIVE','2025-01-01','9999-12-31',TRUE),
(6,'B006','APSRTC','AC Seater',40,'Ashok','AP01FF6666','ACTIVE','2025-01-01','9999-12-31',TRUE),
(7,'B007','GSRTC','Non-AC',48,'Tata','GJ01GG7777','ACTIVE','2025-01-01','9999-12-31',TRUE),
(8,'B008','RSRTC','AC Sleeper',36,'Volvo','RJ01HH8888','ACTIVE','2025-01-01','9999-12-31',TRUE),
(9,'B009','WBSTC','AC Seater',40,'Ashok','WB01II9999','ACTIVE','2025-01-01','9999-12-31',TRUE),
(10,'B010','MSRTC','Non-AC',52,'Tata','MH01JJ0000','ACTIVE','2025-01-01','9999-12-31',TRUE),
(11,'B011','VRL','AC Seater',45,'Volvo','KA02AA1111','ACTIVE','2025-01-01','9999-12-31',TRUE),
(12,'B012','SRS','Sleeper',38,'Ashok','KA02BB2222','ACTIVE','2025-01-01','9999-12-31',TRUE),
(13,'B013','Orange','AC Sleeper',36,'Scania','MH02CC3333','ACTIVE','2025-01-01','9999-12-31',TRUE),
(14,'B014','KSRTC','AC Seater',42,'Tata','KA02DD4444','ACTIVE','2025-01-01','9999-12-31',TRUE),
(15,'B015','TNSTC','Non-AC',50,'Ashok','TN02EE5555','ACTIVE','2025-01-01','9999-12-31',TRUE),
(16,'B016','APSRTC','Sleeper',40,'Volvo','AP02FF6666','ACTIVE','2025-01-01','9999-12-31',TRUE),
(17,'B017','GSRTC','AC Seater',44,'Tata','GJ02GG7777','ACTIVE','2025-01-01','9999-12-31',TRUE),
(18,'B018','RSRTC','Non-AC',48,'Ashok','RJ02HH8888','ACTIVE','2025-01-01','9999-12-31',TRUE),
(19,'B019','WBSTC','Sleeper',36,'Volvo','WB02II9999','ACTIVE','2025-01-01','9999-12-31',TRUE),
(20,'B020','MSRTC','AC Sleeper',40,'Scania','MH02JJ0000','ACTIVE','2025-01-01','9999-12-31',TRUE);


INSERT INTO DIM_DATE VALUES
(1,'2025-01-01',2025,1,1,'Wednesday',FALSE),
(2,'2025-01-02',2025,1,2,'Thursday',FALSE),
(3,'2025-01-03',2025,1,3,'Friday',FALSE),
(4,'2025-01-04',2025,1,4,'Saturday',TRUE),
(5,'2025-01-05',2025,1,5,'Sunday',TRUE),
(6,'2025-01-06',2025,1,6,'Monday',FALSE),
(7,'2025-01-07',2025,1,7,'Tuesday',FALSE),
(8,'2025-01-08',2025,1,8,'Wednesday',FALSE),
(9,'2025-01-09',2025,1,9,'Thursday',FALSE),
(10,'2025-01-10',2025,1,10,'Friday',FALSE),
(11,'2025-01-11',2025,1,11,'Saturday',TRUE),
(12,'2025-01-12',2025,1,12,'Sunday',TRUE),
(13,'2025-01-13',2025,1,13,'Monday',FALSE),
(14,'2025-01-14',2025,1,14,'Tuesday',FALSE),
(15,'2025-01-15',2025,1,15,'Wednesday',FALSE),
(16,'2025-01-16',2025,1,16,'Thursday',FALSE),
(17,'2025-01-17',2025,1,17,'Friday',FALSE),
(18,'2025-01-18',2025,1,18,'Saturday',TRUE),
(19,'2025-01-19',2025,1,19,'Sunday',TRUE),
(20,'2025-01-20',2025,1,20,'Monday',FALSE);



INSERT INTO FACT_BOOKING VALUES
('BK001','L001',1,1,1,1,1,1200,10,1080,'UPI','CONFIRMED'),
('BK002','L002',2,2,2,2,2,1500,5,1425,'CARD','CONFIRMED'),
('BK003','L003',3,3,3,3,1,800,0,800,'CASH','CONFIRMED'),
('BK004','L004',4,4,4,4,2,2000,10,1800,'UPI','CONFIRMED'),
('BK005','L005',5,5,5,5,1,900,5,855,'CARD','CONFIRMED'),
('BK006','L006',6,6,6,6,1,1100,0,1100,'UPI','CONFIRMED'),
('BK007','L007',7,7,7,7,2,1600,10,1440,'CARD','CONFIRMED'),
('BK008','L008',8,8,8,8,1,950,5,902,'UPI','CONFIRMED'),
('BK009','L009',9,9,9,9,1,1400,0,1400,'CARD','CONFIRMED'),
('BK010','L010',10,10,10,10,2,1800,10,1620,'UPI','CONFIRMED'),
('BK011','L011',11,11,11,11,1,1000,5,950,'CARD','CONFIRMED'),
('BK012','L012',12,12,12,12,2,2200,10,1980,'UPI','CONFIRMED'),
('BK013','L013',13,13,13,13,1,1300,0,1300,'UPI','CONFIRMED'),
('BK014','L014',14,14,14,14,1,900,5,855,'CARD','CONFIRMED'),
('BK015','L015',15,15,15,15,2,1700,10,1530,'UPI','CONFIRMED'),
('BK016','L016',16,16,16,16,1,1100,0,1100,'CARD','CONFIRMED'),
('BK017','L017',17,17,17,17,1,1250,5,1187,'UPI','CONFIRMED'),
('BK018','L018',18,18,18,18,2,2100,10,1890,'CARD','CONFIRMED'),
('BK019','L019',19,19,19,19,1,950,0,950,'UPI','CONFIRMED'),
('BK020','L020',20,20,20,20,2,1900,10,1710,'CARD','CONFIRMED');




CREATE OR REPLACE DYNAMIC TABLE DT_REDBUS_STAR
TARGET_LAG = '1 MINUTE'
WAREHOUSE = COMPUTE_WH
AS
SELECT
    f.BOOKING_ID,
    f.BOOKING_LINE_ID,

    d.DATE_VALUE AS BOOKING_DATE,
    d.YEAR,
    d.MONTH,
    d.DAY_NAME,

    c.CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.SEGMENT,
    c.CITY,
    c.STATE,

    r.ROUTE_ID,
    r.SOURCE_CITY,
    r.DEST_CITY,
    r.DISTANCE_KM,
    r.REGION,

    b.BUS_ID,
    b.OPERATOR_NAME,
    b.BUS_TYPE,
    b.SEAT_CAPACITY,

    f.PASSENGER_COUNT,
    f.FARE_AMOUNT,
    f.DISCOUNT_PCT,
    f.NET_AMOUNT,
    f.PAYMENT_MODE,
    f.BOOKING_STATUS
FROM FACT_BOOKING f
JOIN DIM_DATE d       ON f.SK_DATE = d.SK_DATE
JOIN DIM_CUSTOMER c   ON f.SK_CUSTOMER = c.SK_CUSTOMER
JOIN DIM_ROUTE r      ON f.SK_ROUTE = r.SK_ROUTE
JOIN DIM_BUS b        ON f.SK_BUS = b.SK_BUS;



select * from dt_redbus_star





CREATE OR REPLACE TABLE FACT_BOOKING_BASE (
    BOOKING_ID           STRING,
    BOOKING_LINE_ID      NUMBER,

    SK_BOOKING_DATE      NUMBER,
    SK_JOURNEY_DATE      NUMBER,

    SK_CUSTOMER          NUMBER,
    SK_ROUTE             NUMBER,
    SK_BUS               NUMBER,

    PASSENGER_COUNT      NUMBER,
    FARE                 NUMBER,
    DISCOUNT             NUMBER,
    NET_AMOUNT           NUMBER,

    STATUS               STRING,
    PAYMENT              STRING
);

CREATE OR REPLACE FILE FORMAT FF_REDBUS_CSV
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('', 'NULL');




create or replace storage integration S3_REDBUS_INT
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::498504718091:role/raunakroles'
  storage_allowed_locations = ('s3://raunakjhaa/analytic/');

desc integration S3_REDBUS_INT


CREATE OR REPLACE FILE FORMAT FF_REDBUS_CSV
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1
TRIM_SPACE = TRUE
NULL_IF = ('NULL', 'null', '')
EMPTY_FIELD_AS_NULL = TRUE;




CREATE OR REPLACE STAGE STG_REDBUS
URL = 's3://raunakjhaa/analytic/'
STORAGE_INTEGRATION = S3_REDBUS_INT
FILE_FORMAT = FF_REDBUS_CSV;

list @stg_redbus

COPY INTO FACT_BOOKING_BASE
FROM @stg_redbus
ON_ERROR = 'CONTINUE';



select * from fact_booking_base




CREATE OR REPLACE DYNAMIC TABLE DT_FACT_BOOKING
TARGET_LAG = '5 minutes'
WAREHOUSE = WH_ANALYTICS
AS
SELECT
    BOOKING_ID,
    BOOKING_LINE_ID,

    SK_BOOKING_DATE,
    SK_JOURNEY_DATE,

    SK_CUSTOMER,
    SK_ROUTE,
    SK_BUS,

    PASSENGER_COUNT,
    FARE,
    DISCOUNT,
    NET_AMOUNT,

    STATUS,
    PAYMENT
FROM FACT_BOOKING_BASE;


select * from dt_fact_booking


show dynamic tables


