-- CREATE OR REPLACE DATABASE TRAINING_DB;
-- CREATE OR REPLACE SCHEMA TRAINING_DB.RETAIL;

-- CREATE OR REPLACE TABLE TRAINING_DB.RETAIL.PRODUCT_SALES (
--     SALE_ID        NUMBER AUTOINCREMENT,
--     SALE_DATE      DATE NOT NULL,
--     PRODUCT_ID     STRING NOT NULL,
--     PRODUCT_NAME   STRING NOT NULL,
--     CATEGORY       STRING NOT NULL,
--     REGION         STRING NOT NULL,
--     UNITS_SOLD     NUMBER(10,0) NOT NULL,
--     UNIT_PRICE     NUMBER(10,2) NOT NULL,
--     DISCOUNT_PCT   NUMBER(5,2) DEFAULT 0,
--     CHANNEL        STRING NOT NULL
-- );


-- INSERT INTO TRAINING_DB.RETAIL.PRODUCT_SALES
-- (SALE_DATE, PRODUCT_ID, PRODUCT_NAME, CATEGORY, REGION, UNITS_SOLD, UNIT_PRICE, DISCOUNT_PCT, CHANNEL)
-- VALUES
-- ('2025-01-01','P001','iPhone 15','Electronics','IN',10,80000,5,'Online'),
-- ('2025-01-02','P002','Galaxy S23','Electronics','US',8,75000,10,'Online'),
-- ('2025-01-03','P003','Air Fryer','Home Appliances','IN',5,12000,0,'Retail'),
-- ('2025-01-04','P004','Running Shoes','Footwear','UK',12,6000,15,'Distributor'),
-- ('2025-01-05','P005','Bluetooth Speaker','Electronics','SG',7,4000,0,'Retail'),
-- ('2025-01-06','P006','Microwave Oven','Home Appliances','US',3,18000,5,'Online'),
-- ('2025-01-07','P007','Leather Wallet','Accessories','IN',20,1500,0,'Retail'),
-- ('2025-01-08','P008','Smart Watch','Electronics','UK',6,15000,10,'Online'),
-- ('2025-01-09','P009','Backpack','Accessories','US',9,3000,0,'Distributor'),
-- ('2025-01-10','P010','Coffee Maker','Home Appliances','SG',4,9000,5,'Retail');

-- SELECT COUNT(*) FROM TRAINING_DB.RETAIL.PRODUCT_SALES;

------------ganesh_sample_question_whatsapp 07/01/26

CREATE OR REPLACE TABLE sales_data (
    order_id STRING,
    order_date DATE,
    category STRING,
    product STRING,
    quantity NUMBER(10,0),
    revenue NUMBER(12,2)
);


INSERT INTO sales_data VALUES
-- January
('O1001','2025-01-02','Electronics','4K TV 55"',2,110000.00),
('O1002','2025-01-03','Electronics','Laptop 14"',1,72000.00),
('O1003','2025-01-04','Electronics','Wireless Earbuds',5,17500.00),
('O1004','2025-01-05','Electronics','Microwave Oven',1,18000.00),
('O1005','2025-01-06','Electronics','Gaming Console',3,135000.00),

('O1006','2025-01-02','Furniture','Office Chair',3,25500.00),
('O1007','2025-01-04','Furniture','Standing Desk',2,44000.00),
('O1008','2025-01-07','Furniture','Bookshelf',2,24000.00),
('O1009','2025-01-09','Furniture','Sofa 3-Seater',1,55000.00),

('O1010','2025-01-03','Grocery','Rice 10kg',4,3000.00),
('O1011','2025-01-05','Grocery','Olive Oil 1L',6,5700.00),
('O1012','2025-01-08','Grocery','Blender',3,8400.00),
('O1013','2025-01-10','Grocery','Milk 1L',10,700.00),

-- February
('O1014','2025-02-02','Electronics','4K TV 55"',1,55000.00),
('O1015','2025-02-03','Electronics','Laptop 14"',2,140000.00),
('O1016','2025-02-04','Electronics','Wireless Earbuds',10,35000.00),
('O1017','2025-02-06','Electronics','Microwave Oven',2,36000.00),
('O1018','2025-02-08','Electronics','Gaming Console',1,45000.00),

('O1019','2025-02-01','Furniture','Office Chair',1,8500.00),
('O1020','2025-02-03','Furniture','Standing Desk',1,22000.00),
('O1021','2025-02-05','Furniture','Bookshelf',4,48000.00),
('O1022','2025-02-07','Furniture','Sofa 3-Seater',2,110000.00),

('O1023','2025-02-02','Grocery','Rice 10kg',8,6000.00);



















