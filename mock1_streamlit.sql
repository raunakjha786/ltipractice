CREATE OR REPLACE TABLE sales_data (
    order_id STRING,
    order_date DATE,
    category STRING,
    product STRING,
    quantity NUMBER(10,0),
    sales_amount NUMBER(12,2)
);


INSERT INTO sales_data (order_id, order_date, category, product, quantity, sales_amount) VALUES
-- January - Electronics
('O1001', '2025-01-02', 'Electronics', '4K TV 55"', 2, 110000.00),
('O1002', '2025-01-03', 'Electronics', 'Laptop 14"', 1, 72000.00),
('O1003', '2025-01-04', 'Electronics', 'Wireless Earbuds', 5, 17500.00),
('O1004', '2025-01-05', 'Electronics', 'Microwave Oven', 1, 18000.00),
('O1005', '2025-01-06', 'Electronics', 'Gaming Console', 3, 135000.00),

-- January - Furniture
('O1006', '2025-01-02', 'Furniture', 'Office Chair', 3, 25500.00),
('O1007', '2025-01-04', 'Furniture', 'Standing Desk', 2, 44000.00),
('O1008', '2025-01-07', 'Furniture', 'Bookshelf', 2, 24000.00),
('O1009', '2025-01-09', 'Furniture', 'Sofa 3-Seater', 1, 55000.00),

-- January - Grocery
('O1010', '2025-01-03', 'Grocery', 'Rice 10kg', 4, 3000.00),
('O1011', '2025-01-05', 'Grocery', 'Olive Oil 1L', 6, 5700.00),
('O1012', '2025-01-08', 'Grocery', 'Blender', 3, 8400.00),
('O1013', '2025-01-10', 'Grocery', 'Milk 1L', 10, 700.00),

-- February - Electronics
('O1014', '2025-02-02', 'Electronics', '4K TV 55"', 1, 55000.00),
('O1015', '2025-02-03', 'Electronics', 'Laptop 14"', 2, 140000.00),
('O1016', '2025-02-04', 'Electronics', 'Wireless Earbuds', 10, 35000.00),
('O1017', '2025-02-06', 'Electronics', 'Microwave Oven', 2, 36000.00),
('O1018', '2025-02-08', 'Electronics', 'Gaming Console', 1, 45000.00),

-- February - Furniture
('O1019', '2025-02-01', 'Furniture', 'Office Chair', 1, 8500.00),
('O1020', '2025-02-03', 'Furniture', 'Standing Desk', 1, 22000.00),
('O1021', '2025-02-05', 'Furniture', 'Bookshelf', 4, 48000.00),
('O1022', '2025-02-07', 'Furniture', 'Sofa 3-Seater', 2, 110000.00),

-- February - Grocery
('O1023', '2025-02-02', 'Grocery', 'Rice 10kg', 8, 6000.00),
('O1024', '2025-02-04', 'Grocery', 'Olive Oil 1L', 12, 11160.00),
('O1025', '2025-02-06', 'Grocery', 'Blender', 5, 14000.00),
('O1026', '2025-02-09', 'Grocery', 'Milk 1L', 20, 1400.00),

-- March - Electronics
('O1027', '2025-03-01', 'Electronics', '4K TV 55"', 1, 54500.00),
('O1028', '2025-03-02', 'Electronics', 'Laptop 14"', 1, 71000.00),
('O1029', '2025-03-03', 'Electronics', 'Wireless Earbuds', 7, 24150.00),
('O1030', '2025-03-05', 'Electronics', 'Microwave Oven', 1, 17800.00),
('O1031', '2025-03-06', 'Electronics', 'Gaming Console', 2, 90000.00),

-- March - Furniture
('O1032', '2025-03-02', 'Furniture', 'Office Chair', 2, 17000.00),
('O1033', '2025-03-04', 'Furniture', 'Standing Desk', 3, 66900.00),
('O1034', '2025-03-07', 'Furniture', 'Bookshelf', 1, 11900.00),
('O1035', '2025-03-09', 'Furniture', 'Sofa 3-Seater', 1, 56000.00),

-- March - Grocery
('O1036', '2025-03-01', 'Grocery', 'Rice 10kg', 10, 7600.00),
('O1037', '2025-03-03', 'Grocery', 'Olive Oil 1L', 9, 8370.00),
('O1038', '2025-03-05', 'Grocery', 'Blender', 2, 5600.00),
('O1039', '2025-03-08', 'Grocery', 'Milk 1L', 15, 1050.00);

