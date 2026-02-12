-----------------------------------------UDF ------------------------------

CREATE OR REPLACE TABLE demoitems (
  itemid      STRING,
  productname STRING,
  quantity    NUMBER(10,0),
  unitprice   NUMBER(10,2)
);



INSERT INTO demoitems (itemid, productname, quantity, unitprice) VALUES
('I001', '  smartPhone  x  ', 2, 34999.50),
('I002', 'SMARTphone  X',     1, 35999.00),
('I003', 'Noise Cancel Headset', 3, 7999.00),
('I004', 'office  Chair',     1, 11999.00),
('I005', 'Standing   Desk',   2, 28999.00);


---- creatting udf(string)

CREATE OR REPLACE FUNCTION CLEAN_PRODUCT_NAME(name STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'clean_name'
AS
$$
def clean_name(name):
    if name is None:
        return None
    # remove extra spaces, convert to title case
    return " ".join(name.strip().split()).title()
$$;



---creating python udf (number)


CREATE OR REPLACE FUNCTION CALCULATE_TOTAL_VALUE(qty NUMBER, price NUMBER)
RETURNS NUMBER(12,2)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'calc_total'
AS
$$
def calc_total(qty, price):
    if qty is None or price is None:
        return 0
    return round(qty * price, 2)
$$;


--showing the output 


SELECT
    itemid,
    CLEAN_PRODUCT_NAME(productname) AS clean_product_name,
    quantity,
    unitprice,
    CALCULATE_TOTAL_VALUE(quantity, unitprice) AS total_value
FROM demoitems
ORDER BY itemid;


--- to verfiy the udf exists

SHOW USER FUNCTIONS LIKE '%PRODUCT%';




----------------------------------------------------------------------------------------------------------------


streamlit ---------------------------------



CREATE OR REPLACE TABLE ordersstateplatform (
  orderid     STRING,
  orderdate   DATE,
  state        STRING,
  platform     STRING,       -- e.g., Marketplace, Direct, Partner
  itemscount  NUMBER(10,0),
  totalamount NUMBER(12,2)
);

INSERT INTO ordersstateplatform (orderid, orderdate, state, platform, itemscount, totalamount) VALUES
-- January — Tamil Nadu
('SP1001','2025-01-02','Tamil Nadu','Marketplace', 6, 14250.00),
('SP1002','2025-01-04','Tamil Nadu','Direct',      4, 18990.00),
('SP1003','2025-01-06','Tamil Nadu','Partner',     5,  9800.00),
-- January — Karnataka
('SP1004','2025-01-03','Karnataka','Marketplace',  7, 15600.00),
('SP1005','2025-01-07','Karnataka','Direct',       5, 21850.00),
('SP1006','2025-01-09','Karnataka','Partner',      3,  8200.00),
-- January — Maharashtra
('SP1007','2025-01-05','Maharashtra','Marketplace',8, 16850.00),
('SP1008','2025-01-08','Maharashtra','Direct',     6, 24590.00),
('SP1009','2025-01-10','Maharashtra','Partner',    4,  9100.00),

-- February — Tamil Nadu
('SP1010','2025-02-02','Tamil Nadu','Marketplace', 6, 15200.00),
('SP1011','2025-02-05','Tamil Nadu','Direct',      5, 19840.00),
('SP1012','2025-02-08','Tamil Nadu','Partner',     4, 10250.00),
-- February — Karnataka
('SP1013','2025-02-03','Karnataka','Marketplace',  7, 16050.00),
('SP1014','2025-02-06','Karnataka','Direct',       6, 22500.00),
('SP1015','2025-02-09','Karnataka','Partner',      3,  8600.00),
-- February — Maharashtra
('SP1016','2025-02-04','Maharashtra','Marketplace',8, 17590.00),
('SP1017','2025-02-07','Maharashtra','Direct',     6, 25920.00),
('SP1018','2025-02-10','Maharashtra','Partner',    5,  9400.00),

-- March — Tamil Nadu
('SP1019','2025-03-01','Tamil Nadu','Marketplace', 6, 15900.00),
('SP1020','2025-03-03','Tamil Nadu','Direct',      5, 20550.00),
('SP1021','2025-03-06','Tamil Nadu','Partner',     4, 10800.00),
-- March — Karnataka
('SP1022','2025-03-02','Karnataka','Marketplace',  7, 16380.00),
('SP1023','2025-03-05','Karnataka','Direct',       6, 23140.00),
('SP1024','2025-03-08','Karnataka','Partner',      3,  8900.00),
-- March — Maharashtra
('SP1025','2025-03-04','Maharashtra','Marketplace',8, 18360.00),
('SP1026','2025-03-07','Maharashtra','Direct',     6, 26790.00),
('SP1027','2025-03-09','Maharashtra','Partner',    5,  9700.00);




----streamlit code with line chart area chart bar chart 



import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import plotly.express as px

# -----------------------------------------
# Page Config
# -----------------------------------------
st.set_page_config(
    page_title="State & Platform Orders Dashboard",
    layout="wide"
)

st.title("📦 Orders by State & Platform")

# -----------------------------------------
# Snowflake Session
# -----------------------------------------
session = get_active_session()

# -----------------------------------------
# Load Data
# -----------------------------------------
df = session.sql("""
    SELECT
        orderid,
        orderdate,
        state,
        platform,
        itemscount,
        totalamount
    FROM ordersstateplatform
""").to_pandas()

# -----------------------------------------
# Data Type Fix (IMPORTANT)
# -----------------------------------------
df["ORDERDATE"] = pd.to_datetime(df["ORDERDATE"])

# -----------------------------------------
# Sidebar Filters
# -----------------------------------------
st.sidebar.header("Filters")

states = ["ALL"] + sorted(df["STATE"].unique().tolist())
platforms = ["ALL"] + sorted(df["PLATFORM"].unique().tolist())

selected_state = st.sidebar.selectbox("Select State", states)
selected_platform = st.sidebar.selectbox("Select Platform", platforms)

min_date = df["ORDERDATE"].min().date()
max_date = df["ORDERDATE"].max().date()

start_date, end_date = st.sidebar.date_input(
    "Select Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

# Convert sidebar dates to pandas Timestamp
start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)

# -----------------------------------------
# Apply Filters
# -----------------------------------------
filtered_df = df.copy()

if selected_state != "ALL":
    filtered_df = filtered_df[filtered_df["STATE"] == selected_state]

if selected_platform != "ALL":
    filtered_df = filtered_df[filtered_df["PLATFORM"] == selected_platform]

filtered_df = filtered_df[
    (filtered_df["ORDERDATE"] >= start_date) &
    (filtered_df["ORDERDATE"] <= end_date)
]

# -----------------------------------------
# KPI Section
# -----------------------------------------
total_orders = filtered_df["ORDERID"].nunique()
total_items = filtered_df["ITEMSCOUNT"].sum()
total_revenue = filtered_df["TOTALAMOUNT"].sum()

c1, c2, c3 = st.columns(3)
c1.metric("Total Orders", total_orders)
c2.metric("Total Items Sold", int(total_items))
c3.metric("Total Revenue", f"₹ {total_revenue:,.2f}")

st.divider()

# -----------------------------------------
# BAR CHART – Revenue by State & Platform
# -----------------------------------------
st.subheader("📊 Revenue by State and Platform")

bar_df = (
    filtered_df
    .groupby(["STATE", "PLATFORM"], as_index=False)["TOTALAMOUNT"]
    .sum()
)

st.bar_chart(
    bar_df,
    x="STATE",
    y="TOTALAMOUNT",
    color="PLATFORM"
)

# -----------------------------------------
# LINE CHART – Revenue Trend Over Time
# -----------------------------------------
st.subheader("📈 Revenue Trend Over Time")

line_df = (
    filtered_df
    .groupby("ORDERDATE", as_index=False)["TOTALAMOUNT"]
    .sum()
    .sort_values("ORDERDATE")
)

st.line_chart(
    line_df,
    x="ORDERDATE",
    y="TOTALAMOUNT"
)

# -----------------------------------------
# AREA CHART – Cumulative Revenue
# -----------------------------------------
st.subheader("📉 Cumulative Revenue Trend")

area_df = line_df.copy()
area_df["CUMULATIVE_REVENUE"] = area_df["TOTALAMOUNT"].cumsum()

st.area_chart(
    area_df,
    x="ORDERDATE",
    y="CUMULATIVE_REVENUE"
)

st.divider()

# -----------------------------------------
# Detail Table
# -----------------------------------------
st.subheader("📋 Order Details")

st.dataframe(
    filtered_df.sort_values("ORDERDATE", ascending=False),
    use_container_width=True
)

st.caption("Data Source: ordersstateplatform table in Snowflake")

# -----------------------------------------
# PIE CHART – Revenue Share by Platform
# -----------------------------------------
st.subheader("🥧 Revenue Share by Platform")

pie_df = (
    filtered_df
    .groupby("PLATFORM", as_index=False)["TOTALAMOUNT"]
    .sum()
)

if pie_df.empty:
    st.info("No data available for pie chart.")
else:
    fig_pie = px.pie(
        pie_df,
        names="PLATFORM",
        values="TOTALAMOUNT",
        title="Revenue Contribution by Platform"
    )
    st.plotly_chart(fig_pie, use_container_width=True)




-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------cortex question


CREATE OR REPLACE TABLE retailknowledge (
  docid    STRING,
  title     STRING,
  body      STRING,
  category  STRING,
  source    STRING
);

INSERT INTO retailknowledge (docid, title, body, category, source) VALUES
('R-001','Inventory Reconciliation Guide',
'Perform daily cycle counts, compare with POS exports, investigate deltas over 2%. Log adjustments with reason codes.',
'Guide','kb/guides/inventoryreconciliation.txt'),

('R-002','Returns & Exchanges Policy',
'Customers may return items within 15 days with receipt. Electronics: seal intact; apparel: tags attached. Refunds via original payment method.',
'Policy','kb/policies/returnsexchanges.txt'),

('R-003','Cash Handling Procedure',
'Open till with supervisor, count float, record variances at shift end, deposit sealed envelope in vault drop box.',
'Procedure','kb/procedures/cashhandling.txt'),

('R-004','Loyalty Program FAQ',
'Enroll at checkout with phone number. Points post within 24 hours. Redeem on eligible items; exclusions apply.',
'FAQ','kb/faqs/loyaltyprogram.txt'),

('R-005','Visual Merchandising Guide',
'Follow planogram, place high-velocity SKUs at eye level, ensure end-cap compliance, refresh signage weekly.',
'Guide','kb/guides/visual_merchandising.txt');



-- cprtex code 


--verify the udf
SELECT * FROM retailknowledge;


--create cortex search aservice 


CREATE OR REPLACE CORTEX SEARCH SERVICE retail_search_svc
ON body
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 hour'
AS
SELECT
  docid,
  title,
  body,
  category,
  source
FROM retailknowledge;

--- to show cortex searc h service 


SHOW CORTEX SEARCH SERVICES;



--- basic semantic query 


SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  'retail_search_svc',
  '{
    "query": "How do refunds work for electronics?",
    "columns": ["DOCID","TITLE","CATEGORY","BODY"],
    "limit": 3
  }'
) AS RAW_JSON;


--- inspect json structure 

WITH j AS (
  SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'retail_search_svc',
      '{
        "query": "returns policy",
        "columns": ["DOCID","TITLE","CATEGORY","BODY"],
        "limit": 3
      }'
    )
  ) AS r
)
SELECT
  r:results[0]:title::STRING      AS title_test,
  r:results[0]:category::STRING   AS category_test,
  r:results[0]:docid::STRING     AS docid_test,
  r:results[0]:"@scores":cosine_similarity::FLOAT AS score_test
FROM j;



----------------------------------------------------------------------------------------------------------------------------------

-------------------snowpark


CREATE OR REPLACE TABLE servicecatalog (
  serviceid    STRING,
  servicename  STRING,
  domain        STRING
);

-- Fact: subscription line items (usage)
CREATE OR REPLACE TABLE subscriptionusage (
  subid          STRING,
  subdate        DATE,
  customerid     STRING,
  serviceid      STRING,
  seats           NUMBER(10,0),
  priceperseat  NUMBER(10,2)
);

-- Sample services (different dataset & columns from previous question)
INSERT INTO servicecatalog (serviceid, servicename, domain) VALUES
('SVC01','Cloud Storage','Collaboration'),
('SVC02','Video Conferencing','Collaboration'),
('SVC03','CRM Basic','Sales & CRM'),
('SVC04','Email Suite','Productivity'),
('SVC05','Analytics Pro','Analytics'),
('SVC06','HR Portal','HR & Admin'),
('SVC07','Ticketing System','Support'),
('SVC08','Marketing Automation','Marketing'),
('SVC09','Team Chat','Collaboration'),
('SVC10','Code Repository','IT & DevOps');

-- Subscription usage (Jan–Mar 2025), varied seats and priceperseat
INSERT INTO subscriptionusage (subid, subdate, customerid, serviceid, seats, priceperseat) VALUES
-- January
('SUB1001','2025-01-03','C001','SVC01', 50,  120.00),
('SUB1002','2025-01-04','C002','SVC02', 25,  200.00),
('SUB1003','2025-01-05','C003','SVC03', 15,  450.00),
('SUB1004','2025-01-06','C004','SVC04', 80,   90.00),
('SUB1005','2025-01-07','C005','SVC05', 20,  600.00),
('SUB1006','2025-01-08','C006','SVC06', 40,  150.00),
('SUB1007','2025-01-09','C007','SVC07', 35,  220.00),
('SUB1008','2025-01-10','C001','SVC08', 30,  300.00),
('SUB1009','2025-01-11','C002','SVC09', 120,  40.00),
('SUB1010','2025-01-12','C003','SVC10', 60,  110.00),

-- February
('SUB1011','2025-02-02','C004','SVC01', 70,  115.00),
('SUB1012','2025-02-03','C005','SVC02', 40,  195.00),
('SUB1013','2025-02-04','C006','SVC03', 18,  460.00),
('SUB1014','2025-02-05','C007','SVC04', 90,   88.00),
('SUB1015','2025-02-06','C001','SVC05', 25,  620.00),
('SUB1016','2025-02-07','C002','SVC06', 55,  155.00),
('SUB1017','2025-02-08','C003','SVC07', 45,  230.00),
('SUB1018','2025-02-09','C004','SVC08', 35,  310.00),
('SUB1019','2025-02-10','C005','SVC09', 140,  42.00),
('SUB1020','2025-02-11','C006','SVC10', 75,  115.00),

-- March
('SUB1021','2025-03-01','C007','SVC01', 60,  118.00),
('SUB1022','2025-03-02','C001','SVC02', 50,  198.00),
('SUB1023','2025-03-03','C002','SVC03', 22,  470.00),
('SUB1024','2025-03-04','C003','SVC04', 85,   92.00),
('SUB1025','2025-03-05','C004','SVC05', 30,  610.00),
('SUB1026','2025-03-06','C005','SVC06', 65,  160.00),
('SUB1027','2025-03-07','C006','SVC07', 50,  240.00),
('SUB1028','2025-03-08','C007','SVC08', 40,  320.00),
('SUB1029','2025-03-09','C001','SVC09', 160,  41.00),
('SUB1030','2025-03-10','C002','SVC10', 90,  118.00);




-----snowapark code now 


from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col,
    sum as sf_sum,
    avg as sf_avg,
    count as sf_count,
    rank
)
from snowflake.snowpark.window import Window

# --------------------------------------------------
# STEP 1: Create Snowpark Session
# --------------------------------------------------
session = Session.builder.config("connection_name", "default").create()
print("Snowpark session created")

# --------------------------------------------------
services_df = session.table("SERVICECATALOG")
usage_df = session.table("SUBSCRIPTIONUSAGE")

# --------------------------------------------------
# STEP 3: Join Fact + Dimension
# --------------------------------------------------
joined_df = usage_df.join(
    services_df,
    "SERVICEID"
)

# --------------------------------------------------
# STEP 4: Derived Metric (Revenue)
# --------------------------------------------------
revenue_df = joined_df.with_column(
    "REVENUE",
    col("SEATS") * col("PRICEPERSEAT")
)

# --------------------------------------------------
# STEP 5: Aggregate by Domain + Service
# --------------------------------------------------
agg_df = revenue_df.group_by(
    "DOMAIN",
    "SERVICENAME"
).agg(
    sf_sum("REVENUE").alias("TOTAL_REVENUE"),
    sf_avg("REVENUE").alias("AVG_REVENUE"),
    sf_count("*").alias("USAGE_COUNT")
)

# --------------------------------------------------
# STEP 6: Rank Services BY DOMAIN (Correct)
# --------------------------------------------------
rank_window = Window.partition_by("DOMAIN").order_by(col("TOTAL_REVENUE").desc())

ranked_df = agg_df.with_column(
    "SERVICE_RANK",
    rank().over(rank_window)
)

# --------------------------------------------------
# STEP 7: Post-Aggregation Filters (HAVING)
# - Top 3 services per domain
# - Revenue threshold lowered to realistic value
# --------------------------------------------------
filtered_df = ranked_df.filter(
    (col("SERVICE_RANK") <= 3) &
    (col("TOTAL_REVENUE") > 30000)
)

# --------------------------------------------------
# STEP 8: Final Sort
# --------------------------------------------------
final_df = filtered_df.sort(
    col("DOMAIN"),
    col("TOTAL_REVENUE").desc()
)

final_df.show()

------------------------------------------------------------------------------------------------------------------------------



Snowpark: Join Two DataFrames, Aggregate, and Apply Two Post-Aggregation Filters


CREATE OR REPLACE TABLE productmap (
  productid   STRING,
  productname STRING,
  category     STRING
);

-- Sales line items
CREATE OR REPLACE TABLE salesitems (
  orderid    STRING,
  orderdate  DATE,
  customerid STRING,
  productid  STRING,
  qty         NUMBER(10,0),
  unitprice  NUMBER(10,2)
);

INSERT INTO productmap (productid, productname, category) VALUES
('Z001','Smartwatch Pro','Electronics'),
('Z002','Bluetooth Speaker','Electronics'),
('Z003','Ergonomic Mouse','Electronics'),
('Z004','Executive Chair','Furniture'),
('Z005','Workstation Desk','Furniture');

INSERT INTO salesitems (orderid, orderdate, customerid, productid, qty, unitprice) VALUES
('SI1001','2025-01-03','CU001','Z001', 3, 12000.00),
('SI1002','2025-01-04','CU002','Z002', 5,  4500.00),
('SI1003','2025-01-05','CU003','Z003', 4,  2800.00),
('SI1004','2025-01-06','CU004','Z004', 2, 15000.00),
('SI1005','2025-01-07','CU005','Z005', 1, 38000.00),
('SI1006','2025-02-02','CU006','Z001', 2, 11800.00),
('SI1007','2025-02-04','CU007','Z002', 8,  4300.00),
('SI1008','2025-02-06','CU008','Z003', 7,  2900.00),
('SI1009','2025-02-08','CU009','Z004', 1, 15200.00),
('SI1010','2025-02-10','CU010','Z005', 3, 37000.00),
('SI1011','2025-03-02','CU011','Z001', 4, 11900.00),
('SI1012','2025-03-04','CU012','Z002', 6,  4400.00),
('SI1013','2025-03-06','CU013','Z003', 9,  3000.00),
('SI1014','2025-03-08','CU014','Z004', 2, 14900.00),
('SI1015','2025-03-10','CU015','Z005', 2, 39000.00);



snowpark code 

from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col,
    sum as sf_sum,
    avg as sf_avg,
    count as sf_count
)

# --------------------------------------------------
# STEP 1: Create Snowpark Session
# --------------------------------------------------
session = Session.builder.config("connection_name", "default").create()
print("Snowpark session created")

# --------------------------------------------------
# STEP 2: Load Tables
# --------------------------------------------------
products_df = session.table("PRODUCTMAP")
sales_df = session.table("SALESITEMS")

# --------------------------------------------------
# STEP 3: Join Sales with Product Map
# --------------------------------------------------
joined_df = sales_df.join(
    products_df,
    sales_df["PRODUCTID"] == products_df["PRODUCTID"],
    how="inner"
)

joined_df.show()

# --------------------------------------------------
# STEP 4: Derived Column (Sales Value)
# sales_value = qty * unitprice
# --------------------------------------------------
sales_value_df = joined_df.with_column(
    "SALES_VALUE",
    col("QTY") * col("UNITPRICE")
)

sales_value_df.show()

# --------------------------------------------------
# STEP 5: Aggregate by Category + Product
# --------------------------------------------------
agg_df = sales_value_df.group_by(
    "CATEGORY",
    "PRODUCTNAME"
).agg(
    sf_sum("SALES_VALUE").alias("TOTAL_SALES"),
    sf_avg("SALES_VALUE").alias("AVG_SALES"),
    sf_count("*").alias("ORDER_COUNT")
)

agg_df.show()

# --------------------------------------------------
# STEP 6: TWO Post-Aggregation Filters (HAVING-like)
# 1) TOTAL_SALES > 100000
# 2) ORDER_COUNT >= 3
# --------------------------------------------------
filtered_df = agg_df.filter(
    (col("TOTAL_SALES") > 100000) &
    (col("ORDER_COUNT") >= 3)
)

filtered_df.show()

# --------------------------------------------------
# STEP 7: Sort Descending by TOTAL_SALES
# --------------------------------------------------
final_df = filtered_df.sort(
    col("TOTAL_SALES").desc()
)

final_df.show()


-------------------------------------------------------------------------------------------------------------------------------------------
coretx question


Dataset Context\ Create and populate the kb_articles table with mixed categories: “guide”, “procedure”, “policy”, “faq”.
Setup SQL (Run First)
CREATE OR REPLACE TABLE kbarticles (
  docid    STRING,
  title     STRING,
  content   STRING,
  category  STRING,
  source    STRING
);

INSERT INTO kbarticles (docid, title, content, category, source) VALUES
('A-001','Laptop Setup Guide',
'Install OS updates, configure Wi-Fi, enable antivirus, sign in with corporate SSO.',
'Guide','kb/guides/laptopsetup.txt'),
('A-002','Remote Login Policy',
'Access to corporate systems must use VPN and MFA; sessions auto-expire after inactivity.',
'Policy','kb/policies/remotelogin.txt'),
('A-003','Printer Troubleshooting',
'Check power and connectivity, reinstall drivers, run diagnostic tool, contact support.',
'FAQ','kb/faqs/printertroubleshooting.txt'),
('A-004','Onboarding Procedure',
'Create user account, assign roles, provision devices, send welcome kit.',
'Procedure','kb/procedures/onboarding.txt'),
('A-005','Two-Factor Setup',
'Install authenticator app, scan QR, save backup codes, test login.',
'Guide','kb/guides/2fasetup.txt');



--- coretx code 


--- to create cortex search srvice 

CREATE OR REPLACE CORTEX SEARCH SERVICE kb_articles_search
ON content
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 hour'
AS
SELECT
  docid,
  title,
  content,
  category,
  source
FROM kbarticles;



--run basic semantic service 

SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  'kb_articles_search',
  '{
    "query": "how to setup laptop",
    "columns": ["DOCID","TITLE","CATEGORY","CONTENT"],
    "limit": 3
  }'
) AS raw_result;



--inspect json
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'kb_articles_search',
    '{
      "query": "VPN access policy",
      "columns": ["DOCID","TITLE","CATEGORY","CONTENT"],
      "limit": 3
    }'
  )
) AS r;



------------------TEST SINGLE FIELD EXTRACTION (NO FLATTEN YET)

WITH j AS (
  SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'kb_articles_search',
      '{
        "query": "two factor authentication",
        "columns": ["DOCID","TITLE","CATEGORY","CONTENT"],
        "limit": 3
      }'
    )
  ) AS r
)
SELECT
  r:results[0]:TITLE::STRING     AS title_test,
  r:results[0]:CATEGORY::STRING  AS category_test,
  r:results[0]:DOCID::STRING     AS docid_test,
  r:results[0]:"@scores":cosine_similarity::FLOAT AS score_test
FROM j;


----------------------------- FINAL FLATTENED TABULAR OUTPUT (MOST IMPORTANT)

WITH j AS (
  SELECT
    'vpn policy query' AS query,
    PARSE_JSON(
      SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'kb_articles_search',
        '{
          "query": "vpn access policy",
          "columns": ["DOCID","TITLE","CATEGORY","CONTENT"],
          "limit": 5
        }'
      )
    ) AS r
)
SELECT
  query,
  f.value:TITLE::STRING     AS matched_title,
  f.value:CONTENT::STRING  AS snippet,
  f.value:CATEGORY::STRING AS category,
  f.value:DOCID::STRING    AS doc_id,
  f.value:"@scores":cosine_similarity::FLOAT AS score
FROM j,
LATERAL FLATTEN(input => r:results) f
ORDER BY score DESC;


--------------------streamlit code



CREATE OR REPLACE TABLE orderscitychannel (
  orderid   STRING,
  orderdate DATE,
  city       STRING,
  channel    STRING,
  ordervalue NUMBER(12,2)
);

INSERT INTO orderscitychannel (orderid, orderdate, city, channel, ordervalue) VALUES
-- Chennai
('OC2001','2025-01-02','Chennai','Online', 14250.00),
('OC2002','2025-01-04','Chennai','Retail', 18990.00),
('OC2003','2025-01-06','Chennai','Wholesale',  9800.00),
-- Bengaluru
('OC2004','2025-01-03','Bengaluru','Online', 15600.00),
('OC2005','2025-01-07','Bengaluru','Retail', 21850.00),
('OC2006','2025-01-09','Bengaluru','Wholesale',  8200.00),
-- Mumbai
('OC2007','2025-01-05','Mumbai','Online', 16850.00),
('OC2008','2025-01-08','Mumbai','Retail', 24590.00),
('OC2009','2025-01-10','Mumbai','Wholesale',  9100.00),

-- February
('OC2010','2025-02-02','Chennai','Online', 15200.00),
('OC2011','2025-02-05','Chennai','Retail', 19840.00),
('OC2012','2025-02-08','Chennai','Wholesale', 10250.00),
('OC2013','2025-02-03','Bengaluru','Online', 16050.00),
('OC2014','2025-02-06','Bengaluru','Retail', 22500.00),
('OC2015','2025-02-09','Bengaluru','Wholesale',  8600.00),
('OC2016','2025-02-04','Mumbai','Online', 17590.00),
('OC2017','2025-02-07','Mumbai','Retail', 25920.00),
('OC2018','2025-02-10','Mumbai','Wholesale',  9400.00),

-- March
('OC2019','2025-03-01','Chennai','Online', 15900.00),
('OC2020','2025-03-03','Chennai','Retail', 20550.00),
('OC2021','2025-03-06','Chennai','Wholesale', 10800.00),
('OC2022','2025-03-02','Bengaluru','Online', 16380.00),
('OC2023','2025-03-05','Bengaluru','Retail', 23140.00),
('OC2024','2025-03-08','Bengaluru','Wholesale',  8900.00),
('OC2025','2025-03-04','Mumbai','Online', 18360.00),
('OC2026','2025-03-07','Mumbai','Retail', 26790.00),
('OC2027','2025-03-09','Mumbai','Wholesale',  9700.00);




streamlit code 



import streamlit as st
from snowflake.snowpark.context import get_active_session

# -------------------------------------------------
# Page Config
# -------------------------------------------------
st.set_page_config(
    page_title="City-wise Orders by Channel",
    layout="wide"
)

st.title("📊 City-wise Orders Dashboard")
st.caption("Data Source: orderscitychannel table in Snowflake")

# -------------------------------------------------
# Snowflake Session
# -------------------------------------------------
session = get_active_session()

# -------------------------------------------------
# Load Base Data
# -------------------------------------------------
base_df = session.sql("""
    SELECT
        orderid,
        orderdate,
        city,
        channel,
        ordervalue
    FROM orderscitychannel
""").to_pandas()

# -------------------------------------------------
# Sidebar Filters
# -------------------------------------------------
st.sidebar.header("🔎 Filters")

city_list = sorted(base_df["CITY"].unique().tolist())
channel_list = sorted(base_df["CHANNEL"].unique().tolist())

selected_cities = st.sidebar.multiselect(
    "Select City",
    city_list,
    default=city_list
)

selected_channels = st.sidebar.multiselect(
    "Select Channel",
    channel_list,
    default=channel_list
)

min_date = base_df["ORDERDATE"].min()
max_date = base_df["ORDERDATE"].max()

date_range = st.sidebar.date_input(
    "Select Date Range",
    [min_date, max_date],
    min_value=min_date,
    max_value=max_date
)

# -------------------------------------------------
# Apply Filters
# -------------------------------------------------
filtered_df = base_df[
    (base_df["CITY"].isin(selected_cities)) &
    (base_df["CHANNEL"].isin(selected_channels)) &
    (base_df["ORDERDATE"] >= date_range[0]) &
    (base_df["ORDERDATE"] <= date_range[1])
]

if filtered_df.empty:
    st.warning("No data available for selected filters.")
    st.stop()

# -------------------------------------------------
# KPI Section
# -------------------------------------------------
total_orders = filtered_df["ORDERID"].nunique()
total_revenue = filtered_df["ORDERVALUE"].sum()
avg_order_value = filtered_df["ORDERVALUE"].mean()

k1, k2, k3 = st.columns(3)
k1.metric("Total Orders", total_orders)
k2.metric("Total Revenue", f"₹ {total_revenue:,.2f}")
k3.metric("Avg Order Value", f"₹ {avg_order_value:,.2f}")

st.divider()

# -------------------------------------------------
# BAR CHART — City vs Total Order Value
# -------------------------------------------------
st.subheader("🏙️ Total Order Value by City & Channel")

bar_df = (
    filtered_df
    .groupby(["CITY", "CHANNEL"], as_index=False)["ORDERVALUE"]
    .sum()
)

st.bar_chart(
    bar_df,
    x="CITY",
    y="ORDERVALUE",
    color="CHANNEL",
    use_container_width=True
)

st.divider()

# -------------------------------------------------
# LINE CHART — Order Value Trend Over Time
# -------------------------------------------------
st.subheader("📈 Order Value Trend Over Time")

line_df = (
    filtered_df
    .groupby("ORDERDATE", as_index=False)["ORDERVALUE"]
    .sum()
    .sort_values("ORDERDATE")
)

st.line_chart(
    line_df,
    x="ORDERDATE",
    y="ORDERVALUE",
    use_container_width=True
)

st.divider()

# -------------------------------------------------
# PIE CHART — Channel Share
# -------------------------------------------------
st.subheader("🥧 Order Value Share by Channel")

pie_df = (
    filtered_df
    .groupby("CHANNEL", as_index=False)["ORDERVALUE"]
    .sum()
)

st.dataframe(pie_df, use_container_width=True)

st.bar_chart(
    pie_df,
    x="CHANNEL",
    y="ORDERVALUE",
    use_container_width=True
)

st.divider()

# -------------------------------------------------
# Detail Table
# -------------------------------------------------
st.subheader("📋 Order Details")

st.dataframe(
    filtered_df.sort_values("ORDERDATE", ascending=False),
    use_container_width=True
)




-----------------------------------------------------------------------------------------------------------------------

json question

---verfiy the data lloaded 

SELECT doc
FROM profileraw;



---inspect top level fields
SELECT
  doc:user_id::STRING   AS user_id,
  doc:name::STRING      AS name,
  doc:meta.plan::STRING AS plan,
  doc:meta.region::STRING AS region,
  doc:meta.signup_ts::TIMESTAMP_NTZ AS signup_ts
FROM profileraw;



------Inspect Sessions Array (Before FLATTEN)



SELECT
  doc:user_id::STRING AS user_id,
  doc:sessions        AS sessions_array
FROM profileraw;




--FLATEN THE SESSION ARRAY 


SELECT
  doc:user_id::STRING AS user_id,
  doc:name::STRING    AS name,
  f.value:session_id::STRING AS session_id,
  f.value:start_ts::TIMESTAMP_NTZ AS session_start_ts,
  f.value:active::BOOLEAN AS active
FROM profileraw,
LATERAL FLATTEN(input => doc:sessions) f;



---FILTER ACTIVE SESSIONS ONLY


SELECT
  doc:user_id::STRING AS user_id,
  f.value:session_id::STRING AS session_id,
  f.value:start_ts::TIMESTAMP_NTZ AS session_start_ts
FROM profileraw,
LATERAL FLATTEN(input => doc:sessions) f
WHERE f.value:active::BOOLEAN = TRUE;



--COUNT SESSION PER USER

SELECT
  doc:user_id::STRING AS user_id,
  COUNT(*) AS total_sessions
FROM profileraw,
LATERAL FLATTEN(input => doc:sessions) f
GROUP BY user_id
ORDER BY total_sessions DESC;


--COUNT ACTIVE SESSION PER USER

SELECT
  doc:user_id::STRING AS user_id,
  COUNT(*) AS active_sessions
FROM profileraw,
LATERAL FLATTEN(input => doc:sessions) f
WHERE f.value:active::BOOLEAN = TRUE
GROUP BY user_id;



--OPTIONAL CREATE STRUCTURED VIEW 



CREATE OR REPLACE VIEW user_sessions_flat AS
SELECT
  doc:user_id::STRING AS user_id,
  doc:name::STRING AS name,
  doc:meta.plan::STRING AS plan,
  doc:meta.region::STRING AS region,
  f.value:session_id::STRING AS session_id,
  f.value:start_ts::TIMESTAMP_NTZ AS session_start_ts,
  f.value:active::BOOLEAN AS active
FROM profileraw,
LATERAL FLATTEN(input => doc:sessions) f;

SELECT * FROM user_sessions_flat;

------------------------------





-- Create demo table
CREATE OR REPLACE TABLE demoitems (
  itemid       STRING,
  productname  STRING,
  quantity     NUMBER(10,0),
  unitprice    NUMBER(10,2)
);

-- Insert sample data
INSERT INTO demoitems (itemid, productname, quantity, unitprice) VALUES
('I001', '  smartPhone  x  ', 2, 34999.50),
('I002', 'SMARTphone  X',     1, 35999.00),
('I003', 'Noise Cancel Headset', 3, 7999.00),
('I004', 'office  Chair',     1, 11999.00),
('I005', 'Standing   Desk',   2, 28999.00);



from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.functions import (

    sum as sf_sum,
    avg as sf_avg,
    min as sf_min,
    max as sf_max,
    upper,
    trim,
    regexp_replace
)


from snowflake.snowpark import session

session = Session.builder.config("connection_name", "default").create()

print("Snowpark session created")

demo_df = session.table("DEMOITEMS")

# Show raw data
print("📊 Raw Data:")
demo_df.show()

# Data cleansing: trim spaces, collapse multiple spaces, convert to uppercase
clean_df = demo_df.select(
    col("itemid"),
    upper(trim(regexp_replace(col("productname"), r'\s+', ' '))).alias("clean_productname"),
    col("quantity"),
    col("unitprice"),
    (col("quantity") * col("unitprice")).alias("total_price")
)

print("✨ Cleaned Data:")
clean_df.show()



--------------------------------------------------------------------------------------------------

----MY NEW EXACT
-----------------------

---------------------------------

----------------------------------------------


--------------------





---------------------------------UDF (SNOWFLAKE)------------------------------

CREATE OR REPLACE TABLE demoitems (
  itemid      STRING,
  productname STRING,
  quantity    NUMBER(10,0),
  unitprice   NUMBER(10,2)
);



INSERT INTO demoitems (itemid, productname, quantity, unitprice) VALUES
('I001', '  smartPhone  x  ', 2, 34999.50),
('I002', 'SMARTphone  X',     1, 35999.00),
('I003', 'Noise Cancel Headset', 3, 7999.00),
('I004', 'office  Chair',     1, 11999.00),
('I005', 'Standing   Desk',   2, 28999.00);


---- creatting udf(string)

CREATE OR REPLACE FUNCTION CLEAN_PRODUCT_NAME(name STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'clean_name'
AS
$$
def clean_name(name):
    if name is None:
        return None
    # remove extra spaces, convert to title case
    return " ".join(name.strip().split()).title()
$$;



---creating python udf (number)


CREATE OR REPLACE FUNCTION CALCULATE_TOTAL_VALUE(qty NUMBER, price NUMBER)
RETURNS NUMBER(12,2)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'calc_total'
AS
$$
def calc_total(qty, price):
    if qty is None or price is None:
        return 0
    return round(qty * price, 2)
$$;


--showing the output 


SELECT
    itemid,
    CLEAN_PRODUCT_NAME(productname) AS clean_product_name,
    quantity,
    unitprice,
    CALCULATE_TOTAL_VALUE(quantity, unitprice) AS total_value
FROM demoitems
ORDER BY itemid;


--- to verfiy the udf exists

SHOW USER FUNCTIONS LIKE '%PRODUCT%';

--===========================================================

-- Streamlit

CREATE OR REPLACE TABLE ordersstateplatform (
  orderid     STRING,
  orderdate   DATE,
  state        STRING,
  platform     STRING,       -- e.g., Marketplace, Direct, Partner
  itemscount  NUMBER(10,0),
  totalamount NUMBER(12,2)
);

INSERT INTO ordersstateplatform (orderid, orderdate, state, platform, itemscount, totalamount) VALUES
-- January — Tamil Nadu
('SP1001','2025-01-02','Tamil Nadu','Marketplace', 6, 14250.00),
('SP1002','2025-01-04','Tamil Nadu','Direct',      4, 18990.00),
('SP1003','2025-01-06','Tamil Nadu','Partner',     5,  9800.00),
-- January — Karnataka
('SP1004','2025-01-03','Karnataka','Marketplace',  7, 15600.00),
('SP1005','2025-01-07','Karnataka','Direct',       5, 21850.00),
('SP1006','2025-01-09','Karnataka','Partner',      3,  8200.00),
-- January — Maharashtra
('SP1007','2025-01-05','Maharashtra','Marketplace',8, 16850.00),
('SP1008','2025-01-08','Maharashtra','Direct',     6, 24590.00),
('SP1009','2025-01-10','Maharashtra','Partner',    4,  9100.00),

-- February — Tamil Nadu
('SP1010','2025-02-02','Tamil Nadu','Marketplace', 6, 15200.00),
('SP1011','2025-02-05','Tamil Nadu','Direct',      5, 19840.00),
('SP1012','2025-02-08','Tamil Nadu','Partner',     4, 10250.00),
-- February — Karnataka
('SP1013','2025-02-03','Karnataka','Marketplace',  7, 16050.00),
('SP1014','2025-02-06','Karnataka','Direct',       6, 22500.00),
('SP1015','2025-02-09','Karnataka','Partner',      3,  8600.00),
-- February — Maharashtra
('SP1016','2025-02-04','Maharashtra','Marketplace',8, 17590.00),
('SP1017','2025-02-07','Maharashtra','Direct',     6, 25920.00),
('SP1018','2025-02-10','Maharashtra','Partner',    5,  9400.00),

-- March — Tamil Nadu
('SP1019','2025-03-01','Tamil Nadu','Marketplace', 6, 15900.00),
('SP1020','2025-03-03','Tamil Nadu','Direct',      5, 20550.00),
('SP1021','2025-03-06','Tamil Nadu','Partner',     4, 10800.00),
-- March — Karnataka
('SP1022','2025-03-02','Karnataka','Marketplace',  7, 16380.00),
('SP1023','2025-03-05','Karnataka','Direct',       6, 23140.00),
('SP1024','2025-03-08','Karnataka','Partner',      3,  8900.00),
-- March — Maharashtra
('SP1025','2025-03-04','Maharashtra','Marketplace',8, 18360.00),
('SP1026','2025-03-07','Maharashtra','Direct',     6, 26790.00),
('SP1027','2025-03-09','Maharashtra','Partner',    5,  9700.00);




import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import plotly.express as px

# -----------------------------------------
# Page Config
# -----------------------------------------
st.set_page_config(
    page_title="State & Platform Orders Dashboard",
    layout="wide"
)

st.title("📦 Orders by State & Platform")

# -----------------------------------------
# Snowflake Session
# -----------------------------------------
session = get_active_session()

# -----------------------------------------
# Load Data
# -----------------------------------------
df = session.sql("""
    SELECT
        orderid,
        orderdate,
        state,
        platform,
        itemscount,
        totalamount
    FROM ordersstateplatform
""").to_pandas()

# -----------------------------------------
# Data Type Fix (IMPORTANT)
# -----------------------------------------
df["ORDERDATE"] = pd.to_datetime(df["ORDERDATE"])

# -----------------------------------------
# Sidebar Filters
# -----------------------------------------
st.sidebar.header("Filters")

states = ["ALL"] + sorted(df["STATE"].unique().tolist())
platforms = ["ALL"] + sorted(df["PLATFORM"].unique().tolist())

selected_state = st.sidebar.selectbox("Select State", states)
selected_platform = st.sidebar.selectbox("Select Platform", platforms)

min_date = df["ORDERDATE"].min().date()
max_date = df["ORDERDATE"].max().date()

start_date, end_date = st.sidebar.date_input(
    "Select Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

# Convert sidebar dates to pandas Timestamp
start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)

# -----------------------------------------
# Apply Filters
# -----------------------------------------
filtered_df = df.copy()

if selected_state != "ALL":
    filtered_df = filtered_df[filtered_df["STATE"] == selected_state]

if selected_platform != "ALL":
    filtered_df = filtered_df[filtered_df["PLATFORM"] == selected_platform]

filtered_df = filtered_df[
    (filtered_df["ORDERDATE"] >= start_date) &
    (filtered_df["ORDERDATE"] <= end_date)
]

# -----------------------------------------
# KPI Section
# -----------------------------------------
total_orders = filtered_df["ORDERID"].nunique()
total_items = filtered_df["ITEMSCOUNT"].sum()
total_revenue = filtered_df["TOTALAMOUNT"].sum()

c1, c2, c3 = st.columns(3)
c1.metric("Total Orders", total_orders)
c2.metric("Total Items Sold", int(total_items))
c3.metric("Total Revenue", f"₹ {total_revenue:,.2f}")

st.divider()

# -----------------------------------------
# BAR CHART – Revenue by State & Platform
# -----------------------------------------
st.subheader("📊 Revenue by State and Platform")

bar_df = (
    filtered_df
    .groupby(["STATE", "PLATFORM"], as_index=False)["TOTALAMOUNT"]
    .sum()
)

st.bar_chart(
    bar_df,
    x="STATE",
    y="TOTALAMOUNT",
    color="PLATFORM"
)

# -----------------------------------------
# LINE CHART – Revenue Trend Over Time
# -----------------------------------------
st.subheader("📈 Revenue Trend Over Time")

line_df = (
    filtered_df
    .groupby("ORDERDATE", as_index=False)["TOTALAMOUNT"]
    .sum()
    .sort_values("ORDERDATE")
)

st.line_chart(
    line_df,
    x="ORDERDATE",
    y="TOTALAMOUNT"
)

# -----------------------------------------
# AREA CHART – Cumulative Revenue
# -----------------------------------------
st.subheader("📉 Cumulative Revenue Trend")

area_df = line_df.copy()
area_df["CUMULATIVE_REVENUE"] = area_df["TOTALAMOUNT"].cumsum()

st.area_chart(
    area_df,
    x="ORDERDATE",
    y="CUMULATIVE_REVENUE"
)

st.divider()

# -----------------------------------------
# Detail Table
# -----------------------------------------
st.subheader("📋 Order Details")

st.dataframe(
    filtered_df.sort_values("ORDERDATE", ascending=False),
    use_container_width=True
)

st.caption("Data Source: ordersstateplatform table in Snowflake")

# -----------------------------------------
# PIE CHART – Revenue Share by Platform
# -----------------------------------------
st.subheader("🥧 Revenue Share by Platform")

pie_df = (
    filtered_df
    .groupby("PLATFORM", as_index=False)["TOTALAMOUNT"]
    .sum()
)

if pie_df.empty:
    st.info("No data available for pie chart.")
else:
    fig_pie = px.pie(
        pie_df,
        names="PLATFORM",
        values="TOTALAMOUNT",
        title="Revenue Contribution by Platform"
    )
    st.plotly_chart(fig_pie, use_container_width=True)





--==============================================================================
-- CORTEX


CREATE OR REPLACE TABLE retailknowledge (
  docid    STRING,
  title     STRING,
  body      STRING,
  category  STRING,
  source    STRING
);

INSERT INTO retailknowledge (docid, title, body, category, source) VALUES
('R-001','Inventory Reconciliation Guide',
'Perform daily cycle counts, compare with POS exports, investigate deltas over 2%. Log adjustments with reason codes.',
'Guide','kb/guides/inventoryreconciliation.txt'),

('R-002','Returns & Exchanges Policy',
'Customers may return items within 15 days with receipt. Electronics: seal intact; apparel: tags attached. Refunds via original payment method.',
'Policy','kb/policies/returnsexchanges.txt'),

('R-003','Cash Handling Procedure',
'Open till with supervisor, count float, record variances at shift end, deposit sealed envelope in vault drop box.',
'Procedure','kb/procedures/cashhandling.txt'),

('R-004','Loyalty Program FAQ',
'Enroll at checkout with phone number. Points post within 24 hours. Redeem on eligible items; exclusions apply.',
'FAQ','kb/faqs/loyaltyprogram.txt'),

('R-005','Visual Merchandising Guide',
'Follow planogram, place high-velocity SKUs at eye level, ensure end-cap compliance, refresh signage weekly.',
'Guide','kb/guides/visual_merchandising.txt');



select * from retailknowledge;

--create cortex search aservice 


CREATE OR REPLACE CORTEX SEARCH SERVICE retail_search_svc
ON body
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 hour'
AS
SELECT
  docid,
  title,
  body,
  category,
  source
FROM retailknowledge;

--- to show cortex searc h service 


SHOW CORTEX SEARCH SERVICES;



--- basic semantic query 


SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  'retail_search_svc',
  '{
    "query": "How do refunds work for electronics?",
    "columns": ["DOCID","TITLE","CATEGORY","BODY"],
    "limit": 3
  }'
) AS RAW_JSON;


--- inspect json structure 

WITH j AS (
  SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'retail_search_svc',
      '{
        "query": "returns policy",
        "columns": ["DOCID","TITLE","CATEGORY","BODY"],
        "limit": 3
      }'
    )
  ) AS r
)
SELECT
  r:results[0]:TITLE::STRING      AS title_test,
  r:results[0]:CATEGORY::STRING   AS category_test,
  r:results[0]:DOCID::STRING     AS docid_test,
  r:results[0]:"@scores":cosine_similarity::FLOAT AS score_test
FROM j;

--=====================================================

-- SNOWPARK

CREATE OR REPLACE TABLE servicecatalog (
  serviceid    STRING,
  servicename  STRING,
  domain        STRING
);

-- Fact: subscription line items (usage)
CREATE OR REPLACE TABLE subscriptionusage (
  subid          STRING,
  subdate        DATE,
  customerid     STRING,
  serviceid      STRING,
  seats           NUMBER(10,0),
  priceperseat  NUMBER(10,2)
);

-- Sample services (different dataset & columns from previous question)
INSERT INTO servicecatalog (serviceid, servicename, domain) VALUES
('SVC01','Cloud Storage','Collaboration'),
('SVC02','Video Conferencing','Collaboration'),
('SVC03','CRM Basic','Sales & CRM'),
('SVC04','Email Suite','Productivity'),
('SVC05','Analytics Pro','Analytics'),
('SVC06','HR Portal','HR & Admin'),
('SVC07','Ticketing System','Support'),
('SVC08','Marketing Automation','Marketing'),
('SVC09','Team Chat','Collaboration'),
('SVC10','Code Repository','IT & DevOps');

-- Subscription usage (Jan–Mar 2025), varied seats and priceperseat
INSERT INTO subscriptionusage (subid, subdate, customerid, serviceid, seats, priceperseat) VALUES
-- January
('SUB1001','2025-01-03','C001','SVC01', 50,  120.00),
('SUB1002','2025-01-04','C002','SVC02', 25,  200.00),
('SUB1003','2025-01-05','C003','SVC03', 15,  450.00),
('SUB1004','2025-01-06','C004','SVC04', 80,   90.00),
('SUB1005','2025-01-07','C005','SVC05', 20,  600.00),
('SUB1006','2025-01-08','C006','SVC06', 40,  150.00),
('SUB1007','2025-01-09','C007','SVC07', 35,  220.00),
('SUB1008','2025-01-10','C001','SVC08', 30,  300.00),
('SUB1009','2025-01-11','C002','SVC09', 120,  40.00),
('SUB1010','2025-01-12','C003','SVC10', 60,  110.00),

-- February
('SUB1011','2025-02-02','C004','SVC01', 70,  115.00),
('SUB1012','2025-02-03','C005','SVC02', 40,  195.00),
('SUB1013','2025-02-04','C006','SVC03', 18,  460.00),
('SUB1014','2025-02-05','C007','SVC04', 90,   88.00),
('SUB1015','2025-02-06','C001','SVC05', 25,  620.00),
('SUB1016','2025-02-07','C002','SVC06', 55,  155.00),
('SUB1017','2025-02-08','C003','SVC07', 45,  230.00),
('SUB1018','2025-02-09','C004','SVC08', 35,  310.00),
('SUB1019','2025-02-10','C005','SVC09', 140,  42.00),
('SUB1020','2025-02-11','C006','SVC10', 75,  115.00),

-- March
('SUB1021','2025-03-01','C007','SVC01', 60,  118.00),
('SUB1022','2025-03-02','C001','SVC02', 50,  198.00),
('SUB1023','2025-03-03','C002','SVC03', 22,  470.00),
('SUB1024','2025-03-04','C003','SVC04', 85,   92.00),
('SUB1025','2025-03-05','C004','SVC05', 30,  610.00),
('SUB1026','2025-03-06','C005','SVC06', 65,  160.00),
('SUB1027','2025-03-07','C006','SVC07', 50,  240.00),
('SUB1028','2025-03-08','C007','SVC08', 40,  320.00),
('SUB1029','2025-03-09','C001','SVC09', 160,  41.00),
('SUB1030','2025-03-10','C002','SVC10', 90,  118.00);




# from snowflake.snowpark import Session
# from snowflake.snowpark.functions import (
#     col,
#     sum as sf_sum,
#     avg as sf_avg,
#     count as sf_count,
#     rank
# )
# from snowflake.snowpark.window import Window

# # --------------------------------------------------
# # STEP 1: Create Snowpark Session
# # --------------------------------------------------
# session = Session.builder.config("connection_name", "default").create()
# print("Snowpark session created")

# # --------------------------------------------------
# services_df = session.table("SERVICECATALOG")
# usage_df = session.table("SUBSCRIPTIONUSAGE")

# # --------------------------------------------------
# # STEP 3: Join Fact + Dimension
# # --------------------------------------------------
# joined_df = usage_df.join(
#     services_df,
#     "SERVICEID"
# )

# # --------------------------------------------------
# # STEP 4: Derived Metric (Revenue)
# # --------------------------------------------------
# revenue_df = joined_df.with_column(
#     "REVENUE",
#     col("SEATS") * col("PRICEPERSEAT")
# )

# # --------------------------------------------------
# # STEP 5: Aggregate by Domain + Service
# # --------------------------------------------------
# agg_df = revenue_df.group_by(
#     "DOMAIN",
#     "SERVICENAME"
# ).agg(
#     sf_sum("REVENUE").alias("TOTAL_REVENUE"),
#     sf_avg("REVENUE").alias("AVG_REVENUE"),
#     sf_count("*").alias("USAGE_COUNT")
# )

# # --------------------------------------------------
# # STEP 6: Rank Services BY DOMAIN (Correct)
# # --------------------------------------------------
# rank_window = Window.partition_by("DOMAIN").order_by(col("TOTAL_REVENUE").desc())

# ranked_df = agg_df.with_column(
#     "SERVICE_RANK",
#     rank().over(rank_window)
# )

# # --------------------------------------------------
# # STEP 7: Post-Aggregation Filters (HAVING)
# # - Top 3 services per domain
# # - Revenue threshold lowered to realistic value
# # --------------------------------------------------
# filtered_df = ranked_df.filter(
#     (col("SERVICE_RANK") <= 3) &
#     (col("TOTAL_REVENUE") > 30000)
# )

# # --------------------------------------------------
# # STEP 8: Final Sort
# # --------------------------------------------------
# final_df = filtered_df.sort(
#     col("DOMAIN"),
#     col("TOTAL_REVENUE").desc()
# )

# final_df.show()





--===============================================================

-- Snowpark

CREATE OR REPLACE TABLE productmap (
  productid   STRING,
  productname STRING,
  category     STRING
);

-- Sales line items
CREATE OR REPLACE TABLE salesitems (
  orderid    STRING,
  orderdate  DATE,
  customerid STRING,
  productid  STRING,
  qty         NUMBER(10,0),
  unitprice  NUMBER(10,2)
);

INSERT INTO productmap (productid, productname, category) VALUES
('Z001','Smartwatch Pro','Electronics'),
('Z002','Bluetooth Speaker','Electronics'),
('Z003','Ergonomic Mouse','Electronics'),
('Z004','Executive Chair','Furniture'),
('Z005','Workstation Desk','Furniture');

INSERT INTO salesitems (orderid, orderdate, customerid, productid, qty, unitprice) VALUES
('SI1001','2025-01-03','CU001','Z001', 3, 12000.00),
('SI1002','2025-01-04','CU002','Z002', 5,  4500.00),
('SI1003','2025-01-05','CU003','Z003', 4,  2800.00),
('SI1004','2025-01-06','CU004','Z004', 2, 15000.00),
('SI1005','2025-01-07','CU005','Z005', 1, 38000.00),
('SI1006','2025-02-02','CU006','Z001', 2, 11800.00),
('SI1007','2025-02-04','CU007','Z002', 8,  4300.00),
('SI1008','2025-02-06','CU008','Z003', 7,  2900.00),
('SI1009','2025-02-08','CU009','Z004', 1, 15200.00),
('SI1010','2025-02-10','CU010','Z005', 3, 37000.00),
('SI1011','2025-03-02','CU011','Z001', 4, 11900.00),
('SI1012','2025-03-04','CU012','Z002', 6,  4400.00),
('SI1013','2025-03-06','CU013','Z003', 9,  3000.00),
('SI1014','2025-03-08','CU014','Z004', 2, 14900.00),
('SI1015','2025-03-10','CU015','Z005', 2, 39000.00);



# from snowflake.snowpark import Session
# from snowflake.snowpark.functions import (
#     col,
#     sum as sf_sum,
#     avg as sf_avg,
#     count as sf_count
# )

# # --------------------------------------------------
# # STEP 1: Create Snowpark Session
# # --------------------------------------------------
# session = Session.builder.config("connection_name", "default").create()
# print("Snowpark session created")

# # --------------------------------------------------
# # STEP 2: Load Tables
# # --------------------------------------------------
# products_df = session.table("PRODUCTMAP")
# sales_df = session.table("SALESITEMS")

# # --------------------------------------------------
# # STEP 3: Join Sales with Product Map
# # --------------------------------------------------
# joined_df = sales_df.join(
#     products_df,
#     sales_df["PRODUCTID"] == products_df["PRODUCTID"],
#     how="inner"
# )

# joined_df.show()

# # --------------------------------------------------
# # STEP 4: Derived Column (Sales Value)
# # sales_value = qty * unitprice
# # --------------------------------------------------
# sales_value_df = joined_df.with_column(
#     "SALES_VALUE",
#     col("QTY") * col("UNITPRICE")
# )

# sales_value_df.show()

# # --------------------------------------------------
# # STEP 5: Aggregate by Category + Product
# # --------------------------------------------------
# agg_df = sales_value_df.group_by(
#     "CATEGORY",
#     "PRODUCTNAME"
# ).agg(
#     sf_sum("SALES_VALUE").alias("TOTAL_SALES"),
#     sf_avg("SALES_VALUE").alias("AVG_SALES"),
#     sf_count("*").alias("ORDER_COUNT")
# )

# agg_df.show()

# # --------------------------------------------------
# # STEP 6: TWO Post-Aggregation Filters (HAVING-like)
# # 1) TOTAL_SALES > 100000
# # 2) ORDER_COUNT >= 3
# # --------------------------------------------------
# filtered_df = agg_df.filter(
#     (col("TOTAL_SALES") > 100000) &
#     (col("ORDER_COUNT") >= 3)
# )

# filtered_df.show()

# # --------------------------------------------------
# # STEP 7: Sort Descending by TOTAL_SALES
# # --------------------------------------------------
# final_df = filtered_df.sort(
#     col("TOTAL_SALES").desc()
# )

# final_df.show()




--============================================================

-- Cortex


CREATE OR REPLACE TABLE kbarticles (
  docid    STRING,
  title     STRING,
  content   STRING,
  category  STRING,
  source    STRING
);

INSERT INTO kbarticles (docid, title, content, category, source) VALUES
('A-001','Laptop Setup Guide',
'Install OS updates, configure Wi-Fi, enable antivirus, sign in with corporate SSO.',
'Guide','kb/guides/laptopsetup.txt'),
('A-002','Remote Login Policy',
'Access to corporate systems must use VPN and MFA; sessions auto-expire after inactivity.',
'Policy','kb/policies/remotelogin.txt'),
('A-003','Printer Troubleshooting',
'Check power and connectivity, reinstall drivers, run diagnostic tool, contact support.',
'FAQ','kb/faqs/printertroubleshooting.txt'),
('A-004','Onboarding Procedure',
'Create user account, assign roles, provision devices, send welcome kit.',
'Procedure','kb/procedures/onboarding.txt'),
('A-005','Two-Factor Setup',
'Install authenticator app, scan QR, save backup codes, test login.',
'Guide','kb/guides/2fasetup.txt');



--- coretx code 


--- to create cortex search srvice 

CREATE OR REPLACE CORTEX SEARCH SERVICE kb_articles_search
ON content
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 hour'
AS
SELECT
  docid,
  title,
  content,
  category,
  source
FROM kbarticles;



--run basic semantic service 

SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  'kb_articles_search',
  '{
    "query": "how to setup laptop",
    "columns": ["DOCID","TITLE","CATEGORY","CONTENT"],
    "limit": 3
  }'
) AS raw_result;



--inspect json
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'kb_articles_search',
    '{
      "query": "VPN access policy",
      "columns": ["DOCID","TITLE","CATEGORY","CONTENT"],
      "limit": 3
    }'
  )
) AS r;


------------------TEST SINGLE FIELD EXTRACTION (NO FLATTEN YET)

WITH j AS (
  SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'kb_articles_search',
      '{
        "query": "two factor authentication",
        "columns": ["DOCID","TITLE","CATEGORY","CONTENT"],
        "limit": 3
      }'
    )
  ) AS r
)
SELECT
  r:results[0]:TITLE::STRING     AS title_test,
  r:results[0]:CATEGORY::STRING  AS category_test,
  r:results[0]:DOCID::STRING     AS docid_test,
  r:results[0]:"@scores":cosine_similarity::FLOAT AS score_test
FROM j;


----------------------------- FINAL FLATTENED TABULAR OUTPUT (MOST IMPORTANT)

WITH j AS (
  SELECT
    'vpn policy query' AS query,
    PARSE_JSON(
      SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'kb_articles_search',
        '{
          "query": "vpn access policy",
          "columns": ["DOCID","TITLE","CATEGORY","CONTENT"],
          "limit": 5
        }'
      )
    ) AS r
)
SELECT
  query,
  f.value:TITLE::STRING     AS matched_title,
  f.value:CONTENT::STRING  AS snippet,
  f.value:CATEGORY::STRING AS category,
  f.value:DOCID::STRING    AS doc_id,
  f.value:"@scores":cosine_similarity::FLOAT AS score
FROM j,
LATERAL FLATTEN(input => r:results) f
ORDER BY score DESC;

--================================================

-- Streamlit

CREATE OR REPLACE TABLE orderscitychannel (
  orderid   STRING,
  orderdate DATE,
  city       STRING,
  channel    STRING,
  ordervalue NUMBER(12,2)
);

INSERT INTO orderscitychannel (orderid, orderdate, city, channel, ordervalue) VALUES
-- Chennai
('OC2001','2025-01-02','Chennai','Online', 14250.00),
('OC2002','2025-01-04','Chennai','Retail', 18990.00),
('OC2003','2025-01-06','Chennai','Wholesale',  9800.00),
-- Bengaluru
('OC2004','2025-01-03','Bengaluru','Online', 15600.00),
('OC2005','2025-01-07','Bengaluru','Retail', 21850.00),
('OC2006','2025-01-09','Bengaluru','Wholesale',  8200.00),
-- Mumbai
('OC2007','2025-01-05','Mumbai','Online', 16850.00),
('OC2008','2025-01-08','Mumbai','Retail', 24590.00),
('OC2009','2025-01-10','Mumbai','Wholesale',  9100.00),

-- February
('OC2010','2025-02-02','Chennai','Online', 15200.00),
('OC2011','2025-02-05','Chennai','Retail', 19840.00),
('OC2012','2025-02-08','Chennai','Wholesale', 10250.00),
('OC2013','2025-02-03','Bengaluru','Online', 16050.00),
('OC2014','2025-02-06','Bengaluru','Retail', 22500.00),
('OC2015','2025-02-09','Bengaluru','Wholesale',  8600.00),
('OC2016','2025-02-04','Mumbai','Online', 17590.00),
('OC2017','2025-02-07','Mumbai','Retail', 25920.00),
('OC2018','2025-02-10','Mumbai','Wholesale',  9400.00),

-- March
('OC2019','2025-03-01','Chennai','Online', 15900.00),
('OC2020','2025-03-03','Chennai','Retail', 20550.00),
('OC2021','2025-03-06','Chennai','Wholesale', 10800.00),
('OC2022','2025-03-02','Bengaluru','Online', 16380.00),
('OC2023','2025-03-05','Bengaluru','Retail', 23140.00),
('OC2024','2025-03-08','Bengaluru','Wholesale',  8900.00),
('OC2025','2025-03-04','Mumbai','Online', 18360.00),
('OC2026','2025-03-07','Mumbai','Retail', 26790.00),
('OC2027','2025-03-09','Mumbai','Wholesale',  9700.00);



import streamlit as st
from snowflake.snowpark.context import get_active_session

# -------------------------------------------------
# Page Config
# -------------------------------------------------
st.set_page_config(
    page_title="City-wise Orders by Channel",
    layout="wide"
)

st.title("📊 City-wise Orders Dashboard")
st.caption("Data Source: orderscitychannel table in Snowflake")

# -------------------------------------------------
# Snowflake Session
# -------------------------------------------------
session = get_active_session()

# -------------------------------------------------
# Load Base Data
# -------------------------------------------------
base_df = session.sql("""
    SELECT
        orderid,
        orderdate,
        city,
        channel,
        ordervalue
    FROM orderscitychannel
""").to_pandas()

# -------------------------------------------------
# Sidebar Filters
# -------------------------------------------------
st.sidebar.header("🔎 Filters")

city_list = sorted(base_df["CITY"].unique().tolist())
channel_list = sorted(base_df["CHANNEL"].unique().tolist())

selected_cities = st.sidebar.multiselect(
    "Select City",
    city_list,
    default=city_list
)

selected_channels = st.sidebar.multiselect(
    "Select Channel",
    channel_list,
    default=channel_list
)

min_date = base_df["ORDERDATE"].min()
max_date = base_df["ORDERDATE"].max()

date_range = st.sidebar.date_input(
    "Select Date Range",
    [min_date, max_date],
    min_value=min_date,
    max_value=max_date
)

# -------------------------------------------------
# Apply Filters
# -------------------------------------------------
filtered_df = base_df[
    (base_df["CITY"].isin(selected_cities)) &
    (base_df["CHANNEL"].isin(selected_channels)) &
    (base_df["ORDERDATE"] >= date_range[0]) &
    (base_df["ORDERDATE"] <= date_range[1])
]

if filtered_df.empty:
    st.warning("No data available for selected filters.")
    st.stop()

# -------------------------------------------------
# KPI Section
# -------------------------------------------------
total_orders = filtered_df["ORDERID"].nunique()
total_revenue = filtered_df["ORDERVALUE"].sum()
avg_order_value = filtered_df["ORDERVALUE"].mean()

k1, k2, k3 = st.columns(3)
k1.metric("Total Orders", total_orders)
k2.metric("Total Revenue", f"₹ {total_revenue:,.2f}")
k3.metric("Avg Order Value", f"₹ {avg_order_value:,.2f}")

st.divider()

# -------------------------------------------------
# BAR CHART — City vs Total Order Value
# -------------------------------------------------
st.subheader("🏙️ Total Order Value by City & Channel")

bar_df = (
    filtered_df
    .groupby(["CITY", "CHANNEL"], as_index=False)["ORDERVALUE"]
    .sum()
)

st.bar_chart(
    bar_df,
    x="CITY",
    y="ORDERVALUE",
    color="CHANNEL",
    use_container_width=True
)

st.divider()

# -------------------------------------------------
# LINE CHART — Order Value Trend Over Time
# -------------------------------------------------
st.subheader("📈 Order Value Trend Over Time")

line_df = (
    filtered_df
    .groupby("ORDERDATE", as_index=False)["ORDERVALUE"]
    .sum()
    .sort_values("ORDERDATE")
)

st.line_chart(
    line_df,
    x="ORDERDATE",
    y="ORDERVALUE",
    use_container_width=True
)

st.divider()

# -------------------------------------------------
# PIE CHART — Channel Share
# -------------------------------------------------
st.subheader("🥧 Order Value Share by Channel")

pie_df = (
    filtered_df
    .groupby("CHANNEL", as_index=False)["ORDERVALUE"]
    .sum()
)

st.dataframe(pie_df, use_container_width=True)

st.bar_chart(
    pie_df,
    x="CHANNEL",
    y="ORDERVALUE",
    use_container_width=True
)

st.divider()

# -------------------------------------------------
# Detail Table
# -------------------------------------------------
st.subheader("📋 Order Details")

st.dataframe(
    filtered_df.sort_values("ORDERDATE", ascending=False),
    use_container_width=True
)






--=========================================================

-- JSON

-- Table for raw JSON
CREATE OR REPLACE TABLE profileraw (
  doc VARIANT
);

-- Internal stage
CREATE OR REPLACE STAGE profilesstage;

-- JSON file format
CREATE OR REPLACE FILE FORMAT ffjsonprofiles
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE;

-- After uploading profiles.json to the stage:
COPY INTO profileraw
FROM @profilesstage/profiles.json
FILE_FORMAT = (FORMAT_NAME = ffjsonprofiles);


SELECT doc
FROM profileraw;



---inspect top level fields
SELECT
  doc:user_id::STRING   AS user_id,
  doc:name::STRING      AS name,
  doc:meta.plan::STRING AS plan,
  doc:meta.region::STRING AS region,
  doc:meta.signup_ts::TIMESTAMP_NTZ AS signup_ts
FROM profileraw;



------Inspect Sessions Array (Before FLATTEN)



SELECT
  doc:user_id::STRING AS user_id,
  doc:sessions        AS sessions_array
FROM profileraw;




--FLATEN THE SESSION ARRAY 


SELECT
  doc:user_id::STRING AS user_id,
  doc:name::STRING    AS name,
  f.value:session_id::STRING AS session_id,
  f.value:start_ts::TIMESTAMP_NTZ AS session_start_ts,
  f.value:active::BOOLEAN AS active
FROM profileraw,
LATERAL FLATTEN(input => doc:sessions) f;



---FILTER ACTIVE SESSIONS ONLY


SELECT
  doc:user_id::STRING AS user_id,
  f.value:session_id::STRING AS session_id,
  f.value:start_ts::TIMESTAMP_NTZ AS session_start_ts
FROM profileraw,
LATERAL FLATTEN(input => doc:sessions) f
WHERE f.value:active::BOOLEAN = TRUE;



--COUNT SESSION PER USER

SELECT
  doc:user_id::STRING AS user_id,
  COUNT(*) AS total_sessions
FROM profileraw,
LATERAL FLATTEN(input => doc:sessions) f
GROUP BY user_id
ORDER BY total_sessions DESC;


--COUNT ACTIVE SESSION PER USER

SELECT
  doc:user_id::STRING AS user_id,
  COUNT(*) AS active_sessions
FROM profileraw,
LATERAL FLATTEN(input => doc:sessions) f
WHERE f.value:active::BOOLEAN = TRUE
GROUP BY user_id;



--OPTIONAL CREATE STRUCTURED VIEW 



CREATE OR REPLACE VIEW user_sessions_flat AS
SELECT
  doc:user_id::STRING AS user_id,
  doc:name::STRING AS name,
  doc:meta.plan::STRING AS plan,
  doc:meta.region::STRING AS region,
  f.value:session_id::STRING AS session_id,
  f.value:start_ts::TIMESTAMP_NTZ AS session_start_ts,
  f.value:active::BOOLEAN AS active
FROM profileraw,
LATERAL FLATTEN(input => doc:sessions) f;

select * from user_sessions_flat;

--=================================================

-- UDF (PYTHON)

-- Create demo table
CREATE OR REPLACE TABLE demoitems (
  itemid       STRING,
  productname  STRING,
  quantity     NUMBER(10,0),
  unitprice    NUMBER(10,2)
);

-- Insert sample data
INSERT INTO demoitems (itemid, productname, quantity, unitprice) VALUES
('I001', '  smartPhone  x  ', 2, 34999.50),
('I002', 'SMARTphone  X',     1, 35999.00),
('I003', 'Noise Cancel Headset', 3, 7999.00),
('I004', 'office  Chair',     1, 11999.00),
('I005', 'Standing   Desk',   2, 28999.00);




from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.functions import (

    sum as sf_sum,
    avg as sf_avg,
    min as sf_min,
    max as sf_max,
    upper,
    trim,
    regexp_replace
)


from snowflake.snowpark import session

session = Session.builder.config("connection_name", "default").create()

print("Snowpark session created")

demo_df = session.table("DEMOITEMS")

# Show raw data
print("📊 Raw Data:")
demo_df.show()

# Data cleansing: trim spaces, collapse multiple spaces, convert to uppercase
clean_df = demo_df.select(
    col("itemid"),
    upper(trim(regexp_replace(col("productname"), r'\s+', ' '))).alias("clean_productname"),
    col("quantity"),
    col("unitprice"),
    (col("quantity") * col("unitprice")).alias("total_price")
)

print("✨ Cleaned Data:")
clean_df.show()

-------------------------------------------------------------------

UDF(ALTERNATIVE)

from snowflake.snowpark import Session
from snowflake.snowpark.functions import udf, col
from snowflake.snowpark.types import StringType

# -------------------------------------------------------------------
# 1. Create Snowpark session
# -------------------------------------------------------------------
session = Session.builder.config("connection_name", "default").create()
print("✅ Snowpark session created")

# -------------------------------------------------------------------
# 2. Define normal Python function (NOT a UDF yet)
# -------------------------------------------------------------------
def clean_product_name(name: str) -> str:
    if name is None:
        return None
    # Trim, collapse spaces, uppercase
    return " ".join(name.strip().split()).upper()

# -------------------------------------------------------------------
# 3. Register the function as a Snowflake Python UDF
# -------------------------------------------------------------------
clean_product_name_udf = udf(
    func=clean_product_name,
    return_type=StringType(),
    input_types=[StringType()],
    name="CLEAN_PRODUCT_NAME_UDF",
    replace=True,
    session=session
)

print("✅ UDF registered successfully")

# -------------------------------------------------------------------
# 4. Read table
# -------------------------------------------------------------------
demo_df = session.table("DEMOITEMS")

print("📊 Raw Data")
demo_df.show()

# -------------------------------------------------------------------
# 5. Use the UDF in a Snowpark DataFrame
# -------------------------------------------------------------------
udf_df = demo_df.select(
    col("ITEMID"),
    clean_product_name_udf(col("PRODUCTNAME")).alias("CLEAN_PRODUCTNAME"),
    col("QUANTITY"),
    col("UNITPRICE"),
    (col("QUANTITY") * col("UNITPRICE")).alias("TOTAL_PRICE")
)

print("✨ Transformed Data using UDF")
udf_df.show()




















