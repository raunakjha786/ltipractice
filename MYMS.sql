MOCK2_CLONECREATE OR REPLACE TABLE kbarticles (
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



--semantic query 1

SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  'kb_articles_search',
  '{
    "query": "new employee laptop setup",
    "columns": ["DOCID","TITLE","CATEGORY","CONTENT"],
    "limit": 3
  }'
) AS raw_result;



--semnatic query 2

SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  'kb_articles_search',
  '{
    "query": "policy for remote login",
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
          "query": "policy for remote login",
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
WHERE f.value:CATEGORY::STRING IN ('guide','procedure')
ORDER BY score DESC;


ORDER BY score DESC;

in this query add metadat filter ( eg category in ('guide', 'procedure') and re-run query to complete result

---------------------------------------------------------------------------------

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








--================================================


CREATE OR REPLACE TABLE ordersstateplatform (
  orderid     STRING,
  orderdate   DATE,
  state        STRING,
  platform     STRING,       -- e.g., Marketplace, Direct, Partner
  itemscount  NUMBER(10,0),
  totalamount NUMBER(12,2)
);



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

st.title(" Orders by State & Platform")

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
st.subheader(" Revenue by State and Platform")

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
st.subheader(" Revenue Trend Over Time")

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
st.subheader(" Cumulative Revenue Trend")

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
st.subheader(" SUMMARY TABLE")

st.dataframe(
    filtered_df.sort_values("ORDERDATE", ascending=False),
    use_container_width=True
)

st.caption("Data Source: ordersstateplatform table in Snowflake")

# -----------------------------------------
# PIE CHART – Revenue Share by Platform
# -----------------------------------------
st.subheader("Revenue Share by Platform")

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


-----------------------------------

CREATE OR REPLACE TABLE demoitems (
  itemid     STRING,
  productname STRING,
  quantity    NUMBER(10,0),
  unitprice  NUMBER(10,2)
);


INSERT INTO demoitems (itemid, productname, quantity, unitprice) VALUES
('I001', '  smartPhone  x  ', 2, 34999.50),
('I002', 'SMARTphone  X',     1, 35999.00),
('I003', 'Noise Cancel Headset', 3, 7999.00),
('I004', 'office  Chair',     1, 11999.00),
('I005', 'Standing   Desk',   2, 28999.00);





