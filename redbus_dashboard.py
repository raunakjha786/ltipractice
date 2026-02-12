import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="RedBus Analytics", layout="wide")
session = get_active_session()

st.title("RedBus Analytics Dashboard")

@st.cache_data(ttl=600)
def load_data():
    return session.sql("SELECT * FROM REDBUS_ANALYTICS_DB.DW.V_BOOKING_ANALYTICS").to_pandas()

df = load_data()

with st.sidebar:
    st.header("Filters")
    regions = ["All"] + sorted(df["REGION"].dropna().unique().tolist())
    selected_region = st.selectbox("Region", regions)
    
    operators = ["All"] + sorted(df["OPERATOR_NAME"].dropna().unique().tolist())
    selected_operator = st.selectbox("Operator", operators)
    
    bus_types = ["All"] + sorted(df["BUS_TYPE"].dropna().unique().tolist())
    selected_bus_type = st.selectbox("Bus Type", bus_types)
    
    statuses = ["All"] + sorted(df["BOOKING_STATUS"].dropna().unique().tolist())
    selected_status = st.selectbox("Booking Status", statuses)

filtered_df = df.copy()
if selected_region != "All":
    filtered_df = filtered_df[filtered_df["REGION"] == selected_region]
if selected_operator != "All":
    filtered_df = filtered_df[filtered_df["OPERATOR_NAME"] == selected_operator]
if selected_bus_type != "All":
    filtered_df = filtered_df[filtered_df["BUS_TYPE"] == selected_bus_type]
if selected_status != "All":
    filtered_df = filtered_df[filtered_df["BOOKING_STATUS"] == selected_status]

tab1, tab2, tab3, tab4 = st.tabs(["Executive Summary", "Route Performance", "Operator/Fleet", "Explorer"])

with tab1:
    col1, col2, col3, col4, col5 = st.columns(5)
    total_bookings = len(filtered_df)
    total_revenue = filtered_df["FARE_AMOUNT"].sum()
    total_net_revenue = filtered_df["NET_AMOUNT"].sum()
    cancellation_rate = (filtered_df["IS_CANCELLED"].sum() / total_bookings * 100) if total_bookings > 0 else 0
    avg_fare = filtered_df["FARE_AMOUNT"].mean()
    
    col1.metric("Total Bookings", f"{total_bookings:,}")
    col2.metric("Gross Revenue", f"₹{total_revenue:,.0f}")
    col3.metric("Net Revenue", f"₹{total_net_revenue:,.0f}")
    col4.metric("Cancellation Rate", f"{cancellation_rate:.1f}%")
    col5.metric("Avg Fare", f"₹{avg_fare:,.0f}")
    
    st.subheader("Revenue Trend by Journey Month")
    monthly = filtered_df.groupby("JOURNEY_MONTH_NAME").agg({"NET_AMOUNT": "sum", "BOOKING_ID": "count"}).reset_index()
    month_order = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
    monthly["JOURNEY_MONTH_NAME"] = pd.Categorical(monthly["JOURNEY_MONTH_NAME"], categories=month_order, ordered=True)
    monthly = monthly.sort_values("JOURNEY_MONTH_NAME")
    st.bar_chart(monthly.set_index("JOURNEY_MONTH_NAME")["NET_AMOUNT"])
    
    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Bookings by Payment Mode")
        payment_dist = filtered_df["PAYMENT_MODE"].value_counts().reset_index()
        payment_dist.columns = ["Payment Mode", "Count"]
        st.bar_chart(payment_dist.set_index("Payment Mode"))
    with col_b:
        st.subheader("Bookings by Seat Type")
        seat_dist = filtered_df["SEAT_TYPE"].value_counts().reset_index()
        seat_dist.columns = ["Seat Type", "Count"]
        st.bar_chart(seat_dist.set_index("Seat Type"))

with tab2:
    st.subheader("Top 10 Routes by Revenue")
    top_routes = filtered_df.groupby("ROUTE_NAME").agg(
        {"NET_AMOUNT": "sum", "BOOKING_ID": "count", "PASSENGER_COUNT": "sum"}
    ).reset_index().rename(columns={"BOOKING_ID": "BOOKINGS", "PASSENGER_COUNT": "PASSENGERS"})
    top_routes = top_routes.nlargest(10, "NET_AMOUNT")
    st.dataframe(top_routes, use_container_width=True)
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Revenue by Region")
        region_rev = filtered_df.groupby("REGION")["NET_AMOUNT"].sum().reset_index()
        st.bar_chart(region_rev.set_index("REGION"))
    with col2:
        st.subheader("Bookings by Source City (Top 10)")
        src_city = filtered_df["SOURCE_CITY"].value_counts().head(10).reset_index()
        src_city.columns = ["City", "Bookings"]
        st.bar_chart(src_city.set_index("City"))

with tab3:
    st.subheader("Operator Performance")
    operator_perf = filtered_df.groupby("OPERATOR_NAME").agg(
        {"NET_AMOUNT": "sum", "BOOKING_ID": "count", "PASSENGER_COUNT": "sum", "IS_CANCELLED": "sum"}
    ).reset_index()
    operator_perf.columns = ["Operator", "Net Revenue", "Bookings", "Passengers", "Cancellations"]
    operator_perf["Cancel Rate %"] = (operator_perf["Cancellations"] / operator_perf["Bookings"] * 100).round(1)
    operator_perf = operator_perf.sort_values("Net Revenue", ascending=False)
    st.dataframe(operator_perf, use_container_width=True)
    
    st.subheader("Revenue by Bus Type")
    bus_type_rev = filtered_df.groupby("BUS_TYPE")["NET_AMOUNT"].sum().reset_index()
    st.bar_chart(bus_type_rev.set_index("BUS_TYPE"))

with tab4:
    st.subheader("Booking Explorer")
    st.write(f"Showing {len(filtered_df):,} records")
    display_cols = ["BOOKING_ID", "JOURNEY_DATE_KEY", "CUSTOMER_NAME", "ROUTE_NAME", "OPERATOR_NAME", 
                    "BUS_TYPE", "PASSENGER_COUNT", "FARE_AMOUNT", "NET_AMOUNT", "PAYMENT_MODE", "BOOKING_STATUS"]
    st.dataframe(filtered_df[display_cols].head(500), use_container_width=True)
