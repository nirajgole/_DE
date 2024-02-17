import streamlit as st
import pandas as pd
from streamlit_autorefresh import st_autorefresh

# page settings
st.set_page_config(layout="wide")

# update every 5 mins
st_autorefresh(interval=5 * 60 * 1000, key="dataframerefresh")


# Initialize connection.
conn = st.connection("postgresql", type="sql")

# Perform query.
df = conn.query("SELECT * FROM exchange_rates;", ttl="10m")
df = pd.DataFrame(data=df)
df.index = df.index + 1

# tabs
tab1, tab2, tab3 = st.tabs(["Data", "Insights", "Analytics"])

with tab1:
    st.header("Coins")
    st.dataframe(df, use_container_width=True)

with tab2:
    st.header("INR Rates")
    # highest_rate =df.nlargest(1,['price_usd']).iloc[0][]
    col1, col2 = st.columns(2)
    highest_rate = df.query("rate == rate.max()").iloc[0]
    col1.metric(
        label="Highest",
        value=highest_rate["rate"],
        delta=highest_rate["asset_id_quote"],
    )

    lowest_rate = df.query("rate == rate.min()").iloc[0]
    col2.metric(
        label="Lowest", value=lowest_rate["rate"], delta=lowest_rate["asset_id_quote"]
    )


with tab3:
    st.header("Analytics")
    non_crypto_assets = ["INR", "GBP", "JPY", "EUR", "CHF", "AED", "USD"]
    st.bar_chart(
        data=df[df["asset_id_quote"].isin(non_crypto_assets)].head(5),
        x="asset_id_quote",
        y="rate",
    )
