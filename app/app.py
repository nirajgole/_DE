import streamlit as st
import pandas as pd

# page settings
st.set_page_config(layout="wide")

# Initialize connection.
conn = st.connection("postgresql", type="sql")

# Perform query.
df = conn.query("SELECT * FROM assets;", ttl="10m")
df = pd.DataFrame(data=df)
df.index = df.index + 1

# tabs
tab1, tab2, tab3 = st.tabs(["Data", "Insights", "Analytics"])

with tab1:
    st.header("Coins")
    st.dataframe(df, use_container_width=True)

with tab2:
    st.header("USD Rates")
    # highest_rate =df.nlargest(1,['price_usd']).iloc[0][]
    col1, col2 = st.columns(2)
    highest_rate = df.query("price_usd == price_usd.max()").iloc[0]
    col1.metric(
        label="Highest", value=highest_rate["price_usd"], delta=highest_rate["name"]
    )
    # col1.metric("Temperature", "70 °F", "1.2 °F")

    lowest_rate = df.query("price_usd == price_usd.min()").iloc[0]
    col2.metric(
        label="Lowest", value=lowest_rate["price_usd"], delta=lowest_rate["name"]
    )

    st.divider()

    st.header("Coins")

    col1, col2 = st.columns(2)
    coin_count = (
        df[["asset_id", "type_is_crypto"]].groupby(["type_is_crypto"]).agg(["count"])
    )
    col1.metric(label="Total Crypto Assets", value=coin_count.loc[1])
    col2.metric(label="Total Non-Crypto Assets", value=coin_count.loc[0])


with tab3:
    st.header("Analytics")
    non_crypto_assets = ["INR", "GBP", "JPY", "EUR", "CHF", "AED"]
    st.bar_chart(
        data=df[df["asset_id"].isin(non_crypto_assets)].head(5),
        x="asset_id",
        y="price_usd",
    )

# add autorefresh for every 5 min
