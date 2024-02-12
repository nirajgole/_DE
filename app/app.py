import streamlit as st
import pandas as pd

# Initialize connection.
conn = st.connection("postgresql", type="sql")

# Perform query.
df = conn.query("SELECT * FROM assets limit 5;", ttl="10m")
df.index = df.index + 1
st.dataframe(df, use_container_width=True)
# Print results.
# for row in df.itertuples():
#     st.write(f"{row.name} has a :{row.type_is_crypto}:")

# add autorefresh for every 5 mins
