import requests
from psycopg2 import connect, sql
from psycopg2.extras import execute_values

API_DOCS='https://docs.coinapi.io/market-data/rest-api/exchange-rates/exchange-rates-get-all-current-rates'

API_KEY = "F378990E-7EBB-4E52-8DBD-034132475264"
BASE_URL = "https://rest.coinapi.io"
DB_PASSWORD = "root"

# API
url = f"{BASE_URL}/v1/exchangerate/INR"
headers = {"X-CoinAPI-Key": API_KEY}
response = requests.get(url, headers=headers, timeout=30)
data = response.json()
print(data)

dataset = [
    (rate["asset_id_quote"], rate["rate"], rate["time"]) for rate in data["rates"]
]

# DB connection
TABLE_NAME = "inr_exchange_rate"

conn = connect(host="localhost", database="test", user="postgres", password=DB_PASSWORD)
cur = conn.cursor()

cur.execute(
    f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        asset_id_quote VARCHAR(255),
        rate FLOAT,
        time time
    )
"""
)

# for rate in data:
#     cur.execute("""
#         INSERT INTO exchange_rates (asset_id_base, asset_id_quote, rate, time)
#         VALUES (%s, %s, %s, %s)
#     """, (rate['asset_id_base'], rate['asset_id_quote'], rate['rate'], rate['time']))

# conn.commit()


query_text = 'INSERT INTO {table} (asset_id_quote, rate, time) VALUES %s'
query = sql.SQL(query_text).format(table=sql.Identifier(TABLE_NAME))

# execute_values(
#     cur,
#     """
#     INSERT INTO {TABLE_NAME} (asset_id_quote, rate, time)
#     VALUES %s
#     """,
#     dataset,
# )


execute_values(cur, query.as_string(cur), dataset)

conn.commit()
