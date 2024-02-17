import requests
from psycopg2 import connect, sql
from psycopg2.extras import execute_values

API_DOCS = "https://docs.coinapi.io/market-data/rest-api/exchange-rates/exchange-rates-get-all-current-rates"
API_KEY = "F378990E-7EBB-4E52-8DBD-034132475264"
BASE_URL = "https://rest.coinapi.io"
DB_PASSWORD = "docker"

# API
url = f"{BASE_URL}/v1/exchangerate/INR/"
params = {"invert": "true"}
headers = {"X-CoinAPI-Key": API_KEY}
response = requests.get(url, headers=headers, timeout=30, params=params)
data = response.json()
print(data["rates"][1:5])

#maintain column order of dataset same as query order
dataset = [
    (
        item["asset_id_quote"],
        item["rate"],
        item["time"],
    )
    for item in data["rates"]
]

# DB connection
if dataset:
    conn = connect(
        host="database", database="coindb", user="docker", password=DB_PASSWORD
    )
    cur = conn.cursor()

    TABLE_NAME = "exchange_rates"

    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        asset_id_quote VARCHAR(50) PRIMARY KEY,
        rate numeric,
        exchange_time timestamptz
        )
    """
    )

    query_text = """
    INSERT INTO {table}
    (asset_id_quote, rate, exchange_time)
    VALUES %s
    ON CONFLICT(asset_id_quote)
    DO UPDATE SET
    rate = EXCLUDED.rate,
    exchange_time = EXCLUDED.exchange_time
    """

    query = sql.SQL(query_text).format(table=sql.Identifier(TABLE_NAME))

    execute_values(cur, query.as_string(cur), dataset)

    conn.commit()
    cur.close()
    conn.close()
    print("Job run.")
