import requests
from psycopg2 import connect, sql
from psycopg2.extras import execute_values

API_DOCS = "https://docs.coinapi.io/market-data/rest-api/exchange-rates/exchange-rates-get-all-current-rates"
API_KEY = "F378990E-7EBB-4E52-8DBD-034132475264"
BASE_URL = "https://rest.coinapi.io"
DB_PASSWORD = "docker"

# API
url = f"{BASE_URL}/v1/assets"
headers = {"X-CoinAPI-Key": API_KEY}
response = requests.get(url, headers=headers, timeout=30)
data = response.json()
print(data[1:5])

dataset = [
    (
        item["asset_id"],
        item["name"],
        item["type_is_crypto"],
        item.get("price_usd", None),
        item.get("data_start", None),
        item.get("data_end", None),
        item["volume_1hrs_usd"],
        item["volume_1day_usd"],
        item["volume_1mth_usd"],
    )
    for item in data
]

# DB connection
if dataset:
    conn = connect(
        host="0.0.0.0", database="coindb", user="docker", password=DB_PASSWORD
    )
    cur = conn.cursor()

    TABLE_NAME = "assets"

    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        asset_id VARCHAR(50) PRIMARY KEY,
        name VARCHAR(100),
        type_is_crypto smallint,
        price_usd FLOAT,
        data_start DATE,
        data_end DATE,
        volume_1hrs_usd decimal,
        volume_1day_usd decimal,
        volume_1mth_usd decimal
        )
    """
    )

    query_text = """
    INSERT INTO {table}
    (asset_id, name, type_is_crypto, price_usd, data_start, data_end, volume_1hrs_usd, volume_1day_usd, volume_1mth_usd)
    VALUES %s
    ON CONFLICT(asset_id)
    DO UPDATE SET
    price_usd = EXCLUDED.price_usd,
    data_start = EXCLUDED.data_start,
    data_end = EXCLUDED.data_end,
    volume_1hrs_usd = EXCLUDED.volume_1hrs_usd,
    volume_1day_usd = EXCLUDED.volume_1day_usd,
    volume_1mth_usd = EXCLUDED.volume_1mth_usd
    """

    query = sql.SQL(query_text).format(table=sql.Identifier(TABLE_NAME))

    execute_values(cur, query.as_string(cur), dataset)

    conn.commit()
    cur.close()
    conn.close()
    print('Job run.')
