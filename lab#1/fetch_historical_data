# Importing required modules
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable
import requests
from datetime import datetime, timedelta
import snowflake.connector
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'stock_price_prediction',
    default_args=default_args,
    description='A simple stock data pipeline',
    schedule_interval='@daily',
)

@task
def get_stock_data():
    api_key = Variable.get("Vantage_API_Key")
    symbols = ["NFLX", "MSFT"]
    results = []

    # Calculate the date 90 days ago
    today = datetime.now().date()
    ninety_days_ago = today - timedelta(days=90)

    for symbol in symbols:
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
        response = requests.get(url)
        response.raise_for_status() 
        data = response.json()

        for date_str, daily_data in data["Time Series (Daily)"].items():
          date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
          if date_obj >= ninety_days_ago:
            stock_info = {
                "date": date_str,
                "open": daily_data["1. open"],
                "high": daily_data["2. high"],
                "low": daily_data["3. low"],
                "close": daily_data["4. close"],
                "volume": daily_data["5. volume"],
                "symbol": symbol 
            }
            results.append(stock_info)
    return results

@task
def create_new_table():
    conn = snowflake.connector.connect(
        user=Variable.get("Snowflake_Username"),
        password=Variable.get("Snowflake_Password"),
        account=Variable.get("Snowflake_Account"),
    )
    create_table_query = """
    CREATE TABLE IF NOT EXISTS NEW_DATA.raw_data.prediction_table (
        id INTEGER AUTOINCREMENT PRIMARY KEY,
        name STRING,
        value FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cur = conn.cursor()
    cur.execute(create_table_query)
    conn.commit()
    cur.close()

@task
def load_to_snowflake(stock_data):
    conn = snowflake.connector.connect(
        user=Variable.get("Snowflake_Username"),
        password=Variable.get("Snowflake_Password"),
        account=Variable.get("Snowflake_Account"),
    )
    create_table_query = """
    CREATE OR REPLACE TABLE NEW_DATA.raw_data.prediction_table (
        date DATE,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume INTEGER,
        symbol STRING,
        PRIMARY KEY (date, symbol)
    );
    """
    cur = conn.cursor()
    cur.execute(create_table_query)
    insert_query = """
    INSERT INTO NEW_DATA.raw_data.prediction_table (date, open, high, low, close, volume, symbol)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    for record in stock_data:
        cur.execute(insert_query, (record['date'], record['open'], record['high'], record['low'], record['close'], record['volume'], record['symbol']))
    conn.commit()
    cur.close()

@task
def check_table_stats():
    conn = snowflake.connector.connect(
        user=Variable.get("Snowflake_Username"),
        password=Variable.get("Snowflake_Password"),
        account=Variable.get("Snowflake_Account"),
    )
    cur = conn.cursor()
    result = cur.execute("SELECT COUNT(*) FROM NEW_DATA.raw_data.prediction_table")
    count = result.fetchone()
    print(f"Number of records in the table: {count}")
    cur.close()
    conn.close()

with dag:
    stock_data = get_stock_data()
    create_new_table()
    load_to_snowflake(stock_data)
    check_table_stats()
