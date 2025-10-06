from datetime import timedelta
import requests
import pandas as pd
import os
from dotenv import load_dotenv
from pathlib import Path   
from sqlalchemy import create_engine, Engine
import time

from sqlalchemy import create_engine,MetaData, Table, text
import psycopg2

# Load environment variables from .env file
env_path = Path(".env") ##to be changed in server
load_dotenv(dotenv_path=env_path)


def test_db_connection(user, password, host, port, database, sslmode):
    """
    Function to test direct connection to PostgreSQL.
    """
    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
            sslmode=sslmode

        )
        print("Connection successful!")
        connection.close()
 
    except Exception as e:
        print(f"Error testing connection: {e}")

test_db_connection(
    user=os.getenv('user'),
    password=os.getenv('password'),
    host=os.getenv('host'),
    port=int(os.getenv('port')),
    database=os.getenv('database'),
    sslmode=os.getenv('sslmode')
)


def get_last_date(
    table_name: str,
    date_column: str,
    user: str,
    password: str,
    host: str,
    port,
    database: str,
    sslmode: str
) :
    """
    Get the most recent (MAX) date value from the specified table and column.

    Args:
        table_name (str): Name of the database table.
        date_column (str): Column containing date values.
        user (str): Database user.
        password (str): Database password.
        host (str): Database host (IP or hostname).
        port (str): Database port (usually 5432 for PostgreSQL).
        database (str): Database name.
        sslmode (str): SSL mode ("require", "disable", etc.).

    Returns:
        pd.Timestamp | None: The latest date found in the table, or None if empty.
    """
    connection_string = (
        f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode={sslmode}"
    )
    try:
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            query = f"SELECT MAX({date_column}) AS last_date FROM \"{table_name}\""
            df = pd.read_sql(query, conn)
            return df['last_date'][0]
    
   
        # Use parameterized queries to avoid SQL injection
        query = f"SELECT MAX({date_column}) AS last_date FROM {table_name}"

        # Establish connection and execute query
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)

            # Check if the result is not empty
            if not df.empty:
                return df['last_date'].iloc[0]  # Using iloc to safely access the first row
            else:
                print("No data found.")
                return None




    except Exception as e:
        print(f"Error loading DataFrame: {e}")
        raise



last_date = get_last_date(
    table_name = 'api_wb_FinanceReport',
    date_column = 'date_to',

    user=os.getenv('user'),
    password=os.getenv('password'),
    host=os.getenv('host'),
    port=int(os.getenv('port')),
    database=os.getenv('database'),
    sslmode=os.getenv('sslmode')
)

print(f"Last date in the table: {last_date}")

def calculate_fetch_window( last_date):
    """
    Calculate the date range for fetching data.

    Logic:
        - If last_date is None → start from a fixed default date (2024-01-01).
        - Otherwise:
            * date_from = the next Monday after last_date
            * date_to   = the last Sunday before today

    Args:
        last_date (str | pd.Timestamp | None): The last date stored in the database.

    Returns:
        tuple[pd.Timestamp, pd.Timestamp]: (date_from, date_to) defining the fetch window.
    """
   
    last_date = pd.to_datetime(last_date)

    if last_date is None:
        # Default start date if table is empty
        date_from = pd.Timestamp('2025-01-01')
    else:
        # Go to the next Monday after last_date
        days_back = last_date.weekday()
        date_from = last_date - timedelta(days=days_back)  + timedelta(days=7)

        # Define date_to as the last completed Sunday before today
        today = pd.Timestamp.today().normalize()
        days_back_to_sunday = today.weekday() + 1
        date_to = today - timedelta(days=days_back_to_sunday) + timedelta(days=7)

    return date_from, date_to


# --- Compute date_from and date_to based on last_date ---
date_from, date_to = calculate_fetch_window(last_date)
print(f"Fetching data from {date_from} to {date_to}\n")





# Example using generator expression
def fetch_data(api_url, headers, chunk_size=10000):
    params = {
            'dateFrom': date_from.strftime('%Y-%m-%dT%H:%M:%SZ'), ##"2025-09-22T00:00:00",  ##date_from.strftime('%Y-%m-%dT%H:%M:%SZ'), ###"2025-07-07T00:00:00"
            "limit": 10000,
            'dateTo': date_to.strftime("%Y-%m-%dT%H:%M:%SZ"),  ## "2025-09-28T23:59:59",## date_to.strftime("%Y-%m-%d") ,## "2025-09-28T23:59:59",
            'rrdid': 0
            
    }

    rrd_id = 0
    def data_generator():
        nonlocal rrd_id
        counter = 0
        while True:
            # if counter == 3:  # to be commented
            #     break
             # Update the rrd_id parameter for pagination
            print(params)
            params['rrdid'] = rrd_id
            print(params)
            print(f"Sleeping for 61 seconds to respect API rate limits. fetch number {counter} ...")
            time.sleep(61)
            response = requests.get(api_url, params=params, headers=headers)

            if response.status_code != 200:
                print(f"Error: {response.status_code}")
                break

            data = response.json()
            total_records = len(data)
            print(f"Fetched {total_records} records.")
            

            if total_records == 0:
                print("No more records found.")
                yield pd.DataFrame()
                break
            
            if total_records < chunk_size:
                print("Fetched less than chunk size, ending pagination.")
                yield pd.DataFrame(data)
                break

            counter += 1
            print(f"Processing chunk {counter} with {total_records} records...")
            print(f"{params['dateFrom']},  {params['dateTo']}, {params['rrdid']}")

            # Move the cursor forward
            rrd_id = data[-1]['rrd_id']

            # Yield each chunk as a DataFrame
            yield pd.DataFrame(data)

         

           
         
            

    # Use pd.concat to join the generated DataFrames
    try:
        return pd.concat(data_generator(), ignore_index=True) 
    except BaseException:
        return pd.DataFrame()

def extract_data() -> pd.DataFrame:
    """
    Extract data from the API and return as a DataFrame.
    """

    wb_api_token = os.getenv('wb_api_token')

    headers = {
        'Content-Type': 'application/json',
        'Authorization': wb_api_token 
    }
    api_url = 'https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod'


    print("Starting data extraction from API...")
    df = fetch_data(api_url, headers)
    print("Data extraction completed.")
    if df.empty:
        print("No data fetched from the API.")
        return pd.DataFrame()  # Return an empty DataFrame if no data
    else:   
        print("DataFrame Info:")
        print(df.info())
        return df