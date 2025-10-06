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

def get_last_date() :
    '''
    Retrieve the most recent date from the 'date_to' column in the 'api_wb_FinanceReport' table.
    Returns:
        The most recent date as a string, or None if the table is empty or an error occurs.
    Raises:
        Exception: If there is an error connecting to the database or executing the query.
    '''
    connection_string = (
        f"postgresql://{os.getenv('user')}:{os.getenv('password')}@{os.getenv('host')}:{int(os.getenv('port'))}/{os.getenv('database')}?sslmode={os.getenv('sslmode')}"
    )
    table_name = 'api_wb_FinanceReport'
    date_column = 'date_to'
    try:
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            query = f"SELECT MAX({date_column}) AS last_date FROM \"{table_name}\" "
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

def calculate_fetch_window():
    ''' 
    Calculate the date range for fetching data from the API based on the last date in the database.
    Returns:
        A tuple containing the start date (date_from) and end date (date_to) as pandas Timestamps.       
    '''
    last_date = get_last_date()
    last_date = pd.to_datetime(last_date)
    print(f"Last date in the table: {last_date}")

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
        date_to = today - timedelta(days=days_back_to_sunday) 
        print(f"Calculated date_to: {date_to}")
        print(f"Calculated date_from: {date_from}")

    return date_from, date_to

def extract_data():
    ''' 
    Fetch data from the API in chunks and return as a concatenated DataFrame.
    Returns:
        A pandas DataFrame containing the fetched data, or an empty DataFrame if no data is
        available.
    '''
   
    headers = {
        'Content-Type': 'application/json',
        'Authorization': os.getenv('wb_api_token')
    }

    api_url = 'https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod'

    # --- Compute date_from and date_to based on last_date ---
    date_from, date_to = calculate_fetch_window()
    print(f"Fetching data from {date_from} to {date_to}\n")

    chunk_size = 10000 # Number of records to fetch per request

    params = {
            'dateFrom': date_from.strftime('%Y-%m-%dT%H:%M:%SZ'), ##"2025-09-22T00:00:00",  ##date_from.strftime('%Y-%m-%dT%H:%M:%SZ'), 
            "limit": chunk_size,
            'dateTo': date_to.strftime("%Y-%m-%dT%H:%M:%SZ"),  ## "2025-09-28T23:59:59",## date_to.strftime("%Y-%m-%d") ,
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
