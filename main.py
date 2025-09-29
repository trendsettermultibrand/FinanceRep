from extract  import extract_data
from load import load_to_db
import pandas as pd
import os
from pathlib import Path   
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = Path(".env") ##to be changed in server
load_dotenv(dotenv_path=env_path)


def main():
    """
    Main function to run the ETL process.
    """
    wb_api_token = os.getenv('wb_api_token')

    headers = {
        'Content-Type': 'application/json',
        'Authorization': wb_api_token 
    }
    api_url = 'https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod'

    print("ETL process started.")
    df = extract_data(api_url, headers)
    if not df.empty:
        load_to_db(df)
        print(f"Successfully completed ETL process.")
    else:
        print("No data available to load.")
    print("ETL process finished.")


if __name__ == "__main__":
    """
    Entry point of the ETL pipeline.
    """
    main()   
