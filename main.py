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
    print("ETL process started.")
    df = extract_data()
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
