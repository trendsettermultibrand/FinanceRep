from datetime import timedelta
from importlib import metadata
import requests
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, text
import os
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy.engine import Engine


# Load environment variables from .env file
env_path = Path(".env") ##to be changed in server
load_dotenv(dotenv_path=env_path)


def load_to_db( df: pd.DataFrame) -> None:
    """
    Load a DataFrame into the 'api_wb_Financereport' table.

    Args:
        df (pd.DataFrame): DataFrame to be processed and inserted into the database.
    Raises:
        Exception: If the DataFrame is empty or an error occurs during insertion.
    """

    table_name = "api_wb_FinanceReport"
    connection_string = (
        f"postgresql://{os.getenv('user')}:{os.getenv('password')}@{os.getenv('host')}:{int(os.getenv('port'))}/{os.getenv('database')}?sslmode={os.getenv('sslmode')}"
    )

    try:
        
        engine = create_engine(connection_string)
        print("SQLAlchemy engine created successfully.")

        ## Checking if table have the same rows as df , if not we need to add new columns
        
        # Initialize MetaData
        metadata = MetaData()

        metadata.bind = engine
        my_table = Table(table_name, metadata, autoload_with=engine)

        # Get existing columns in the table
        existing_columns = [column.name for column in my_table.columns]
        print(f"Existing columns in '{table_name}': {existing_columns}")

        # Add new columns if they do not exist
        new_columns = [col for col in df.columns if col not in existing_columns]
  
        if new_columns:
            print(f"New columns to add: {new_columns}")
            for new_column in new_columns:
                print(f"Adding column: {new_column}")
                query = text(f'ALTER TABLE "{table_name}" ADD COLUMN "{new_column}" VARCHAR') 
                with engine.begin() as conn:
                    conn.execute(query)

 
        # Insert data into the table using df.to_sql
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        print("DataFrame loaded into the database successfully.")
    except Exception as e:
        print(f"Error loading DataFrame: {e}")
        raise