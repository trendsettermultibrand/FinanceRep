from datetime import timedelta
from importlib import metadata
import requests
import pandas as pd
import mail
from sqlalchemy import create_engine, MetaData, Table, text
import os
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy.engine import Engine


# Load environment variables from .env file
env_path = Path(".env") ##to be changed in server
load_dotenv(dotenv_path=env_path)


def create_my_engine(
    df: pd.DataFrame,
    user: str,
    password: str,
    host: str,
    port: str,
    database: str,
    table_name: str,
    sslmode: str
) -> None:
    """
    Create a PostgreSQL connection using SQLAlchemy and load a DataFrame into the given table.

    Args:
        df (pd.DataFrame): The DataFrame to insert into the table.
        user (str): Database username.
        password (str): Database password.
        host (str): Database host (IP or domain).
        port (str): Database port (default 5432).
        database (str): Database name.
        table_name (str): Target table name.
        sslmode (str): SSL mode (e.g., "require").

    Raises:
        Exception: If any connection or loading error occurs.
    """
    connection_string = (
        f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode={sslmode}"
    )

    try:
    
        engine = create_engine(connection_string)
        print("SQLAlchemy engine created successfully.")

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


def load_to_db(df: pd.DataFrame) -> None:
    """
    Load a DataFrame into the 'api_wb_financereport_cpy' table.

    Args:
        df (pd.DataFrame): DataFrame to be processed and inserted into the database.

    Raises:
        Exception: If the DataFrame is empty or an error occurs during insertion.
    """
    try:
        print("Processing file...")

        # Check if DataFrame is empty
        if df.empty:
            print("There is no data to process.")
            return  # Stop processing if no data

        # Load DataFrame into the database
        create_my_engine(
           df=df,
            user=os.getenv('user'),
            password=os.getenv('password'),
            host=os.getenv('host'),
            port=int(os.getenv('port')),
            database=os.getenv('database'),
            sslmode=os.getenv('sslmode'),
            table_name="api_wb_FinanceReport",
        )

        print("Finished processing file.")

        # Send success notification
        subject = "Finance Reports - Successful"
        body = "Finance Reports script completed. No errors detected."
        mail.send_success_email(subject, body)
        print("Success email sent.")

    except Exception as e:
        print(f"Error processing file: {e}")

        # Send failure notification
        subject1 = "Finance Reports - Error"
        body1 = "Finance Reports script failed. Please check the logs."
        mail.send_error_email(subject1, body1)
        print("Error email sent.")
        raise
