from extract  import extract_data
from load     import load_to_db
from mail     import send_success_email, send_error_email
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
    try:
        print("ETL process started.")

        df = extract_data()
        if not df.empty:
            load_to_db(df)
            print(f"Successfully completed ETL process.")
        else:
            print("No data available to load.")
        print("ETL process finished.")


        #Send success notification
        subject = "Finance Reports - Successful"
        body = "Finance Reports script completed. No errors detected."
        send_success_email(subject, body)
        print("Success email sent.")


    except Exception as e:

        # Send failure notification
        subject1 = "Finance Reports - Error"
        body1 = f"Finance Reports script failed. Please check the logs.\nError details: {e}"
        send_error_email(subject1, body1)
        print("Error email sent.")
        raise



if __name__ == "__main__":
    """
    Entry point of the ETL pipeline.
    """
    main()   