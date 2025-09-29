import logging
from email.mime.text import MIMEText
import smtplib
from time import sleep
from dotenv import load_dotenv
import os
from pathlib import Path

env_path = Path(".env") ##to be changed in server
load_dotenv(dotenv_path=env_path)


SMTP_SERVER = os.getenv('SMTP_SERVER')
SMTP_PORT = os.getenv('SMTP_PORT')
SMTP_USER = os.getenv('SMTP_USER')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD')
FROM_EMAIL = os.getenv('FROM_EMAIL')
TO_EMAIL = os.getenv('TO_EMAIL')

def send_error_email(subject, body, retries=3, delay=5):
    """Send an error email with retries."""
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = FROM_EMAIL
    msg['To'] = TO_EMAIL

    attempt = 0
    while attempt < retries:
        try:
            server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=10)
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
            server.quit()
            logging.info("Error email sent successfully.")
            return
        except Exception as e:
            attempt += 1
            logging.error(f"Attempt {attempt}: Failed to send error email: {e}")
            if attempt < retries:
                logging.info(f"Retrying to send email in {delay} seconds...")
                sleep(delay)
            else:
                logging.critical("All attempts to send error email have failed.")
                raise

def send_success_email(subject, body, retries=3, delay=5):
    """Send a success email with retries."""
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = FROM_EMAIL
    msg['To'] = TO_EMAIL

    attempt = 0
    while attempt < retries:
        try:
            server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=10)
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
            server.quit()
            logging.info("Success email sent successfully.")
            return
        except Exception as e:
            attempt += 1
            logging.error(f"Attempt {attempt}: Failed to send success email: {e}")
            if attempt < retries:
                logging.info(f"Retrying to send email in {delay} seconds...")
                sleep(delay)
            else:
                logging.critical("All attempts to send success email have failed.")
                raise
