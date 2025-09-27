import os
import re
import argparse
from pathlib import Path
from dotenv import load_dotenv

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Parse command line arguments
parser = argparse.ArgumentParser(description='Send email notification')
parser.add_argument('--email', required=True, help='Recipient email address')
parser.add_argument('--subject', required=True, help='Email subject')
parser.add_argument('--body', required=True, help='Email body')
args = parser.parse_args()

BASE_DIR = Path(__file__).resolve().parent.parent.parent
env_path = os.path.join(BASE_DIR, '.env')

load_dotenv(env_path)

GMAIL_USER = os.getenv("GMAIL_USER")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")


def is_valid_email(email):
    """Check if the email address is valid."""
    # Regular expression for email validation
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None


def send_email(user, app_password, to, subject, body):
    # Validate email first
    if not is_valid_email(to):
        print(f"Error: '{to}' is not a valid email address.")
        return False
        
    try:
        # Build email
        msg = MIMEMultipart()
        msg['From'] = user
        msg['To'] = to
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain', 'utf-8'))

        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(user, app_password)
            server.sendmail(user, to, msg.as_string())
            print(f'Email successfully sent to {to}')
        return True
    except Exception as e:
        print(f'Error sending email: {e}')
        return False


if __name__ == '__main__':
    send_email(
        GMAIL_USER, 
        GMAIL_APP_PASSWORD, 
        args.email, 
        args.subject, 
        args.body
    )