import os
from pathlib import Path
from dotenv import load_dotenv

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

BASE_DIR = Path(__file__).resolve().parent.parent.parent
env_path = os.path.join(BASE_DIR, '.env')

load_dotenv(env_path)

GMAIL_USER = os.getenv("GMAIL_USER")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")


def send_email(user, app_password, to, subject, body):
    try:
        # 构建邮件
        msg = MIMEMultipart()
        msg['From'] = user
        msg['To'] = to
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain', 'utf-8'))

        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(user, app_password)
            server.sendmail(user, to, msg.as_string())
    except Exception as e:
        print('Some Error Occurred!')


if __name__ == '__main__':
    send_email(GMAIL_USER, GMAIL_APP_PASSWORD, '2088617032@qq.com', 'CNAScope Task Finished Notation', 'Your CNAScope Job has completed')
