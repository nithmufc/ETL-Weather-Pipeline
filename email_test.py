import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Email configuration
sender_email = os.environ.get("SENDER_EMAIL")
app_password = os.environ.get("APP_PASSWORD")
recipient_email = os.environ.get("RECIPIENT_EMAIL")

# Create a simple email message
subject = "Test Email"
body = "This is a test email sent from Python."
message = MIMEMultipart()
message["From"] = "Nithyanand S N"
message["To"] = recipient_email
message["Subject"] = subject
message.attach(MIMEText(body, "plain"))

# Connect to the SMTP server
smtp_server = "smtp.gmail.com"  # Use the appropriate SMTP server for your email provider
smtp_port = 587  # Use the appropriate port for your email provider
server = smtplib.SMTP(smtp_server, smtp_port)
server.starttls()

# Login to the email account
server.login(sender_email, app_password)

# Send the email
server.sendmail(sender_email, recipient_email, message.as_string())

# Quit the SMTP server
server.quit()

print("Test email sent successfully!")
