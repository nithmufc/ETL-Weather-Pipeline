import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from dotenv import load_dotenv

def send_email_function(create_email_body):
    # Load environment variables from .env file
    load_dotenv()

    # Email configuration
    sender_email = os.environ.get("SENDER_EMAIL")
    app_password = os.environ.get("APP_PASSWORD")
    recipient_email = os.environ.get("RECIPIENT_EMAIL")

    # Create a simple email message
    subject = "Weather Forecast Summary"
    body = create_email_body()

    # Skip sending email if the body is empty
    if not body:
        return

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

    print("Weather forecast email sent successfully!")

def create_email_body(extract, api_key, is_rain_forecasted):
    # List of cities
    cities = ["London", "Leeds", "Nottingham", "Manchester", "Bangalore"]

    email_body = ""

    # Loop through cities and create the email body
    for city in cities:
        # Extract data
        weather_data = extract(city, api_key)

        if weather_data is not None:
            # Print message based on rain forecast
            if is_rain_forecasted(weather_data):
                email_body += f"Rain forecasted for {city} tomorrow!\n"
            else:
                email_body += f"No rain forecast for {city} tomorrow.\n"

    return email_body

# Call the email function when the script is executed
if __name__ == "__main__":
    from etl_script import extract, is_rain_forecasted  # Import functions from etl_script
    api_key = "your_api_key"  # Replace with your actual API key
    send_email_function(lambda: create_email_body(extract, api_key, is_rain_forecasted))
