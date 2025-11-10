import smtplib
from email.mime.text import MIMEText
from datetime import date

msg = MIMEText(f"Daily data pipeline completed for {date.today()}. Report generated successfully.")
msg["Subject"] = "Daily Automation Report"
msg["From"] = "your_email@gmail.com"
msg["To"] = "recipient_email@gmail.com"

with smtplib.SMTP("smtp.gmail.com", 587) as server:
    server.starttls()
    server.login("your_email@gmail.com", "your_app_password")
    server.send_message(msg)

print("Report email sent.")
