# from django.core.mail import send_mail
# from django.conf import settings
# from datetime import datetime
# import pytz

# def send_shut_height_alert(plant_no, machine_no, prev_height, new_height, alert_time=None):
#     if alert_time is None:
#         alert_time = datetime.now(pytz.timezone('Asia/Kolkata'))
#     date_str = alert_time.strftime('%d-%b-%Y • %H:%M %p')

#     subject = f"🚨 AtomOne – Automated Machine Alert (Plant {plant_no})"
#     message = f"""
# Hello Team,
# AtomOne Real-Time Monitoring System has detected a critical change in machine parameters.
# Please review the details below:

# 🛠 Machine Shut Height Change Details

# Parameter            Value
# Plant                {plant_no}
# Machine Number       {machine_no}
# Previous Shut Height {prev_height} mm
# New Shut Height      {new_height} mm
# Change Detected At   {date_str}

# ⚡ Required Action
# Please verify this adjustment immediately to ensure:
# ✔ Stable production
# ✔ Quality consistency
# ✔ Machine safety

# Your prompt attention is appreciated.

# Regards,
# AtomOne Automation System
# Smart • Reliable • Real-Time Monitoring

# 📌 Note
# This is an automated message from AtomOne. Replies to this email are not monitored.
# """
#     send_mail(
#         subject=subject,
#         message=message,
#         from_email=settings.EMAIL_HOST_USER,
#         recipient_list=settings.ALERT_EMAIL_RECIPIENTS,
#         fail_silently=False,
#     )
#     print(f"✅ Shut Height Alert Email sent: Plant {plant_no}, Machine {machine_no}, {prev_height}->{new_height}")



from django.core.mail import send_mail
from django.conf import settings
from datetime import datetime
import pytz


def send_shut_height_alert(plant_no, machine_no, prev_height, new_height, alert_time=None):
    if alert_time is None:
        alert_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    date_str = alert_time.strftime('%d-%b-%Y • %H:%M %p')

    # ✅ FIX: Format heights with 2 decimal places to preserve trailing zeros
    prev_height_formatted = f"{float(prev_height):.2f}"
    new_height_formatted = f"{float(new_height):.2f}"

    subject = f"🚨 AtomOne – Automated Machine Alert (Plant {plant_no})"
    message = f"""
Hello Team,
AtomOne Real-Time Monitoring System has detected a critical change in machine parameters.
Please review the details below:


🛠 Machine Shut Height Change Details


Parameter            Value
Plant                {plant_no}
Machine Number       {machine_no}
Previous Shut Height {prev_height_formatted} mm
New Shut Height      {new_height_formatted} mm
Change Detected At   {date_str}


⚡ Required Action
Please verify this adjustment immediately to ensure:
✔ Stable production
✔ Quality consistency
✔ Machine safety


Your prompt attention is appreciated.


Regards,
AtomOne Automation System
Smart • Reliable • Real-Time Monitoring


📌 Note
This is an automated message from AtomOne. Replies to this email are not monitored.
"""
    send_mail(
        subject=subject,
        message=message,
        from_email=settings.EMAIL_HOST_USER,
        recipient_list=settings.ALERT_EMAIL_RECIPIENTS,
        fail_silently=False,
    )
    print(f"✅ Shut Height Alert Email sent: Plant {plant_no}, Machine {machine_no}, {prev_height_formatted}->{new_height_formatted}")
