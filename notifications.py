import mysql.connector
from datetime import datetime
import time  # for sleep
import requests
import smtplib
from email.mime.text import MIMEText

# ================== DATABASE CONFIG ==================
db_config = {
    "host": "switchback.proxy.rlwy.net",
    "user": "root",
    "port": 44750,
    "password": "qYxlhEiaEvtiRvKaFyigDPtXSSCpddMv",
    "database": "railway",
}

# ================== SMS CONFIG ==================
SMS_API_URL = "http://www.universalsmsadvertising.com/universalsmsapi.php"
SMS_USER = "8960853914"
SMS_PASS = "8960853914"
SENDER_ID = "FRTLLP"

# ================== EMAIL CONFIG ==================
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = "testwebservice71@gmail.com"
EMAIL_PASS = "akuu vulg ejlg ysbt"

# ===================================================

def build_message(ntf_typ, devnm):
    messages = {
        1: f"WARNING!! The Temperature of {devnm} has dipped below the lower limit. Please take necessary action- Regards Fertisense LLP",
        2: f"WARNING!! The Temperature of {devnm} has crossed the higher limit. Please take necessary action- Regards Fertisense LLP",
        3: f"WARNING!! The {devnm} is offline. Please take necessary action- Regards Fertisense LLP",
        4: f"WARNING!! The level of liquid nitrogen in {devnm} is low. Please take necessary action- Regards Fertisense LLP",
        5: f"INFO!! The device {devnm} is back online. No action is required - Regards Fertisense LLP",
        6: f"INFO!! The level of Liquid Nitrogen is back to normal for {devnm}. No action is required - Regards Fertisense LLP",
        7: f"INFO!! The temperature levels are back to normal for {devnm}. No action is required - Regards Fertisense LLP",
        8: f"WARNING!! The room temperature reading in {devnm} has dipped below the lower limit. Please take necessary action- Regards Fertisense LLP",
        9: f"WARNING!! The room temperature reading in {devnm} has gone above the higher limit. Please take necessary action- Regards Fertisense LLP",
        10: f"INFO!! The room temperature levels are back to normal in {devnm}. No action is required - Regards Fertisense LLP",
        11: f"WARNING!! The humidity reading in {devnm} has dipped below the lower limit. Please take necessary action- Regards Fertisense LLP",
        12: f"WARNING!! The humidity reading in {devnm} has gone above the higher limit. Please take necessary action- Regards Fertisense LLP",
        13: f"INFO!! The humidity levels are back to normal in {devnm}. No action is required - Regards Fertisense LLP",
        14: f"WARNING!! The VOC reading in {devnm} has dipped below the lower limit. Please take necessary action- Regards Fertisense LLP",
        15: f"WARNING!! The VOC reading in {devnm} has gone above the higher limit. Please take necessary action- Regards Fertisense LLP",
        16: f"INFO!! The VOC levels are back to normal in {devnm}. No action is required - Regards Fertisense LLP",
    }
    return messages.get(ntf_typ, f"Alert for {devnm} - Regards Fertisense LLP")

def send_sms(phone, message):
    if not phone:
        print("❌ Phone number empty. Skipping SMS.")
        return
    try:
        params = {
            "user_name": SMS_USER,
            "user_password": SMS_PASS,
            "mobile": phone,
            "sender_id": SENDER_ID,
            "type": "F",
            "text": message
        }
        response = requests.get(SMS_API_URL, params=params)
        print(f"✅ SMS sent to {phone}. Response:", response.text)
    except Exception as e:
        print("❌ SMS failed:", e)

def send_email(subject, message, email_ids):
    if not email_ids:
        print("❌ No email recipients. Skipping.")
        return
    try:
        msg = MIMEText(message)
        msg["Subject"] = subject
        msg["From"] = EMAIL_USER
        msg["To"] = ", ".join(email_ids)
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, email_ids, msg.as_string())
        server.quit()
        print(f"✅ Email sent to {email_ids}")
    except Exception as e:
        print("❌ Email failed:", e)

def get_contact_info(device_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT ORGANIZATION_ID, CENTRE_ID
            FROM master_device
            WHERE DEVICE_ID = %s
        """, (device_id,))
        device = cursor.fetchone()
        if not device:
            return [], []
        org_id = device["ORGANIZATION_ID"]
        centre_id = device["CENTRE_ID"]
        cursor.execute("""
            SELECT USER_ID_id
            FROM userorganizationcentrelink
            WHERE ORGANIZATION_ID_id = %s
              AND CENTRE_ID_id = %s
        """, (org_id, centre_id))
        users_link = cursor.fetchall()
        user_ids = [u["USER_ID_id"] for u in users_link]
        if not user_ids:
            return [], []
        format_strings = ','.join(['%s'] * len(user_ids))
        query = f"""
            SELECT USER_ID, PHONE, EMAIL, SEND_SMS, SEND_EMAIL
            FROM master_user
            WHERE USER_ID IN ({format_strings})
              AND (SEND_SMS = 1 OR SEND_EMAIL = 1)
        """
        cursor.execute(query, tuple(user_ids))
        users = cursor.fetchall()
        phone_numbers = [u["PHONE"] for u in users if u["SEND_SMS"] == 1]
        email_ids = [u["EMAIL"] for u in users if u["SEND_EMAIL"] == 1]
        return phone_numbers, email_ids
    except Exception as e:
        print("❌ Error in get_contact_info:", e)
        return [], []
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()

def check_and_notify():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT ID, DEVICE_ID, PARAMETER_ID, ALARM_DATE, ALARM_TIME, SMS_DATE, EMAIL_DATE
            FROM iot_api_devicealarmlog
            WHERE IS_ACTIVE=1 AND SMS_TIME IS NULL AND EMAIL_TIME IS NULL
        """)
        alarms = cursor.fetchall()
        if not alarms:
            print("✅ No new alarms to notify.")
            return
        print("📊 Alarm Table Status:")
        print("ID | Device | Param | Raised Time | SMS | Email")
        print("-" * 80)
        for alarm in alarms:
            alarm_id = alarm["ID"]
            devid = alarm["DEVICE_ID"]
            param_id = alarm["PARAMETER_ID"]
            alarm_date = alarm["ALARM_DATE"]
            alarm_time = alarm["ALARM_TIME"]

            # Time calculation
            fixed_time = (datetime.min + alarm_time).time()
            raised_time = datetime.combine(alarm_date, fixed_time)
            now = datetime.now()
            diff_seconds = (now - raised_time).total_seconds()
            print(f"⏱ Alarm {alarm_id}: diff_seconds={diff_seconds}, raised_time={raised_time}, now={now}")

            # ✅ For testing, bypass negative diff_seconds
            if diff_seconds >= -3600:  # allow 1 hour past/future for testing
                cursor.execute("SELECT device_name FROM master_device WHERE device_id=%s", (devid,))
                row = cursor.fetchone()
                devnm = row["device_name"] if row else f"Device-{devid}"
                cursor.execute("""
                    SELECT 
                        MP.UPPER_THRESHOLD,
                        MP.LOWER_THRESHOLD,
                        DRL.READING AS CURRENT_READING
                    FROM master_device MD
                    LEFT JOIN iot_api_devicesensorlink DSL ON DSL.DEVICE_ID = MD.DEVICE_ID
                    LEFT JOIN iot_api_sensorparameterlink SPL ON SPL.SENSOR_ID = DSL.SENSOR_ID
                    LEFT JOIN iot_api_masterparameter MP ON MP.PARAMETER_ID = SPL.PARAMETER_ID
                    LEFT JOIN device_reading_log DRL ON DRL.DEVICE_ID = MD.DEVICE_ID
                    WHERE MD.DEVICE_ID = %s
                    ORDER BY DRL.READING_DATE DESC, DRL.READING_TIME DESC
                    LIMIT 1
                """, (devid,))
                reading_row = cursor.fetchone()
                if not reading_row:
                    print(f"⚠️ No reading found for device {devnm}")
                    continue

                upth = reading_row["UPPER_THRESHOLD"]
                lowth = reading_row["LOWER_THRESHOLD"]
                currreading = reading_row["CURRENT_READING"]
                print(f"Device {devnm}: Lower={lowth}, Upper={upth}, Current={currreading}")

                if currreading < lowth:
                    ntf_typ = 1
                elif currreading > upth:
                    ntf_typ = 2
                else:
                    ntf_typ = 7

                message = build_message(ntf_typ, devnm)

                # Debug: Show who will receive notifications
                phones, emails = get_contact_info(devid)
                print("Phones to send:", phones)
                print("Emails to send:", emails)

                # Send SMS
                for phone in phones:
                    send_sms(phone, message)

                # Send Email
                send_email("IoT Alarm Notification", message, emails)

                now_ts = datetime.now()
                cursor.execute("""
                    UPDATE iot_api_devicealarmlog
                    SET SMS_DATE=%s, SMS_TIME=%s, EMAIL_DATE=%s, EMAIL_TIME=%s
                    WHERE ID=%s
                """, (now_ts.date(), now_ts.time(), now_ts.date(), now_ts.time(), alarm_id))
                conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print("❌ Error in check_and_notify:", e)

if __name__ == "__main__":
    while True:
        check_and_notify()
        print("⏳ Waiting 1 minutes for next check...")
        time.sleep(1 * 60)  # 5 minutes interval


# import mysql.connector
# from datetime import datetime, time
# import requests
# import smtplib
# from email.mime.text import MIMEText
# devid_for_sms = None
# phone_numbers=""
# email_ids=""
# # ================== DATABASE CONFIG ==================
# db_config = {
#     "host": "switchback.proxy.rlwy.net",
#     "user": "root",
#     "port": 44750,
#     "password": "qYxlhEiaEvtiRvKaFyigDPtXSSCpddMv",
#     "database": "railway",
# }

# # ================== SMS CONFIG ==================
# SMS_API_URL = "http://www.universalsmsadvertising.com/universalsmsapi.php"
# SMS_USER = "8960853914"
# SMS_PASS = "8960853914"
# SENDER_ID = "FRTLLP"
# TO_PHONE_NUMBER = phone_numbers   # your number here

# # ================== EMAIL CONFIG ==================
# SMTP_SERVER = "smtp.gmail.com"
# SMTP_PORT = 587
# EMAIL_USER = "testwebservice71@gmail.com"
# EMAIL_PASS = "akuu vulg ejlg ysbt"
# #TO_EMAIL = "testwebservice71@gmail.com"
# TO_EMAIL = email_ids

# # ===================================================

# def build_message(ntf_typ, devnm):
#     messages = {
#         1: f"WARNING!! The Temperature of {devnm} has dipped below the lower limit. Please take necessary action- Regards Fertisense LLP",
#         2: f"WARNING!! The Temperature of {devnm} has crossed the higher limit. Please take necessary action- Regards Fertisense LLP",
#         3: f"WARNING!! The {devnm} is offline. Please take necessary action- Regards Fertisense LLP",
#         4: f"WARNING!! The level of liquid nitrogen in {devnm} is low. Please take necessary action- Regards Fertisense LLP",
#         5: f"INFO!! The device {devnm} is back online. No action is required - Regards Fertisense LLP",
#         6: f"INFO!! The level of Liquid Nitrogen is back to normal for {devnm}. No action is required - Regards Fertisense LLP",
#         7: f"INFO!! The temperature levels are back to normal for {devnm}. No action is required - Regards Fertisense LLP",
#         8: f"WARNING!! The room temperature reading in {devnm} has dipped below the lower limit. Please take necessary action- Regards Fertisense LLP",
#         9: f"WARNING!! The room temperature reading in {devnm} has gone above the higher limit. Please take necessary action- Regards Fertisense LLP",
#         10: f"INFO!! The room temperature levels are back to normal in {devnm}. No action is required - Regards Fertisense LLP",
#         11: f"WARNING!! The humidity reading in {devnm} has dipped below the lower limit. Please take necessary action- Regards Fertisense LLP",
#         12: f"WARNING!! The humidity reading in {devnm} has gone above the higher limit. Please take necessary action- Regards Fertisense LLP",
#         13: f"INFO!! The humidity levels are back to normal in {devnm}. No action is required - Regards Fertisense LLP",
#         14: f"WARNING!! The VOC reading in {devnm} has dipped below the lower limit. Please take necessary action- Regards Fertisense LLP",
#         15: f"WARNING!! The VOC reading in {devnm} has gone above the higher limit. Please take necessary action- Regards Fertisense LLP",
#         16: f"INFO!! The VOC levels are back to normal in {devnm}. No action is required - Regards Fertisense LLP",
#     }
#     return messages.get(ntf_typ, f"Alert for {devnm} - Regards Fertisense LLP")

# def send_sms(phone, message):
#     print("🔹 Sending SMS...")
#     try:
#         params = {
#             "user_name": SMS_USER,
#             "user_password": SMS_PASS,
#             "mobile": phone,
#             "sender_id": SENDER_ID,
#             "type": "F",
#             "text": message
#         }
#         response = requests.get(SMS_API_URL, params=params)
#         print("✅ SMS sent! Response:", response.text)
#     except Exception as e:
#         print("❌ SMS failed:", e)

# def send_email(subject, message, email_ids):
#     if not email_ids:
#         print("❌ No email recipients. Skipping.")
#         return

#     print("🔹 Sending Email...")
#     try:
#         msg = MIMEText(message)
#         msg["Subject"] = subject
#         msg["From"] = EMAIL_USER
#         msg["To"] = ", ".join(email_ids)  # <- Must be a string, not list

#         server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
#         server.starttls()
#         server.login(EMAIL_USER, EMAIL_PASS)
#         server.sendmail(EMAIL_USER, email_ids, msg.as_string())
#         server.quit()
#         print("✅ Email sent successfully!")
#     except Exception as e:
#         print("❌ Email failed:", e)
# import mysql.connector

# # fetch phone number and email to send sms and email
# def get_contact_info(device_id):
#     try:
#         conn = mysql.connector.connect(**db_config)
#         cursor = conn.cursor(dictionary=True)

#         # Step 1: Get org_id & centre_id from device
#         cursor.execute("""
#             SELECT ORGANIZATION_ID, CENTRE_ID
#             FROM master_device
#             WHERE DEVICE_ID = %s
#         """, (device_id,))
#         device = cursor.fetchone()
#         if not device:
#             return [], []

#         org_id = device["ORGANIZATION_ID"]
#         centre_id = device["CENTRE_ID"]

#         # Step 2: Get USER_IDs linked to this org & centre
#         cursor.execute("""
#             SELECT USER_ID_id
#             FROM userorganizationcentrelink
#             WHERE ORGANIZATION_ID_id = %s
#               AND CENTRE_ID_id = %s
#         """, (org_id, centre_id))
#         users_link = cursor.fetchall()
#         user_ids = [u["USER_ID_id"] for u in users_link]

#         if not user_ids:
#             return [], []

#         # Step 3: Get user contact info from master_user
#         format_strings = ','.join(['%s'] * len(user_ids))  # for IN clause
#         query = f"""
#             SELECT USER_ID, PHONE, EMAIL, SEND_SMS, SEND_EMAIL
#             FROM master_user
#             WHERE USER_ID IN ({format_strings})
#               AND (SEND_SMS = 1 OR SEND_EMAIL = 1)
#         """
#         cursor.execute(query, tuple(user_ids))
#         users = cursor.fetchall()

#         # Step 4: Collect phone numbers and emails
#         phone_numbers = [u["PHONE"] for u in users if u["SEND_SMS"] == 1]
#         email_ids     = [u["EMAIL"] for u in users if u["SEND_EMAIL"] == 1]

#         return phone_numbers, email_ids

#     except Exception as e:
#         print("❌ Error in get_contact_info:", e)
#         return [], []

#     finally:
#         if 'cursor' in locals():
#             cursor.close()
#         if 'conn' in locals() and conn.is_connected():
#             conn.close()


# def check_and_notify():
#     try:
#         conn = mysql.connector.connect(**db_config)
#         cursor = conn.cursor(dictionary=True)

#         # Fetch active alarms not yet notified
#         cursor.execute("""
#             SELECT ID, DEVICE_ID, PARAMETER_ID, ALARM_DATE, ALARM_TIME, SMS_DATE, EMAIL_DATE
#             FROM iot_api_devicealarmlog
#             WHERE IS_ACTIVE=1 AND SMS_TIME IS NULL AND EMAIL_TIME IS NULL
#         """)
#         alarms = cursor.fetchall()

#         if not alarms:
#             print("✅ No new alarms to notify.")
#             return

#         print("📊 Alarm Table Status:")
#         print("ID | Device | Param | Raised Time | SMS | Email")
#         print("-" * 80)

#         for alarm in alarms:
#             alarm_id = alarm["ID"]
#             devid = alarm["DEVICE_ID"]
#             param_id = alarm["PARAMETER_ID"]
#             alarm_date = alarm["ALARM_DATE"]
#             alarm_time = alarm["ALARM_TIME"]

#             # Convert alarm time
#             fixed_time = (datetime.min + alarm_time).time()
#             raised_time = datetime.combine(alarm_date, fixed_time)
#             now = datetime.now()
#             diff_seconds = (now - raised_time).total_seconds()

#             print(f"⏱ Alarm {alarm_id}: diff_seconds={diff_seconds}")

#             if diff_seconds >= 60:  # Notify only after 1 minute
#                 # Fetch device name
#                 cursor.execute("SELECT device_name FROM master_device WHERE device_id=%s", (devid,))
#                 row = cursor.fetchone()
#                 devnm = row["device_name"] if row else f"Device-{devid}"

#                 # Fetch latest reading and thresholds
#                 cursor.execute("""
#                     SELECT 
#                         MP.UPPER_THRESHOLD,
#                         MP.LOWER_THRESHOLD,
#                         DRL.READING AS CURRENT_READING
#                     FROM master_device MD
#                     LEFT JOIN iot_api_devicesensorlink DSL ON DSL.DEVICE_ID = MD.DEVICE_ID
#                     LEFT JOIN iot_api_sensorparameterlink SPL ON SPL.SENSOR_ID = DSL.SENSOR_ID
#                     LEFT JOIN iot_api_masterparameter MP ON MP.PARAMETER_ID = SPL.PARAMETER_ID
#                     LEFT JOIN device_reading_log DRL ON DRL.DEVICE_ID = MD.DEVICE_ID
#                     WHERE MD.DEVICE_ID = %s
#                     ORDER BY DRL.READING_DATE DESC, DRL.READING_TIME DESC
#                     LIMIT 1
#                 """, (devid,))
#                 reading_row = cursor.fetchone()

#                 if not reading_row:
#                     print(f"⚠️ No reading found for device {devnm}")
#                     continue

#                 upth = reading_row["UPPER_THRESHOLD"]
#                 lowth = reading_row["LOWER_THRESHOLD"]
#                 currreading = reading_row["CURRENT_READING"]

#                 print(f"Device {devnm}: Lower={lowth}, Upper={upth}, Current={currreading}")

#                 # Determine notification type
#                 if currreading < lowth:
#                     ntf_typ = 1
#                 elif currreading > upth:
#                     ntf_typ = 2
#                 else:
#                     ntf_typ = 7

#                 message = build_message(ntf_typ, devnm)

#                 # ✅ Fetch contact info dynamically
#                 phones, emails = get_contact_info(devid)

#                 # Send SMS
#                 for phone in phones:
#                     send_sms(phone, message)

#                 # Send Email
#                 send_email("IoT Alarm Notification", message, emails)

#                 # Update alarm timestamps
#                 now_ts = datetime.now()
#                 cursor.execute("""
#                     UPDATE iot_api_devicealarmlog
#                     SET SMS_DATE=%s, SMS_TIME=%s, EMAIL_DATE=%s, EMAIL_TIME=%s
#                     WHERE ID=%s
#                 """, (now_ts.date(), now_ts.time(), now_ts.date(), now_ts.time(), alarm_id))
#                 conn.commit()

#         cursor.close()
#         conn.close()

#     except Exception as e:
#         print("❌ Error in check_and_notify:", e)

# if __name__ == "__main__":
#     check_and_notify()


