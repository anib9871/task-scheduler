import mysql.connector
from datetime import datetime, time
import time as t  # <- renamed to avoid conflict
import requests
import smtplib
from email.mime.text import MIMEText
import pytz  # <- For timezone handling

devid_for_sms = None
phone_numbers=""
email_ids=""

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

# ================== TIMEZONE CONFIG ==================
TZ = pytz.timezone("Asia/Singapore")  # Singapore timezone

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
    print("🔹 Sending SMS...")
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
        print("✅ SMS sent! Response:", response.text)
    except Exception as e:
        print("❌ SMS failed:", e)

def send_email(subject, message, email_ids):
    if not email_ids:
        print("❌ No email recipients. Skipping.")
        return
    print("🔹 Sending Email...")
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
        print("✅ Email sent successfully!")
    except Exception as e:
        print("❌ Email failed:", e)

# fetch phone number and email to send sms and email
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
            SELECT ID, DEVICE_ID, PARAMETER_ID, ALARM_DATE, ALARM_TIME, SMS_DATE, EMAIL_DATE, SMS_TIME, EMAIL_TIME
            FROM iot_api_devicealarmlog
            WHERE IS_ACTIVE=1
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
            alarm_date = alarm["ALARM_DATE"]
            alarm_time = alarm["ALARM_TIME"]
            fixed_time = (datetime.min + alarm_time).time()
            raised_time = TZ.localize(datetime.combine(alarm_date, fixed_time))  # Singapore timezone
            now = datetime.now(TZ)
            diff_seconds = (now - raised_time).total_seconds()
            print(f"⏱ Alarm {alarm_id}: diff_seconds={diff_seconds}")

            if diff_seconds >= 60:  # only process alarms older than 1 min
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
                phones, emails = get_contact_info(devid)

                # ----------- SECOND NOTIFICATION LOGIC -----------
                # Existing SMS/Email timestamps
                sms_time = None
                email_time = None
                if alarm["SMS_DATE"] and alarm.get("SMS_TIME"):
                    sms_time = TZ.localize(datetime.combine(alarm["SMS_DATE"], alarm["SMS_TIME"]))
                if alarm["EMAIL_DATE"] and alarm.get("EMAIL_TIME"):
                    email_time = TZ.localize(datetime.combine(alarm["EMAIL_DATE"], alarm["EMAIL_TIME"]))

                # Determine if notifications should be sent
                should_notify_sms = (sms_time is None) or ((now - sms_time).total_seconds() >= 6*3600)  # 6 hours
                should_notify_email = (email_time is None) or ((now - email_time).total_seconds() >= 6*3600)

                # Skip if neither notification is due
                if not should_notify_sms and not should_notify_email:
                    print(f"⏳ Alarm {alarm_id} notifications not due yet.")
                    continue

                # ----------- SEND NOTIFICATIONS -----------
                if should_notify_sms:
                    for phone in phones:
                        send_sms(phone, message)
                    print(f"📨 SMS notification sent for Alarm {alarm_id}")

                if should_notify_email:
                    send_email("IoT Alarm Notification", message, emails)
                    print(f"📧 Email notification sent for Alarm {alarm_id}")

                # ----------- UPDATE ALARM LOG -----------
                now_ts = datetime.now(TZ)
                cursor.execute("""
                    UPDATE iot_api_devicealarmlog
                    SET SMS_DATE=%s, SMS_TIME=%s, EMAIL_DATE=%s, EMAIL_TIME=%s
                    WHERE ID=%s
                """, (
                    now_ts.date() if should_notify_sms else alarm["SMS_DATE"],
                    now_ts.time() if should_notify_sms else alarm.get("SMS_TIME"),
                    now_ts.date() if should_notify_email else alarm["EMAIL_DATE"],
                    now_ts.time() if should_notify_email else alarm.get("EMAIL_TIME"),
                    alarm["ID"]
                ))
                conn.commit()

        cursor.close()
        conn.close()
    except Exception as e:
        print("❌ Error in check_and_notify:", e)

if __name__ == "__main__":
    while True:
        check_and_notify()
        print("⏳ Waiting 1 minutes for next check...")
        t.sleep(1 * 60)  # 5 minutes interval

