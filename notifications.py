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
        print("✅ SMS sent:", phone, message)
    except Exception as e:
        print("❌ SMS failed:", e)

def send_email(subject, message, email_ids):
    if not email_ids:
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
        print("✅ Email sent:", message)
    except Exception as e:
        print("❌ Email failed:", e)

# ================== CONTACT INFO ==================
def get_contact_info(device_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT ORGANIZATION_ID, CENTRE_ID FROM master_device WHERE DEVICE_ID = %s
        """, (device_id,))
        device = cursor.fetchone()
        if not device:
            return [], []

        org_id, centre_id = device["ORGANIZATION_ID"], device["CENTRE_ID"]

        cursor.execute("""
            SELECT USER_ID_id FROM userorganizationcentrelink 
            WHERE ORGANIZATION_ID_id=%s AND CENTRE_ID_id=%s
        """, (org_id, centre_id))
        user_ids = [u["USER_ID_id"] for u in cursor.fetchall()]
        if not user_ids:
            return [], []

        format_strings = ','.join(['%s']*len(user_ids))
        cursor.execute(f"""
            SELECT PHONE, EMAIL, SEND_SMS, SEND_EMAIL 
            FROM master_user WHERE USER_ID IN ({format_strings}) 
            AND (SEND_SMS=1 OR SEND_EMAIL=1)
        """, tuple(user_ids))
        users = cursor.fetchall()
        phones = [u["PHONE"] for u in users if u["SEND_SMS"]==1]
        emails = [u["EMAIL"] for u in users if u["SEND_EMAIL"]==1]
        return phones, emails
    except Exception as e:
        print("❌ Error getting contacts:", e)
        return [], []
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()

# ================== CHECK AND NOTIFY ==================
def check_and_notify():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Fetch new alarms (SMS/Email not sent yet)
        cursor.execute("""
            SELECT ID, DEVICE_ID, PARAMETER_ID, ALARM_DATE, ALARM_TIME
            FROM iot_api_devicealarmlog
            WHERE IS_ACTIVE=1 AND (SMS_DATE IS NULL OR EMAIL_DATE IS NULL)
        """)
        alarms = cursor.fetchall()
        if not alarms:
            print("✅ No new alarms.")
            return

        for alarm in alarms:
            alarm_id = alarm["ID"]
            devid = alarm["DEVICE_ID"]
            alarm_date = alarm["ALARM_DATE"]
            alarm_time = (datetime.min + alarm["ALARM_TIME"]).time()
            raised_time = TZ.localize(datetime.combine(alarm_date, alarm_time))
            now = datetime.now(TZ)
            if (now - raised_time).total_seconds() < 60:
                continue  # wait at least 1 min before sending

            # Get device name
            cursor.execute("SELECT device_name FROM master_device WHERE device_id=%s", (devid,))
            devnm = cursor.fetchone()["device_name"]

            # Fetch latest sensor readings and thresholds
            cursor.execute("""
                SELECT MP.PARAMETER_NAME, MP.UPPER_THRESHOLD, MP.LOWER_THRESHOLD, DRL.READING AS CURRENT_READING
                FROM master_device MD
                JOIN iot_api_devicesensorlink DSL ON DSL.DEVICE_ID=MD.DEVICE_ID
                JOIN iot_api_sensorparameterlink SPL ON SPL.SENSOR_ID=DSL.SENSOR_ID
                JOIN iot_api_masterparameter MP ON MP.PARAMETER_ID=SPL.PARAMETER_ID
                JOIN device_reading_log DRL ON DRL.DEVICE_ID=MD.DEVICE_ID
                WHERE MD.DEVICE_ID=%s
                ORDER BY DRL.READING_DATE DESC, DRL.READING_TIME DESC
                LIMIT 1
            """, (devid,))
            reading = cursor.fetchone()
            if not reading:
                print(f"⚠️ No readings for {devnm}")
                continue

            # Determine alert type
            curr, low, high = reading["CURRENT_READING"], reading["LOWER_THRESHOLD"], reading["UPPER_THRESHOLD"]
            sensor = reading["PARAMETER_NAME"]
            if curr < low:
                ntf_typ = 1
            elif curr > high:
                ntf_typ = 2
            else:
                ntf_typ = 7  # back to normal

            message = build_message(ntf_typ, devnm, sensor)
            phones, emails = get_contact_info(devid)
            for phone in phones:
                send_sms(phone, message)
            send_email("IoT Alarm Notification", message, emails)

            now_ts = datetime.now(TZ)
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

# ================== MAIN LOOP ==================
if __name__ == "__main__":
    while True:
        check_and_notify()
        print("⏳ Waiting 1 minute for next check...")
        t.sleep(60)
