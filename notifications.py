import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import requests
import smtplib
from email.mime.text import MIMEText

# ================== CONFIG ==================
db_config = {
    "host": "dpg-d3acrta4d50c73d5v8u0-a.oregon-postgres.render.com",
    "port": 5432,
    "user": "airkpi_mclp_user",
    "password": "cbAT63ju7Y0A5kmAACIOimsc0x5ceZIj",
    "dbname": "airkpi_mclp",
}

SMS_API_URL = "http://www.universalsmsadvertising.com/universalsmsapi.php"
SMS_USER = "8960853914"
SMS_PASS = "8960853914"
SENDER_ID = "FRTLLP"

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = "testwebservice71@gmail.com"
EMAIL_PASS = "akuu vulg ejlg ysbt"

# ================== HELPERS ==================
def get_connection():
    return psycopg2.connect(
        host=db_config["host"],
        port=db_config["port"],
        user=db_config["user"],
        password=db_config["password"],
        dbname=db_config["dbname"],
        cursor_factory=RealDictCursor
    )

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
    print(f"📱 Sending SMS to {phone}...")
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
    print(f"📧 Sending Email to {email_ids}...")
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

# ================== CONTACT INFO ==================
def get_contact_info(device_id):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT "ORGANIZATION_ID", "CENTRE_ID"
            FROM master_device
            WHERE "DEVICE_ID"=%s
        """, (device_id,))
        device = cursor.fetchone()
        if not device:
            return [], []

        org_id, centre_id = device["ORGANIZATION_ID"], device["CENTRE_ID"]

        cursor.execute("""
            SELECT "USER_ID_id"
            FROM userorganizationcentrelink
            WHERE "ORGANIZATION_ID_id"=%s AND "CENTRE_ID_id"=%s
        """, (org_id, centre_id))
        user_ids = [u["USER_ID_id"] for u in cursor.fetchall()]
        if not user_ids:
            return [], []

        format_strings = ','.join(['%s'] * len(user_ids))
        cursor.execute(f"""
            SELECT "USER_ID", "PHONE", "EMAIL", "SEND_SMS", "SEND_EMAIL"
            FROM master_user
            WHERE "USER_ID" IN ({format_strings}) AND ("SEND_SMS"=1 OR "SEND_EMAIL"=1)
        """, tuple(user_ids))
        users = cursor.fetchall()
        phones = [u["PHONE"] for u in users if u["SEND_SMS"] == 1]
        emails = [u["EMAIL"] for u in users if u["SEND_EMAIL"] == 1]
        return phones, emails
    except Exception as e:
        print("❌ get_contact_info error:", e)
        return [], []
    finally:
        cursor.close()
        conn.close()

# ================== CHECK & NOTIFY ==================
def check_and_notify():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT "id", "DEVICE_ID", "PARAMETER_ID", "ALARM_DATE", "ALARM_TIME"
            FROM iot_api_devicealarmlog
            WHERE "IS_ACTIVE"=1 AND "SMS_TIME" IS NULL AND "EMAIL_TIME" IS NULL
        """)
        alarms = cursor.fetchall()
        if not alarms:
            print("✅ No new alarms to notify.")
            return

        print(f"📊 Found {len(alarms)} new alarms.")

        for alarm in alarms:
            alarm_id = alarm["id"]
            devid = alarm["DEVICE_ID"]
            paramid = alarm["PARAMETER_ID"]
            alarm_date = alarm["ALARM_DATE"]
            alarm_time = alarm["ALARM_TIME"]

            raised_time = datetime.combine(alarm_date, alarm_time)
            diff_seconds = (datetime.now() - raised_time).total_seconds()
            print(f"⏱ Alarm {alarm_id} | Device {devid} | Raised {diff_seconds:.1f}s ago")

            if diff_seconds < 60:
                print("⏳ Alarm too fresh (<60s), skipping...")
                continue

            # Fetch device name
            cursor.execute('SELECT "DEVICE_NAME" FROM master_device WHERE "DEVICE_ID"=%s', (devid,))
            row = cursor.fetchone()
            devnm = row["DEVICE_NAME"] if row else f"Device-{devid}"
            print(f"📟 Device: {devnm}")

            # Fetch thresholds + latest reading (direct PARAMETER_ID use)
            cursor.execute("""
                SELECT MP."UPPER_THRESHOLD", MP."LOWER_THRESHOLD", DRL."READING" AS "CURRENT_READING"
                FROM iot_api_devicealarmlog AL
                LEFT JOIN master_device MD ON MD."DEVICE_ID" = AL."DEVICE_ID"
                LEFT JOIN iot_api_masterparameter MP ON MP."PARAMETER_ID" = AL."PARAMETER_ID"
                LEFT JOIN device_reading_log DRL ON DRL."DEVICE_ID" = AL."DEVICE_ID"
                WHERE AL."id" = %s
                ORDER BY DRL."READING_DATE" DESC, DRL."READING_TIME" DESC
                LIMIT 1
            """, (alarm_id,))
            reading_row = cursor.fetchone()

            if not reading_row:
                print("⚠ No reading found, skipping...")
                continue

            upth = reading_row["UPPER_THRESHOLD"]
            lowth = reading_row["LOWER_THRESHOLD"]
            currreading = reading_row["CURRENT_READING"]

            print(f"📈 Reading: {currreading}, Low={lowth}, Up={upth}")

            if lowth is not None and currreading < float(lowth):
                ntf_typ = 1
            elif upth is not None and currreading > float(upth):
                ntf_typ = 2
            else:
                print("✅ Reading normal, skipping notification")
                continue

            message = build_message(ntf_typ, devnm)
            phones, emails = get_contact_info(devid)

            for phone in phones:
                send_sms(phone, message)
            send_email("IoT Alarm Notification", message, emails)

            now_ts = datetime.now()
            cursor.execute("""
                UPDATE iot_api_devicealarmlog
                SET "SMS_DATE"=%s, "SMS_TIME"=%s, "EMAIL_DATE"=%s, "EMAIL_TIME"=%s
                WHERE "id"=%s
            """, (now_ts.date(), now_ts.time(), now_ts.date(), now_ts.time(), alarm_id))
            conn.commit()
    except Exception as e:
        print("❌ check_and_notify error:", e)
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    print("🟢 Script started")
    check_and_notify()
    print("🟢 Script ended")
