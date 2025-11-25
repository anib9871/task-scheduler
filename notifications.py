import mysql.connector
from datetime import datetime, time, timedelta
import time as t  # <- renamed to avoid conflict
import requests
import smtplib
from email.mime.text import MIMEText
import pytz  # <- For timezone handling
import os
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException

devid_for_sms = None
phone_numbers = ""
email_ids = ""
device_name =""
dev_reading =""
# ================== DATABASE CONFIG ==================#
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

# # ================== EMAIL CONFIG ==================
# SMTP_SERVER = "smtp.gmail.com"
# SMTP_PORT = 587
# EMAIL_USER = "testwebservice71@gmail.com"
# EMAIL_PASS = "akuu vulg ejlg ysbt"

# ================== TIMEZONE CONFIG ==================
TZ = pytz.timezone("Asia/Singapore")  # Singapore timezone


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


# def send_email(subject, message, email_ids): ws working with gmail but not working in railway
#     if not email_ids:
#         print("❌ No email recipients. Skipping.")
#         return

#     print("🔹 Sending Email...")
#     try:
#         msg = MIMEText(message)
#         msg["Subject"] = subject
#         msg["From"] = EMAIL_USER
#         msg["To"] = ", ".join(email_ids)

#         server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
#         server.starttls()
#         server.login(EMAIL_USER, EMAIL_PASS)
#         server.sendmail(EMAIL_USER, email_ids, msg.as_string())
#         server.quit()

#         print("✅ Email sent successfully!")
#     except Exception as e:
#         print("❌ Email failed:", e)
#BREVO_API_KEY = "cGbUTq4L6mVM9nsZ"

def send_email_brevo(to_email, subject, html_content):
    print("📧 Sending Email via Brevo...")

    BREVO_API_KEY = os.getenv("BREVO_API_KEY")   # <--- NO HARD CODE
    if not BREVO_API_KEY:
        print("❌ ERROR: BREVO_API_KEY not found in environment variables!")
        return
    try:
        configuration = sib_api_v3_sdk.Configuration()
        configuration.api_key['api-key'] = BREVO_API_KEY

        api_instance = sib_api_v3_sdk.TransactionalEmailsApi(
            sib_api_v3_sdk.ApiClient(configuration)
        )

        email = sib_api_v3_sdk.SendSmtpEmail(
            to=[{"email": to_email}],
            sender={"email": "fertisenseiot@gmail.com", "name": "Fertisense"},
            subject=subject,
            html_content=html_content
        )

        response = api_instance.send_transac_email(email)
        print("✔ Email sent:", response)

    except ApiException as e:
        print("❌ Email failed:", e)


def get_contact_info(device_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT ORGANIZATION_ID, CENTRE_ID
            FROM iot_api_masterdevice
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

        format_strings = ",".join(["%s"] * len(user_ids))
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
        if "cursor" in locals():
            cursor.close()
        if "conn" in locals() and conn.is_connected():
            conn.close()


second_notification_sent = {}


def safe_time(t_value):
    if isinstance(t_value, time):
        return t_value
    try:
        return (datetime.min + t_value).time()
    except:
        return time(0, 0, 0)


def check_and_notify():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # 🔥 UPDATED TABLE NAME
        cursor.execute("""
            SELECT ID, DEVICE_ID, PARAMETER_ID, ALARM_DATE, ALARM_TIME, SMS_DATE, SMS_TIME, EMAIL_DATE, READING
            FROM devicealarmlog
            WHERE IS_ACTIVE = 1
        """)
        alarms = cursor.fetchall()

        if not alarms:
            print("✅ No alarms found.")
            return

        now = datetime.now(TZ)

        for alarm in alarms:
            alarm_id = alarm["ID"]
            devid = alarm["DEVICE_ID"]
            alarm_date = alarm["ALARM_DATE"]
            alarm_time = safe_time(alarm["ALARM_TIME"])
            dev_reading = alarm["READING"]    
            raised_time = TZ.localize(datetime.combine(alarm_date, alarm_time))
            diff_seconds = (now - raised_time).total_seconds()

            first_sms_done = alarm["SMS_DATE"] is not None
            second_sms_done = second_notification_sent.get(alarm_id, False)

            # ================== FIRST NOTIFICATION ==================
            if not first_sms_done and diff_seconds >= 120 and diff_seconds < 600:

                cursor.execute("SELECT device_name FROM iot_api_masterdevice WHERE device_id=%s", (devid,))
                row = cursor.fetchone()
                devnm = row["device_name"] if row else f"Device-{devid}"
                device_name = devnm
                cursor.execute("""
                    SELECT 
                        MP.UPPER_THRESHOLD,
                        MP.LOWER_THRESHOLD,
                        DRL.READING AS CURRENT_READING
                    FROM iot_api_masterdevice MD
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

                currreading = reading_row["CURRENT_READING"]

                if currreading is None:
                    print(f"⚠️ Skipping device {devnm} as current reading is NULL.")
                    continue

                upth = reading_row["UPPER_THRESHOLD"]
                lowth = reading_row["LOWER_THRESHOLD"]

                print(f"Device {devnm}: Lower={lowth}, Upper={upth}, Current={currreading}")

                if currreading < lowth:
                    ntf_typ = 1
                elif currreading > upth:
                    ntf_typ = 2
                else:
                    ntf_typ = 7

                message = build_message(ntf_typ, devnm)
                phones, emails = get_contact_info(devid)

                for phone in phones:
                    send_sms(phone, message)


                # for em in emails:
                #     #email_subject = "IoT Alarm Notification for " & device_name & ". The current reading is  " & dev_reading
                #     email_subject = "IoT Alarm Notification for " + device_name + ". The current reading is " + str(dev_reading)

                #     send_email_brevo(em, email_subject, message)
                for em in emails:
                    email_subject = f"IoT Alarm Notification for {device_name} | Current reading: {dev_reading}"
                    email_body = f"""
                    <h2>⚠ IoT Alert Triggered</h2>
                    <p><b>Device:</b> {device_name}</p>
                    <p><b>Current Reading:</b> {dev_reading}</p>
                    <p><b>Limits:</b> {lowth} - {upth}</p>
                    <p>Please check the device immediately.</p>
                    <p></p>
                    <p></p>
                    <p>Regards</p>
                    <p>Team Fertisense.</p>
                    """

                    send_email_brevo(em, email_subject, email_body)


                now_ts = datetime.now(TZ)

                # 🔥 UPDATED TABLE NAME
                cursor.execute("""
                    UPDATE devicealarmlog
                    SET SMS_DATE=%s, SMS_TIME=%s, EMAIL_DATE=%s, EMAIL_TIME=%s
                    WHERE ID=%s
                """, (now_ts.date(), now_ts.time(), now_ts.date(), now_ts.time(), alarm_id))

                conn.commit()
                print(f"✅ First notification sent for alarm {alarm_id}")

# ================== SECOND NOTIFICATION ==================
            elif first_sms_done and alarm["EMAIL_DATE"] is None:

                first_sms_dt = datetime.combine(
                    alarm["SMS_DATE"], safe_time(alarm["SMS_TIME"])
                )
                first_sms_dt = TZ.localize(first_sms_dt)
                diff_hours = (now - first_sms_dt).total_seconds() / 3600

                if diff_hours >= 6:

                    cursor.execute(
                        "SELECT device_name FROM iot_api_masterdevice WHERE device_id=%s",
                        (devid,),
                    )
                    row = cursor.fetchone()
                    devnm = row["device_name"] if row else f"Device-{devid}"
                    device_name = devnm
                    cursor.execute(
                        """
                        SELECT 
                            MP.UPPER_THRESHOLD,
                            MP.LOWER_THRESHOLD,
                            DRL.READING AS CURRENT_READING
                        FROM iot_api_masterdevice MD
                        LEFT JOIN iot_api_devicesensorlink DSL ON DSL.DEVICE_ID = MD.DEVICE_ID
                        LEFT JOIN iot_api_sensorparameterlink SPL ON SPL.SENSOR_ID = DSL.SENSOR_ID
                        LEFT JOIN iot_api_masterparameter MP ON MP.PARAMETER_ID = SPL.PARAMETER_ID
                        LEFT JOIN device_reading_log DRL ON DRL.DEVICE_ID = MD.DEVICE_ID
                        WHERE MD.DEVICE_ID = %s
                        ORDER BY DRL.READING_DATE DESC, DRL.READING_TIME DESC
                        LIMIT 1
                        """,
                        (devid,),
                    )

                    reading_row = cursor.fetchone()
                    if not reading_row:
                        continue

                    currreading = reading_row["CURRENT_READING"]
                    if currreading is None:
                        print(f"⚠️ Skipping device {devnm} as current reading is NULL.")
                        continue

                    upth = reading_row["UPPER_THRESHOLD"]
                    lowth = reading_row["LOWER_THRESHOLD"]

                    if currreading < lowth:
                        ntf_typ = 1
                    elif currreading > upth:
                        ntf_typ = 2
                    else:
                        ntf_typ = 7

                    message = build_message(ntf_typ, devnm)
                    phones, emails = get_contact_info(devid)

                    for phone in phones:
                        send_sms(phone, message)
                    for em in emails:
                        email_subject = f"IoT Alarm Notification for {device_name} | Current reading: {dev_reading}"
                        email_body = f"""
                        <h2>⚠ IoT Alert Triggered</h2>
                        <p><b>Device:</b> {device_name}</p>
                        <p><b>Current Reading:</b> {dev_reading}</p>
                        <p><b>Limits:</b> {lowth} - {upth}</p>
                        <p>Please check the device immediately.</p>
                        <p></p>
                        <p></p>
                        <p>Regards</p>
                        <p>Team Fertisense.</p>
                        """

                    send_email_brevo(em, email_subject, email_body)

                    # Mark second notification done (NO NEW COLUMN USED)
                    now_ts = datetime.now(TZ)
                    cursor.execute(
                        """
                        UPDATE devicealarmlog
                        SET EMAIL_DATE=%s
                        WHERE ID=%s
                        """,
                        (now_ts.time(), alarm_id),
                    )

                    conn.commit()
                    print(f"✅ Second notification sent for alarm {alarm_id}")

        cursor.close()
        conn.close()

    except Exception as e:
        print("❌ Error in check_and_notify:", e)


if __name__ == "__main__":
    print("🚀 Starting notification check...")
    check_and_notify()
    print("✅ Notification check complete. Exiting now.")


