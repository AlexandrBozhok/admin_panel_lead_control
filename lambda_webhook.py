import os
import json
import logging
import requests
import pymysql
import pytz
import datetime as dt

tz = pytz.timezone('Europe/Kiev')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

host = "https://example.com"
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_name = os.getenv("DB_NAME")
db_password = os.getenv("DB_PASSWORD")

reserv_db_host = os.getenv("RESERV_DB_HOST")
reserv_db_user = os.getenv("RESERV_DB_USER")
reserv_db_name = os.getenv("RESERV_DB_NAME")
reserv_db_password = os.getenv("RESERV_DB_PASSWORD")


def lambda_handler(event, context):
    source = event.get("rawPath").lstrip("/").split("/")[0]
    method = event.get("requestContext").get("http").get("method")
    if source == 'facebook' and method == 'GET':
        token_sent = event.get("queryStringParameters").get("hub.verify_token")
        return verify_fb_token(token_sent, event)
    else:
        if check_host_status(host):
            requests.post(f'{host}/{source}/get_leads', json=json.dumps(event))
            logger.info(f'{host}/{source}/get_leads')
        else:
            status = save_delayed_lead(source, json.dumps(event))
            return status


def save_delayed_lead(source, body):
    try:
        conn = pymysql.connect(host=db_host, user=db_user, password=db_password,
                               db=db_name, connect_timeout=5)
        if conn.open:
            with conn.cursor() as cur:
                cur.execute("""INSERT delayed_leads(date, source, body) VALUES (%s, %s, %s)""", (get_current_datetime(), source, body))
                conn.commit()
                return True
    except Exception:
        conn = pymysql.connect(host=reserv_db_host, user=reserv_db_user,
                               password=reserv_db_password, db=reserv_db_name, connect_timeout=5)
        if conn.open:
            with conn.cursor() as cur:
                cur.execute("""INSERT delayed_leads(date, source, body) VALUES (%s, %s, %s)""", (get_current_datetime(), source, body))
                conn.commit()
                return True
    return False


def check_host_status(host):
    try:
        response = requests.get(url=host, timeout=3)
        if response.status_code == 200:
            return True
    except requests.exceptions.ConnectionError:
        return False
    return False


def verify_fb_token(token_sent, event):
    if token_sent == os.getenv("FB_VERIFY_TOKEN"):
        return event.get("queryStringParameters").get("hub.challenge")
    return 'Invalid verification token'


def get_current_datetime():
    datetime_str = dt.datetime.now().astimezone(tz).strftime('%d-%m-%Y, %H:%M:%S')
    date = dt.datetime.strptime(datetime_str, '%d-%m-%Y, %H:%M:%S')
    return date
