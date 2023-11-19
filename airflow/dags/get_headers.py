import requests
import json
from airflow.models import Variable


def get_headers():
    url = "https://op.itmo.ru/auth/token/login"
    username = Variable.get("username")
    password = Variable.get("password")
    auth_data = {"username": username, "password": password}

    token_txt = requests.post(url, auth_data).text
    token = json.loads(token_txt)["auth_token"]
    headers = {"Content-Type": "application/json", "Authorization": "Token " + token}
    return headers
