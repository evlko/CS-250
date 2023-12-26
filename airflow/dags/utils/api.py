import requests
import json
from airflow.models import Variable


class API:
    URL = "https://op.itmo.ru"

    def __init__(self):
        username = Variable.get("username")
        password = Variable.get("password")
        auth_data = {"username": username, "password": password}

        token_txt = requests.post(url=f"{self.URL}/auth/token/login", data=auth_data).text
        self.token = json.loads(token_txt)["auth_token"]
        self.headers = {"Content-Type": "application/json", "Authorization": "Token " + self.token}

    def get(self, path, params=None):
        if params is None:
            params = {}
        return requests.get(url=f"{self.URL}/api{path}", params=params, headers=self.headers)
