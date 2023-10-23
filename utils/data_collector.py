import json
import os
from typing import Union
import math
import threading
import pandas as pd
import requests

from .commiter import Commiter
from .utils import load_env


from .json_dict_converter import JsonDictConverter


class OPITMODataCollector:
    def __init__(self, envpath=".env"):
        load_env(envpath)
        self._api_url = "https://op.itmo.ru/api"
        self.__login()
        self.converter = JsonDictConverter()
        self.commiter = Commiter(os.environ["GIT_TOKEN"], "CS-250")

    def __login(self) -> None:
        url = "https://op.itmo.ru/auth/token/login"
        auth_data = {
            "username": os.environ["API_USER"],
            "password": os.environ["API_PASSWORD"],
        }
        token_txt = requests.post(url, auth_data, timeout=1000).text
        self.__token = json.loads(token_txt)["auth_token"]
        self._headers = {
            "Content-Type": "application/json",
            "Authorization": "Token " + self.__token,
        }

    def get_academic_plans_ids(self) -> set:
        url = f"{self._api_url}/record/academic_plan/academic_wp_description/all"
        response = requests.get(url, headers=self._headers, timeout=1000)

        if response.status_code != 200:
            raise ValueError(response.text)

        response_json = response.json()
        pages_count = math.ceil(response_json["count"] / len(response_json["results"]))

        print(f"Found {pages_count} pages")
        ids = set()

        def get_page(page: int):
            nonlocal url
            session = requests.Session()
            response = session.get(
                url, headers=self._headers, params={"page": page}, timeout=1000
            )
            response_json = response.json()
            for row in response_json["results"]:
                ids.add(row["id"])
            print("|", end="")

        threads_before = threading.active_count()
        print("Collecting pages:", end=" ")

        for page in range(1, pages_count + 1):
            threading.Thread(target=get_page, args=(page,)).start()

        while True:
            if threading.active_count() == threads_before:
                break

        return ids

    def get_academic_plan_details(self, ids: list) -> dict:
        url = f"{self._api_url}/academicplan/detail/"
        result = {}

        def get_plan(p_id: int):
            nonlocal url
            session = requests.Session()
            response = session.get(url + str(p_id), headers=self._headers)
            result[p_id] = response.json()
            print("|", end="")

        print("Collecting plans:", end=" ")

        for start in range(0, len(ids), 50):
            threads_before = threading.active_count()
            print("\n               ", end=" ")
            for p_id in ids[start : min(start + 50, len(ids))]:
                threading.Thread(target=get_plan, args=(p_id,)).start()
            while True:
                if threading.active_count() == threads_before:
                    break
        return result

    def get_data(self, data_name=None, file_type="csv", api_function=None, **kwargs):
        if data_name is None:
            return api_function(**kwargs)
        return (
            pd.read_csv(f"{data_name}.csv")
            if file_type == "csv"
            else self.converter.from_json(data_name)
        )

    def save_data(self, data: Union[pd.Series, pd.DataFrame, dict], name: str):
        if isinstance(data, dict):
            self.converter.to_json(dictionary=data, json_name=name)
            self.commiter.commit_json(name)
        else:
            self.commiter.commit_pandas(data, name)
