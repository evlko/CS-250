import pandas as pd
import pendulum
import json
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator

from utils.api import API

api = API()
hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")


def get_wp_descriptions():
    hook.run(
        """
        truncate stg.work_programs  restart identity cascade;
        """
    )
    data = pd.DataFrame()

    page = api.get(path="/record/academic_plan/academic_wp_description/all", params={"format": "json", "page": 1})
    pages_count = json.loads(page.text)["count"]
    for page in range(1, pages_count // 10):
        api_response = api.get(path="/record/academic_plan/academic_wp_description/all",
                               params={"format": "json", "page": page})
        page_content = json.loads(api_response.text)["results"]
        for row in page_content:
            df = pd.DataFrame([row], columns=row.keys())
            df["academic_plan_in_field_of_study"] = df[~df["academic_plan_in_field_of_study"].isna()][
                "academic_plan_in_field_of_study"].apply(lambda st_dict: json.dumps(st_dict))
            df["wp_in_academic_plan"] = df[~df["wp_in_academic_plan"].isna()][
                "wp_in_academic_plan"
            ].apply(lambda st_dict: json.dumps(st_dict))
            data = pd.concat([df, data])

    hook.insert_rows("stg.work_programs", data.values, target_fields=data.columns.tolist())


def get_practice():
    hook.run(
        """
        truncate stg.practice  restart identity cascade;
        """
    )
    data = pd.DataFrame()
    page = api.get(path='/practice/', params={'format': 'json', 'page': 1})
    pages_count = json.loads(page.text)["count"]
    for page in range(1, pages_count // 10):
        api_response = api.get(path='/practice/', params={'format': 'json', 'page': page})
        page_content = json.loads(api_response.text)["results"]
        for row in page_content:
            data = pd.concat([data, pd.DataFrame([row], columns=row.keys())])

    hook.insert_rows(
        "stg.practice", data.values, target_fields=data.columns.tolist()
    )


def get_structural_units():
    api_response = api.get(path="/record/structural/workprogram")
    res = list(json.loads(api_response.text))
    data = pd.DataFrame(res)
    data['work_programs'] = data['work_programs'].apply(json.dumps)

    data.to_sql("su_wp", hook.get_sqlalchemy_engine(), schema="stg", if_exists='replace', chunksize=1000, index=False)


with DAG(
        dag_id="get_data",
        start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
        schedule_interval="0 * * * *",
        catchup=False,
) as dag:
    t1 = PythonOperator(task_id="get_wp_descriptions", python_callable=get_wp_descriptions)
    t2 = PythonOperator(task_id="get_structural_units", python_callable=get_structural_units)

# t1 >> t2 >> t3

t1 >> t2
