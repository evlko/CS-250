import pandas as pd
import pendulum
import json
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator

from utils.api import API

api = API()
hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")


def get_up_detail():
    ids = hook.get_records(
        """
        select id as op_id
        from stg.work_programs wp
        where id > 7290
        order by 1
        """
    )

    api_path = "/academicplan/detail"

    target_fields = [
        "id",
        "ap_isu_id",
        "on_check",
        "laboriousness",
        "academic_plan_in_field_of_study",
    ]
    data = pd.DataFrame()

    for op_id in ids:
        op_id = str(op_id[0])
        api_response = api.get(path=f"{api_path}/{op_id}", params={"format": "json"})
        df = pd.DataFrame.from_dict(api_response.json(), orient="index")
        df = df.T
        df["academic_plan_in_field_of_study"] = df[
            ~df["academic_plan_in_field_of_study"].isna()
        ]["academic_plan_in_field_of_study"].apply(lambda st_dict: json.dumps(st_dict))
        df = df[target_fields]
        data = pd.concat([data, df])
    data = data[~data['ap_isu_id'].isna()]

    data.to_sql("up_detail", hook.get_sqlalchemy_engine(), schema="stg", if_exists='replace', chunksize=1000, index=False)


with DAG(
        dag_id="get_up_detail",
        start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
        schedule_interval="0 1 * * *",
        catchup=False,
) as dag:
    t1 = PythonOperator(task_id="get_up_detail", python_callable=get_up_detail)

t1
