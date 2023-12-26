import pandas as pd
import pendulum
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator

from utils.api import API

api = API()
hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")


def get_disc_by_year():
    hook.run(
        """
        truncate stg.disc_by_year  restart identity cascade;
        """
    )

    ids = hook.get_records(
        """
        with t as (
        select 
        (json_array_elements(academic_plan_in_field_of_study::json)->>'ap_isu_id') :: integer as isu_id,
        id
        from stg.work_programs wp)
        select id from t
        where isu_id in
        (
        select id from stg.up_description ud 
        where ((training_period = '2') and (selection_year > '2020'))
           or ((training_period = '4') and (selection_year > '2018'))
        ) and
        id < 7256
        order by id
        """
    )

    api_path = 'record/academicplan/get_wp_by_year'

    data = pd.DataFrame()
    for up_id in ids:
        up_id = str(up_id[0])
        api_response = api.get(path=f"{api_path}/{up_id}", params={"year": "2022/2023"})
        df = pd.DataFrame(api_response.json())
        df = df.groupby(['id', 'title']).agg({'work_programs': lambda x: list(x)}).reset_index()
        data = pd.concat([data, df])

    if len(data) != 0:
        hook.insert_rows(
            "stg.disc_by_year", data.values, target_fields=data.columns.tolist()
        )


with DAG(
        dag_id="get_disc_by_year",
        start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
        schedule_interval="0 3 * * *",
        catchup=False,
) as dag:
    t1 = PythonOperator(task_id="get_disc_by_year", python_callable=get_disc_by_year)

t1
