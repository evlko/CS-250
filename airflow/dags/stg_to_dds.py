import pendulum
import json
import pandas as pd
from ast import literal_eval
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from utils import scd

hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")


def editors():
    hook.run(
        """
    INSERT INTO dds.editors (id, username, first_name, last_name, email, isu_number)
    SELECT DISTINCT
        (json_array_elements((json_array_elements(academic_plan_in_field_of_study::json)->>'editors')::json)->>'id')::integer as id, 
        (json_array_elements((json_array_elements(academic_plan_in_field_of_study::json)->>'editors')::json)->>'username') as username,
        (json_array_elements((json_array_elements(academic_plan_in_field_of_study::json)->>'editors')::json)->>'first_name') as first_name,
        (json_array_elements((json_array_elements(academic_plan_in_field_of_study::json)->>'editors')::json)->>'last_name') as last_name,
        (json_array_elements((json_array_elements(academic_plan_in_field_of_study::json)->>'editors')::json)->>'email') as email,
        (json_array_elements((json_array_elements(academic_plan_in_field_of_study::json)->>'editors')::json)->>'isu_number') as isu_number
    FROM stg.up_detail up
    ON CONFLICT ON CONSTRAINT editors_uindex DO UPDATE 
    SET 
        username = EXCLUDED.username, 
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name, 
        email = EXCLUDED.email, 
        isu_number = EXCLUDED.isu_number;
    """
    )


def states():
    hook.run(
        """
    INSERT INTO dds.states (cop_state, state_name)
    with t as (select distinct (json_array_elements(wp_in_academic_plan::json)->>'status') as cop_states from stg.work_programs wp)
    select cop_states, 
        case when cop_states ='AC' then 'одобрено' 
                when cop_states ='AR' then 'архив'
                when cop_states ='EX' then 'на экспертизе'
                when cop_states ='RE' then 'на доработке'
                else 'в работе'
        end as state_name
    from t
    ON CONFLICT ON CONSTRAINT state_name_uindex DO UPDATE 
    SET 
        id = EXCLUDED.id, 
        cop_state = EXCLUDED.cop_state;
    """
    )


def units():
    hook.run(
        """
        truncate dds.units restart identity cascade;
        """
    )
    hook.run(
        """
        INSERT INTO dds.units (id, unit_title)
        SELECT 
            distinct sw.id, 
            sw.title
        FROM stg.su_wp sw 
        """
    )


def up():
    current_df = hook.get_pandas_df(sql="select * from dds.up")

    hook.run(
        """
        truncate dds.up restart identity cascade;
        """
    )

    new_df = hook.get_pandas_df(sql=
                                """
    select
        distinct (json_array_elements(academic_plan_in_field_of_study::json)->>'id')::integer as id,
        json_array_elements(academic_plan_in_field_of_study::json)->>'title' as title,
        json_array_elements(academic_plan_in_field_of_study::json)->>'qualification' as qualification,
        on_check,
        laboriousness,
        (((json_array_elements(academic_plan_in_field_of_study::json)->>'structural_unit')::json)->>'id')::integer as unit_id,
        (json_array_elements(academic_plan_in_field_of_study::json)->>'year')::integer as year
    from stg.up_detail up
    """
                                )

    new_df['unit_id'] = new_df['unit_id'].fillna(-1)
    new_df['unit_id'] = new_df['unit_id'].astype(float).astype(int)

    current_df['unit_id'] = current_df['unit_id'].fillna(-1)
    current_df['unit_id'] = current_df['unit_id'].astype(float).astype(int)

    current_df = current_df.scd.update(new_df)

    hook.insert_rows(
        "dds.up", current_df.values, target_fields=current_df.columns.tolist()
    )


def wp():
    def get_value(d, key):
        if type(d) != dict:
            return None
        if key not in d:
            return None
        return d[key]

    current_df = hook.get_pandas_df(sql="select * from dds.wp")

    hook.run(
        """
        truncate dds.wp restart identity cascade;
        """
    )

    df_su = hook.get_pandas_df(sql="select * from stg.su_wp")
    df_su = df_su[df_su['work_programs'] != '[]']
    df_su['work_programs'] = df_su['work_programs'].apply(lambda x: json.loads(x))
    df_su = df_su.explode(['work_programs'])

    df_su['unit_id'] = df_su['id']

    df_su['discipline_code'] = df_su['work_programs'].apply(lambda x: get_value(x, 'discipline_code'))

    df_wp = hook.get_pandas_df(sql="""
                select 
                    distinct json_array_elements(wp_in_academic_plan::json)->>'id' as wp_id,
                    json_array_elements(wp_in_academic_plan::json)->>'title' as wp_title,
                    json_array_elements(wp_in_academic_plan::json)->>'description' as wp_description,
                    json_array_elements(wp_in_academic_plan::json)->>'discipline_code' as discipline_code,
                    json_array_elements(wp_in_academic_plan::json)->>'status' as wp_status
                from stg.work_programs wp
    """)

    df_wp = df_wp.merge(df_su, on='discipline_code', how='left')

    df_states = hook.get_pandas_df(sql="select * from dds.states")

    df_wp = df_wp.merge(df_states, left_on='wp_status', right_on='cop_state', how='left')

    df_wp['wp_status'] = df_wp['state_name']
    df_wp['id'] = df_wp['wp_id']
    df_wp['unit_id'] = df_wp['unit_id'].fillna(-1)
    df_wp['unit_id'] = df_wp['unit_id'].astype(int)

    df_wp = df_wp[['id', 'discipline_code', 'wp_title', 'wp_status', 'unit_id', 'wp_description']]

    df_wp = df_wp.drop_duplicates(subset=['id'])

    current_df['unit_id'] = current_df['unit_id'].fillna(-1)
    current_df['unit_id'] = current_df['unit_id'].astype(float).astype(int)

    current_df = current_df.scd.update(df_wp)

    hook.insert_rows(
        "dds.wp", current_df.values, target_fields=current_df.columns.tolist()
    )


def wp_inter():
    hook.run(
        """
        truncate dds.up_editor restart identity cascade;
        """
    )
    hook.run(
        """
        truncate dds.discipline_up restart identity cascade;
        """
    )

    hook.run(
        """
        INSERT INTO dds.up_editor (up_id, editor_id)
        SELECT
            id::integer as up_id,
            (json_array_elements((json_array_elements(academic_plan_in_field_of_study::json)->>'editors')::json)->>'id')::integer as editor_id
        FROM stg.up_detail up
    """
    )

    hook.run(
        """
        INSERT INTO dds.discipline_up (wp_id, up_id)
        WITH t as (
        SELECT id,
            (json_array_elements(wp_in_academic_plan::json)->>'id')::integer as wp_id,
            json_array_elements(academic_plan_in_field_of_study::json)->>'id' as up_id
        FROM stg.work_programs wp)
        SELECT
            t.wp_id, (json_array_elements(wp.academic_plan_in_field_of_study::json)->>'id')::integer as up_id
        FROM t
        JOIN stg.work_programs wp
        ON t.id = wp.id
    """
    )





with DAG(
        dag_id="stg_to_dds",
        start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
        schedule_interval="0 4 * * *",
        catchup=False,
) as dag:
    # t1 = PythonOperator(task_id="editors", python_callable=editors)
    # t2 = PythonOperator(task_id="states", python_callable=states)
    # t3 = PythonOperator(task_id="units", python_callable=units)
    # t4 = PythonOperator(task_id="up", python_callable=up)
    # t5 = PythonOperator(task_id="wp", python_callable=wp)
    t6 = PythonOperator(task_id="wp_inter", python_callable=wp_inter)

# [t1, t2, t3] >> t4 >> t5 >> t6

t6

