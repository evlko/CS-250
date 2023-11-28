import pendulum
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator

hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")


def cdm_up():
    hook.run(
        """
        truncate cdm.up restart identity cascade;
        """
    )

    hook.run("""
        INSERT INTO cdm.up
        SELECT up.*, un.*, concat(e.isu_number, ' ', e.first_name, ' ', e.last_name) as editor_fullname
            FROM dds.up up
         LEFT JOIN dds.units un ON up.unit_id = un.id
         LEFT JOIN dds.up_editor ue on up.id = ue.up_id
         LEFT JOIN dds.editors e on ue.editor_id = e.id
    """)


def cdm_discipline():
    hook.run(
        """
        truncate cdm.discipline restart identity cascade;
        """
    )

    hook.run("""
        INSERT INTO cdm.discipline
        select d.*,
       u.unit_title,
       up.title    as up_title,
       up.qualification,
       up.on_check as up_on_check,
       up.year     as up_year
        from dds.discipline d
         left join dds.units u on d.unit_id = u.id
         left join dds.discipline_up du on d.id = du.wp_id
         full join dds.up up on du.up_id = up.id
         left join dds.units un on un.id = up.unit_id
    """)


with DAG(
        dag_id="dds_to_cdm",
        start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
        schedule_interval="0 1 * * *",
        catchup=False,
) as dag:
    t1 = PythonOperator(task_id="up", python_callable=cdm_up)
    t2 = PythonOperator(task_id="discipline", python_callable=cdm_discipline)

t1 >> t2
