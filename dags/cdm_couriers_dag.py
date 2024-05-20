from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor

from lib import DWHConnection, DDSDMUpdater

@task
def dds_updater_task(table_name: str,
                     query_path: str,
                     dwh_conn: DWHConnection) -> bool:
    dds_updater = DDSDMUpdater(table_name, query_path)
    is_updated = dds_updater.update(dwh_conn)
    return is_updated

@dag(
    dag_id='cdm_couriers',
    start_date=datetime(2024, 4, 1),
    catchup=True,
    schedule_interval='@monthly',
    is_paused_upon_creation=False,
    max_active_runs=1
)
def cdm_couriers_dag():
    
    dag_sensor = ExternalTaskSensor(
        task_id='dag_sensor',
        external_dag_id='stg_dds_cdm_delivery_restaurant_context',
        check_existence=True
    )

    af_dwh_conn_id = 'PG_WAREHOUSE_CONNECTION'
    query_file = '//lessons/dags/sql/cdm_couriers_month_report.sql'


    @task(task_id='cdm_monthly_courier_report')
    def cdm_couriers_report_upsert(af_dwh_conn_id: str,
                                    query_path: str,
                                    **context):
        ds = datetime.fromisoformat(context['ds'])
        print(ds)
        pg_upsert = PostgresOperator(
            task_id='cdm_couriers_month_report',
            postgres_conn_id=af_dwh_conn_id,
            sql=query_file,
            params={'year': ds.year, 'month': ds.month}
        )

    dag_sensor >> cdm_couriers_report_upsert(af_dwh_conn_id, query_file)

_ = cdm_couriers_dag()
    