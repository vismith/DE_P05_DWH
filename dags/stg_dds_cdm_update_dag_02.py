from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task, task_group
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import chain


from workers import DeliveryApiStgDayLoader, CourierApiStgLoader
from lib import DWHConnection, DDSDMUpdater

@task
def dds_updater_task(table_name: str,
                     query_path: str,
                     dwh_conn: DWHConnection) -> bool:
    dds_updater = DDSDMUpdater(table_name, query_path)
    is_updated = dds_updater.update(dwh_conn)
    return is_updated


@dag(
    dag_id='stg_dds_cdm_delivery_restaurant_context',
    start_date=datetime(2024, 4, 1) - timedelta(days=7),
    catchup=True,
    schedule_interval='@daily',
    is_paused_upon_creation=False,
    max_active_runs=1
)
def dds_cdm_updater():
   
    af_dwh_conn_id = 'PG_WAREHOUSE_CONNECTION'
    
    @task_group(group_id='delivery_api_loaders')
    def delivery_api_loaders(dwh_af_conn_name):        
        dwh_uri = BaseHook.get_connection(dwh_af_conn_name).get_uri()
        
        @task(task_id='deliveries_copy_stg')
        def delivery_load(**context):
            ds = context['ds']
            DeliveryApiStgDayLoader(dwh_uri).load(ds)

        @task(task_id='couriers_copy_stg')
        def courier_load():
            CourierApiStgLoader(dwh_uri).load()

        [delivery_load(), courier_load()]
    
    sql_path = '//lessons/dags/sql/'
    
    dds_layers = {
        'first_layer': {'dm_users': 'dds_dm_users_insert.sql',
                        'dm_restaurants': 'dds_dm_restaurants_insert.sql',
                        'dm_timestamps': 'dds_dm_timestamps_insert.sql',
                        'dm_couriers': 'dds_dm_couriers.sql'},
        'second_layer': {'dm_products': 'dds_dm_products_insert.sql',
                        'dm_orders': 'dds_dm_orders_insert.sql'},
        'third_layer': {'fct_product_sales': 'dds_fct_product_sales_insert.sql',
                        'fct_deliveries': 'dds_fct_deliveries_insert.sql'}
        }
    
    task_groups = []
    for layer, entities  in dds_layers.items():
        @task_group(group_id=f'{layer}')
        def dds_layer_updaters(af_dwh_conn_id=af_dwh_conn_id,
                                    sql_dir=sql_path,
                                    dds_entities=entities) -> None:
            dwh_conn = DWHConnection(af_dwh_conn_id)
    
            for table, file_name in dds_entities.items():
                file_path = Path(sql_dir).resolve().joinpath(file_name).as_posix()
                is_updated = dds_updater_task.override(
                    task_id=f'{table}_updater'
                    )(table, file_path, dwh_conn)
                print(f'is updated: {is_updated}')
        task_groups.append(dds_layer_updaters())
    
    cdm_entities = {'dm_settlement_report': 'cdm_restaurant_report_insert.sql'}

    @task_group(group_id='cdm_update')
    def cdm_updater(af_dwh_conn_id=af_dwh_conn_id,
                                    sql_dir=sql_path,
                                    cdm_entities=cdm_entities) -> None:
            dwh_conn = DWHConnection(af_dwh_conn_id)
            
            for table, file_name in cdm_entities.items():
                file_path = Path(sql_dir).resolve().joinpath(file_name).as_posix()
                is_updated = dds_updater_task.override(
                    task_id=f'{table}_updater'
                    )(table, file_path, dwh_conn)
                print(f'is updated: {is_updated}')

    chain(delivery_api_loaders(af_dwh_conn_id), *task_groups, cdm_updater())

dds_update_dag = dds_cdm_updater()


    