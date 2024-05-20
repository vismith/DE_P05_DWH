from datetime import datetime, timedelta
import json
from typing import List

from psycopg import sql, connect

from .wf_settings import CourierWFSettings, DeliveryWFSettings
from .db_entities import Table


class DbAdapter:
    def __init__(self, uri:str, table: Table, data:List):
        self.uri = uri
        self.table = table
        self.data = data
        self.table_data = None
        self.create_copy_query()

    def set_table_data(self):
        raise NotImplementedError
    
    def _sql_columns(self) -> str:
        return sql.SQL(', ').join([sql.Identifier(c) for c in self.table.columns])

    def create_copy_query(self):
        self.copy_query = sql.SQL('copy {table}({columns}) from stdin').format(
            table=sql.Identifier(self.table.schema, self.table.name),
            columns= self._sql_columns()
            )
        
    def sql_copy(self):
        raise NotImplementedError
        

class DeliveriesDbAdapter(DbAdapter):
    def __init__(self, uri: str, table: Table, data: List):
        super().__init__(uri, table, data)

    def set_table_data(self):
        self.table_data = [(r['order_ts'], 
                            json.dumps(r, ensure_ascii=False)
                            ) for r in self.data]
    
    def sql_copy(self):
        self.set_table_data()
        print(f'data count: {len(self.table_data)}')
        with connect(self.uri) as cn:
            with cn.cursor() as cr:
                with cr.copy(self.copy_query) as cp:
                    for i, row in enumerate(self.table_data):
                        cp.write_row(row)
                
                self.set_settings()
                print(f'log: {self.settings}')
                
                dl_wf_st = DeliveryWFSettings(self.uri)
                dl_wf_st.wright_settings(cr, self.settings)
    
    def sql_day_copy(self):
        self.set_table_data()
        print(f'data count: {len(self.table_data)}')
        with connect(self.uri) as cn:
            with cn.cursor() as cr:
                with cr.copy(self.copy_query) as cp:
                    for i, row in enumerate(self.table_data):
                        cp.write_row(row)
                
                self.set_settings()
                print(f'log: {self.settings}')
                
                dl_wf_st = DeliveryWFSettings(self.uri)
                dl_wf_st.wright_settings(cr, self.settings)


    def set_settings(self):
        next_ts = (datetime.fromisoformat(self.data[-1]['order_ts']) + 
               timedelta(seconds=1)).isoformat(sep=' ', timespec='seconds')
        
        st =  {'last_order_ts': self.data[-1]['order_ts'],
               'next_load_ts': next_ts,
               }
        st_json = json.dumps(st)
        self.settings = {'entity_name': 'deliveries',
            'settings': st_json}

    
        
class CouriersDbAdapter(DbAdapter):
    def __init__(self, uri: str, table: Table, data: dict, offset: int = 0):
        super().__init__(uri, table, data)
        self.offset = offset

    def set_table_data(self):
        self.table_data = [(r['_id'],
                            json.dumps(r, ensure_ascii=False)
                            ) for r in self.data]
    def sql_copy(self):
        self.set_table_data()
        print(f'data count: {len(self.table_data)}')
        with connect(self.uri) as cn:
            with cn.cursor() as cr:
                with cr.copy(self.copy_query) as cp:
                    for row in self.table_data:
                        cp.write_row(row)
                self.set_settings()
                print(f'log: {self.settings}')

                crs_wf_st = CourierWFSettings(self.uri)
                crs_wf_st.wright_settings(cr, self.settings)


    def set_settings(self):
        next_offset = self.offset + len(self.data)
        st = {'offset': next_offset}
        st_json = json.dumps(st)
        self.settings = {'entity_name': 'couriers', 
            'settings': st_json}
        



