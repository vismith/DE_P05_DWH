from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from psycopg import sql

from lib import PGConnection



@dataclass
class DDSFWUpdateTS:
    entity_name: str
    update_ts: datetime = datetime(1900, 1, 1)

class DDSWFSettings:
    def __init__(self, update_ts_inst: Optional[DDSFWUpdateTS] = None) -> None:
        self._update_ts_inst = update_ts_inst
        self.key = self._update_ts_inst.entity_name
    
    def save_update_ts(self, conn: PGConnection) -> datetime:
        self.update_query = sql.SQL(
            '''
            insert into dds.wf_settings (entity_name, update_ts)
            values ({entity_name}, {update_ts})
            on conflict (entity_name) do update set
            update_ts = excluded.update_ts
            returning update_ts
            '''
            ).format(entity_name=self._update_ts_inst.entity_name,
                     update_ts=self._update_ts_inst.update_ts)
        with conn.connection() as cn:
            with cn.cursor() as cr:
                print(f'insert query: f{self.update_query.as_string(cn)}')
                cr.execute(self.update_query)
                returning = cr.fetchone()
                print(f'returning result: {returning}')
        return returning[0]
    
    def get_update_ts(self, conn:PGConnection) -> Optional[DDSFWUpdateTS]:
        self.select_setting = sql.SQL(
            '''
            select entity_name, update_ts from dds.wf_settings
            where entity_name = {entity_name}
            '''
            ).format(entity_name=self._update_ts_inst.entity_name)
          
        with conn.connection() as cn:
            with cn.cursor() as cr:
                cr.execute(self.select_setting)
                result = cr.fetchone()
                print(f'select result: {result}')
        return DDSFWUpdateTS(*result) if result is not None else None
        

    

