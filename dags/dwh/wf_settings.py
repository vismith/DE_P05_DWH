from datetime import datetime

from psycopg import sql, Cursor, connect

from .db_entities import Table

class WFSettings:
    def __init__(self, uri: str = None, table: Table = None) -> None:
        if table:
            self.table = table
        else:
            self.table = Table('stg', 'wf_settings', ['entity_name', 'settings'])
        self._uri = uri
        self.read_cnd = None

        
    @staticmethod
    def list_columns_convert(columns):
        return sql.SQL(', ').join(
            [sql.Identifier(column) for column in columns]
            )
    
    @staticmethod
    def dict_values_convert(columns, data):
        return sql.SQL(', ').join(
            (data[column] for column in columns)
            )

    
    def read_settings(self):
        
        select_query = sql.SQL(
            '''
            select {columns}
            from {table}
            where {cnd_column} = {cnd_value}
            ''').format(
                columns=WFSettings.list_columns_convert(self.table.columns),
                table=sql.Identifier(self.table.schema, self.table.name),
                cnd_column=sql.Identifier(self.read_cnd['column']),
                cnd_value=self.read_cnd['value'])
        with connect(self._uri) as cn:
            with cn.cursor() as cr:
                cr.execute(select_query)
                rtn = cr.fetchone()
        return rtn
    
    def wright_settings(self, cr: Cursor, settings):
        
        insert_query = sql.SQL(
            '''
            insert into {table} ({columns})
            values({values})
            on conflict (entity_name) do update set
            settings = excluded.settings
            '''
            ).format(table=sql.Identifier(self.table.schema, self.table.name),
                    columns=WFSettings.list_columns_convert(self.table.columns),
                    values=WFSettings.dict_values_convert(self.table.columns, settings)
                    )
        
        cr.execute(insert_query)
    
class DeliveryWFSettings(WFSettings):
    def __init__(self, uri) -> None:
        super().__init__(uri=uri)
        self.read_cnd = {'column': 'entity_name', 'value': 'deliveries'}
        self._coursor = None
    
    def next_load_ts(self):
        result = super().read_settings()
        if result is None:
            load_ts = datetime(2022, 1, 1).isoformat(sep=' ', timespec='seconds')
        else:
            load_ts = result[1]['next_load_ts'] 
        return load_ts
    
    
    
class CourierWFSettings(WFSettings):
    def __init__(self, uri) -> None:
        super().__init__(uri=uri)
        self.read_cnd = {'column': 'entity_name', 'value': 'couriers'}

    def get_offset(self):
        result =  super().read_settings()
        print(f'wf settings: {result}')
        if result is None:
            offset = 0
        else:
            offset = result[1]['offset']
        return offset

    
    
    