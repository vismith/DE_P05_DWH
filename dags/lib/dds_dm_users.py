from pathlib import Path
from psycopg import sql
from lib import DDSFWUpdateTS, PGConnection


class DDSDMUpdater:
    def __init__(self, table_name: str, query_path: str) -> None:
        self.query = query_path
        self.update_ts = DDSFWUpdateTS(table_name)

    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, query_file: str) -> None:
        query_path = Path(query_file)
        try:
            with open(query_path) as f:
                self._query = f.read()
        except FileExistsError as e:
            print(f'The file not found: {query_path}')


    def update(self, conn: PGConnection) -> bool:
        sql_insert = sql.SQL(self._query) #.format(update_ts=self.update_ts)
        with conn.connection() as cn:
            print(f'query: {sql_insert.as_string(cn)}')
            with cn.cursor() as cr:
                cr.execute(sql_insert)
                wf_update = cr.fetchone()
        print(f'query result: {wf_update}')
        if wf_update is not None:
            self.update_ts.update_ts = wf_update[1]
        return bool(wf_update)
        

