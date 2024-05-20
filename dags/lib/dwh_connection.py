from airflow.hooks.base import BaseHook
from lib import PGConnection

class DWHConnection(PGConnection):
    def __init__(self, af_conn_id: str):
        self.af_conn_id = af_conn_id
        super().__init__(BaseHook.get_connection(af_conn_id).get_uri())
    