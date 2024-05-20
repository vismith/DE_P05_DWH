from api import DeliveriesApi
from dwh import DeliveriesDbAdapter, Table, DeliveryWFSettings

class DeliveryApiStgLoader:
    def __init__(self, uri) -> None:
        self.uri = uri
        self.wf_settings = DeliveryWFSettings(self.uri)
        self.stg_table = Table('stg', 'deliveries', ['order_ts', 'object_value'])

    def load(self, limit=50):
        next_load_ts = self.wf_settings.next_load_ts()
        dlv_api = DeliveriesApi()
        dlvs = dlv_api.get_by_time(next_load_ts, limit=limit)
        
        if len(dlvs):
            stg_copy = DeliveriesDbAdapter(self.uri, self.stg_table, dlvs)
            stg_copy.sql_copy()

class DeliveryApiStgDayLoader(DeliveryApiStgLoader):
    def __init__(self, uri) -> None:
        super().__init__(uri)

    def load(self, day: str) -> bool:
        dlv_api = DeliveriesApi()
        dlvs = dlv_api.get_day(day)
        if len(dlvs):
            stg_copy = DeliveriesDbAdapter(self.uri, self.stg_table, dlvs)
            stg_copy.sql_day_copy()
        return True
