from dwh import CourierWFSettings, CouriersDbAdapter, Table
from api import CouriersApi

class CourierApiStgLoader:
    def __init__(self, uri) -> None:
        self.uri = uri
        self.wf_settings = CourierWFSettings(self.uri)
        self.stg_table = Table('stg', 'couriers', ['courier_id', 'object_value'])

    def load(self, limit=50):
        cr_offset = self.wf_settings.get_offset()
        print(cr_offset)
        cr_api = CouriersApi()
        crs = cr_api.get_all_offset(offset=cr_offset)
        
        if len(crs) == 0:
            print('There are no new records to add.')
        else:
            stg_copy = CouriersDbAdapter(self.uri, self.stg_table, crs, cr_offset)
            stg_copy.sql_copy()

