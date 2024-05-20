from datetime import datetime, timedelta, date
from itertools import chain
import json
from typing import List
from urllib.parse import urlencode, quote, urljoin, urlparse
import requests


class DeliveryApiBase:
    _url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'
    # headers values was removed
    _headers = {
        'X-Nickname': '',
        'X-Cohort': '',
        'X-API-KEY': ''
        }
    _params = {
       'sort_direction': 'asc',
       'limit': 50
    }
    def _set_urlencode(self):
        params_encode = urlencode(self.params, safe=' :', quote_via=quote)
        self.urlencode = urlparse(self.base_url)._replace(
            query=params_encode).geturl()
    
    def _request(self):
        self._set_urlencode()
        api_response = requests.get(self.urlencode, headers=self._headers)
        api_response.raise_for_status()
        json_response = api_response.json()
        return json_response
    
    @staticmethod
    def dt_ft(dt) -> str:
        if isinstance(dt, datetime):
            return dt.isoformat(sep=' ', timespec='seconds')
        elif isinstance(dt, date):
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            raise ValueError(f'{dt}')

class DeliveriesApi(DeliveryApiBase):
    def __init__(self):
        self._params = {**DeliveryApiBase._params, 'sort_field': 'date'}
        self.base_url = urljoin(DeliveryApiBase._url, 'deliveries')

    

    def get_by_time(self, from_ts:str = '2020-01-01 00:00:00', limit: int = 50):
        self.from_dt = datetime.fromisoformat(from_ts)
        self.from_ts = self.from_dt.isoformat(sep=' ', timespec='seconds')
        
        params = {'from': self.from_ts}
        self.params = {**self._params, 'from': self.from_ts}
        self.params['limit'] = limit
    
        return self._request()
    


    def get_from_to(self, from_dt: str = '2022-01-01', to_dt: str = '2022-01-02'):
        self.from_dt = datetime.fromisoformat(from_dt)
        self.from_ts = DeliveriesApi.dt_ft(self.from_dt)
        self.to_dt = datetime.fromisoformat(to_dt)
        self.to_ts = DeliveriesApi.dt_ft(self.to_dt)
        self.params = {**self._params, 'from': self.from_ts, 'to': self.to_ts}
        jrsp = self._request()
        return jrsp

    def get_day(self, day: str):
        self.from_dt = datetime.fromisoformat(day).date()
        self.from_ts = DeliveriesApi.dt_ft(self.from_dt) 
        self.to_dt = self.from_dt + timedelta(days=1)
        self.to_ts = DeliveriesApi.dt_ft(self.to_dt) 
        day_deliveries = []
        while True:
            print(f'{self.from_ts} - {self.to_ts}')
            dl = self.get_from_to(self.from_ts, self.to_ts)
            if not len(dl): break
            self.from_ts = DeliveriesApi.dt_ft(
                    datetime.fromisoformat(dl[-1]['order_ts']) + timedelta(seconds=1))
            day_deliveries = chain(day_deliveries, dl) 
        return list(day_deliveries)         

    def get_by_offset(self, offset: int = 0, limit: int = 50):
        params = {'offset': offset}
        self.params = {**self._params, 'offset': offset}
        self.params['limit'] = limit
        
        return self._request()
    
class CouriersApi(DeliveryApiBase):
    def __init__(self) -> None:
        super().__init__()
        self.params = {**DeliveryApiBase._params,
            'sort_field': '_id', 'offset': 0}
        self.base_url = urljoin(DeliveryApiBase._url, 'couriers')

    def get(self, offset: int = 0, limit: int = 50):
        self.params['offset'] = offset
        self.params['limit'] = limit
        jr = self._request()
        return jr
    
    def get_all_offset(self, offset):
        offset = offset
        limit = 20
        crs = []
        while True:
            print(f'offset: {offset}')
            crs_lmt = self.get(offset, limit)
            print(f'(all) len: {len(crs_lmt)}')
            if len(crs_lmt) == 0: break
            offset += limit
            crs.extend(crs_lmt)
        return crs


