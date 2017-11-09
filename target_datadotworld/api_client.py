import gzip
from io import BytesIO
from time import sleep

import backoff
import requests
from requests import Session, Request
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException

from target_datadotworld.exceptions import convert_requests_exception
from target_datadotworld.utils import to_jsonlines_chunks

MAX_TRIES = 10


class ApiClient(object):
    def __init__(self, api_token, **kwargs):
        self._api_url = kwargs.get('api_url', 'https://api.data.world/v0')
        self._api_token = api_token

    def connection_check(self):
        try:
            requests.get(
                '{}/user'.format(self._api_url),
                headers=self._request_headers()
            ).raise_for_status()
        except RequestException as e:
            raise convert_requests_exception(e)

    def append_stream(self, owner, dataset, stream, records,
                      batch_size=None):

        with requests.Session() as s:
            s.mount(self._api_url, GzipAdapter())

            for chunk in to_jsonlines_chunks(records, batch_size):
                request = Request(
                    'POST',
                    '{}/streams/{}/{}/{}'.format(
                        self._api_url, owner, dataset, stream),
                    data=chunk.encode('utf-8'),
                    headers=self._request_headers(
                        addl_headers={
                            'Content-Type': 'application/json-l'}),
                ).prepare()

                try:
                    ApiClient._retry_if_throttled(
                        request, session=s, timeout=(10, 10)
                    ).raise_for_status()
                except RequestException as e:
                    # TODO Decide what to raise when partially successful
                    raise convert_requests_exception(e)

    def get_dataset(self, owner, dataset):
        try:
            resp = requests.get(
                '{}/datasets/{}/{}'.format(self._api_url, owner, dataset),
                headers=self._request_headers()
            )
            resp.raise_for_status()
            return resp.json()
        except RequestException as e:
            raise convert_requests_exception(e)

    def create_dataset(self, owner, dataset, **kwargs):
        try:
            resp = requests.put(
                '{}/datasets/{}/{}'.format(self._api_url, owner, dataset),
                json=kwargs,
                headers=self._request_headers()
            )
            resp.raise_for_status()
            return resp.json()
        except RequestException as e:
            raise convert_requests_exception(e)

    def _request_headers(self, addl_headers=None):
        if addl_headers is None:
            addl_headers = {}

        from target_datadotworld import __version__
        headers = {
            'Accept': 'application/json',
            'Authorization': 'Bearer {}'.format(self._api_token),
            'Content-Type': 'application/json',
            'User-Agent': 'target-datadotworld - {}'.format(__version__)
        }

        for k in addl_headers:
            headers[k] = addl_headers[k]

        return headers

    @staticmethod
    @backoff.on_predicate(backoff.expo, lambda r: r.status_code == 429,
                          max_tries=lambda: MAX_TRIES)
    def _retry_if_throttled(request, session=None, **kwargs):
        s = session or Session()
        resp = s.send(request, **kwargs)
        if (resp.status_code == 429 and
                resp.headers.get('Retry-After')):
            sleep(int(resp.headers.get('Retry-After')))

        return resp


class GzipAdapter(HTTPAdapter):
    def add_headers(self, request, **kwargs):
        request.headers['Content-Encoding'] = 'gzip'

    def send(self, request, stream=False, **kwargs):
        if stream is True:
            request.body = gzip.GzipFile(filename=request.url,
                                         fileobj=request.body)
        else:
            data = BytesIO()
            compressed_batch = gzip.GzipFile(
                fileobj=data, mode='w')
            compressed_batch.write(request.body)
            compressed_batch.close()
            request.body = data.getvalue()

        return super(GzipAdapter, self).send(request, stream=stream, **kwargs)
