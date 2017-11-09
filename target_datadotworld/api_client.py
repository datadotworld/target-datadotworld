import gzip
from time import sleep

import backoff
import requests
from requests import Session, Request
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from singer import metrics

from target_datadotworld import logger
from target_datadotworld.exceptions import convert_requests_exception
from target_datadotworld.utils import to_chunks, to_jsonlines

MAX_TRIES = 10

class ApiClient(object):
    def __init__(self, api_token, **kwargs):
        self._api_url = kwargs.get('api_url', 'https://api.data.world/v0')
        self._conn_timeout = kwargs.get('connect_timeout', 3.05)
        self._read_timeout = kwargs.get('read_timeout', 600)
        self._api_token = api_token

    def connection_check(self):
        with metrics.http_request_timer('user') as t:
            try:
                requests.get(
                    '{}/user'.format(self._api_url),
                    headers=self._request_headers(),
                    timeout=(self._conn_timeout, self._read_timeout)
                ).raise_for_status()
            except RequestException as e:
                raise convert_requests_exception(e)

    def append_stream(self, owner, dataset, stream, records, **kwargs):
        with metrics.http_request_timer('append') as t:
            t.tags['stream'] = stream
            request = Request(
                'POST',
                '{}/streams/{}/{}/{}'.format(
                    self._api_url, owner, dataset, stream),
                data=to_jsonlines(records).encode('utf-8'),
                headers=self._request_headers(
                    addl_headers={
                        'Content-Type': 'application/json-l'}),
            ).prepare()

            try:
                ApiClient._retry_if_throttled(
                    request, session=kwargs.get('session'),
                    timeout=(self._conn_timeout, self._read_timeout)
                ).raise_for_status()
            except RequestException as e:
                raise convert_requests_exception(e)

    async def append_stream_chunked(
            self, owner, dataset, stream, queue, chunk_size):

        with requests.Session() as s:
            s.mount(self._api_url, GzipAdapter())
            with metrics.Counter(
                    'batch_count', tags={'stream': stream}) as counter:
                async for chunk in to_chunks(queue, chunk_size):
                    logger.info('Uploading {} records in batch #{} '
                                'from {} stream '.format(
                        len(chunk), counter.value, stream))
                    self.append_stream(owner, dataset, stream,
                                       chunk, session=s)
                    counter.increment()

    def get_dataset(self, owner, dataset):
        with metrics.http_request_timer('dataset') as t:
            try:
                resp = requests.get(
                    '{}/datasets/{}/{}'.format(self._api_url, owner, dataset),
                    headers=self._request_headers(),
                    timeout=(self._conn_timeout, self._read_timeout)
                )
                resp.raise_for_status()
                return resp.json()
            except RequestException as e:
                raise convert_requests_exception(e)

    def create_dataset(self, owner, dataset, **kwargs):
        with metrics.http_request_timer('create_dataset') as t:
            try:
                resp = requests.put(
                    '{}/datasets/{}/{}'.format(self._api_url, owner, dataset),
                    json=kwargs,
                    headers=self._request_headers(),
                    timeout=(self._conn_timeout, self._read_timeout)
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
            request.body = gzip.compress(request.body)
            request.headers['Content-Length'] = len(request.body)

        return super(GzipAdapter, self).send(request, stream=stream, **kwargs)
