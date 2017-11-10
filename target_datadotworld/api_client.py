import gzip
from time import sleep

import backoff
import requests
from requests.adapters import HTTPAdapter, BaseAdapter
from requests.exceptions import RequestException
from singer import metrics

from target_datadotworld import logger
from target_datadotworld.exceptions import convert_requests_exception
from target_datadotworld.utils import to_chunks, to_jsonlines

MAX_TRIES = 10


class ApiClient(object):
    def __init__(self, api_token, **kwargs):
        from target_datadotworld import __version__

        self._api_url = kwargs.get('api_url', 'https://api.data.world/v0')
        self._session = requests.Session()
        default_headers = {
            'Accept': 'application/json',
            'Authorization': 'Bearer {}'.format(api_token),
            'Content-Type': 'application/json',
            'User-Agent': 'target-datadotworld - {}'.format(__version__)
        }
        self._session.headers.update(default_headers)
        self._session.mount(self._api_url,
                            BackoffAdapter(GzipAdapter(HTTPAdapter())))

        self._conn_timeout = kwargs.get('connect_timeout', 3.05)
        self._read_timeout = kwargs.get('read_timeout', 600)

    def connection_check(self):
        with metrics.http_request_timer('user'):
            try:
                self._session.get(
                    '{}/user'.format(self._api_url),
                    timeout=(self._conn_timeout, self._read_timeout)
                ).raise_for_status()
            except RequestException as e:
                raise convert_requests_exception(e)

    def append_stream(self, owner, dataset, stream, records):
        with metrics.http_request_timer('append') as t:
            t.tags['stream'] = stream

            try:
                self._session.post(
                    '{}/streams/{}/{}/{}'.format(
                        self._api_url, owner, dataset, stream),
                    data=to_jsonlines(records).encode('utf-8'),
                    headers={'Content-Type': 'application/json-l'}
                ).raise_for_status()
            except RequestException as e:
                raise convert_requests_exception(e)

    async def append_stream_chunked(
            self, owner, dataset, stream, queue, chunk_size):

        with metrics.Counter(
                'batch_count', tags={'stream': stream}) as counter:

            delayed_exception = None
            # noinspection PyTypeChecker
            async for chunk in to_chunks(queue, chunk_size):
                if delayed_exception is None:
                    try:
                        logger.info('Uploading {} records in batch #{} '
                                    'from {} stream '.format(
                            len(chunk), counter.value, stream))
                        self.append_stream(owner, dataset, stream, chunk)
                        counter.increment()
                    except Exception as e:
                        delayed_exception = e
                else:
                    pass  # Must exhaust queue

            if delayed_exception is not None:
                raise delayed_exception

    def get_dataset(self, owner, dataset):
        with metrics.http_request_timer('dataset'):
            try:
                resp = self._session.get(
                    '{}/datasets/{}/{}'.format(self._api_url, owner, dataset),
                    timeout=(self._conn_timeout, self._read_timeout)
                )
                resp.raise_for_status()
                return resp.json()
            except RequestException as e:
                raise convert_requests_exception(e)

    def create_dataset(self, owner, dataset, **kwargs):
        with metrics.http_request_timer('create_dataset'):
            try:
                resp = self._session.put(
                    '{}/datasets/{}/{}'.format(self._api_url, owner, dataset),
                    json=kwargs,
                    timeout=(self._conn_timeout, self._read_timeout)
                )
                resp.raise_for_status()
                return resp.json()
            except RequestException as e:
                raise convert_requests_exception(e)


class GzipAdapter(BaseAdapter):
    def __init__(self, delegate):
        self._delegate = delegate
        super(GzipAdapter, self).__init__()

    def send(self, request, stream=False, **kwargs):
        if request.body is not None:
            if stream is True:
                request.body = gzip.GzipFile(filename=request.url,
                                             fileobj=request.body)
            else:
                request.body = gzip.compress(request.body)
                request.headers['Content-Length'] = len(request.body)

            request.headers['Content-Encoding'] = 'gzip'
        return self._delegate.send(request, stream=stream, **kwargs)

    def close(self):
        self._delegate.close()


class BackoffAdapter(BaseAdapter):
    def __init__(self, delegate):
        self._delegate = delegate
        super(BackoffAdapter, self).__init__()

    @backoff.on_predicate(backoff.expo,
                          predicate=lambda r: r.status_code == 429,
                          max_tries=lambda: MAX_TRIES)
    def send(self, request, **kwargs):
        resp = self._delegate.send(request, **kwargs)
        if (resp.status_code == 429 and
                resp.headers.get('Retry-After')):
            sleep(int(resp.headers.get('Retry-After')))

        return resp

    def close(self):
        self._delegate.close()
