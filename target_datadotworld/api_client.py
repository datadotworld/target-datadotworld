# target-datadotworld
# Copyright 2017 data.world, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the
# License.
#
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
# This product includes software developed at
# data.world, Inc.(http://data.world/).
import functools
import gzip
from concurrent.futures import ThreadPoolExecutor
from time import sleep

import backoff
import requests
from requests.adapters import HTTPAdapter, BaseAdapter
from requests.exceptions import RequestException
from singer import metrics

from target_datadotworld import logger
from target_datadotworld.exceptions import convert_requests_exception
from target_datadotworld.utils import to_chunks, to_jsonlines

MAX_TRIES = 10  # necessary to configure backoff decorator


class ApiClient(object):
    def __init__(self, api_token, **kwargs):
        """Simple client for data.world API

        :param api_token: API Authorization Token
        :type api_token: str
        """
        from target_datadotworld import __version__

        # The following properties can be overwritten for testing/tuning
        self._api_url = kwargs.get('api_url', 'https://api.data.world/v0')
        self._conn_timeout = kwargs.get('connect_timeout', 3.05)
        self._read_timeout = kwargs.get('read_timeout', 600)
        self._max_threads = kwargs.get('max_threads', 10)

        self._session = requests.Session()
        default_headers = {
            'Accept': 'application/json',
            'Authorization': 'Bearer {}'.format(api_token),
            'Content-Type': 'application/json',
            'User-Agent': 'target-datadotworld - {}'.format(__version__)
        }
        self._session.headers.update(default_headers)

        # TODO Fix and turn GzipAdapter back on (GH Issue #10)
        self._session.mount(self._api_url,
                            BackoffAdapter(HTTPAdapter()))

        # Create a limited thread pool.
        self._executor = ThreadPoolExecutor(
            max_workers=self._max_threads
        )

    def connection_check(self):
        """Verify network connectivity

        Ensures that the client can communicate with data.world's API
        """
        with metrics.http_request_timer('user'):
            try:
                self._session.get(
                    '{}/user'.format(self._api_url),
                    timeout=(self._conn_timeout, self._read_timeout)
                ).raise_for_status()
            except RequestException as e:
                raise convert_requests_exception(e)

    def append_stream(self, owner, dataset, stream, records):
        """Append records to a stream in a data.world dataset

        :param owner: User or organization ID of the owner of the dataset
        :type owner: str
        :param dataset: Dataset ID
        :type dataset: str
        :param stream: Stream ID
        :type stream: str
        :param records: Objects to be appended to the stream
        :type records: iterable

        :raises ApiError: Failure invoking data.world API
        """
        with metrics.http_request_timer('append') as t:
            t.tags['stream'] = stream

            try:
                self._session.post(
                    '{}/streams/{}/{}/{}'.format(
                        self._api_url, owner, dataset, stream),
                    data=to_jsonlines(records).encode('utf-8'),
                    headers={'Content-Type':
                             'application/json-l; charset=utf-8'}
                ).raise_for_status()
            except RequestException as e:
                raise convert_requests_exception(e)

    async def append_stream_chunked(
            self, owner, dataset, stream, queue, chunk_size, loop):
        """Asynchronously append records to a stream in a data.world dataset

        :param owner: User or organization ID of the owner of the dataset
        :type owner: str
        :param dataset: Dataset ID
        :type dataset: str
        :param stream: Stream ID
        :type stream: str
        :param queue: Queue with objects to be appended to the stream
        :type queue: asyncio.Queue
        :param chunk_size: Chunk or batch size
        :type chunk_size: int

        :raises ApiError: Failure invoking data.world API
        """

        with metrics.Counter(
                'batch_count', tags={'stream': stream}) as counter:

            delayed_exception = None
            # noinspection PyTypeChecker
            pending_task = None
            async for chunk in to_chunks(queue, chunk_size):
                if delayed_exception is None:
                    try:
                        logger.info('Uploading {} records in batch #{} '
                                    'from {} stream '.format(
                                        len(chunk), counter.value, stream))

                        if pending_task is not None:
                            # Force chunks to be appended sequentially
                            await pending_task

                        # Call API on separate thread
                        pending_task = loop.run_in_executor(
                            self._executor,
                            functools.partial(self.append_stream,
                                              owner, dataset, stream, chunk)
                        )
                        counter.increment()
                    except Exception as e:
                        delayed_exception = e
                else:
                    pass  # Must exhaust queue

            if pending_task is not None:
                await pending_task

            if delayed_exception is not None:
                raise delayed_exception

    def get_dataset(self, owner, dataset):
        """Fetch dataset info

        :param owner: User or organization ID of the owner of the dataset
        :type owner: str
        :param dataset: Dataset ID
        :type dataset: str

        :returns: Dataset object
        :rtype: object
        """
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
        """Create a new dataset

        :param owner: User or organization ID of the owner of the dataset
        :type owner: str
        :param dataset: Dataset ID
        :type dataset: str
        :param kwargs: Dataset properties
        :type kwargs: dict

        :returns: Response object
        :rtype: object

        .. seealso:: `Dataset properties
            <https://apidocs.data.world/v0/models/datasetcreaterequest>`_
        """
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


# TODO Re-enable test coverage (GH issue #10)
class GzipAdapter(BaseAdapter):  # pragma: no cover
    def __init__(self, delegate):
        """Requests adapter for compressing request bodies

        :param delegate: Adapter to delegate final request processing to
        :type delegate: requests.adapters.BaseAdapter
        """
        self._delegate = delegate
        super(GzipAdapter, self).__init__()

    def send(self, request, stream=False, **kwargs):
        if request.body is not None:
            if stream is True:
                request.body = gzip.GzipFile(filename=request.url,
                                             fileobj=request.body)
                #  TODO Confirm that requests will set Content-Length correctly
            else:
                request.body = gzip.compress(request.body)
                request.headers['Content-Length'] = len(request.body)

            request.headers['Content-Encoding'] = 'gzip'
        return self._delegate.send(request, stream=stream, **kwargs)

    def close(self):
        self._delegate.close()


class BackoffAdapter(BaseAdapter):
    def __init__(self, delegate):
        """Requests adapter for retrying throttled requests (HTTP 429)

        :param delegate: Adapter to delegate final request processing to
        :type delegate: requests.adapters.BaseAdapter
        """
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
