import gzip
import json
from io import StringIO
from time import sleep

import backoff
import requests
from requests import Session, Request
from requests.exceptions import ConnectionError, HTTPError

import target_datadotworld.exceptions as dwex


class ApiClient(object):
    def __init__(self, api_token, **kwargs):
        self._api_url = kwargs.get('api_url', 'https://api.data.world/v0')
        self._api_token = api_token

    def connection_check(self):
        try:
            requests.get(
                '{}/user'.format(self._api_url),
                headers=self.__request_headers()).raise_for_status()
        except ConnectionError:
            raise dwex.ConnectionError(
                'Unable to access {}. '
                'Please check your network connection.'.format(
                    self._api_url))
        except HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                raise dwex.UnauthorizedError(
                    'Unauthorized to access {}. '
                    'Please check your API token.'.format(
                        self._api_url))
            raise

    def append_stream(self, owner, dataset, stream, records,
                      max_records_per_batch=None):

        if max_records_per_batch is None:
            max_records_per_batch = len(records)

        for batch in self.__compress_batches(records, max_records_per_batch):
            request = Request(
                '{}/streams/{}/{}/{}'.format(
                    self._api_url, owner, dataset, stream),
                data=batch,
                headers=self.__request_headers(
                    addl_headers={
                        'Content-Type': 'application/json-l',
                        'Content-Encoding': 'gzip'})
            ).prepare()

            self.__rate_limited_request(request).raise_for_status()

    def get_dataset(self, owner, dataset):
        resp = requests.get(
            '{}/datasets/{}/{}'.format(self._api_url, owner, dataset),
            headers=self.__request_headers()
        ).raise_for_status()
        return resp.json()

    def create_dataset(self, owner, dataset, title, visibility):
        requests.put(
            '{}/datasets/{}/{}'.format(self._api_url, owner, dataset),
            json={'title': title, 'visibility': visibility},
            headers=self.__request_headers()
        ).raise_for_status()

    def __request_headers(self, addl_headers=None):
        if addl_headers is None:
            addl_headers = {}

        headers = {
            'Authorization': 'Bearer {}'.format(self._api_token),
            'User-Agent': self.__user_agent(),
            'Content-Type': 'application/json'
        }

        for k in addl_headers:
            headers[k] = addl_headers[k]

        return headers

    @staticmethod
    def __compress_batches(records, max_records_per_batch):
        batch = None
        for i, r in enumerate(records, 1):
            if batch is None:
                batch = StringIO()
                compressed_batch = gzip.GzipFile(
                    fileobj=batch, mode='w')

            compressed_batch.write(json.dumps(r))
            compressed_batch.write('\n')

            if (i % max_records_per_batch) == 0 or i == len(records):
                compressed_batch.close()
                yield batch.getvalue()
                batch = None

    @staticmethod
    @backoff.on_predicate(backoff.expo, lambda r: r.status_code == 429,
                          max_tries=10)
    def __rate_limited_request(request):
        resp = Session().send(request).raise_for_status()
        if (resp.status_code == 429 and
                resp.headers.get('Retry-After')):
            sleep(int(resp.headers.get('Retry-After')))

        return resp

    @staticmethod
    def __user_agent():
        from target_datadotworld import __version__
        return 'target-datadotworld - {}'.format(__version__)
