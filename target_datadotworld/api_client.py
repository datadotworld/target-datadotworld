import gzip
from io import BytesIO
from time import sleep

import backoff
import requests
from requests import Session, Request
from requests.exceptions import RequestException, HTTPError

import target_datadotworld.exceptions as dwex
from target_datadotworld.utils import to_json_lines


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
            raise ApiClient._wrap_request_exception(e)

    def append_stream(self, owner, dataset, stream, records,
                      max_records_per_batch=None):

        if max_records_per_batch is None:
            max_records_per_batch = len(records)

        for batch in ApiClient._split_records_into_compressed_batches(
                records, max_records_per_batch):
            request = Request(
                'POST',
                '{}/streams/{}/{}/{}'.format(
                    self._api_url, owner, dataset, stream),
                data=batch,
                headers=self._request_headers(
                    addl_headers={
                        'Content-Type': 'application/json-l',
                        'Content-Encoding': 'gzip'})
            ).prepare()

            try:
                ApiClient._rate_limited_request(request).raise_for_status()
            except RequestException as e:
                raise ApiClient._wrap_request_exception(e)

    def get_dataset(self, owner, dataset):
        try:
            resp = requests.get(
                '{}/datasets/{}/{}'.format(self._api_url, owner, dataset),
                headers=self._request_headers()
            ).raise_for_status()
            return resp.json()
        except RequestException as e:
            raise ApiClient._wrap_request_exception(e)

    def create_dataset(self, owner, dataset, title, visibility):
        try:
            requests.put(
                '{}/datasets/{}/{}'.format(self._api_url, owner, dataset),
                json={'title': title, 'visibility': visibility},
                headers=self._request_headers()
            ).raise_for_status()
        except RequestException as e:
            raise ApiClient._wrap_request_exception(e)

    def _request_headers(self, addl_headers=None):
        if addl_headers is None:
            addl_headers = {}

        headers = {
            'Authorization': 'Bearer {}'.format(self._api_token),
            'User-Agent': ApiClient._user_agent(),
            'Content-Type': 'application/json'
        }

        for k in addl_headers:
            headers[k] = addl_headers[k]

        return headers

    @staticmethod
    @backoff.on_predicate(backoff.expo, lambda r: r.status_code == 429,
                          max_tries=10)
    def _rate_limited_request(request, **kwargs):
        resp = Session().send(request, **kwargs)
        if (resp.status_code == 429 and
                resp.headers.get('Retry-After')):
            sleep(int(resp.headers.get('Retry-After')))

        return resp

    @staticmethod
    def _split_records_into_compressed_batches(records, max_records_per_batch):
        batch = []
        for i, r in enumerate(records, 1):
            batch.append(r)

            if len(batch) == max_records_per_batch or i == len(records):
                data = BytesIO()
                compressed_batch = gzip.GzipFile(
                    fileobj=data, mode='w')
                compressed_batch.write(
                    to_json_lines(batch).encode(encoding='utf-8'))
                compressed_batch.close()
                yield data.getvalue()
                batch = []

    @staticmethod
    def _user_agent():
        from target_datadotworld import __version__
        return 'target-datadotworld - {}'.format(__version__)

    @staticmethod
    def _wrap_request_exception(req_exception):
        wrappers = {
            401: dwex.UnauthorizedError,
            403: dwex.ForbiddenError,
            404: dwex.NotFoundError,
            429: dwex.TooManyRequestsError
        }

        req = req_exception.request
        if (isinstance(req_exception, HTTPError) and
                req_exception.response is not None):
            resp = req_exception.response
            wrapper = wrappers.get(resp.status_code, dwex.ApiError)
            return wrapper(request=req, response=resp)
        elif isinstance(req_exception, ConnectionError):
            return dwex.ConnectionError(req_exception.request)
        else:
            return req_exception
