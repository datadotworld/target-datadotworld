import gzip
import time

import pytest
import requests
import responses
from doublex import assert_that
from hamcrest import has_entry, contains_inanyorder, \
    equal_to, close_to
from requests import Request
from requests.exceptions import ConnectionError, HTTPError

import target_datadotworld.exceptions as dwex
from target_datadotworld.api_client import ApiClient
from target_datadotworld.utils import to_json_lines


class TestApiClient(object):
    @pytest.fixture()
    def api_client(self):
        return ApiClient(api_token='just_a_test_token')

    @pytest.fixture(params=[0, 5, 10, 15])
    def records_generator(self, request):
        def records_generator(size):
            for i in range(size):
                yield {'id': i}

        return lambda: records_generator(request.param)

    @pytest.mark.parametrize('batch_size', [3, None])
    def test_append_stream(self, api_client, records_generator, batch_size):
        with responses.RequestsMock() as rsps:

            call_count = 0

            def count_call(req):
                nonlocal call_count
                call_count += 1
                return 200, {}, None

            rsps.add_callback(
                'POST',
                '{}/streams/owner/dataset/stream'.format(
                    api_client._api_url),
                callback=count_call)

            api_client.append_stream(
                'owner', 'dataset', 'stream', records_generator(),
                max_records_per_batch=batch_size)

            if batch_size is not None:
                assert_that(call_count, equal_to(
                    (len(list(records_generator())) // batch_size) + 1))
            else:
                assert_that(call_count, equal_to(1))

    @responses.activate
    def test_append_stream_error(self, api_client):
        responses.add(
            'POST',
            '{}/streams/owner/dataset/stream'.format(api_client._api_url),
            status=404)
        with pytest.raises(dwex.ApiError):
            api_client.append_stream(
                'owner', 'dataset', 'stream', [{'hello': 'world'}])

    @responses.activate
    def test_connection_check(self, api_client):
        responses.add(
            'GET', '{}/user'.format(api_client._api_url),
            body='{"name": "user"}', status=200)

        api_client.connection_check()

    @responses.activate
    def test_connection_check_401(self, api_client):
        responses.add(
            'GET', '{}/user'.format(api_client._api_url),
            status=401)

        with pytest.raises(dwex.UnauthorizedError):
            api_client.connection_check()

    @responses.activate
    def test_connection_check_offline(self, api_client):
        responses.add(
            'GET', '{}/user'.format(api_client._api_url),
            body=ConnectionError(
                request=Request('GET', '{}/user'.format(api_client._api_url))))

        with pytest.raises(dwex.ConnectionError):
            api_client.connection_check()

    @responses.activate
    def test_create_dataset(self, api_client):
        expected_resp = {'message': 'Success'}
        responses.add(
            'PUT', '{}/datasets/owner/dataset'.format(api_client._api_url),
            json=expected_resp, status=200)

        resp = api_client.create_dataset('owner', 'dataset',
                                         title='Dataset', visibility='OPEN')
        assert_that(resp, equal_to(expected_resp))

    @responses.activate
    def test_create_dataset_error(self, api_client):
        responses.add(
            'PUT', '{}/datasets/owner/dataset'.format(api_client._api_url),
            status=400)
        with pytest.raises(dwex.ApiError):
            api_client.create_dataset('owner', 'dataset',
                                      title='Dataset', visibility='OPEN')

    @responses.activate
    def test_get_dataset(self, api_client):
        expected_resp = {'title': 'Dataset'}
        responses.add(
            'GET', '{}/datasets/owner/dataset'.format(api_client._api_url),
            json=expected_resp, status=200)

        resp = api_client.get_dataset('owner', 'dataset')
        assert_that(resp, equal_to(expected_resp))

    @responses.activate
    def test_get_dataset_error(self, api_client):
        responses.add(
            'GET', '{}/datasets/owner/dataset'.format(api_client._api_url),
            status=403)
        with pytest.raises(dwex.ApiError):
            api_client.get_dataset('owner', 'dataset')

    def test__request_headers(self, api_client):
        headers = api_client._request_headers()
        assert_that(headers.keys(), contains_inanyorder(
            'Accept', 'Authorization', 'Content-Type', 'User-Agent'))

    def test__request_headers_addl(self, api_client):
        headers = api_client._request_headers(addl_headers={
            'Content-Encoding': 'gzip'})
        assert_that(headers.keys(), contains_inanyorder(
            'Accept', 'Authorization', 'Content-Encoding',
            'Content-Type', 'User-Agent'))

    def test__request_headers_replace(self, api_client):
        headers = api_client._request_headers(addl_headers={
            'Content-Type': 'applicatoin/json-l'})
        assert_that(headers.keys(), contains_inanyorder(
            'Accept', 'Authorization', 'Content-Type', 'User-Agent'))

        assert_that(headers, has_entry('Content-Type', 'applicatoin/json-l'))

    @responses.activate
    def test__rate_limited_request(self):
        responses.add('GET', 'https://acme.inc/api', body='', status=429)
        responses.add('GET', 'https://acme.inc/api', body='', status=200)

        resp = ApiClient._rate_limited_request(
            requests.Request('GET', 'https://acme.inc/api').prepare())

        assert_that(resp.status_code, equal_to(200))

    def test__rate_limited_request_delayed(self):
        with responses.RequestsMock() as rsps:
            first_attempt_time = None

            def retry_after(req):
                nonlocal first_attempt_time
                if first_attempt_time is not None:
                    time_between_calls = time.time() - first_attempt_time
                    assert_that(time_between_calls, close_to(10, 1))
                    return 200, {}, ''
                else:
                    first_attempt_time = time.time()
                    return 429, {'Retry-After': '10'}, ''

            rsps.add_callback('GET', 'https://acme.inc/api',
                              callback=retry_after)

            resp = ApiClient._rate_limited_request(
                requests.Request('GET', 'https://acme.inc/api').prepare())

            assert_that(resp.status_code, equal_to(200))

    @responses.activate
    def test__rate_limited_request_error(self):
        responses.add('GET', 'https://acme.inc/api', body='', status=429)

        resp = ApiClient._rate_limited_request(
            requests.Request('GET', 'https://acme.inc/api').prepare())

        assert_that(resp.status_code, equal_to(429))

    def test__split_records_into_compressed_batches(self, records_generator):
        all_records = list(records_generator())

        def verify_batch(i, batch):
            nonlocal all_records
            first_record = i * 10
            last_record = min((i + 1) * 10, len(all_records))
            expected_records = all_records[first_record:last_record]
            assert_that(
                gzip.decompress(batch),
                equal_to(to_json_lines(expected_records).encode()))

        # process generator
        for i, batch in enumerate(
                ApiClient._split_records_into_compressed_batches(
                    records_generator(), max_records_per_batch=10)):
            verify_batch(i, batch)

        # process list
        for i, batch in enumerate(
                ApiClient._split_records_into_compressed_batches(
                    all_records, max_records_per_batch=10)):
            verify_batch(i, batch)

    @pytest.mark.parametrize("status_code,expected_error", [
        (400, dwex.ApiError),
        (401, dwex.UnauthorizedError),
        (403, dwex.ForbiddenError),
        (404, dwex.NotFoundError),
        (422, dwex.ApiError),
        (429, dwex.TooManyRequestsError)
    ])
    @responses.activate
    def test__wrap_request_exception_http(self, status_code, expected_error):
        responses.add('GET', 'https://acme.inc/api', status=status_code)
        with pytest.raises(expected_error):
            try:
                requests.get('https://acme.inc/api').raise_for_status()
            except HTTPError as e:
                raise ApiClient._wrap_request_exception(e)

    def test__wrap_request_exception_offline(self):
        responses.add('GET', 'https://acme.inc/api', body=ConnectionError())
        with pytest.raises(dwex.ConnectionError):
            try:
                requests.get('https://acme.inc/api').raise_for_status()
            except ConnectionError as e:
                raise ApiClient._wrap_request_exception(e)
