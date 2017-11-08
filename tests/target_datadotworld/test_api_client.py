import gzip
import time

import pytest
import requests
import responses
from doublex import assert_that
from hamcrest import has_entry, contains_inanyorder, \
    equal_to, close_to, instance_of
from requests.exceptions import ConnectionError, HTTPError

import target_datadotworld.exceptions as dwex
from target_datadotworld.api_client import ApiClient
from target_datadotworld.utils import to_json_lines


class TestApiClient(object):
    @pytest.fixture()
    def api_client(self):
        return ApiClient(api_token='just_a_test_token')

    @pytest.fixture(params=[0, 5, 10, 15])
    def records(self, request):
        return [{'id': i} for i in range(request.param)]

    def test_append_stream(self):
        pass

    def test_append_stream_404(self):
        pass

    def test_append_stream_429(self):
        pass

    def test_append_stream_error(self):
        pass

    def test_connection_check(self, api_client):
        with responses.RequestsMock() as rsps:
            rsps.add(
                method='GET', url='https://api.data.world/v0/user',
                body='', status=200)

            api_client.connection_check()

    def test_connection_check_401(self, api_client):
        with responses.RequestsMock() as rsps:
            rsps.add(
                method='GET', url='https://api.data.world/v0/user',
                body='', status=401)

            with pytest.raises(dwex.UnauthorizedError):
                api_client.connection_check()

    def test_connection_check_offline(self, api_client):
        with responses.RequestsMock() as rsps:
            rsps.add(
                method='GET', url='https://api.data.world/v0/user',
                body=ConnectionError())

            with pytest.raises(dwex.ConnectionError):
                api_client.connection_check()

    def test_create_dataset(self):
        pass

    def test_create_dataset_403(self):
        pass

    def test_get_dataset(self):
        pass

    def test_create_dataset_error(self):
        pass

    def test__request_headers(self, api_client):
        headers = api_client._request_headers()
        assert_that(headers.keys(), contains_inanyorder(
            'Authorization', 'User-Agent', 'Content-Type'))

    def test__request_headers_addl(self, api_client):
        headers = api_client._request_headers(addl_headers={
            'Content-Encoding': 'gzip'})
        assert_that(headers.keys(), contains_inanyorder(
            'Authorization', 'User-Agent', 'Content-Type', 'Content-Encoding'))

    def test__request_headers_replace(self, api_client):
        headers = api_client._request_headers(addl_headers={
            'Content-Type': 'applicatoin/json-l'})
        assert_that(headers.keys(), contains_inanyorder(
            'Authorization', 'User-Agent', 'Content-Type'))

        assert_that(headers, has_entry('Content-Type', 'applicatoin/json-l'))

    def test__rate_limited_request(self):
        with responses.RequestsMock() as rsps:
            rsps.add('GET', 'https://acme.inc/api', body='', status=429)
            rsps.add('GET', 'https://acme.inc/api', body='', status=200)

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

    def test__rate_limited_request_error(self):
        with responses.RequestsMock() as rsps:
            rsps.add('GET', 'https://acme.inc/api', body='', status=429)

            resp = ApiClient._rate_limited_request(
                requests.Request('GET', 'https://acme.inc/api').prepare())

            assert_that(resp.status_code, equal_to(429))

    def test__split_records_into_compressed_batches(self, records):
        for i, batch in enumerate(
                ApiClient._split_records_into_compressed_batches(
                    records, max_records_per_batch=10)):
            expected_records = records[i * 10:min((i + 1) * 10, len(records))]
            assert_that(
                gzip.decompress(batch),
                equal_to(to_json_lines(expected_records).encode()))

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
        with pytest.raises(HTTPError) as e:
            requests.get('https://acme.inc/api').raise_for_status()
            assert_that(ApiClient._wrap_request_exception(e),
                        instance_of(expected_error))

    def test__wrap_request_exception_offline(self):
        responses.add('GET', 'https://acme.inc/api', body=ConnectionError())
        with pytest.raises(ConnectionError) as e:
            requests.get('https://acme.inc/api').raise_for_status()
            assert_that(ApiClient._wrap_request_exception(e),
                        instance_of(dwex.ConnectionError))
