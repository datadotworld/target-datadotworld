import gzip
import time
from math import ceil

import pytest
import requests
import responses
from doublex import assert_that
from hamcrest import has_entry, contains_inanyorder, \
    equal_to, close_to
from requests import Request
from requests.exceptions import ConnectionError, HTTPError

import target_datadotworld.exceptions as dwex
from target_datadotworld import api_client
from target_datadotworld.api_client import ApiClient
from target_datadotworld.utils import to_jsonlines


class TestApiClient(object):
    @pytest.fixture()
    def api_client(self):
        return ApiClient(api_token='just_a_test_token')

    @pytest.fixture(params=[5, 10, 15])
    def records_generator(self, request):
        def records_generator(size):
            for i in range(size):
                yield {'id': i}

        return lambda: records_generator(request.param)

    @pytest.mark.parametrize('batch_size', [3, None])
    def test_append_stream(self, api_client, records_generator, batch_size):

        with responses.RequestsMock() as rsps:

            all_records = list(records_generator())
            call_count = 0

            def verify_body_and_count(req):
                nonlocal call_count, all_records, batch_size

                if batch_size is not None:
                    first_record = call_count * batch_size
                    last_record = min((call_count + 1) * batch_size,
                                      len(all_records))
                    expected_records = all_records[first_record:last_record]
                else:
                    expected_records = all_records

                assert_that(
                    gzip.decompress(req.body).decode('utf-8'),
                    equal_to(to_jsonlines(expected_records)))

                call_count += 1
                return 200, {}, None

            rsps.add_callback(
                'POST',
                '{}/streams/owner/dataset/stream'.format(
                    api_client._api_url),
                callback=verify_body_and_count)

            api_client.append_stream(
                'owner', 'dataset', 'stream', records_generator(),
                batch_size=batch_size)

            if batch_size is not None:
                assert_that(call_count, equal_to(
                    (ceil(len(list(records_generator())) / batch_size))))
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
    def test__retry_if_throttled(self):
        responses.add('GET', 'https://acme.inc/api', body='', status=429)
        responses.add('GET', 'https://acme.inc/api', body='', status=200)

        resp = ApiClient._retry_if_throttled(
            requests.Request('GET', 'https://acme.inc/api').prepare())

        assert_that(resp.status_code, equal_to(200))

    def test__retry_if_throttled_delayed(self):
        with responses.RequestsMock() as rsps:
            first_attempt_time = None

            def retry_after(req):
                nonlocal first_attempt_time
                if first_attempt_time is not None:
                    time_between_calls = time.time() - first_attempt_time
                    assert_that(time_between_calls, close_to(5, 1))
                    return 200, {}, ''
                else:
                    first_attempt_time = time.time()
                    return 429, {'Retry-After': '5'}, ''

            rsps.add_callback('GET', 'https://acme.inc/api',
                              callback=retry_after)

            resp = ApiClient._retry_if_throttled(
                requests.Request('GET', 'https://acme.inc/api').prepare())

            assert_that(resp.status_code, equal_to(200))

    @responses.activate
    def test__retry_if_throttled_error(self, monkeypatch):
        monkeypatch.setattr(api_client, 'MAX_TRIES', 2)

        responses.add('GET', 'https://acme.inc/api', body='', status=429)

        resp = ApiClient._retry_if_throttled(
            requests.Request('GET', 'https://acme.inc/api').prepare())

        assert_that(resp.status_code, equal_to(429))

