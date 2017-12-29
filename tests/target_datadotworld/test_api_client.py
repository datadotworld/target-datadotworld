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

import asyncio
import time

import pytest
import responses
from doublex import assert_that
from hamcrest import equal_to, close_to
from requests import Request
from requests.exceptions import ConnectionError

import target_datadotworld.exceptions as dwex
from target_datadotworld import api_client
from target_datadotworld.api_client import ApiClient
from target_datadotworld.utils import to_jsonlines


class TestApiClient(object):
    @pytest.fixture()
    def client(self):
        return ApiClient(api_token='just_a_test_token')

    def test_append_stream(self, client, records_generator):
        with responses.RequestsMock() as rsps:
            all_records = list(records_generator())
            call_count = 0

            def verify_body_and_count(req):
                nonlocal call_count, all_records

                assert_that(
                    req.body.decode('utf-8'),
                    equal_to(to_jsonlines(all_records)))

                call_count += 1
                return 200, {}, None

            rsps.add_callback(
                'POST',
                '{}/streams/owner/dataset/stream'.format(
                    client._api_url),
                callback=verify_body_and_count)

            client.append_stream(
                'owner', 'dataset', 'stream', records_generator())

            assert_that(call_count, equal_to(1))

    @pytest.mark.asyncio
    @pytest.mark.parametrize('chunk_size', [3, 5])
    async def test_append_stream_chunked(
            self, client, records_queue, chunk_size, event_loop):

        with responses.RequestsMock() as rsps:
            queue, all_records = records_queue
            call_count = 0

            def verify_body_and_count(req):
                nonlocal call_count, all_records, chunk_size

                first_record = call_count * chunk_size
                last_record = min((call_count + 1) * chunk_size,
                                  len(all_records))
                expected_records = all_records[first_record:last_record]

                assert_that(
                    req.body.decode('utf-8'),
                    equal_to(to_jsonlines(expected_records)))

                call_count += 1
                return 200, {}, None

            rsps.add_callback(
                'POST',
                '{}/streams/owner/dataset/stream'.format(
                    client._api_url),
                callback=verify_body_and_count)

            consumer = asyncio.ensure_future(client.append_stream_chunked(
                'owner', 'dataset', 'stream', queue,
                chunk_size=chunk_size, loop=event_loop), loop=event_loop)
            await queue.join()
            await consumer

    @pytest.mark.asyncio
    @pytest.mark.parametrize('chunk_size', [3, 5])
    async def test_append_stream_chunked_error(
            self, client, records_queue, chunk_size, event_loop):

        queue, all_records = records_queue

        responses.add(
            'POST',
            '{}/streams/owner/dataset/stream'.format(client._api_url),
            status=200)
        responses.add(
            'POST',
            '{}/streams/owner/dataset/stream'.format(client._api_url),
            status=404)

        with pytest.raises(dwex.ApiError):
            consumer = asyncio.ensure_future(client.append_stream_chunked(
                'owner', 'dataset', 'stream', queue,
                chunk_size=chunk_size, loop=event_loop), loop=event_loop)
            await queue.join()
            await consumer

    @responses.activate
    def test_append_stream_error(self, client):
        with responses.RequestsMock() as rsps:
            rsps.add(
                'POST',
                '{}/streams/owner/dataset/stream'.format(client._api_url),
                status=404)
            with pytest.raises(dwex.ApiError):
                client.append_stream(
                    'owner', 'dataset', 'stream', [{'hello': 'world'}])

    @responses.activate
    def test_connection_check(self, client):
        responses.add(
            'GET', '{}/user'.format(client._api_url),
            body='{"name": "user"}', status=200)

        client.connection_check()

    @responses.activate
    def test_connection_check_401(self, client):
        responses.add(
            'GET', '{}/user'.format(client._api_url),
            status=401)

        with pytest.raises(dwex.UnauthorizedError):
            client.connection_check()

    @responses.activate
    def test_connection_check_offline(self, client):
        responses.add(
            'GET', '{}/user'.format(client._api_url),
            body=ConnectionError(
                request=Request('GET', '{}/user'.format(client._api_url))))

        with pytest.raises(dwex.ConnectionError):
            client.connection_check()

    @responses.activate
    def test_create_dataset(self, client):
        expected_resp = {'message': 'Success'}
        responses.add(
            'PUT', '{}/datasets/owner/dataset'.format(client._api_url),
            json=expected_resp, status=200)

        resp = client.create_dataset('owner', 'dataset',
                                     title='Dataset', visibility='OPEN')
        assert_that(resp, equal_to(expected_resp))

    @responses.activate
    def test_create_dataset_error(self, client):
        responses.add(
            'PUT', '{}/datasets/owner/dataset'.format(client._api_url),
            status=400)
        with pytest.raises(dwex.ApiError):
            client.create_dataset('owner', 'dataset',
                                  title='Dataset', visibility='OPEN')

    @responses.activate
    def test_get_dataset(self, client):
        expected_resp = {'title': 'Dataset'}
        responses.add(
            'GET', '{}/datasets/owner/dataset'.format(client._api_url),
            json=expected_resp, status=200)

        resp = client.get_dataset('owner', 'dataset')
        assert_that(resp, equal_to(expected_resp))

    @responses.activate
    def test_get_dataset_error(self, client):
        responses.add(
            'GET', '{}/datasets/owner/dataset'.format(client._api_url),
            status=403)
        with pytest.raises(dwex.ApiError):
            client.get_dataset('owner', 'dataset')

    @responses.activate
    def test__retry_if_throttled(self, client):
        user_endpoint = '{}/user'.format(client._api_url)
        responses.add('GET', user_endpoint, body='', status=429)
        responses.add('GET', user_endpoint, body='', status=200)

        client.connection_check()

    def test__retry_if_throttled_delayed(self, client):
        user_endpoint = '{}/user'.format(client._api_url)

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

            rsps.add_callback('GET', user_endpoint,
                              callback=retry_after)

            client.connection_check()

    @responses.activate
    def test__retry_if_throttled_error(self, monkeypatch, client):
        monkeypatch.setattr(api_client, 'MAX_TRIES', 2)
        user_endpoint = '{}/user'.format(client._api_url)

        responses.add('GET', user_endpoint, body='', status=429)

        with pytest.raises(dwex.TooManyRequestsError):
            client.connection_check()
