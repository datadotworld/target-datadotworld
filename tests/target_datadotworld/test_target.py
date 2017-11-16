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

import json
from copy import copy
from os import path

import pytest
from doublex import assert_that, ProxySpy, called, Stub
from hamcrest import has_entries, equal_to, not_none, only_contains, none
from requests import Request, Response

from target_datadotworld.api_client import ApiClient
from target_datadotworld.exceptions import ConfigError, NotFoundError, \
    UnparseableMessageError, MissingSchemaError
from target_datadotworld.target import TargetDataDotWorld


class TestTarget(object):
    @pytest.fixture()
    def api_client(self, monkeypatch):
        def create_dataset(self, owner, dataset, **kwargs):
            assert_that(owner, not_none())
            assert_that(dataset, not_none())
            assert_that(kwargs.keys(),
                        only_contains('title', 'license', 'visibility'))

            return {}

        async def append_stream_chunked(
                self, owner, dataset, stream, queue, chunk_size, loop):
            while True:
                item = await queue.get()
                queue.task_done()
                if item is None:
                    break

        monkeypatch.setattr(ApiClient,
                            'connection_check', lambda self: True)
        monkeypatch.setattr(ApiClient,
                            'get_dataset', lambda self, owner, ds: {})
        monkeypatch.setattr(ApiClient, 'create_dataset', create_dataset)
        monkeypatch.setattr(ApiClient, 'append_stream_chunked',
                            append_stream_chunked)

        return ProxySpy(ApiClient('no_token_needed'))

    @pytest.fixture()
    def target(self, sample_config, api_client):
        return TargetDataDotWorld(sample_config, api_client=api_client)

    @pytest.fixture()
    def sample_config(self):
        return {
            'api_token': 'eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJwcm9kLXVzZXItY2xpZW'
                         '50OnJhZmFlbCIsImlzcyI6ImFnZW50OnJhZmFlbDo6YjY1NTgxO'
                         'DItMjRkNy00MWZiLTkxNTAtNjZlNDBhNjNjNjQ5IiwiaWF0Ijox'
                         'NTA1MTY0NTQ4LCJyb2xlIjpbInVzZXJfYXBpX3JlYWQiLCJ1c2V'
                         'yX2FwaV93cml0ZSJdLCJnZW5lcmFsLXB1cnBvc2UiOnRydWV9.n'
                         '9FsdsBZ03wx0A-QK1wq2tGyinaqUcjaotp-rnWCMoMOY83ivypu'
                         'B3FcjTGzJPFIGZbJsES_bx0itijwz5mQvg',
            'dataset_id': 'my-dataset',
            'dataset_title': 'My Dataset',
            'dataset_license': 'Other',
            'dataset_owner': 'rafael',
            'dataset_visibility': 'PRIVATE'
        }

    @pytest.fixture(params=[
        ('api_token', 'eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJwcm9kLXVzZXItY2xpZW'
                      '50OnJhZmFlbCIsImlzcyI6ImFnZW50OnJhZmFlbDo6YjY1NTgxO'
                      'DItMjRkNy00MWZiLTkxNTAtNjZl'),
        ('dataset_license', 'NOTALICENSE'),
        ('dataset_owner', 'x'),
        ('dataset_owner', 'Mr.X'),
        ('dataset_visibility', 'Invisible'),
        ('dataset_title', 'xyz' * 43),
        ('dataset_title', 'xy'),
        ('namespace', 'me too me too')
    ])
    def invalid_config(self, request, sample_config):
        invalid_config = copy(sample_config)
        invalid_config[request.param[0]] = request.param[1]
        return invalid_config

    def test_config_minimal(self, sample_config):
        minimal_config = {
            'api_token': sample_config['api_token'],
            'dataset_id': sample_config['dataset_id']
        }
        target = TargetDataDotWorld(minimal_config)
        expected_config = copy(sample_config)
        expected_config['dataset_license'] = None
        expected_config['dataset_title'] = expected_config['dataset_id']
        assert_that(target.config, has_entries(expected_config))

    def test_config_namespace(self, sample_config):
        minimal_config = {
            'api_token': sample_config['api_token'],
            'namespace': sample_config['dataset_title']
        }
        target = TargetDataDotWorld(minimal_config)
        expected_config = copy(sample_config)
        expected_config['dataset_license'] = None
        assert_that(target.config, has_entries(expected_config))

    def test_config_complete(self, sample_config):
        target = TargetDataDotWorld(sample_config)
        assert_that(target.config, has_entries(sample_config))

    def test_config_incomplete(self, sample_config):
        incomplete_config = {
            'dataset_id': sample_config['dataset_id']
        }
        with pytest.raises(ConfigError):
            TargetDataDotWorld(incomplete_config)

    def test_config_invalid(self, invalid_config):
        with pytest.raises(ConfigError):
            TargetDataDotWorld(invalid_config)

    @pytest.mark.asyncio
    async def test_process_lines(self, target, api_client, test_files_path):
        with open(path.join(test_files_path, 'fixerio.jsonl')) as file:
            result = await target.process_lines(file)
            assert_that(api_client.append_stream_chunked, called().times(1))
            assert_that(result, equal_to(
                json.loads('{"start_date": "2017-11-09"}')))

    @pytest.mark.asyncio
    async def test_process_lines_new_dataset(self, target, api_client,
                                             test_files_path, monkeypatch):

        def get_dataset(self, owner, dataset):
            raise NotFoundError(Stub(Request), Stub(Response))

        monkeypatch.setattr(ApiClient, 'get_dataset', get_dataset)

        with open(path.join(test_files_path, 'fixerio.jsonl')) as file:
            await target.process_lines(file)
            assert_that(api_client.create_dataset, called())

    @pytest.mark.asyncio
    async def test_process_lines_unparseable(self, target,
                                             test_files_path):
        with pytest.raises(UnparseableMessageError):
            with open(path.join(test_files_path,
                                'fixerio-broken.jsonl')) as file:
                await target.process_lines(file)

    @pytest.mark.asyncio
    async def test_process_lines_missing_schema(self, target,
                                                test_files_path):
        with pytest.raises(MissingSchemaError):
            with open(path.join(test_files_path,
                                'fixerio-noschema.jsonl')) as file:
                await target.process_lines(file)

    @pytest.mark.asyncio
    async def test_process_lines_multiple_streams(self, target, api_client,
                                                  test_files_path):
        with open(path.join(test_files_path,
                            'fixerio-multistream.jsonl')) as file:
            await target.process_lines(file)
            assert_that(api_client.append_stream_chunked, called().times(2))

    @pytest.mark.asyncio
    async def test_process_no_state(self, target, test_files_path):
        with open(path.join(test_files_path, 'fixerio-nostate.jsonl')) as file:
            result = await target.process_lines(file)
            assert_that(result, none())
