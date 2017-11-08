from copy import copy

import pytest
from doublex import assert_that
from hamcrest import has_entries

from target_datadotworld.exceptions import ConfigError
from target_datadotworld.target import TargetDataDotWorld


class TestTarget(object):
    @pytest.fixture()
    def api_client(self):
        pass

    @pytest.fixture()
    def logger(self):
        # TODO verify logging
        pass

    @pytest.fixture()
    def metrics(self):
        # TODO verify metrics
        pass

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
            'dataset_title': 'My Dataset',
            'default_license': 'Other',
            'default_owner': 'rafael',
            'default_visibility': 'PRIVATE'
        }

    @pytest.fixture(params=[
        ('api_token', 'eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJwcm9kLXVzZXItY2xpZW'
                      '50OnJhZmFlbCIsImlzcyI6ImFnZW50OnJhZmFlbDo6YjY1NTgxO'
                      'DItMjRkNy00MWZiLTkxNTAtNjZl'),
        ('default_license', 'NOTALICENSE'),
        ('default_owner', 'x'),
        ('default_owner', 'Mr.X'),
        ('default_visibility', 'Invisible'),
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
            'dataset_title': sample_config['dataset_title']
        }
        target = TargetDataDotWorld(minimal_config)
        expected_config = copy(sample_config)
        expected_config['default_license'] = None
        assert_that(target.config, has_entries(expected_config))

    def test_config_namespace(self, sample_config):
        minimal_config = {
            'api_token': sample_config['api_token'],
            'namespace': sample_config['dataset_title']
        }
        target = TargetDataDotWorld(minimal_config)
        expected_config = copy(sample_config)
        expected_config['default_license'] = None
        assert_that(target.config, has_entries(expected_config))

    def test_config_complete(self, sample_config):
        target = TargetDataDotWorld(sample_config)
        assert_that(target.config, has_entries(sample_config))

    def test_config_incomplete(self, sample_config):
        incomplete_config = {
            'dataset_title': sample_config['dataset_title']
        }
        with pytest.raises(ConfigError):
            TargetDataDotWorld(incomplete_config)

    def test_config_invalid(self, invalid_config):
        with pytest.raises(ConfigError):
            TargetDataDotWorld(invalid_config)

    def test_record_single(self):
        pass

    def test_record_multiple(self):
        pass

    def test_record_multiple_streams(self):
        pass

    def test_record_multiple_batches(self):
        pass

    def test_record_invalid(self):
        pass

    def test_schema(self):
        pass

    def test_schema_invalid(self):
        pass

    def test_schema_missing(self):
        pass

    def test_state(self):
        pass

    def test_unknown_message(self):
        pass
