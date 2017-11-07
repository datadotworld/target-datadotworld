import responses
import pytest
from requests.exceptions import ConnectionError

from target_datadotworld.api_client import ApiClient
import target_datadotworld.exceptions as dwex


class TestApiClient(object):

    @pytest.fixture()
    def api_client(self):
        return ApiClient(api_token='not_a_valid_token')

    def test_connection_check(self, api_client):
        with responses.RequestsMock() as rsps:
            rsps.add(
                method='GET', url='https://api.data.world/v0/user',
                body='{}', status=200)

            api_client.connection_check()

    def test_connection_check_401(self, api_client):
        with responses.RequestsMock() as rsps:
            rsps.add(
                method='GET', url='https://api.data.world/v0/user',
                body='{}', status=401)

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

    def test_create_dataset_error(self):
        pass

    def test_append_stream(self):
        pass

    def test_append_stream_404(self):
        pass

    def test_append_stream_429(self):
        pass

    def test_append_stream_error(self):
        pass