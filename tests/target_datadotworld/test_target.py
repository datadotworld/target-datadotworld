import pytest


class TestTarget(object):
    @pytest.fixture()
    def api_client(self):
        pass

    @pytest.fixture()
    def logger(self):
        pass

    @pytest.fixture()
    def metrics(self):
        pass

    def test_config_minimal(self):
        # token
        # default visibility
        pass

    def test_config_invalid_token(self):
        pass

    def test_config_insufficient_scopes(self):
        pass

    def test_config_complete(self):
        # org account
        pass

    def test_config_invalid(self):
        pass

    def test_config_missing(self):
        pass

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
