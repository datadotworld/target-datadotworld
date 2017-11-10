# data.world-py
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

import pytest
import requests
import requests.exceptions as rqex
import responses

from target_datadotworld.exceptions import ApiError, UnauthorizedError, \
    ForbiddenError, NotFoundError, TooManyRequestsError, \
    ConnectionError, convert_requests_exception


@pytest.mark.parametrize("status_code,expected_error", [
    (400, ApiError),
    (401, UnauthorizedError),
    (403, ForbiddenError),
    (404, NotFoundError),
    (422, ApiError),
    (429, TooManyRequestsError)
])
@responses.activate
def test_convert_requests_exception(status_code, expected_error):
    responses.add('GET', 'https://acme.inc/api', status=status_code)
    with pytest.raises(expected_error):
        try:
            requests.get('https://acme.inc/api').raise_for_status()
        except rqex.HTTPError as e:
            raise convert_requests_exception(e)


def test_convert_requests_exception_offline():
    responses.add('GET', 'https://acme.inc/api', body=rqex.ConnectionError())
    with pytest.raises(ConnectionError):
        try:
            requests.get('https://acme.inc/api').raise_for_status()
        except rqex.ConnectionError as e:
            raise convert_requests_exception(e)
