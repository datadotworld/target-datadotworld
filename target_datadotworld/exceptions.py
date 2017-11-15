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

import requests.exceptions as rqex


def convert_requests_exception(req_exception):
    """Convert common HTTP errors to the appropriate ApiError sub-type"""
    wrappers = {
        401: UnauthorizedError,
        403: ForbiddenError,
        404: NotFoundError,
        429: TooManyRequestsError
    }

    req = req_exception.request
    if (isinstance(req_exception, rqex.HTTPError) and
            req_exception.response is not None):
        resp = req_exception.response
        wrapper = wrappers.get(resp.status_code, ApiError)
        return wrapper(request=req, response=resp)
    elif isinstance(req_exception, rqex.ConnectionError):
        return ConnectionError(req_exception.request)
    else:
        return req_exception


class Error(Exception):
    """Base class for all custom exceptions"""
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return '{}: {}'.format(
            self.__class__.__name__, self.message)

    def __repr__(self):
        return '{}(message=\'{}\')'.format(
            self.__class__.__name__, self.message)


class ConfigError(Error):
    """Target configuration error

    Used to indicate that the provided config file fails to satisfy this
    target's requirements
    """
    def __init__(self, cause=None):
        super(ConfigError, self).__init__(
            'Invalid configuration (Cause: {})'.format(cause or 'unspecified'))


class TokenError(ConfigError):
    """Token configuration error

    Used to indicate that the token provided in the configuration file is
    invalid (i.e. not a valid JWT)
    """
    def __init__(self):
        super(TokenError, self).__init__('Invalid API token')


class MissingSchemaError(Error):
    """Missing schema error

    Used to indicate that the tap emitted a RECORD message before emitting
    a SCHEMA message for the respective stream
    """
    def __init__(self, stream):
        super(MissingSchemaError, self).__init__(
            'Found record for stream {} before corresponding '
            'schema'.format(stream))


class UnparseableMessageError(Error):
    """Unparseable message error

    Used to indicate that the tap emitted a message that cannot be parsed
    """
    def __init__(self, message, cause):
        super(UnparseableMessageError, self).__init__(
            'Unable to parse message {} (Cause: {})'.format(message, cause))


class InvalidRecordError(Error):
    """Invalid record error

    Used to indicate that the tap emitted a message whose payload fail to
    satisfy the required JSON schema
    """
    def __init__(self, stream, cause):
        super(InvalidRecordError, self).__init__(
            'Found invalid record for stream {} (Cause: {})'.format(stream,
                                                                    cause))


class ApiError(Error):
    """Base class for all API exceptions"""
    def __init__(self, request, response,
                 cause='API Error', solution='Check server message'):
        try:
            server_message = response.json().get('message', 'unspecified')
        except (ValueError, AttributeError):
            server_message = 'unspecified'

        message = 'Error invoking {}: {}. {}. Server message: {}'.format(
            request.url if hasattr(request, 'url') else 'unspecified',
            cause, solution, server_message
        )
        super(ApiError, self).__init__(message)


class ConnectionError(ApiError):
    """Connection error

    Used to indicate that the target is unable to connect to data.world's API
    """
    def __init__(self, request,
                 cause='Unable to connect',
                 solution='Check network connection'):
        super(ConnectionError, self).__init__(
            request, None, cause, solution)


class UnauthorizedError(ApiError):
    """Unauthorized error

    Used to indicate that the server returned an HTTP 401 error
    """
    def __init__(self, request, response,
                 cause='Unauthorized',
                 solution='Check your API token'):
        super(UnauthorizedError, self).__init__(
            request, response, cause, solution)


class ForbiddenError(ApiError):
    """Forbidden error

    Used to indicate that the server returned an HTTP 403 error
    """
    def __init__(
            self, request, response,
            cause='Access denied',
            solution='Check permissions to access resource (e.g. dataset)'):
        super(ForbiddenError, self).__init__(
            request, response, cause, solution)


class NotFoundError(ApiError):
    """Not Found error

    Used to indicate that the server returned an HTTP 404 error
    """
    def __init__(self, request, response,
                 cause='Resource (e.g. dataset) does not exist',
                 solution='Check IDs in URL'):
        super(NotFoundError, self).__init__(
            request, response, cause, solution)


class TooManyRequestsError(ApiError):
    """Too Many Requests error

    Used to indicate that the server returned an HTTP 429 error
    """
    def __init__(self, request, response,
                 cause='Too many requests',
                 solution='Make less API requests less frequently'):
        super(TooManyRequestsError, self).__init__(
            request, response, cause, solution)
