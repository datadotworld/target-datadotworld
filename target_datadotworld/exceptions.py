import requests.exceptions as rqex


def convert_requests_exception(req_exception):
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
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return '{}: {}'.format(
            self.__class__.__name__, self.message)

    def __repr__(self):
        return '{}(message=\'{}\')'.format(
            self.__class__.__name__, self.message)


class ConfigError(Error):
    def __init__(self, cause=None):
        super(ConfigError, self).__init__(
            'Invalid configuration (Cause: {})'.format(cause or 'unspecified'))


class TokenError(ConfigError):
    def __init__(self):
        super(TokenError, self).__init__('Invalid API token')


class MissingSchemaError(Error):
    def __init__(self, stream):
        super(MissingSchemaError, self).__init__(
            'Found record for stream {} before corresponding '
            'schema'.format(stream))


class UnparseableMessageError(Error):
    def __init__(self, message, cause):
        super(UnparseableMessageError, self).__init__(
            'Unable to parse message {} (Cause: {})'.format(message, cause))


class InvalidRecordError(Error):
    def __init__(self, stream, cause):
        super(InvalidRecordError, self).__init__(
            'Found invalid record for stream {} (Cause: {})'.format(stream,
                                                                    cause))


class ApiError(Error):
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
    def __init__(self, request,
                 cause='Unable to connect',
                 solution='Check network connection'):
        super(ConnectionError, self).__init__(
            request, None, cause, solution)


class UnauthorizedError(ApiError):
    def __init__(self, request, response,
                 cause='Unauthorized',
                 solution='Check your API token'):
        super(UnauthorizedError, self).__init__(
            request, response, cause, solution)


class ForbiddenError(ApiError):
    def __init__(
            self, request, response,
            cause='Access denied',
            solution='Check permissions to access resource (e.g. dataset)'):
        super(ForbiddenError, self).__init__(
            request, response, cause, solution)


class NotFoundError(ApiError):
    def __init__(self, request, response,
                 cause='Resource (e.g. dataset) does not exist',
                 solution='Check IDs in URL'):
        super(NotFoundError, self).__init__(
            request, response, cause, solution)


class TooManyRequestsError(ApiError):
    def __init__(self, request, response,
                 cause='Too many requests',
                 solution='Make less API requests less frequently'):
        super(TooManyRequestsError, self).__init__(
            request, response, cause, solution)
