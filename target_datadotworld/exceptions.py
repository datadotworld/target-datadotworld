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
    pass


class TokenError(ConfigError):
    pass


class ApiError(Error):
    def __init__(self, request, response,
                 cause='API Error', solution='Check server message'):
        message = 'Error invoking {}: {}. {}. Server message: {}'.format(
            request.url, cause, solution,
            response.json().get('message', 'unspecified')
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
