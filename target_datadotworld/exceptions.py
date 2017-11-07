class Error(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message

    def __repr__(self):
        return '{}(message=\'{}\')'.format(
            self.__class__.__name__, self.message)


class ConfigError(Error):
    pass


class TokenError(ConfigError):
    pass


class ConnectionError(Error):
    pass


class UnauthorizedError(Error):
    pass
