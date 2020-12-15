class InvalidVersionError(Exception):
    pass


class ResourceConflict(Exception):
    def __init__(self, msg, errors):
        super().__init__(msg)
        self.errors = errors


class ValidationError(Exception):
    def __init__(self, msg):
        super().__init__(msg)
