class InvalidVersionError(Exception):
    pass


class ResourceConflict(Exception):
    def __init__(self, msg, errors):
        super().__init__(msg)
        self.errors = errors


class ValidationError(Exception):
    pass


class DeleteConflict(Exception):
    pass


class ResourceNotFoundError(Exception):
    pass
