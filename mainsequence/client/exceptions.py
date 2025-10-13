class ApiError(Exception):
    def __init__(self, message, response=None, payload=None):
        super().__init__(message)
        self.response = response
        self.payload = payload
        self.status_code = getattr(response, "status_code", None)


class ConflictError(ApiError):
    pass
