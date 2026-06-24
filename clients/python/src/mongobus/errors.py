class MongoBusError(Exception):
    """Base class for all MongoBus client errors."""


class ClaimCheckNotSupportedError(MongoBusError):
    """Raised when a consumer receives a claim-check payload, which this client does not yet support."""
