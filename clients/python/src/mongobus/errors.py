class MongoBusError(Exception):
    """Base class for all MongoBus client errors."""


class ClaimCheckNotSupportedError(MongoBusError):
    """Raised when a consumer receives a claim-check payload, which this client does not yet support."""


class ClaimCheckError(MongoBusError):
    """Raised on claim-check storage or decoding failures (e.g. a decompression-bomb guard trip)."""
