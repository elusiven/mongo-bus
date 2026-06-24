from mongobus.claimcheck import core
from mongobus.errors import ClaimCheckError, MongoBusError


def test_constants_match_dotnet():
    assert core.OBJECT_CONTENT_TYPE == "application/json"
    assert core.CREATED_AT_KEY == "x-mongobus-created-at"
    assert core.COMPRESSION_KEY == "x-mongobus-compression"
    assert core.COMPRESSION_GZIP == "gzip"
    assert core.DEFAULT_THRESHOLD_BYTES == 256 * 1024
    assert core.DEFAULT_MAX_DECOMPRESSED_BYTES == 100 * 1024 * 1024


def test_claim_check_error_is_a_mongobus_error():
    assert issubclass(ClaimCheckError, MongoBusError)
