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


def test_reference_to_data_omits_none_fields():
    ref = core.ClaimCheckReference(provider="gridfs", container="claimcheck", key="abc", length=10)
    data = core.reference_to_data(ref)
    assert data == {"provider": "gridfs", "container": "claimcheck", "key": "abc", "length": 10}
    for absent in ("contentType", "metadata", "createdAt"):
        assert absent not in data


def test_reference_to_data_includes_optional_fields():
    ref = core.ClaimCheckReference(
        provider="gridfs", container="claimcheck", key="abc", length=10,
        content_type="application/json", metadata={"x-mongobus-compression": "gzip"},
        created_at="2026-06-24T12:00:00Z",
    )
    data = core.reference_to_data(ref)
    assert data["contentType"] == "application/json"
    assert data["metadata"] == {"x-mongobus-compression": "gzip"}
    assert data["createdAt"] == "2026-06-24T12:00:00Z"


def test_reference_from_data_round_trips():
    data = {
        "provider": "gridfs", "container": "claimcheck", "key": "abc", "length": 10,
        "contentType": "application/json",
        "metadata": {"x-mongobus-compression": "gzip"},
        "createdAt": "2026-06-24T12:00:00Z",
    }
    ref = core.reference_from_data(data)
    assert ref.provider == "gridfs"
    assert ref.container == "claimcheck"
    assert ref.key == "abc"
    assert ref.length == 10
    assert ref.content_type == "application/json"
    assert ref.metadata == {"x-mongobus-compression": "gzip"}
    assert ref.created_at == "2026-06-24T12:00:00Z"


def test_reference_from_data_with_only_required_fields():
    ref = core.reference_from_data({"provider": "s3", "container": "b", "key": "k", "length": 5})
    assert ref.content_type is None
    assert ref.metadata is None
    assert ref.created_at is None


import pytest

from mongobus.constants import CLAIM_CHECK_CONTENT_TYPE


def test_is_claim_check_true_for_claim_check_content_type():
    assert core.is_claim_check({"dataContentType": CLAIM_CHECK_CONTENT_TYPE}) is True


def test_is_claim_check_false_otherwise():
    assert core.is_claim_check({"dataContentType": "application/json"}) is False
    assert core.is_claim_check({}) is False


def test_should_offload_forced_by_use_claim_check():
    assert core.should_offload(size=1, threshold_bytes=999, enabled=False, use_claim_check=True) is True


def test_should_offload_threshold_when_enabled():
    assert core.should_offload(size=100, threshold_bytes=100, enabled=True, use_claim_check=None) is True
    assert core.should_offload(size=99, threshold_bytes=100, enabled=True, use_claim_check=None) is False


def test_should_offload_disabled_and_not_forced():
    assert core.should_offload(size=10_000, threshold_bytes=100, enabled=False, use_claim_check=None) is False


def test_gzip_round_trips():
    original = b'{"orderId": "123"}' * 100
    compressed = core.gzip_compress(original)
    assert core.gzip_decompress(compressed, max_bytes=1_000_000) == original


def test_gzip_decompress_guards_against_bombs():
    compressed = core.gzip_compress(b"a" * 10_000)
    with pytest.raises(core_error_type()):
        core.gzip_decompress(compressed, max_bytes=100)


def core_error_type():
    from mongobus.errors import ClaimCheckError
    return ClaimCheckError


def test_new_blob_key_is_32_char_hex():
    key = core.new_blob_key()
    assert len(key) == 32
    assert "-" not in key
    int(key, 16)
