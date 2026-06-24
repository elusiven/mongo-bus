import pytest

from mongobus.errors import MongoBusError, ClaimCheckNotSupportedError


def test_claim_check_error_is_a_mongobus_error():
    assert issubclass(ClaimCheckNotSupportedError, MongoBusError)


def test_claim_check_error_can_be_raised_with_message():
    with pytest.raises(MongoBusError, match="claim-check"):
        raise ClaimCheckNotSupportedError("claim-check not supported")
