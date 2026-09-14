import pytest

from mongobus import constants
from mongobus._sync.pump import Consumer


def _handler(ctx):  # pragma: no cover - never invoked
    pass


def _consumer(**overrides):
    values = {
        "endpoint_id": "ep",
        "type_id": "SongRequested",
        "handler": _handler,
        "max_attempts": 3,
        "idempotent": True,
    }
    values.update(overrides)
    return Consumer(**values)


def test_lock_seconds_defaults_to_sixty():
    assert _consumer().lock_seconds == constants.DEFAULT_LOCK_SECONDS == 60


def test_lock_seconds_accepts_the_minimum():
    assert _consumer(lock_seconds=3).lock_seconds == 3


@pytest.mark.parametrize("invalid", [2, 0, -5, 3.0, "120", True, None])
def test_lock_seconds_rejects_anything_but_an_int_of_at_least_three(invalid):
    with pytest.raises(ValueError, match="lock_seconds must be an int >= 3"):
        _consumer(lock_seconds=invalid)
