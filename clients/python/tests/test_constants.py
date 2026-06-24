from mongobus import constants


def test_collection_names_match_dotnet():
    assert constants.INBOX_COLLECTION == "bus_inbox"
    assert constants.BINDINGS_COLLECTION == "bus_bindings"


def test_status_values_match_dotnet():
    assert constants.STATUS_PENDING == "Pending"
    assert constants.STATUS_PROCESSED == "Processed"
    assert constants.STATUS_DEAD == "Dead"


def test_envelope_defaults_match_dotnet():
    assert constants.SPEC_VERSION == "1.0"
    assert constants.DEFAULT_SOURCE == "urn:mongobus:unknown"
    assert constants.CLAIM_CHECK_CONTENT_TYPE == "application/vnd.mongobus.claim-check+json"


def test_runtime_defaults():
    assert constants.DEFAULT_LOCK_SECONDS == 60
    assert constants.DEFAULT_MAX_ATTEMPTS == 10
    assert constants.DEFAULT_POLL_SECONDS == 0.05
