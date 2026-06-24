from datetime import timedelta

from mongobus import indexes


def _keys(spec):
    return spec[0]


def _options(spec):
    return spec[1]


def test_default_ttl_matches_dotnet():
    assert indexes.DEFAULT_PROCESSED_MESSAGE_TTL == timedelta(days=7)


def test_inbox_specs_include_lock_and_dedup_indexes():
    specs = indexes.inbox_index_specs(processed_message_ttl=None)
    key_sets = [_keys(s) for s in specs]
    assert [("EndpointId", 1), ("Status", 1), ("VisibleUtc", 1), ("LockedUntilUtc", 1)] in key_sets
    assert [("EndpointId", 1), ("CloudEventId", 1)] in key_sets


def test_inbox_specs_omit_ttl_when_ttl_is_none():
    specs = indexes.inbox_index_specs(processed_message_ttl=None)
    assert all("expireAfterSeconds" not in _options(s) for s in specs)
    assert len(specs) == 2


def test_inbox_specs_include_ttl_when_ttl_given():
    specs = indexes.inbox_index_specs(processed_message_ttl=timedelta(days=7))
    ttl_specs = [s for s in specs if "expireAfterSeconds" in _options(s)]
    assert len(ttl_specs) == 1
    keys, options = ttl_specs[0]
    assert keys == [("CreatedUtc", 1)]
    assert options["expireAfterSeconds"] == 7 * 24 * 60 * 60


def test_inbox_ttl_seconds_truncate_to_int():
    specs = indexes.inbox_index_specs(processed_message_ttl=timedelta(seconds=90, milliseconds=500))
    ttl_specs = [s for s in specs if "expireAfterSeconds" in _options(s)]
    assert ttl_specs[0][1]["expireAfterSeconds"] == 90


def test_bindings_spec_is_unique_topic_endpoint():
    keys, options = indexes.bindings_index_spec()
    assert keys == [("Topic", 1), ("EndpointId", 1)]
    assert options == {"unique": True}
