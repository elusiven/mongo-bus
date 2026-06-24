from mongobus import context
from mongobus.context import ConsumeContext


def _ctx():
    env = {"id": "ce-1", "type": "OrderPlaced", "source": "s",
           "correlationId": "corr-1", "causationId": "cause-0", "subject": "sub",
           "data": {"orderId": "123"}}
    raw = {"Attempt": 2}
    return ConsumeContext.from_message(env, raw)


def test_from_message_maps_envelope_fields():
    ctx = _ctx()
    assert ctx.cloud_event_id == "ce-1"
    assert ctx.type_id == "OrderPlaced"
    assert ctx.correlation_id == "corr-1"
    assert ctx.causation_id == "cause-0"
    assert ctx.subject == "sub"
    assert ctx.source == "s"
    assert ctx.attempt == 2
    assert ctx.data == {"orderId": "123"}


def test_current_context_is_none_by_default():
    assert context.current_context() is None


def test_use_context_sets_and_resets():
    ctx = _ctx()
    with context.use_context(ctx):
        assert context.current_context() is ctx
    assert context.current_context() is None


def test_resolve_correlation_prefers_explicit():
    assert context.resolve_correlation_id("explicit") == "explicit"


def test_resolve_correlation_falls_back_to_context():
    with context.use_context(_ctx()):
        assert context.resolve_correlation_id(None) == "corr-1"


def test_resolve_correlation_generates_when_no_context():
    cid = context.resolve_correlation_id(None)
    assert len(cid) == 32


def test_resolve_causation_uses_context_cloud_event_id():
    with context.use_context(_ctx()):
        assert context.resolve_causation_id(None) == "ce-1"


def test_resolve_causation_is_none_without_context():
    assert context.resolve_causation_id(None) is None


def test_resolve_causation_prefers_explicit():
    # Covers the `return explicit` branch (line 63 in context.py).
    assert context.resolve_causation_id("explicit-cause") == "explicit-cause"
