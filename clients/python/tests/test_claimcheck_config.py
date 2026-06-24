from mongobus.claimcheck import core
from mongobus.claimcheck.config import ClaimCheckConfig


class _StubProvider:
    name = "stub"

    def put(self, data, *, content_type, metadata):
        return core.ClaimCheckReference(provider="stub", container="c", key="k", length=len(data))

    def open_read(self, reference):
        return b""


def test_config_defaults_match_spec():
    cfg = ClaimCheckConfig(provider=_StubProvider())
    assert cfg.enabled is False
    assert cfg.threshold_bytes == core.DEFAULT_THRESHOLD_BYTES
    assert cfg.compress is False
    assert cfg.max_decompressed_bytes == core.DEFAULT_MAX_DECOMPRESSED_BYTES


def test_stub_provider_satisfies_protocol():
    provider: core.ClaimCheckProvider = _StubProvider()
    ref = provider.put(b"hello", content_type="application/json", metadata=None)
    assert ref.key == "k"
