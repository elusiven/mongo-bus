def test_top_level_exports_config_and_gridfs():
    from mongobus import (
        AsyncGridFsClaimCheckProvider,
        ClaimCheckConfig,
        GridFsClaimCheckProvider,
    )

    assert ClaimCheckConfig is not None
    assert GridFsClaimCheckProvider.name == "gridfs"
    assert AsyncGridFsClaimCheckProvider.name == "gridfs"


def test_importing_mongobus_does_not_require_boto3(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def deny_boto3(name, *args, **kwargs):
        if name == "boto3" or name.startswith("boto3."):
            raise AssertionError("boto3 must not be imported at package import time")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", deny_boto3)
    import importlib

    import mongobus

    importlib.reload(mongobus)  # re-import under the deny hook
