from dataclasses import dataclass

from .core import (
    DEFAULT_MAX_DECOMPRESSED_BYTES,
    DEFAULT_THRESHOLD_BYTES,
    AsyncClaimCheckProvider,
    ClaimCheckProvider,
)


@dataclass
class ClaimCheckConfig:
    provider: ClaimCheckProvider | AsyncClaimCheckProvider
    enabled: bool = False
    threshold_bytes: int = DEFAULT_THRESHOLD_BYTES
    compress: bool = False
    max_decompressed_bytes: int = DEFAULT_MAX_DECOMPRESSED_BYTES
