"""MongoBus Python client — wire-compatible with the .NET MongoBus implementation."""

from ._sync.bus import MongoBus

__version__ = "0.1.0"
__all__ = ["MongoBus", "__version__"]
