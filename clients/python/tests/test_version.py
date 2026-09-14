import re
from pathlib import Path

import mongobus

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


def test_package_version_is_0_2_0_in_pyproject_and_module():
    declared = re.search(
        r'^version = "([^"]+)"$', PYPROJECT.read_text(encoding="utf-8"), re.MULTILINE
    ).group(1)
    assert declared == mongobus.__version__ == "0.2.0"
