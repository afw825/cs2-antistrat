import importlib

import pytest

pytestmark = pytest.mark.unit


def test_antistrat_package_importable() -> None:
    module = importlib.import_module("antistrat")
    assert module is not None
