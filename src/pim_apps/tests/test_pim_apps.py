import pytest
import pim_apps


def test_project_defines_author_and_version():
    assert hasattr(pim_apps, '__author__')
    assert hasattr(pim_apps, '__version__')
