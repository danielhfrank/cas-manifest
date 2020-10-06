import tempfile

from hashfs import HashFS
import pytest
"""
Shared pytest fixtures
"""


@pytest.fixture(scope='module')
def fs_instance():
    with tempfile.TemporaryDirectory() as tmpdir:
        fs = HashFS(tmpdir, depth=1, width=2)
        yield fs
