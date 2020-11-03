from io import BytesIO
import tempfile
import zipfile

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


@pytest.fixture
def zip_addr(fs_instance):
    buf = BytesIO()
    zf = zipfile.ZipFile(buf, mode='w')
    zf.writestr('df.txt', 'roolz')
    zf.close()
    buf.seek(0)
    return fs_instance.put(buf)
