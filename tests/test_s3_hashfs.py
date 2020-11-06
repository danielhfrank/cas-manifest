from io import StringIO
import os
from pathlib import Path
import tempfile

import boto3
from moto import mock_s3
import pytest

from cas_manifest.s3_hashfs import S3HashFS, S3CasInfo

BUCKET = 'facet-models-test'


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'


@pytest.fixture(scope='function')
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3', region_name='us-east-1')


@pytest.fixture
def s3_conn(s3):
    s3.create_bucket(Bucket=BUCKET)
    yield s3


@pytest.fixture
def fs(s3_conn, tmpdir):
    cas_info = S3CasInfo(BUCKET, 'cas')
    yield S3HashFS(Path(tmpdir), s3_conn, cas_info)


def test_s3_hashfs(fs, s3_conn):
    contents = "DFDFDF"
    buf = StringIO(contents)
    buf.seek(0)
    addr = fs.put(buf)
    retrieved = fs.open(addr.id, mode='r').read()
    assert(retrieved == contents)
    with tempfile.TemporaryDirectory() as tmpdir2:
        # Create another fs instance with a different local dir, confirm we can read
        # the remote path
        fs2 = S3HashFS(Path(tmpdir2), s3_conn, fs.s3_cas_info)
        retrieved2 = fs2.open(addr.id, mode='r').read()
        assert(retrieved2 == contents)


def test_empty_s3_hashfs(fs):
    # First show that `get` will return None on a missing key instead of throwing an error
    get_res = fs.get('asdf')
    assert(get_res is None)
    # Next show that `open` will return an IOError
    with pytest.raises(IOError):
        fs.open('asdfasd')


def test_extensions(fs):
    contents = "DFDFDF"
    buf = StringIO(contents)
    buf.seek(0)
    put_addr = fs.put(buf, extension='.txt')
    # Remove from local cache to ensure that we send lookup to s3
    Path(put_addr.abspath).unlink()
    get_addr = fs.get(put_addr.id)
    assert(put_addr.abspath == get_addr.abspath)
