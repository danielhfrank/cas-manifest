from io import StringIO, BytesIO
from pathlib import Path
from typing import Optional, Union

from botocore.client import BaseClient
from botocore.exceptions import ClientError
from hashfs import HashFS, HashAddress
from pydantic.dataclasses import dataclass


@dataclass
class S3CasInfo:
    bucket: str
    prefix: str


class S3HashFS(HashFS):

    def __init__(self, local_path: Path, s3_conn: BaseClient, s3_cas_info: S3CasInfo):
        super().__init__(local_path, depth=1, width=2)
        self.local_path = local_path
        self.s3_conn = s3_conn
        self.s3_cas_info = s3_cas_info

    def _make_s3_path(self, hash_str: str) -> str:
        sharded_path = super().shard(hash_str)
        return f'{self.s3_cas_info.prefix}/{"/".join(sharded_path)}'

    def get(self, file) -> Optional[HashAddress]:
        if not super().exists(file):
            # Get the object from s3
            key = self._make_s3_path(file)
            expected_local_path = Path(super().idpath(file))
            expected_local_path.parent.mkdir(exist_ok=True)
            try:
                self.s3_conn.download_file(self.s3_cas_info.bucket, key, str(expected_local_path))
            except ClientError as e:
                if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
                    # Key not found, return `None` to conform to HashFS api
                    return None
                else:
                    raise
        return super().get(file)

    def open(self, file, mode='rb') -> Union[StringIO, BytesIO]:
        # First, call `get` to ensure that we have a local copy, then `open` from super
        hash_addr = self.get(file)
        if hash_addr is None:
            raise IOError(f"Not found: {file}")
        else:
            return super().open(hash_addr.id, mode=mode)

    def put(self, file) -> HashAddress:
        # First put the file in the local cache, from which we'll get its hash addr
        hash_addr = super().put(file)
        s3_key = self._make_s3_path(hash_addr.id)
        local_path = super().realpath(hash_addr.id)
        self.s3_conn.upload_file(local_path, self.s3_cas_info.bucket, s3_key)
        return hash_addr
