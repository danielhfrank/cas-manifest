from io import StringIO, BytesIO
from pathlib import Path
import re
from typing import Optional, Union

from botocore.client import BaseClient
from botocore.exceptions import ClientError
from hashfs import HashFS, HashAddress
from pydantic.dataclasses import dataclass


@dataclass
class S3CasInfo:
    bucket: str
    prefix: str


def is_key_not_found(error: ClientError):
    return error.response['ResponseMetadata']['HTTPStatusCode'] == 404


def get_key_from_response(response: dict) -> Optional[str]:
    """Return the s3 key to download, from the response to a list_objects_v2 call
    Returns None if no key found, otherwise arbitrarily picks first one listed.
    """
    keys = [contents['Key'] for contents in response.get('Contents', [])]
    if len(keys) == 0:
        return None
    else:
        return keys[0]


def get_extension(s3_key: str) -> Optional[str]:
    match = re.match(r'.*(\.\w+)\Z', s3_key)
    if match is None:
        return None
    else:
        return match.groups()[0]


def normalize_extension(extension: Optional[str]) -> str:
    if extension is None:
        return ''
    else:
        if extension.startswith('.'):
            return extension
        else:
            return f'.{extension}'


class S3HashFS(HashFS):

    def __init__(self, local_path: Path, s3_conn: BaseClient, s3_cas_info: S3CasInfo):
        super().__init__(local_path, depth=1, width=2)
        self.local_path = local_path
        self.s3_conn = s3_conn
        self.s3_cas_info = s3_cas_info

    def _make_s3_path(self, hash_str: str, extension: str = None) -> str:
        sharded_path = super().shard(hash_str)
        extension_str = normalize_extension(extension)
        return f'{self.s3_cas_info.prefix}/{"/".join(sharded_path)}{extension_str}'

    def _get_key_to_download(self, expected_key) -> Optional[str]:
        resp = self.s3_conn.list_objects_v2(Bucket=self.s3_cas_info.bucket,
                                            Prefix=expected_key)
        return get_key_from_response(resp)

    def _check_remote_key_exists(self, key) -> bool:
        try:
            self.s3_conn.head_object(Bucket=self.s3_cas_info.bucket, Key=key)
        except ClientError as client_error:
            # Check to see if this was a 404
            if client_error.response.get('Error', {}).get('Code') == '404':
                # Yes, the string '404'
                return False
            else:
                raise
        return True

    def get(self, file) -> Optional[HashAddress]:
        if not super().exists(file):
            # Get the object from s3
            expected_key = self._make_s3_path(file)

            key = self._get_key_to_download(expected_key)
            if key is None:
                # Key not found, return `None` to conform to HashFS api
                return None

            key_extension = get_extension(key)
            expected_local_path = Path(super().idpath(file, extension=key_extension))
            expected_local_path.parent.mkdir(parents=True, exist_ok=True)

            self.s3_conn.download_file(self.s3_cas_info.bucket, key, str(expected_local_path))
        return super().get(file)

    def open(self, file, mode='rb') -> Union[StringIO, BytesIO]:
        # First, call `get` to ensure that we have a local copy, then `open` from super
        hash_addr = self.get(file)
        if hash_addr is None:
            raise IOError(f"Not found: {file}")
        else:
            return super().open(hash_addr.id, mode=mode)

    def put(self, file, extension=None) -> HashAddress:
        # First put the file in the local cache, from which we'll get its hash addr
        hash_addr = super().put(file, extension=extension)
        s3_key = self._make_s3_path(hash_addr.id, extension=extension)
        local_path = super().realpath(hash_addr.id)
        # Now, see if the remote store has the object
        if not self._check_remote_key_exists(s3_key):
            # and if not, upload it
            self.s3_conn.upload_file(local_path, self.s3_cas_info.bucket, s3_key)
        return hash_addr
