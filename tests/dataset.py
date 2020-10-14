from __future__ import annotations

from abc import ABC, abstractmethod
from io import StringIO
from pathlib import Path
import shutil
import tempfile
from typing import List, Optional
from zipfile import ZipFile

from hashfs import HashFS
import pandas as pd

from cas_manifest.ref import Ref
from cas_manifest.registerable import Registerable, Serializable
from cas_manifest.serde import Serde


class PandasDataset(Registerable):
    """
    Another impl of CSVDataset, but agnostic to its serde
    """

    path: Ref
    column_names: List[str]


class Dataset(Registerable, ABC):

    @abstractmethod
    def load_from(self, fs: HashFS):
        pass

    def close(self):
        pass


class CSVDataset(Dataset):

    path: Ref
    column_names: List[str]

    def load_from(self, fs: HashFS) -> pd.DataFrame:
        addr = fs.get(self.path.hash_str)
        df = pd.read_csv(addr.abspath, names=self.column_names)
        assert isinstance(df, pd.DataFrame)
        return df


class ZipDataset(Dataset):

    path: Ref

    tmpdir_path: Optional[Path] = None

    @property
    def exclude_fields(self):
        return {'tmpdir_path'}

    def load_from(self, fs: HashFS):
        addr = fs.get(self.path.hash_str)
        zf = ZipFile(addr.abspath)
        tmpdir = tempfile.mkdtemp()
        zf.extractall(tmpdir)
        self.tmpdir_path = Path(tmpdir)
        return self.tmpdir_path

    def close(self):
        if self.tmpdir_path and self.tmpdir_path.exists():
            shutil.rmtree(self.tmpdir_path)
        self.tmpdir_path = None


class CSVSerde(Serde[pd.DataFrame, PandasDataset]):

    def deserialize(self, dried: PandasDataset, fs: HashFS) -> pd.DataFrame:
        addr = fs.get(dried.path.hash_str)
        df = pd.read_csv(addr.abspath, names=dried.column_names)
        assert isinstance(df, pd.DataFrame)
        return df

    # @classmethod
    def serialize(self, inst: pd.DataFrame, fs: HashFS) -> PandasDataset:
        buf = StringIO()
        inst.to_csv(buf, header=False, index=False)
        buf.seek(0)
        addr = fs.put(buf)
        return PandasDataset(path=Ref(addr.id), column_names=inst.columns.tolist())
