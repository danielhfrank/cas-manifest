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
from cas_manifest.registerable import Registerable, Serde


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
        return pd.read_csv(addr.abspath, names=self.column_names)


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


class CSVSerializer(Serde[pd.DataFrame, CSVDataset]):

    def serialize(self, df: pd.DataFrame) -> CSVDataset:
        buf = StringIO()
        df.to_csv(buf, header=False, index=False)
        buf.seek(0)
        addr = self.fs.put(buf)
        return CSVDataset(path=Ref(addr.id), column_names=df.columns.tolist())

    def deserialize(self, dried: CSVDataset) -> pd.DataFrame:
        return dried.load_from(self.fs)
