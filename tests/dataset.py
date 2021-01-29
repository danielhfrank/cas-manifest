from __future__ import annotations

from abc import ABC, abstractmethod
import os
from pathlib import Path
import shutil
import tempfile
from typing import List, Optional
from zipfile import ZipFile

from hashfs import HashFS
import numpy as np
import pandas as pd

from cas_manifest.ref import Ref
from cas_manifest.registerable import Registerable, Serializable


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


class CSVSerializable(Serializable[pd.DataFrame]):

    column_names: List[str]
    path: Ref

    @classmethod
    def pack(cls, inst: pd.DataFrame, fs: HashFS) -> CSVSerializable:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir) / 'tmp.csv'
            with open(tmp_path, mode='w') as f:
                inst.to_csv(f, header=False, index=False)
            csv_addr = fs.put(tmp_path)
            return CSVSerializable(path=Ref(csv_addr.id), column_names=inst.columns.to_list())

    def unpack(self, fs: HashFS) -> pd.DataFrame:
        addr = fs.get(self.path.hash_str)
        df = pd.read_csv(addr.abspath, names=self.column_names)
        return df


class NPYSerializable(Serializable[pd.DataFrame]):

    column_names: List[str]
    path: Ref

    @classmethod
    def pack(cls, inst: pd.DataFrame, fs: HashFS) -> NPYSerializable:
        with tempfile.TemporaryFile() as f:
            np.save(f, inst.values)
            f.seek(0)
            addr = fs.put(f)
        return NPYSerializable(path=Ref(addr.id), column_names=inst.columns.to_list())

    def unpack(self, fs: HashFS) -> pd.DataFrame:
        addr = fs.get(self.path.hash_str)
        arr = np.load(addr.abspath)
        return pd.DataFrame(arr, columns=self.column_names)


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


class ZipSerializable(Serializable[Path]):

    path: Ref

    @classmethod
    def pack(cls, inst: Path, fs: HashFS) -> ZipSerializable:
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = Path(tmpdir) / 'tmp.zip'
            zf = ZipFile(zip_path, mode='w')
            for root, dirs, files in os.walk(inst):
                for file in files:
                    abs_path = Path(root) / file
                    rel_path = abs_path.relative_to(inst)
                    zf.write(abs_path, rel_path)
            zf.close()
            zip_addr = fs.put(zip_path)
            return ZipSerializable(path=Ref(zip_addr.id))

    def unpack(self, fs: HashFS) -> Path:
        addr = fs.get(self.path.hash_str)
        zf = ZipFile(addr.abspath)
        tmpdir = tempfile.mkdtemp()
        zf.extractall(tmpdir)
        return Path(tmpdir)

    @classmethod
    def close(cls, inst: Path):
        shutil.rmtree(inst)
