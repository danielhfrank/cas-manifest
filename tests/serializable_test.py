from pathlib import Path

from .dataset import ZipSerializable, CSVSerializable
from .opaque_example import OpaqueObject, OpaqueSerializable

import pandas as pd


from cas_manifest.ref import Ref
from cas_manifest.registry import SerializableRegistry


def test_serde(fs_instance):
    obj = OpaqueObject()

    addr = OpaqueSerializable.dump(obj, fs_instance)

    registry = SerializableRegistry(
        fs=fs_instance, classes=[OpaqueSerializable])
    with registry.open(addr.id) as loaded_obj:
        assert(obj.a == loaded_obj.a)


def test_zip_serializable(zip_addr, fs_instance):
    # First, take a pre-built zipfile (from contents in memory) and save to the fs
    # See conftest.py for construction of zip_addr
    zs = ZipSerializable(path=Ref(zip_addr.id))
    zs_addr = zs.self_dump(fs_instance)

    registry: SerializableRegistry[Path] = \
        SerializableRegistry(fs=fs_instance, classes=[ZipSerializable])

    # Extract the zipfile to a tmpdir path
    with registry.open(zs_addr.id) as tmpdir_path:
        # And assert that its contents are as expected
        assert_zip_contents(tmpdir_path)

        # Now, use ZipSerializable.dump to exercise a different path for saving the zipfile
        zs_addr_2 = ZipSerializable.dump(tmpdir_path, fs_instance)

    # And now extract that again and check that the contents are as expected
    # The hashes will be different due to differences in e.g. metadata of file creation time
    with registry.open(zs_addr_2.id) as tmpdir_path:
        assert_zip_contents(tmpdir_path)


def assert_zip_contents(tmpdir_path: Path):
    assert(tmpdir_path.is_dir())
    assert(set(tmpdir_path.iterdir()) == {tmpdir_path / 'df.txt'})
    assert((tmpdir_path / 'df.txt').read_text() == 'roolz')


def test_csv_serializable(fs_instance):
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    # One way to do it: "pack" the object to get back a serialized class
    serialized = CSVSerializable.pack(df, fs_instance)
    # This allows us to introspect serialized metadata
    assert serialized.column_names == ['a', 'b']
    # And we can get the original object back
    df_2 = serialized.unpack(fs_instance)
    pd.testing.assert_frame_equal(df, df_2)
