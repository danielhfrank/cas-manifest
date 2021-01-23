from pathlib import Path

from .dataset import ZipSerializable, CSVSerializable, NPYSerializable
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


def test_pd_registry(fs_instance):
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    # Another way to do it - "dump" the structure and its serialized form all to hashfs,
    # getting back just a hash string to reference it:
    addr = CSVSerializable.dump(df, fs_instance)
    # Now we can get this back using a Registry - we need this to know what classes to expect
    registry: SerializableRegistry[pd.DataFrame] = \
        SerializableRegistry(fs=fs_instance, classes=[CSVSerializable])
    # We can get the serialized form
    serialized = registry.load(addr.id)
    assert serialized.column_names == ['a', 'b']
    # Or we can load up the whole thing in one line
    with registry.open(addr.id) as df_2:
        pd.testing.assert_frame_equal(df, df_2)

    # Try another serialization format
    npy_addr = NPYSerializable.dump(df, fs_instance)
    # Create a new registry that will accept either serialization format
    registry_2: SerializableRegistry[pd.DataFrame] = \
        SerializableRegistry(fs=fs_instance, classes=[
                             CSVSerializable, NPYSerializable])
    # load the model
    with registry_2.open(npy_addr.id) as npy_df:
        pd.testing.assert_frame_equal(df, npy_df)
