from pathlib import Path

from .dataset import ZipSerializable
from .opaque_example import OpaqueObject, OpaqueSerializable

from cas_manifest.ref import Ref
from cas_manifest.registry import SerializableRegistry


def test_serde(fs_instance):
    obj = OpaqueObject()

    addr = OpaqueSerializable.dump(obj, fs_instance)

    registry = SerializableRegistry(fs=fs_instance, classes=[OpaqueSerializable])
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

    # and now extract that again and check that the contents are as expected
    with registry.open(zs_addr_2.id) as tmpdir_path:
        assert_zip_contents(tmpdir_path)


def assert_zip_contents(tmpdir_path: Path):
    assert(tmpdir_path.is_dir())
    assert(set(tmpdir_path.iterdir()) == {tmpdir_path / 'df.txt'})
    assert((tmpdir_path / 'df.txt').read_text() == 'roolz')
