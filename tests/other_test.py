from .opaque_example import OpaqueObject, OpaqueSerializable

from cas_manifest.registry import SerializableRegistry


def test_serde(fs_instance):
    obj = OpaqueObject()

    addr = OpaqueSerializable.dump(obj, fs_instance)

    registry = SerializableRegistry(fs=fs_instance, classes=[OpaqueSerializable])
    with registry.open(addr.id) as loaded_obj:
        assert(obj.a == loaded_obj.a)
