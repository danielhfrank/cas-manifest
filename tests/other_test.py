from .opaque_example import OpaqueObject, OpaqueSerializable

from cas_manifest.registry import SerializableRegistry


def test_serde(fs_instance):
    obj = OpaqueObject()

    # TODO should be able to combine these into a single method
    serializable = OpaqueSerializable.save(obj, fs_instance)
    addr = serializable.dump(fs_instance)

    registry = SerializableRegistry(fs=fs_instance, classes=[OpaqueSerializable])
    # TODO probably want some context manager stuff around this
    loaded_obj = registry.open(addr.id)
    assert(obj.a == loaded_obj.a)
