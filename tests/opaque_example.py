from __future__ import annotations

from io import BytesIO
import random
import pickle

from hashfs import HashFS

from cas_manifest.ref import Ref
from cas_manifest.registerable import Serializable


class OpaqueObject():

    def __init__(self):
        self.a = random.randint(0, 100)


class OpaqueSerializable(Serializable[OpaqueObject]):

    pickle_ref: Ref

    def open(self, fs: HashFS) -> OpaqueObject:
        with fs.open(self.pickle_ref.hash_str) as f:
            loaded = pickle.load(f)
            if not isinstance(loaded, OpaqueObject):
                raise ValueError('boom')
            return loaded

    @classmethod
    def save(cls, inst, fs) -> OpaqueSerializable:
        buf = BytesIO()
        pickle.dump(inst, buf)
        buf.seek(0)
        addr = fs.put(buf)
        return OpaqueSerializable(pickle_ref=Ref(addr.id))
