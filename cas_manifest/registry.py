import contextlib
import json

from hashfs import HashFS
from pydantic.dataclasses import dataclass

from typing import List, Type, TypeVar, Generic, Generator

from .registerable import Registerable, Serializable

T = TypeVar('T', bound=Registerable)


class ArbitraryTypeConfig:
    arbitrary_types_allowed = True


@dataclass(config=ArbitraryTypeConfig)
class Registry(Generic[T]):

    fs: HashFS
    classes: List[Type['T']]

    def load(self, hash_str: str) -> T:
        with self.fs.open(hash_str) as f:
            contents = json.load(f)
            try:
                class_title = contents['class']
                internal_registry = {cls.schema()['title']: cls for cls in self.classes}
                try:
                    klass = internal_registry[class_title]
                    return klass(**contents['value'])
                except KeyError:
                    known_classes = ','.join(internal_registry.keys())
                    raise ValueError(f'Not a recognized class: {class_title} ({known_classes})')
            except KeyError:
                raise ValueError(f'Not a serialized object: {hash_str}')


DeserializedBase = TypeVar('DeserializedBase')


class SerializableRegistry(Generic[DeserializedBase], Registry[Serializable[DeserializedBase]]):

    @contextlib.contextmanager
    def open(self, hash_str: str) -> Generator[DeserializedBase, None, None]:
        serialized = self.load(hash_str)
        deserialized = serialized.unpack(self.fs)
        try:
            yield deserialized
        finally:
            serialized.close(deserialized)
