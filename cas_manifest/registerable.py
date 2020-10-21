from __future__ import annotations

from abc import ABC, abstractmethod
from io import StringIO
import json

from typing import Type, TypeVar, Generic

from hashfs import HashFS, HashAddress
from pydantic import BaseModel
from pydantic.json import pydantic_encoder


class Registerable(BaseModel):

    @property
    def exclude_fields(self):
        return set()

    def self_dump(self, fs: HashFS) -> HashAddress:
        json_repr = json.dumps({
            'class': self.schema()['title'],
            'value': self.dict(exclude=self.exclude_fields)
        }, default=pydantic_encoder)

        buf = StringIO(json_repr)
        return fs.put(buf)


Deserialized = TypeVar('Deserialized')

S = TypeVar('S', bound='Serializable')


class Serializable(Generic[Deserialized], Registerable, ABC):

    @abstractmethod
    def unpack(self, fs: HashFS) -> Deserialized:
        pass

    @classmethod
    @abstractmethod
    def pack(cls: Type[S], inst: Deserialized, fs: HashFS) -> S:
        pass

    @classmethod
    def dump(cls, inst: Deserialized, fs: HashFS) -> HashAddress:
        packed = cls.pack(inst, fs)
        return packed.self_dump(fs)

    @classmethod
    def close(cls, inst: Deserialized) -> None:
        """Optionally close resources associated with Deserialized instance.

        :param inst: Whatever is deserialized and used by the application
        :type inst: Deserialized
        """
        pass
