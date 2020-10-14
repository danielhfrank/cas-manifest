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

    def dump(self, fs: HashFS) -> HashAddress:
        json_repr = json.dumps({
            'class': self.schema()['title'],
            'value': self.dict(exclude=self.exclude_fields)
        }, default=pydantic_encoder)

        buf = StringIO(json_repr)
        return fs.put(buf)


Deserialized = TypeVar('Deserialized')
# Serialized = TypeVar

S = TypeVar('S', bound='Serializable')


class Serializable(Generic[Deserialized], Registerable, ABC):

    @abstractmethod
    def open(self, fs: HashFS) -> Deserialized:
        # requires a Serde[Deserialized, cls]
        pass

    @classmethod
    @abstractmethod
    def save(cls: Type[S], inst: Deserialized, fs: HashFS) -> S:
        pass
