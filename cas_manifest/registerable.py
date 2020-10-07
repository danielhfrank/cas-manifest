from abc import ABC, abstractmethod
from io import StringIO
import json

from typing import TypeVar, Generic

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


Hydrated = TypeVar('Hydrated')
Dried = TypeVar('Dried', bound=Registerable)


class Serializer(Generic[Hydrated, Dried], BaseModel, ABC):

    class Config:
        arbitrary_types_allowed = True

    fs: HashFS

    @abstractmethod
    def serialize(self, obj: Hydrated) -> Dried:
        pass
