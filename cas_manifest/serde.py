from abc import ABC, abstractmethod
from typing import TypeVar, Generic

from hashfs import HashFS


Hydrated = TypeVar('Hydrated')
Dried = TypeVar('Dried')


class Serde(Generic[Hydrated, Dried], ABC):

    @abstractmethod
    def serialize(self, inst: Hydrated, fs: HashFS) -> Dried:
        pass

    @abstractmethod
    def deserialize(self, dried: Dried, fs: HashFS) -> Hydrated:
        pass
