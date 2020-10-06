from io import StringIO
import json

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
