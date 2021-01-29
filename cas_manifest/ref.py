from hashfs import HashAddress
from pydantic import validator
from pydantic.validators import str_validator
from pydantic.dataclasses import dataclass


@dataclass
class Ref:

    hash_str: str

    @validator('hash_str', pre=True)
    def validate_hash_str(cls, v):
        if isinstance(v, HashAddress):
            return v.id
        else:
            return str_validator(v)
