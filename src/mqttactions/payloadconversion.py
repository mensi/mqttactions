import json
from typing import Callable, Dict, Union

_converters = []


def converter(func):
    _converters.append(func)
    return func


@converter
def to_bytes(payload: bytes) -> bytes:
    return payload


@converter
def to_str(payload: bytes) -> str:
    return payload.decode('utf8')


@converter
def to_dict(payload: bytes) -> dict:
    return json.loads(payload)


converter_by_type: Dict[type, Callable] = {}
for converter in _converters:
    return_type = converter.__annotations__['return']
    assert converter.__name__ == f'to_{return_type.__name__}', "Incorrectly named converter method."
    converter_by_type[return_type] = converter

ConvertedPayload = Union[bytes, str, dict]
