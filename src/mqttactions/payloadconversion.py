import inspect
import json
from typing import Callable, Dict, Union, get_origin

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

PayloadFilter = Union[None, ConvertedPayload, Callable[[ConvertedPayload], bool]]


def matches_filter(payload: ConvertedPayload, payload_filter: PayloadFilter) -> bool:
    if payload_filter is None:
        return True
    elif callable(payload_filter):
        return payload_filter(payload)
    else:
        return payload == payload_filter


def get_filter_type(payload_filter: PayloadFilter) -> type:
    if payload_filter is None:
        return bytes

    if callable(payload_filter):
        params = inspect.signature(payload_filter).parameters
        if len(params) != 1:
            raise ValueError("Filter callback must take exactly one argument.")

        argtype = next(iter(params.values())).annotation
        if argtype is inspect._empty:
            # No type annotation, just give it the raw payload then...
            return bytes

        # Normalize any type annotations from typing
        if get_origin(argtype) is not None:
            argtype = get_origin(argtype)
        return argtype
    return payload_filter.__class__
