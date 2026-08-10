from __future__ import annotations

from typing import Any

import orjson

JSONDecodeError = orjson.JSONDecodeError


def loads(value: bytes | bytearray | memoryview | str) -> Any:
    return orjson.loads(value)


def dumpb(value: Any, *, indent: bool = False, sort_keys: bool = False) -> bytes:
    option = 0
    if indent:
        option |= orjson.OPT_INDENT_2
    if sort_keys:
        option |= orjson.OPT_SORT_KEYS
    return orjson.dumps(value, option=option)


def dumps(value: Any, *, indent: int | None = None, sort_keys: bool = False) -> str:
    return dumpb(value, indent=indent is not None, sort_keys=sort_keys).decode()
