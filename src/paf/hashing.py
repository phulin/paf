from __future__ import annotations

from pathlib import Path

import xxhash

ALGORITHM = "xxh3-64"


def new_digest() -> xxhash.xxh3_64:
    return xxhash.xxh3_64()


def digest_bytes(value: bytes) -> str:
    return xxhash.xxh3_64_hexdigest(value)


def digest_text(value: str) -> str:
    return digest_bytes(value.encode())


def digest_file(path: Path) -> str:
    digest = new_digest()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def digest_fields(domain: str, *fields: str | bytes) -> str:
    digest = new_digest()
    digest.update(domain.encode())
    digest.update(b"\0")
    for field in fields:
        value = field.encode() if isinstance(field, str) else field
        digest.update(len(value).to_bytes(8, "big"))
        digest.update(value)
    return digest.hexdigest()


def tagged_digest_bytes(value: bytes) -> str:
    return f"{ALGORITHM}:{digest_bytes(value)}"
