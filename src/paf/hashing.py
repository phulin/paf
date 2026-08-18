from __future__ import annotations

import hashlib
from pathlib import Path

import xxhash

ALGORITHM = "xxh3-64"
STABLE_ALGORITHM = "sha256"


def new_digest() -> xxhash.xxh3_64:
    return xxhash.xxh3_64()


def digest_bytes(value: bytes) -> str:
    return xxhash.xxh3_64_hexdigest(value)


def digest_text(value: str) -> str:
    return digest_bytes(value.encode())


def stable_digest_bytes(value: bytes) -> str:
    """Hash a durable identity whose value must survive implementation changes."""

    return hashlib.sha256(value).hexdigest()


def stable_digest_text(value: str) -> str:
    return stable_digest_bytes(value.encode())


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


def stable_digest_fields(domain: str, *fields: str | bytes) -> str:
    digest = hashlib.sha256()
    digest.update(domain.encode())
    digest.update(b"\0")
    for field in fields:
        value = field.encode() if isinstance(field, str) else field
        digest.update(len(value).to_bytes(8, "big"))
        digest.update(value)
    return digest.hexdigest()


def tagged_digest_bytes(value: bytes) -> str:
    return f"{ALGORITHM}:{digest_bytes(value)}"


def tagged_digest_text(value: str) -> str:
    return tagged_digest_bytes(value.encode())


def is_legacy_digest(value: object) -> bool:
    return isinstance(value, str) and (value.startswith(f"{STABLE_ALGORITHM}:") or len(value) == 64)


def migrate_digest_bytes(stored: object, value: bytes) -> str | None:
    """Return the canonical XXH digest when a stored current or legacy digest matches."""

    if not isinstance(stored, str):
        return None
    current = tagged_digest_bytes(value)
    if stored in {current, current.removeprefix(f"{ALGORITHM}:")}:
        return current
    legacy = stable_digest_bytes(value)
    if stored in {legacy, f"{STABLE_ALGORITHM}:{legacy}"}:
        return current
    return None


def migrate_digest_text(stored: object, value: str) -> str | None:
    return migrate_digest_bytes(stored, value.encode())
