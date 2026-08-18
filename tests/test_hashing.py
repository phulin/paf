from pathlib import Path

from paf.hashing import (
    ALGORITHM,
    digest_bytes,
    digest_fields,
    digest_file,
    digest_text,
    migrate_digest_bytes,
    stable_digest_bytes,
    tagged_digest_bytes,
)


def test_xxh3_64_digest_is_stable(tmp_path: Path) -> None:
    value = b"abc"
    path = tmp_path / "value.bin"
    path.write_bytes(value)

    assert ALGORITHM == "xxh3-64"
    assert digest_bytes(value) == "78af5f94892f3950"
    assert digest_text("abc") == digest_bytes(value)
    assert digest_file(path) == digest_bytes(value)


def test_field_digest_preserves_boundaries_and_domain() -> None:
    joined = digest_fields("domain", "ab", "c")

    assert len(joined) == 16
    assert joined != digest_fields("domain", "a", "bc")
    assert joined != digest_fields("other-domain", "ab", "c")


def test_legacy_sha_digest_migrates_only_when_content_matches() -> None:
    value = b"persisted content"
    current = tagged_digest_bytes(value)
    legacy = stable_digest_bytes(value)

    assert migrate_digest_bytes(current, value) == current
    assert migrate_digest_bytes(digest_bytes(value), value) == current
    assert migrate_digest_bytes(legacy, value) == current
    assert migrate_digest_bytes(f"sha256:{legacy}", value) == current
    assert migrate_digest_bytes(legacy, b"changed content") is None
