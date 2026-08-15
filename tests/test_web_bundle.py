from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from scripts import web_bundle

ROOT = Path(__file__).resolve().parents[1]


def copy_bundle_fixture(tmp_path: Path) -> Path:
    shutil.copytree(ROOT / "web", tmp_path / "web", ignore=shutil.ignore_patterns("node_modules"))
    shutil.copytree(ROOT / "src" / "paf" / "web_dist", tmp_path / "src" / "paf" / "web_dist")
    return tmp_path


def test_committed_web_bundle_is_fresh_and_hashed() -> None:
    web_bundle.check(ROOT)
    manifest = json.loads(
        (ROOT / "src" / "paf" / "web_dist" / web_bundle.MANIFEST_NAME).read_text()
    )

    assert "index.html" in manifest["files"]
    assets = [name for name in manifest["files"] if name.startswith("assets/")]
    assert assets
    assert all(web_bundle.HASHED_ASSET.fullmatch(name) for name in assets)


def test_freshness_check_detects_changed_frontend_input(tmp_path: Path) -> None:
    root = copy_bundle_fixture(tmp_path)
    source = root / "web" / "src" / "main.tsx"
    source.write_text(source.read_text() + "\n", encoding="utf-8")

    with pytest.raises(web_bundle.BundleError, match=r"changed input: web/src/main\.tsx"):
        web_bundle.check(root)


def test_freshness_check_detects_changed_or_untracked_bundle_file(tmp_path: Path) -> None:
    root = copy_bundle_fixture(tmp_path)
    assets = root / "src" / "paf" / "web_dist" / "assets"
    first_asset = next(assets.iterdir())
    first_asset.write_bytes(first_asset.read_bytes() + b"corrupt")
    (assets / "extra-12345678.js").write_text("", encoding="utf-8")

    with pytest.raises(web_bundle.BundleError) as raised:
        web_bundle.check(root)

    assert "changed bundle file" in str(raised.value)
    assert "new bundle file: assets/extra-12345678.js" in str(raised.value)
