from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from paf.hashing import digest_file
from paf.interface_fingerprint import (
    ModuleFingerprint,
    _run_helper,
    aggregate_module_digests,
)


def _module(name: str, interface: str, artifact: str) -> ModuleFingerprint:
    return ModuleFingerprint(
        module=name,
        source=f"{name}.lean",
        artifact=f"{name}.olean",
        imports=(),
        artifact_digest=artifact,
        interface_digest=interface,
        declaration_count=1,
    )


def test_work_unit_aggregate_is_order_independent() -> None:
    first = _module("Example.First", "interface-1", "artifact-1")
    second = _module("Example.Second", "interface-2", "artifact-2")

    assert aggregate_module_digests((first, second), lean_version="lean") == (
        aggregate_module_digests((second, first), lean_version="lean")
    )


@pytest.mark.skipif(shutil.which("lake") is None, reason="Lean toolchain is unavailable")
def test_lean_helper_erases_proofs_but_retains_interfaces(tmp_path: Path) -> None:
    (tmp_path / "lean-toolchain").write_text("leanprover/lean4:v4.33.0\n", encoding="utf-8")
    (tmp_path / "lakefile.toml").write_text('name = "fingerprint_test"\n', encoding="utf-8")
    source = tmp_path / "InterfaceCase.lean"
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir()

    variants = {
        "proof-a": "theorem stable : True := by trivial\n",
        "proof-b": "theorem stable : True := by exact True.intro\n",
        "statement": "theorem stable : 1 = 1 := by rfl\n",
        "definition-a": "def exposed : Nat := 1\n",
        "definition-b": "def exposed : Nat := 2\n",
        "private-a": "private def hidden : Nat := 1\ntheorem stable : True := by trivial\n",
        "private-b": "private def hidden : Nat := 2\ntheorem stable : True := by trivial\n",
    }
    artifacts: dict[str, Path] = {}
    for name, text in variants.items():
        source.write_text(text, encoding="utf-8")
        artifact = artifact_dir / f"{name}.olean"
        completed = subprocess.run(
            ["lake", "env", "lean", "-o", str(artifact), str(source)],
            cwd=tmp_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        assert completed.returncode == 0, completed.stdout
        artifacts[name] = artifact

    requests = tuple(
        ("InterfaceCase", artifact, artifact_dir / f"{name}.sanitized.olean")
        for name, artifact in artifacts.items()
    )
    values = _run_helper(tmp_path, requests, timeout_seconds=120)
    assert all(value["module"] == "InterfaceCase" for value in values)
    digests = {name: digest_file(artifact_dir / f"{name}.sanitized.olean") for name in variants}
    assert digests["proof-a"] == digests["proof-b"]
    assert digests["proof-a"] != digests["statement"]
    assert digests["definition-a"] != digests["definition-b"]
    assert digests["private-a"] == digests["private-b"]
