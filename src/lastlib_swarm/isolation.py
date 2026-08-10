from __future__ import annotations

import asyncio
import fnmatch
import hashlib
import os
import shutil
import stat
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

from lastlib_swarm.models import Chapter, SwarmSettings


@dataclass(frozen=True)
class FileFingerprint:
    kind: str
    digest: str
    mode: int


@dataclass(frozen=True)
class IsolationResult:
    accepted: bool
    generation: int
    changed_paths: tuple[str, ...] = ()
    out_of_scope_paths: tuple[str, ...] = ()
    error: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "accepted": self.accepted,
            "generation": self.generation,
            "changed_paths": list(self.changed_paths),
            "out_of_scope_paths": list(self.out_of_scope_paths),
            "error": self.error,
        }


def fuse_overlay_available() -> bool:
    return (
        Path("/dev/fuse").exists()
        and shutil.which("fuse-overlayfs") is not None
        and shutil.which("fusermount3") is not None
        and shutil.which("rsync") is not None
    )


def _excluded(relative: str, prefixes: tuple[str, ...]) -> bool:
    return any(relative == prefix or relative.startswith(prefix + "/") for prefix in prefixes)


def _fingerprint(path: Path) -> FileFingerprint:
    metadata = path.lstat()
    mode = stat.S_IMODE(metadata.st_mode)
    if stat.S_ISREG(metadata.st_mode):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        return FileFingerprint("file", digest, mode)
    if stat.S_ISLNK(metadata.st_mode):
        digest = hashlib.sha256(os.readlink(path).encode()).hexdigest()
        return FileFingerprint("symlink", digest, mode)
    return FileFingerprint("special", "", mode)


def tree_manifest(root: Path, *, excluded: tuple[str, ...]) -> dict[str, FileFingerprint]:
    result: dict[str, FileFingerprint] = {}
    for directory, names, filenames in os.walk(root, followlinks=False):
        current = Path(directory)
        relative_directory = current.relative_to(root).as_posix()
        names[:] = [
            name
            for name in names
            if not _excluded(
                name if relative_directory == "." else f"{relative_directory}/{name}",
                excluded,
            )
        ]
        for name in (*names, *filenames):
            path = current / name
            relative = path.relative_to(root).as_posix()
            if _excluded(relative, excluded) or path.is_dir():
                continue
            result[relative] = _fingerprint(path)
    return result


def _matches_scope(relative: str, chapter: Chapter) -> bool:
    return any(fnmatch.fnmatchcase(relative, pattern) for pattern in chapter.scope)


class SharedWorkspace:
    def __init__(self, repo: Path) -> None:
        self.root = repo

    async def collect(self, _chapter: Chapter) -> IsolationResult:
        return IsolationResult(accepted=True, generation=0)

    async def close(self) -> None:
        return


class SharedIsolation:
    name = "shared"

    def __init__(self, settings: SwarmSettings) -> None:
        self.settings = settings

    async def prepare(self) -> None:
        return

    async def acquire(self, _run_id: str) -> SharedWorkspace:
        return SharedWorkspace(self.settings.repo)

    async def close(self) -> None:
        return


class FuseWorkspace:
    def __init__(
        self,
        manager: FuseOverlayIsolation,
        *,
        slot: int,
        generation: int,
        base: Path,
        upper: Path,
        work: Path,
        merged: Path,
    ) -> None:
        self.manager = manager
        self.slot = slot
        self.generation = generation
        self.base = base
        self.upper = upper
        self.work = work
        self.root = merged
        self.closed = False

    async def collect(self, chapter: Chapter) -> IsolationResult:
        base = tree_manifest(self.base, excluded=self.manager.excluded)
        merged = tree_manifest(self.root, excluded=self.manager.excluded)
        changed = tuple(
            sorted(path for path in set(base) | set(merged) if base.get(path) != merged.get(path))
        )
        outside = tuple(path for path in changed if not _matches_scope(path, chapter))
        if outside:
            return IsolationResult(
                accepted=False,
                generation=self.generation,
                changed_paths=changed,
                out_of_scope_paths=outside,
                error="agent changed files outside its exclusive scope",
            )
        return await self.manager.import_changes(
            chapter,
            generation=self.generation,
            base_manifest=base,
            merged_manifest=merged,
            merged_root=self.root,
            changed=changed,
        )

    async def close(self) -> None:
        if not self.closed:
            self.closed = True
            await self.manager.release(self)


class FuseOverlayIsolation:
    name = "fuse-overlay"

    def __init__(self, settings: SwarmSettings) -> None:
        self.settings = settings
        identity = hashlib.sha256(str(settings.state_dir).encode()).hexdigest()[:12]
        self.root = (
            Path(tempfile.gettempdir())
            / f"lastlib-swarm-{os.getuid()}"
            / f"{identity}-{os.getpid()}"
        )
        self.generations = self.root / "generations"
        self.slots = self.root / "slots"
        excluded = {
            ".git",
            ".swarm",
            ".venv",
            ".pytest_cache",
            ".lake",
            "lean/.lake",
        }
        with suppress(ValueError):
            excluded.add(settings.state_dir.relative_to(settings.repo).as_posix())
        self.excluded = tuple(sorted(excluded))
        self._available: asyncio.Queue[int] = asyncio.Queue()
        for slot in range(settings.max_agents):
            self._available.put_nowait(slot)
        self._lock = asyncio.Lock()
        self._revision = 0
        self._generation_paths: dict[int, Path] = {}
        self._generation_references: dict[int, int] = {}

    async def prepare(self) -> None:
        if not fuse_overlay_available():
            raise ValueError(
                "fuse-overlay isolation requires fuse-overlayfs, fusermount3, rsync, and /dev/fuse"
            )
        if self.settings.bypass_approvals_and_sandbox:
            raise ValueError(
                "fuse-overlay isolation requires the Codex workspace sandbox; "
                "disable bypass_approvals_and_sandbox"
            )
        if self.settings.sandbox != "workspace-write":
            raise ValueError(
                "fuse-overlay isolation requires sandbox = 'workspace-write' "
                "to confine Codex to the mounted workspace"
            )
        self.generations.mkdir(parents=True, exist_ok=True)
        self.slots.mkdir(parents=True, exist_ok=True)

    async def _run(self, *command: str) -> None:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        output, _ = await process.communicate()
        if process.returncode:
            raise RuntimeError(
                f"{' '.join(command[:1])} failed ({process.returncode}): "
                f"{output.decode(errors='replace')[-4000:]}"
            )

    async def _generation(self) -> tuple[int, Path]:
        generation = self._revision
        existing = self._generation_paths.get(generation)
        if existing is not None:
            return generation, existing
        destination = self.generations / f"{generation:08d}"
        destination.mkdir(parents=True, exist_ok=False)
        command = ["rsync", "-a"]
        command.extend(f"--exclude=/{path}" for path in self.excluded if ".lake" not in path)
        command.extend(
            (
                f"--link-dest={self.settings.repo}",
                f"{self.settings.repo}/",
                f"{destination}/",
            )
        )
        await self._run(*command)
        self._generation_paths[generation] = destination
        self._generation_references[generation] = 0
        return generation, destination

    async def acquire(self, run_id: str) -> FuseWorkspace:
        slot = await self._available.get()
        generation: int | None = None
        slot_root = self.slots / f"{slot:04d}-{run_id}"
        merged = slot_root / "merged"
        try:
            async with self._lock:
                generation, base = await self._generation()
                self._generation_references[generation] += 1
            upper = slot_root / "upper"
            work = slot_root / "work"
            for path in (upper, work, merged):
                path.mkdir(parents=True, exist_ok=False)
            await self._run(
                "fuse-overlayfs",
                "-o",
                f"lowerdir={base},upperdir={upper},workdir={work}",
                str(merged),
            )
            if not os.path.ismount(merged):
                raise RuntimeError(f"fuse-overlayfs did not mount {merged}")
            return FuseWorkspace(
                self,
                slot=slot,
                generation=generation,
                base=base,
                upper=upper,
                work=work,
                merged=merged,
            )
        except Exception:
            if os.path.ismount(merged):
                await self._run("fusermount3", "-u", str(merged))
            if slot_root.exists():
                shutil.rmtree(slot_root)
            if generation is not None:
                async with self._lock:
                    self._generation_references[generation] -= 1
            self._available.put_nowait(slot)
            raise

    async def import_changes(
        self,
        chapter: Chapter,
        *,
        generation: int,
        base_manifest: dict[str, FileFingerprint],
        merged_manifest: dict[str, FileFingerprint],
        merged_root: Path,
        changed: tuple[str, ...],
    ) -> IsolationResult:
        if not changed:
            return IsolationResult(accepted=True, generation=generation)
        async with self._lock:
            current = tree_manifest(self.settings.repo, excluded=self.excluded)
            base_scope = {
                path: value
                for path, value in base_manifest.items()
                if _matches_scope(path, chapter)
            }
            current_scope = {
                path: value for path, value in current.items() if _matches_scope(path, chapter)
            }
            if base_scope != current_scope:
                return IsolationResult(
                    accepted=False,
                    generation=generation,
                    changed_paths=changed,
                    error=(
                        "assigned scope changed after this agent started; "
                        "retry on a fresh generation"
                    ),
                )
            unsupported = [
                relative
                for relative in changed
                if (fingerprint := merged_manifest.get(relative)) is not None
                and fingerprint.kind != "file"
            ]
            if unsupported:
                return IsolationResult(
                    accepted=False,
                    generation=generation,
                    changed_paths=changed,
                    error=f"unsupported changed file type: {unsupported[0]}",
                )
            for relative in changed:
                destination = self.settings.repo / relative
                fingerprint = merged_manifest.get(relative)
                if fingerprint is None:
                    destination.unlink(missing_ok=True)
                    continue
                destination.parent.mkdir(parents=True, exist_ok=True)
                temporary = destination.with_name(f".{destination.name}.swarm-{os.getpid()}")
                shutil.copyfile(merged_root / relative, temporary)
                temporary.chmod(fingerprint.mode)
                os.replace(temporary, destination)
            self._revision += 1
            return IsolationResult(
                accepted=True,
                generation=generation,
                changed_paths=changed,
            )

    async def release(self, workspace: FuseWorkspace) -> None:
        try:
            if os.path.ismount(workspace.root):
                await self._run("fusermount3", "-u", str(workspace.root))
            slot_root = workspace.root.parent
            shutil.rmtree(slot_root, ignore_errors=False)
        finally:
            async with self._lock:
                self._generation_references[workspace.generation] -= 1
                if (
                    workspace.generation != self._revision
                    and self._generation_references[workspace.generation] == 0
                ):
                    path = self._generation_paths.pop(workspace.generation)
                    self._generation_references.pop(workspace.generation)
                    shutil.rmtree(path)
            self._available.put_nowait(workspace.slot)

    async def close(self) -> None:
        if self._available.qsize() != self.settings.max_agents:
            raise RuntimeError("cannot close isolation while workspaces are active")
        if self.root.exists():
            shutil.rmtree(self.root)


IsolationManager = SharedIsolation | FuseOverlayIsolation
Workspace = SharedWorkspace | FuseWorkspace


def create_isolation(settings: SwarmSettings) -> IsolationManager:
    backend = settings.isolation
    if backend == "auto":
        backend = "fuse-overlay" if fuse_overlay_available() else "shared"
    if backend == "fuse-overlay":
        return FuseOverlayIsolation(settings)
    if backend == "shared":
        return SharedIsolation(settings)
    raise ValueError(f"unknown isolation backend: {backend}")
