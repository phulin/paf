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
from functools import cache
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
    cache_generation: int = 0
    changed_paths: tuple[str, ...] = ()
    promoted_cache_paths: tuple[str, ...] = ()
    out_of_scope_paths: tuple[str, ...] = ()
    error: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "accepted": self.accepted,
            "generation": self.generation,
            "cache_generation": self.cache_generation,
            "changed_paths": list(self.changed_paths),
            "promoted_cache_paths": list(self.promoted_cache_paths),
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


@cache
def _globstar_variants(pattern: str) -> tuple[str, ...]:
    """Expand `**/` as either zero or one-or-more path components.

    `fnmatch` treats `**` like an ordinary `*`, so its spelling with a
    following slash accidentally requires at least one child directory. Git
    and pathlib glob semantics allow zero directories as well.
    """

    variants = {pattern}
    pending = [pattern]
    while pending:
        candidate = pending.pop()
        start = 0
        while (index := candidate.find("**/", start)) >= 0:
            collapsed = candidate[:index] + candidate[index + 3 :]
            if collapsed not in variants:
                variants.add(collapsed)
                pending.append(collapsed)
            start = index + 3
    return tuple(sorted(variants))


def _matches_scope(relative: str, chapter: Chapter) -> bool:
    return any(
        fnmatch.fnmatchcase(relative, variant)
        for pattern in chapter.scope
        for variant in _globstar_variants(pattern)
    )


def _is_cache_path(relative: str) -> bool:
    return _excluded(relative, (".lake", "lean/.lake"))


def _is_overlay_marker(relative: str) -> bool:
    return any(part.startswith(".wh.") for part in Path(relative).parts)


def cache_manifest(root: Path) -> dict[str, FileFingerprint]:
    return {
        path: fingerprint
        for path, fingerprint in tree_manifest(
            root,
            excluded=(".git", ".swarm", ".venv", ".pytest_cache"),
        ).items()
        if _is_cache_path(path) and not _is_overlay_marker(path)
    }


def _path_fingerprint(root: Path, relative: str) -> FileFingerprint | None:
    try:
        return _fingerprint(root / relative)
    except FileNotFoundError:
        return None


class SharedWorkspace:
    def __init__(self, repo: Path) -> None:
        self.root = repo

    async def collect(self, _chapter: Chapter, *, promote_cache: bool = False) -> IsolationResult:
        del promote_cache
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
        cache_generation: int,
        base: Path,
        cache: Path,
        upper: Path,
        work: Path,
        merged: Path,
    ) -> None:
        self.manager = manager
        self.slot = slot
        self.generation = generation
        self.cache_generation = cache_generation
        self.base = base
        self.cache = cache
        self.upper = upper
        self.work = work
        self.root = merged
        self.closed = False

    async def collect(self, chapter: Chapter, *, promote_cache: bool = False) -> IsolationResult:
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
                cache_generation=self.cache_generation,
                changed_paths=changed,
                out_of_scope_paths=outside,
                error="agent changed files outside its exclusive scope",
            )
        return await self.manager.import_changes(
            chapter,
            generation=self.generation,
            cache_generation=self.cache_generation,
            cache_root=self.cache,
            upper_root=self.upper,
            base_manifest=base,
            merged_manifest=merged,
            merged_root=self.root,
            changed=changed,
            promote_cache=promote_cache,
        )

    async def close(self) -> None:
        if not self.closed:
            self.closed = True
            await self.manager.release(self)


class FuseOverlayIsolation:
    name = "fuse-overlay"

    def __init__(self, settings: SwarmSettings) -> None:
        self.settings = settings
        self.identity = hashlib.sha256(str(settings.state_dir).encode()).hexdigest()[:12]
        self.parent = Path(tempfile.gettempdir()) / f"lastlib-swarm-{os.getuid()}"
        self.root = self.parent / f"{self.identity}-{os.getpid()}"
        self.generations = self.root / "source-generations"
        self.cache_generations = self.root / "cache-generations"
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
        self._cache_revision = 0
        self._cache_paths: dict[int, Path] = {}
        self._cache_references: dict[int, int] = {}

    async def prepare(self) -> None:
        if not fuse_overlay_available():
            raise ValueError(
                "fuse-overlay isolation requires fuse-overlayfs, fusermount3, rsync, and /dev/fuse"
            )
        await self._clean_stale_roots()
        self.generations.mkdir(parents=True, exist_ok=True)
        self.cache_generations.mkdir(parents=True, exist_ok=True)
        self.slots.mkdir(parents=True, exist_ok=True)

    async def _clean_stale_roots(self) -> None:
        """Reclaim mounts left by dead orchestrators for this state directory."""

        if not self.parent.exists():
            return
        for stale in self.parent.glob(f"{self.identity}-*"):
            if stale == self.root:
                continue
            try:
                pid = int(stale.name.rsplit("-", 1)[1])
                os.kill(pid, 0)
            except ProcessLookupError:
                pass
            except (PermissionError, ValueError):
                continue
            else:
                continue
            for merged in stale.glob("slots/*/merged"):
                if os.path.ismount(merged):
                    await self._unmount(merged)
            shutil.rmtree(stale)

    async def _unmount(self, path: Path) -> None:
        """Unmount normally, then detach a busy mount left by a dead worker."""

        try:
            await self._run("fusermount3", "-u", str(path))
        except RuntimeError as error:
            if "device or resource busy" not in str(error).casefold():
                raise
            await self._run("fusermount3", "-uz", str(path))

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
        command.extend(f"--exclude=/{path}" for path in self.excluded)
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

    async def _copy_cache(
        self,
        source: Path,
        destination: Path,
        *,
        link_destination: Path | None,
    ) -> None:
        command = [
            "rsync",
            "-a",
            "--prune-empty-dirs",
            "--include=*/",
            "--include=/.lake/***",
            "--include=/lean/.lake/***",
            "--exclude=*",
        ]
        if link_destination is not None:
            command.append(f"--link-dest={link_destination}")
        command.extend((f"{source}/", f"{destination}/"))
        await self._run(*command)

    async def _cache_generation(self) -> tuple[int, Path]:
        generation = self._cache_revision
        existing = self._cache_paths.get(generation)
        if existing is not None:
            return generation, existing
        destination = self.cache_generations / f"{generation:08d}"
        destination.mkdir(parents=True, exist_ok=False)
        await self._copy_cache(
            self.settings.repo,
            destination,
            link_destination=self.settings.repo,
        )
        self._cache_paths[generation] = destination
        self._cache_references[generation] = 0
        return generation, destination

    async def acquire(self, run_id: str) -> FuseWorkspace:
        slot = await self._available.get()
        generation: int | None = None
        cache_generation: int | None = None
        slot_root = self.slots / f"{slot:04d}-{run_id}"
        merged = slot_root / "merged"
        try:
            async with self._lock:
                generation, base = await self._generation()
                cache_generation, cache = await self._cache_generation()
                self._generation_references[generation] += 1
                self._cache_references[cache_generation] += 1
            upper = slot_root / "upper"
            work = slot_root / "work"
            for path in (upper, work, merged):
                path.mkdir(parents=True, exist_ok=False)
            await self._run(
                "fuse-overlayfs",
                "-o",
                f"lowerdir={base}:{cache},upperdir={upper},workdir={work}",
                str(merged),
            )
            if not os.path.ismount(merged):
                raise RuntimeError(f"fuse-overlayfs did not mount {merged}")
            return FuseWorkspace(
                self,
                slot=slot,
                generation=generation,
                cache_generation=cache_generation,
                base=base,
                cache=cache,
                upper=upper,
                work=work,
                merged=merged,
            )
        except Exception:
            if os.path.ismount(merged):
                await self._unmount(merged)
            if slot_root.exists():
                shutil.rmtree(slot_root)
            if generation is not None:
                async with self._lock:
                    self._generation_references[generation] -= 1
                    if cache_generation is not None:
                        self._cache_references[cache_generation] -= 1
            self._available.put_nowait(slot)
            raise

    async def import_changes(
        self,
        chapter: Chapter,
        *,
        generation: int,
        cache_generation: int,
        cache_root: Path,
        upper_root: Path,
        base_manifest: dict[str, FileFingerprint],
        merged_manifest: dict[str, FileFingerprint],
        merged_root: Path,
        changed: tuple[str, ...],
        promote_cache: bool,
    ) -> IsolationResult:
        async with self._lock:
            if changed:
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
                        cache_generation=cache_generation,
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
                    cache_generation=cache_generation,
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
                shutil.copy2(merged_root / relative, temporary)
                os.replace(temporary, destination)
            if changed:
                self._revision += 1
            promoted = (
                await self._promote_cache_locked(
                    base_root=cache_root,
                    merged_root=merged_root,
                    upper_root=upper_root,
                )
                if promote_cache
                else ()
            )
            return IsolationResult(
                accepted=True,
                generation=generation,
                cache_generation=cache_generation,
                changed_paths=changed,
                promoted_cache_paths=promoted,
            )

    async def _promote_cache_locked(
        self,
        *,
        base_root: Path,
        merged_root: Path,
        upper_root: Path,
    ) -> tuple[str, ...]:
        delta = cache_manifest(upper_root)
        candidates = tuple(
            sorted(
                path
                for path, fingerprint in delta.items()
                if fingerprint.kind == "file" and _path_fingerprint(base_root, path) != fingerprint
            )
        )
        if not candidates:
            return ()

        current_generation, current_root = await self._cache_generation()
        promotable = tuple(
            path
            for path in candidates
            if _path_fingerprint(current_root, path)
            in {
                _path_fingerprint(base_root, path),
                _path_fingerprint(merged_root, path),
            }
        )
        if not promotable:
            return ()

        next_generation = current_generation + 1
        destination = self.cache_generations / f"{next_generation:08d}"
        destination.mkdir(parents=True, exist_ok=False)
        await self._copy_cache(
            current_root,
            destination,
            link_destination=current_root,
        )
        for relative in promotable:
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(f".{target.name}.cache-{os.getpid()}")
            shutil.copy2(merged_root / relative, temporary)
            os.replace(temporary, target)

        self._cache_revision = next_generation
        self._cache_paths[next_generation] = destination
        self._cache_references[next_generation] = 0
        return promotable

    async def release(self, workspace: FuseWorkspace) -> None:
        try:
            if os.path.ismount(workspace.root):
                await self._unmount(workspace.root)
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
                self._cache_references[workspace.cache_generation] -= 1
                if (
                    workspace.cache_generation != self._cache_revision
                    and self._cache_references[workspace.cache_generation] == 0
                ):
                    cache_path = self._cache_paths.pop(workspace.cache_generation)
                    self._cache_references.pop(workspace.cache_generation)
                    shutil.rmtree(cache_path)
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
