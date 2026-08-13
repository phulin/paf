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


def _mount_points() -> set[Path]:
    """Read mount paths without stat-ing potentially disconnected FUSE endpoints."""

    try:
        lines = Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines()
    except OSError:
        return set()
    return {
        Path(
            field.replace("\\040", " ")
            .replace("\\011", "\t")
            .replace("\\012", "\n")
            .replace("\\134", "\\")
        )
        for line in lines
        if len(fields := line.split()) >= 5
        for field in (fields[4],)
    }


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


def scoped_manifest(root: Path, chapter: Chapter) -> dict[str, FileFingerprint]:
    """Fingerprint only a chapter's exclusive scope in the live worktree."""

    result: dict[str, FileFingerprint] = {}
    for pattern in chapter.scope:
        for path in root.glob(pattern):
            if path.is_dir():
                continue
            relative = path.relative_to(root).as_posix()
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


class SharedWorkspace:
    def __init__(self, repo: Path) -> None:
        self.root = repo

    async def collect(
        self,
        _chapter: Chapter,
        *,
        integration_lock: asyncio.Lock | None = None,
    ) -> IsolationResult:
        del integration_lock
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

    async def refresh_cache(self) -> None:
        return

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

    async def collect(
        self,
        chapter: Chapter,
        *,
        integration_lock: asyncio.Lock | None = None,
    ) -> IsolationResult:
        # Hashing walks the entire source tree. On a large corpus it can
        # otherwise starve the TUI refresh timer just as the agent finishes.
        base, merged = await asyncio.gather(
            asyncio.to_thread(tree_manifest, self.base, excluded=self.manager.excluded),
            asyncio.to_thread(tree_manifest, self.root, excluded=self.manager.excluded),
        )
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
            base_manifest=base,
            merged_manifest=merged,
            merged_root=self.root,
            changed=changed,
            integration_lock=integration_lock,
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
        # Seed immutable source/cache generation zero before the scheduler can
        # launch either agents or coordinator builds. Later agent acquisition
        # never needs to copy the live writable Lake cache.
        async with self._lock:
            await self._generation()
            await self._cache_generation()

    async def _clean_stale_roots(self) -> None:
        """Reclaim mounts left by dead orchestrators for this state directory."""

        if not self.parent.exists():
            return
        mounted = _mount_points()
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
                # os.path.ismount() returns False when lstat() reports ENOTCONN, which is exactly
                # how a dead fuse-overlayfs mount presents. /proc/self/mountinfo remains readable.
                if merged in mounted or os.path.ismount(merged):
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
        # Source generations must not share inodes with the live worktree. A
        # hard-linked snapshot can change underneath an active FUSE mount when
        # another process edits the repository in place. The mount may retain
        # the old contents while a manifest scan of the lower directory sees
        # the new contents, falsely attributing the external edit to the agent.
        command.extend((f"{self.settings.repo}/", f"{destination}/"))
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

    async def refresh_cache(self) -> None:
        """Publish a read-only snapshot of the coordinator-owned main cache.

        Agents can read these immutable snapshots but their overlay writes are
        never promoted. The main worktree remains the only writable build cache.
        """

        async with self._lock:
            previous = self._cache_paths.get(self._cache_revision)
            next_generation = self._cache_revision + 1
            destination = self.cache_generations / f"{next_generation:08d}"
            destination.mkdir(parents=True, exist_ok=False)
            await self._copy_cache(
                self.settings.repo,
                destination,
                link_destination=previous or self.settings.repo,
            )
            self._cache_revision = next_generation
            self._cache_paths[next_generation] = destination
            self._cache_references[next_generation] = 0
            previous_generation = next_generation - 1
            if previous is not None and self._cache_references[previous_generation] == 0:
                self._cache_paths.pop(previous_generation)
                self._cache_references.pop(previous_generation)
                shutil.rmtree(previous)

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
        base_manifest: dict[str, FileFingerprint],
        merged_manifest: dict[str, FileFingerprint],
        merged_root: Path,
        changed: tuple[str, ...],
        integration_lock: asyncio.Lock | None = None,
    ) -> IsolationResult:
        if integration_lock is not None:
            await integration_lock.acquire()
        try:
            async with self._lock:
                if changed:
                    current_scope = await asyncio.to_thread(
                        scoped_manifest,
                        self.settings.repo,
                        chapter,
                    )
                    base_scope = {
                        path: value
                        for path, value in base_manifest.items()
                        if _matches_scope(path, chapter)
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
                return IsolationResult(
                    accepted=True,
                    generation=generation,
                    cache_generation=cache_generation,
                    changed_paths=changed,
                )
        finally:
            if integration_lock is not None:
                integration_lock.release()

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
