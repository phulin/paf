from __future__ import annotations

import asyncio
import os
import shutil
import stat
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from paf import json_codec as json
from paf.hashing import digest_bytes, new_digest
from paf.models import SwarmSettings, WorkUnitLike
from paf.scope import ScopeMatcher


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
    commit: str = ""

    @property
    def stale_scope(self) -> bool:
        """Whether a concurrent integration invalidated this workspace snapshot."""

        return (
            not self.accepted
            and self.error
            == "assigned scope changed after this agent started; retry on a fresh generation"
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "accepted": self.accepted,
            "generation": self.generation,
            "cache_generation": self.cache_generation,
            "changed_paths": list(self.changed_paths),
            "promoted_cache_paths": list(self.promoted_cache_paths),
            "out_of_scope_paths": list(self.out_of_scope_paths),
            "error": self.error,
            "commit": self.commit,
        }


@dataclass
class CacheGeneration:
    """An immutable ordered cache-layer manifest pinned by active agents."""

    layers: tuple[Path, ...]
    references: int = 0


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
        digest = digest_bytes(path.read_bytes())
        return FileFingerprint("file", digest, mode)
    if stat.S_ISLNK(metadata.st_mode):
        digest = digest_bytes(os.readlink(path).encode())
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


def _overlay_delta_paths(
    upper: Path,
    base: Path,
    *,
    excluded: tuple[str, ...],
) -> set[str]:
    """Return source paths represented by one overlay upper directory.

    Walking the merged tree defeats the purpose of an overlay: on a large
    corpus it re-hashes every source file for every completed agent.  The
    upper directory is already the kernel-maintained change index.  Ordinary
    entries name created or copied-up paths, while whiteouts name removals.
    """

    result: set[str] = set()

    def add_base_tree(relative: str) -> None:
        target = base / relative
        if not target.is_dir():
            result.add(relative)
            return
        for directory, names, filenames in os.walk(target, followlinks=False):
            current = Path(directory)
            parent = current.relative_to(base).as_posix()
            parent = "" if parent == "." else parent
            names[:] = [
                name for name in names if not _excluded(f"{parent}/{name}".lstrip("/"), excluded)
            ]
            result.update(
                f"{parent}/{name}".lstrip("/")
                for name in filenames
                if not _excluded(f"{parent}/{name}".lstrip("/"), excluded)
            )

    for directory, names, filenames in os.walk(upper, followlinks=False):
        current = Path(directory)
        parent = current.relative_to(upper).as_posix()
        parent = "" if parent == "." else parent
        names[:] = [
            name for name in names if not _excluded(f"{parent}/{name}".lstrip("/"), excluded)
        ]
        for name in (*names, *filenames):
            path = current / name
            relative = f"{parent}/{name}".lstrip("/")
            if _excluded(relative, excluded):
                continue
            try:
                mode = path.lstat().st_mode
            except FileNotFoundError:
                continue
            if stat.S_ISDIR(mode):
                continue
            if name in {".wh..opq", ".wh..wh..opq"}:
                add_base_tree(parent)
            elif name.startswith(".wh."):
                add_base_tree(f"{parent}/{name.removeprefix('.wh.')}".lstrip("/"))
            else:
                result.add(relative)
    return result


def overlay_delta_manifests(
    base: Path,
    merged: Path,
    upper: Path,
    *,
    excluded: tuple[str, ...],
) -> tuple[dict[str, FileFingerprint], dict[str, FileFingerprint], tuple[str, ...]]:
    """Fingerprint only paths named by an overlay delta."""

    candidates = _overlay_delta_paths(upper, base, excluded=excluded)

    def manifest(root: Path) -> dict[str, FileFingerprint]:
        values: dict[str, FileFingerprint] = {}
        for relative in candidates:
            path = root / relative
            try:
                mode = path.lstat().st_mode
            except FileNotFoundError:
                continue
            if not stat.S_ISDIR(mode):
                values[relative] = _fingerprint(path)
        return values

    base_manifest = manifest(base)
    merged_manifest = manifest(merged)
    changed = tuple(
        sorted(
            relative
            for relative in candidates
            if base_manifest.get(relative) != merged_manifest.get(relative)
        )
    )
    return base_manifest, merged_manifest, changed


def scoped_manifest(root: Path, chapter: WorkUnitLike) -> dict[str, FileFingerprint]:
    """Fingerprint only a chapter's exclusive scope in the live worktree."""

    return {
        path.relative_to(root).as_posix(): _fingerprint(path)
        for path in ScopeMatcher(chapter.scope).files(root)
    }


def _matches_scope(relative: str, chapter: WorkUnitLike) -> bool:
    return ScopeMatcher(chapter.scope).matches(relative)


class SharedWorkspace:
    def __init__(self, repo: Path) -> None:
        self.root = repo
        self.base_manifest: dict[str, FileFingerprint] | None = None

    async def snapshot(self, chapter: WorkUnitLike) -> None:
        self.base_manifest = await asyncio.to_thread(scoped_manifest, self.root, chapter)

    async def collect(
        self,
        chapter: WorkUnitLike,
        *,
        integration_lock: asyncio.Lock | None = None,
    ) -> IsolationResult:
        del integration_lock
        current = await asyncio.to_thread(scoped_manifest, self.root, chapter)
        base = self.base_manifest if self.base_manifest is not None else current
        changed = tuple(
            sorted(path for path in set(base) | set(current) if base.get(path) != current.get(path))
        )
        return IsolationResult(accepted=True, generation=0, changed_paths=changed)

    async def close(self) -> None:
        return


class SharedBuildWorkspace:
    def __init__(self, repo: Path) -> None:
        self.root = repo

    async def finish(
        self,
        *,
        succeeded: bool,
        publish: bool,
        retain: bool = False,
    ) -> tuple[str, ...]:
        del succeeded, publish, retain
        return ()

    async def close(self) -> None:
        return


class SharedIsolation:
    name = "shared"

    def __init__(self, settings: SwarmSettings) -> None:
        self.settings = settings

    async def prepare(self) -> None:
        return

    async def acquire(self, _run_id: str, *, resume_run_id: str = "") -> SharedWorkspace:
        del resume_run_id
        return SharedWorkspace(self.settings.repo)

    async def acquire_build(self, _build_id: str) -> SharedBuildWorkspace:
        return SharedBuildWorkspace(self.settings.repo)

    async def close(self, *, preserve: bool = False) -> None:
        del preserve
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
        resumed_from: str = "",
        lowerdirs: tuple[Path, ...] = (),
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
        self.resumed_from = resumed_from
        self.lowerdirs = lowerdirs
        self.closed = False
        self.preserved = False

    async def collect(
        self,
        chapter: WorkUnitLike,
        *,
        integration_lock: asyncio.Lock | None = None,
    ) -> IsolationResult:
        # The overlay upper is the exact candidate set. Only hash those paths
        # plus this chapter's scope for stale-writer detection.
        base_scope, delta = await asyncio.gather(
            asyncio.to_thread(scoped_manifest, self.base, chapter),
            asyncio.to_thread(
                overlay_delta_manifests,
                self.base,
                self.root,
                self.upper,
                excluded=self.manager.excluded,
            ),
        )
        _, merged, changed = delta
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
            base_manifest=base_scope,
            merged_manifest=merged,
            merged_root=self.root,
            changed=changed,
            integration_lock=integration_lock,
        )

    async def close(self) -> None:
        if not self.closed:
            self.closed = True
            await self.manager.release(self)

    def preserve(self, run_id: str) -> None:
        """Leave this exact overlay available to a later ``--resume`` run."""

        metadata = {
            "version": 1,
            "run_id": run_id,
            "generation": self.generation,
            "cache_generation": self.cache_generation,
            "base": str(self.base),
            "lowerdirs": [str(path) for path in self.lowerdirs],
        }
        (self.root.parent / "resume.json").write_bytes(
            json.dumpb(metadata, indent=True, sort_keys=True)
        )
        (self.root.parents[2] / ".preserved").touch()
        self.preserved = True


class FuseBuildWorkspace:
    """A private coordinator cache transaction backed by an overlay upperdir."""

    def __init__(
        self,
        manager: FuseOverlayIsolation,
        *,
        build_id: str,
        generation: int,
        base: Path,
        layers: tuple[Path, ...],
        upper: Path,
        work: Path,
        merged: Path,
    ) -> None:
        self.manager = manager
        self.build_id = build_id
        self.generation = generation
        self.base = base
        self.layers = layers
        self.upper = upper
        self.work = work
        self.root = merged
        self.closed = False

    async def finish(
        self,
        *,
        succeeded: bool,
        publish: bool,
        retain: bool = False,
    ) -> tuple[str, ...]:
        if self.closed:
            raise RuntimeError("coordinator build workspace already closed")
        self.closed = True
        return await self.manager.finish_build(
            self,
            succeeded=succeeded,
            publish=publish,
            retain=retain,
        )

    async def close(self) -> None:
        if not self.closed:
            self.closed = True
            await self.manager.finish_build(self, succeeded=False, publish=False)


class FuseOverlayIsolation:
    name = "fuse-overlay"

    def __init__(self, settings: SwarmSettings, *, resume: bool = False) -> None:
        self.settings = settings
        self.resume = resume
        self.parent = settings.state_dir / "isolation"
        self.root = self.parent / str(os.getpid())
        self.generations = self.root / "source-generations"
        self.cache_root = self.root / "cache"
        self.cache_layers = self.cache_root / "layers"
        self.cache_builds = self.cache_root / "builds"
        self.cache_compactions = self.cache_root / "compactions"
        self.slots = self.root / "slots"
        lean_cache = (settings.lean_project / ".lake").as_posix()
        excluded = {
            ".git",
            ".paf",
            ".venv",
            ".pytest_cache",
            ".lake",
            lean_cache,
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
        self._pending_source_changes: set[str] = set()
        self._dependency_backing = self.cache_root / "dependencies-unprepared"
        self._dependency_layer = self.cache_root / "dependency-links"
        self._coordinator_layers: tuple[Path, ...] = ()
        self._cache_revision = 0
        self._published_cache_revision = 0
        self._cache_generations: dict[int, CacheGeneration] = {}
        self._active_build_layers: set[Path] = set()
        self._active_compaction_layers: set[Path] = set()
        self._compaction_task: asyncio.Task[None] | None = None
        self._cleanup_tasks: set[asyncio.Task[None]] = set()
        self._cleanup_errors: list[BaseException] = []
        # Recursive deletion walks large Lake trees and holds directory descriptors
        # along the way. Serializing these background walks prevents obsolete cache
        # layers from creating an unbounded system-wide FD spike.
        self._cleanup_limiter = asyncio.Semaphore(1)
        self._deleting_layers: set[Path] = set()
        self._workspace_sequence = 0

    def _create_cache_workspace(self, parent: Path, prefix: str) -> Path:
        """Atomically allocate a workspace beneath this orchestrator's state tree."""

        while True:
            self._workspace_sequence += 1
            workspace = parent / f"{prefix}-{self._workspace_sequence:08d}"
            try:
                workspace.mkdir(parents=True, exist_ok=False)
            except FileExistsError:
                continue
            return workspace

    async def prepare(self) -> None:
        if not fuse_overlay_available():
            raise ValueError(
                "fuse-overlay isolation requires fuse-overlayfs, fusermount3, rsync, and /dev/fuse"
            )
        await self._clean_stale_roots(preserve_resumable=self.resume)
        self.generations.mkdir(parents=True, exist_ok=True)
        self.cache_layers.mkdir(parents=True, exist_ok=True)
        self.cache_builds.mkdir(parents=True, exist_ok=True)
        self.cache_compactions.mkdir(parents=True, exist_ok=True)
        self.slots.mkdir(parents=True, exist_ok=True)
        # Seed immutable source and split cache bases once. Every later build
        # records only its private overlay delta.
        async with self._lock:
            await self._generation()
            await self._seed_cache_layers()

    async def _clean_stale_roots(self, *, preserve_resumable: bool = False) -> None:
        """Reclaim mounts left by dead orchestrators for this state directory."""

        if not self.parent.exists():
            return
        mounted = _mount_points()
        for stale in self.parent.iterdir():
            if stale == self.root:
                continue
            try:
                pid = int(stale.name)
                os.kill(pid, 0)
            except ProcessLookupError:
                pass
            except (PermissionError, ValueError):
                continue
            else:
                continue
            if preserve_resumable and any(stale.glob("slots/*/resume.json")):
                continue
            for merged in stale.glob("**/merged"):
                # os.path.ismount() returns False when lstat() reports ENOTCONN, which is exactly
                # how a dead fuse-overlayfs mount presents. /proc/self/mountinfo remains readable.
                if merged in mounted or os.path.ismount(merged):
                    await self._unmount(merged)
            await asyncio.to_thread(shutil.rmtree, stale)

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
        try:
            output, _ = await process.communicate()
        except BaseException:
            with suppress(ProcessLookupError):
                process.terminate()
            with suppress(BaseException):
                await process.wait()
            raise
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
        previous_revision = max(self._generation_paths, default=None)
        if previous_revision is not None:
            previous = self._generation_paths[previous_revision]
            # Both paths are immutable. Reusing unchanged inodes avoids
            # rewriting the complete repository for every accepted chapter.
            # Imported agent deltas name every changed source, so the full-tree
            # pass only needs metadata comparisons. Those paths are forcibly
            # recopied below in case an edit preserved its mtime and size.
            # Retain checksum comparison as a defensive fallback for revisions
            # advanced by callers that did not provide a delta.
            if not self._pending_source_changes:
                command.append("--checksum")
            command.append(f"--link-dest={previous}")
        command.extend(f"--exclude=/{path}" for path in self.excluded)
        # Source generations must not share inodes with the live worktree. A
        # hard-linked snapshot can change underneath an active FUSE mount when
        # another process edits the repository in place. The mount may retain
        # the old contents while a manifest scan of the lower directory sees
        # the new contents, falsely attributing the external edit to the agent.
        command.extend((f"{self.settings.repo}/", f"{destination}/"))
        await self._run(*command)
        if previous_revision is not None and self._pending_source_changes:
            changed_sources = tuple(
                f"{self.settings.repo}/./{relative}"
                for relative in sorted(self._pending_source_changes)
                if (self.settings.repo / relative).is_file()
            )
            if changed_sources:
                await self._run(
                    "rsync",
                    "-aR",
                    "--ignore-times",
                    *changed_sources,
                    f"{destination}/",
                )
        self._generation_paths[generation] = destination
        self._generation_references[generation] = 0
        self._pending_source_changes.clear()
        for revision, path in tuple(self._generation_paths.items()):
            if revision != generation and self._generation_references[revision] == 0:
                self._generation_paths.pop(revision)
                self._generation_references.pop(revision)
                self._schedule_remove_tree(path)
        return generation, destination

    def _cache_prefixes(self) -> tuple[str, ...]:
        return ".lake", (self.settings.lean_project / ".lake").as_posix()

    def _package_prefixes(self) -> tuple[str, ...]:
        return tuple(f"{prefix}/packages" for prefix in self._cache_prefixes())

    def _is_cache_path(self, relative: str) -> bool:
        return any(
            relative == prefix or relative.startswith(prefix + "/")
            for prefix in self._cache_prefixes()
        )

    def _is_cache_ancestor(self, relative: str) -> bool:
        return any(
            relative == "" or prefix.startswith(relative + "/") for prefix in self._cache_prefixes()
        )

    def _dependency_key(self) -> str:
        digest = new_digest()
        for relative in (
            Path("lean-toolchain"),
            self.settings.lean_project / "lake-manifest.json",
        ):
            digest.update(relative.as_posix().encode())
            path = self.settings.repo / relative
            if path.is_file():
                digest.update(path.read_bytes())
        return digest.hexdigest()

    async def _copy_dependency_cache(self, destination: Path) -> None:
        command = [
            "rsync",
            "-a",
            "--prune-empty-dirs",
            "--include=*/",
        ]
        command.extend(f"--include=/{prefix}/***" for prefix in self._package_prefixes())
        command.extend(("--exclude=*", f"--link-dest={self.settings.repo}"))
        command.extend((f"{self.settings.repo}/", f"{destination}/"))
        await self._run(*command)

    async def _copy_initial_project_cache(self, destination: Path) -> None:
        command = ["rsync", "-a", "--prune-empty-dirs", "--include=*/"]
        command.extend(f"--exclude=/{prefix}/***" for prefix in self._package_prefixes())
        command.extend(f"--include=/{prefix}/***" for prefix in self._cache_prefixes())
        command.extend(("--exclude=*", f"--link-dest={self.settings.repo}"))
        command.extend((f"{self.settings.repo}/", f"{destination}/"))
        await self._run(*command)

    async def _seed_cache_layers(self) -> None:
        dependency = self.cache_root / f"dependencies-{self._dependency_key()}"
        links = self._dependency_layer
        project = self.cache_layers / "00000000-initial"
        dependency.mkdir(parents=True, exist_ok=False)
        links.mkdir(parents=True, exist_ok=False)
        project.mkdir(parents=True, exist_ok=False)
        await asyncio.gather(
            self._copy_dependency_cache(dependency),
            self._copy_initial_project_cache(project),
        )
        # A fuse-overlayfs process keeps backing descriptors for package files
        # loaded by Lean. Putting the package tree in every agent's lowerdir
        # multiplies that descriptor set by the number of agents. Let package
        # paths leave the private overlay through absolute symlinks instead.
        for prefix in self._package_prefixes():
            target = dependency / prefix
            if not target.is_dir():
                continue
            link = links / prefix
            link.parent.mkdir(parents=True, exist_ok=True)
            link.symlink_to(target, target_is_directory=True)
        self._dependency_backing = dependency
        self._coordinator_layers = (project,)
        self._cache_generations[0] = CacheGeneration(self._coordinator_layers)

    def _lower_directories(self, base: Path, layers: tuple[Path, ...]) -> str:
        return ":".join(str(path) for path in (base, *layers, self._dependency_layer))

    def _cache_delta_paths(self, upper: Path) -> tuple[str, ...]:
        paths: list[str] = []
        for directory, names, filenames in os.walk(upper, followlinks=False):
            current = Path(directory)
            for name in (*names, *filenames):
                path = current / name
                if path.is_dir():
                    continue
                relative = path.relative_to(upper).as_posix()
                parent = path.parent.relative_to(upper).as_posix()
                if name.startswith(".wh.") and self._is_cache_ancestor(parent):
                    continue
                if not self._is_cache_path(relative):
                    raise RuntimeError(
                        f"coordinator build wrote outside its private cache: {relative}"
                    )
                if not name.startswith(".wh."):
                    paths.append(relative)
        return tuple(sorted(paths))

    def _apply_cache_whiteouts(self, upper: Path) -> None:
        for marker in upper.rglob(".wh.*"):
            relative_parent = marker.parent.relative_to(upper)
            if not self._is_cache_path(relative_parent.as_posix()):
                continue
            name = marker.name
            if name in {".wh..opq", ".wh..wh..opq"}:
                target = self.settings.repo / relative_parent
            else:
                target = self.settings.repo / relative_parent / name.removeprefix(".wh.")
            if target.is_dir() and not target.is_symlink():
                shutil.rmtree(target)
            else:
                target.unlink(missing_ok=True)

    async def acquire_build(self, build_id: str) -> FuseBuildWorkspace:
        generation: int | None = None
        build_root = self._create_cache_workspace(self.cache_builds, "build")
        upper = build_root / "upper"
        work = build_root / "work"
        merged = build_root / "merged"
        try:
            async with self._lock:
                generation, base = await self._generation()
                layers = self._coordinator_layers
                self._generation_references[generation] += 1
                self._active_build_layers.update(layers)
            for path in (upper, work, merged):
                path.mkdir(parents=True, exist_ok=False)
            await self._run(
                "fuse-overlayfs",
                "-o",
                f"lowerdir={self._lower_directories(base, layers)},upperdir={upper},workdir={work}",
                str(merged),
            )
            if not os.path.ismount(merged):
                raise RuntimeError(f"fuse-overlayfs did not mount {merged}")
            return FuseBuildWorkspace(
                self,
                build_id=build_id,
                generation=generation,
                base=base,
                layers=layers,
                upper=upper,
                work=work,
                merged=merged,
            )
        except BaseException:
            if os.path.ismount(merged):
                await self._unmount(merged)
            if build_root.exists():
                await asyncio.to_thread(shutil.rmtree, build_root)
            if generation is not None:
                async with self._lock:
                    self._generation_references[generation] -= 1
                    self._active_build_layers.difference_update(layers)
            raise

    async def finish_build(
        self,
        workspace: FuseBuildWorkspace,
        *,
        succeeded: bool,
        publish: bool,
        retain: bool = False,
    ) -> tuple[str, ...]:
        if publish and not succeeded:
            raise ValueError("cannot publish an unsuccessful coordinator build")
        build_root = workspace.root.parent
        promoted: tuple[str, ...] = ()
        layer: Path | None = None
        try:
            if os.path.ismount(workspace.root):
                await self._unmount(workspace.root)
            if succeeded or retain:
                promoted = await asyncio.to_thread(self._cache_delta_paths, workspace.upper)
                if promoted:
                    if succeeded:
                        # Preserve the ordinary worktree cache for non-swarm Lake
                        # commands and for seeding the next orchestrator invocation.
                        await asyncio.to_thread(self._apply_cache_whiteouts, workspace.upper)
                        await self._run(
                            "rsync",
                            "-a",
                            "--exclude=.wh.*",
                            f"{workspace.upper}/",
                            f"{self.settings.repo}/",
                        )
                    async with self._lock:
                        next_layer = self._cache_revision + 1
                        layer = self.cache_layers / f"{next_layer:08d}-delta"
                        os.replace(workspace.upper, layer)
                        self._cache_revision = next_layer
                        self._coordinator_layers = (layer, *self._coordinator_layers)
                if publish:
                    async with self._lock:
                        self._published_cache_revision += 1
                        self._cache_generations[self._published_cache_revision] = CacheGeneration(
                            self._coordinator_layers
                        )
                        self._drop_unused_cache_generations_locked()
                async with self._lock:
                    self._start_compaction_locked()
            return promoted
        finally:
            if build_root.exists():
                # Failed Lake builds can leave very large private .lake trees.
                # They are already detached and cannot affect correctness, so
                # reclaim them behind the serialized cleanup worker.
                self._schedule_remove_tree(build_root)
            async with self._lock:
                self._generation_references[workspace.generation] -= 1
                self._active_build_layers.difference_update(workspace.layers)
                self._drop_unused_cache_generations_locked()

    def _drop_unused_cache_generations_locked(self) -> None:
        for generation, snapshot in tuple(self._cache_generations.items()):
            if generation != self._published_cache_revision and snapshot.references == 0:
                self._cache_generations.pop(generation)
        self._collect_unused_layers_locked()

    def _schedule_remove_tree(self, path: Path) -> None:
        if not path.exists() or path in self._deleting_layers:
            return

        async def remove() -> None:
            async with self._cleanup_limiter:
                await asyncio.to_thread(shutil.rmtree, path)

        task = asyncio.create_task(remove())
        self._deleting_layers.add(path)
        self._cleanup_tasks.add(task)

        def finished(completed: asyncio.Task[None]) -> None:
            self._deleting_layers.discard(path)
            self._cleanup_tasks.discard(completed)
            if not completed.cancelled() and (error := completed.exception()) is not None:
                self._cleanup_errors.append(error)

        task.add_done_callback(finished)

    def _collect_unused_layers_locked(self) -> None:
        reachable = {
            *self._coordinator_layers,
            *self._active_build_layers,
            *self._active_compaction_layers,
            *(
                layer
                for generation in self._cache_generations.values()
                for layer in generation.layers
            ),
        }
        for layer in self.cache_layers.iterdir():
            if layer.is_dir() and layer not in reachable:
                self._schedule_remove_tree(layer)

    def _start_compaction_locked(self) -> None:
        if self._compaction_task is not None:
            if not self._compaction_task.done():
                return
            self._compaction_task.result()
            self._compaction_task = None
        if len(self._coordinator_layers) < self.settings.cache_compaction_layers:
            return
        layers = self._coordinator_layers
        self._active_compaction_layers.update(layers)
        self._compaction_task = asyncio.create_task(self._compact_layers(layers))

    async def _compact_layers(self, layers: tuple[Path, ...]) -> None:
        compact_root = self._create_cache_workspace(self.cache_compactions, "compact")
        merged = compact_root / "merged"
        compacted = compact_root / "layer"
        merged.mkdir()
        compacted.mkdir()
        mounted = False
        try:
            await self._run(
                "fuse-overlayfs",
                "-o",
                "lowerdir=" + ":".join(str(path) for path in layers),
                str(merged),
            )
            mounted = True
            if not os.path.ismount(merged):
                raise RuntimeError(f"fuse-overlayfs did not mount {merged}")
            await self._run(
                "rsync",
                "-a",
                f"--link-dest={layers[-1]}",
                f"{merged}/",
                f"{compacted}/",
            )
            await self._unmount(merged)
            mounted = False
            async with self._lock:
                current = self._coordinator_layers
                if len(current) < len(layers) or current[-len(layers) :] != layers:
                    return
                prefix = current[: -len(layers)]
                destination = self.cache_layers / f"compact-{self._cache_revision:08d}"
                os.replace(compacted, destination)
                self._coordinator_layers = (*prefix, destination)
                published = self._cache_generations[self._published_cache_revision]
                if published.layers == current:
                    self._published_cache_revision += 1
                    self._cache_generations[self._published_cache_revision] = CacheGeneration(
                        self._coordinator_layers
                    )
                    self._drop_unused_cache_generations_locked()
        finally:
            if mounted and os.path.ismount(merged):
                await self._unmount(merged)
            if compact_root.exists():
                await asyncio.to_thread(shutil.rmtree, compact_root)
            async with self._lock:
                self._active_compaction_layers.difference_update(layers)
                self._drop_unused_cache_generations_locked()

    async def acquire(self, run_id: str, *, resume_run_id: str = "") -> FuseWorkspace:
        slot = await self._available.get()
        generation: int | None = None
        cache_generation: int | None = None
        slot_root = self.slots / f"{slot:04d}-{run_id}"
        merged = slot_root / "merged"
        try:
            if resume_run_id:
                resumed = await self._resume_workspace(slot, resume_run_id)
                if resumed is not None:
                    return resumed
            async with self._lock:
                generation, base = await self._generation()
                cache_generation = self._published_cache_revision
                cache = self._cache_generations[cache_generation]
                self._generation_references[generation] += 1
                cache.references += 1
            upper = slot_root / "upper"
            work = slot_root / "work"
            for path in (upper, work, merged):
                path.mkdir(parents=True, exist_ok=False)
            await self._run(
                "fuse-overlayfs",
                "-o",
                f"lowerdir={self._lower_directories(base, cache.layers)},"
                f"upperdir={upper},workdir={work}",
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
                cache=self._dependency_layer,
                upper=upper,
                work=work,
                merged=merged,
                lowerdirs=(base, *cache.layers, self._dependency_layer),
            )
        except BaseException:
            if os.path.ismount(merged):
                await self._unmount(merged)
            if slot_root.exists():
                await asyncio.to_thread(shutil.rmtree, slot_root)
            if generation is not None:
                async with self._lock:
                    self._generation_references[generation] -= 1
                    if cache_generation is not None:
                        self._cache_generations[cache_generation].references -= 1
            self._available.put_nowait(slot)
            raise

    async def _resume_workspace(self, slot: int, run_id: str) -> FuseWorkspace | None:
        """Reattach the overlay belonging to the immediately interrupted run."""

        if not self.parent.exists():
            return None
        for metadata_path in sorted(
            self.parent.glob("*/slots/*/resume.json"),
            key=lambda path: path.stat().st_mtime_ns,
            reverse=True,
        ):
            try:
                metadata = json.loads(metadata_path.read_bytes())
            except (OSError, ValueError, TypeError):
                continue
            if not isinstance(metadata, dict) or metadata.get("run_id") != run_id:
                continue
            slot_root = metadata_path.parent
            merged = slot_root / "merged"
            upper = slot_root / "upper"
            work = slot_root / "work"
            base = Path(str(metadata.get("base", "")))
            lowerdirs = tuple(Path(str(path)) for path in metadata.get("lowerdirs", ()))
            if not upper.is_dir() or not work.is_dir() or not base.is_dir() or not lowerdirs:
                continue
            mounted = merged in _mount_points() or os.path.ismount(merged)
            if not mounted:
                await self._run(
                    "fuse-overlayfs",
                    "-o",
                    f"lowerdir={':'.join(str(path) for path in lowerdirs)},"
                    f"upperdir={upper},workdir={work}",
                    str(merged),
                )
                if not os.path.ismount(merged):
                    raise RuntimeError(f"fuse-overlayfs did not remount {merged}")
            return FuseWorkspace(
                self,
                slot=slot,
                generation=int(metadata.get("generation", 0)),
                cache_generation=int(metadata.get("cache_generation", 0)),
                base=base,
                cache=lowerdirs[-1],
                upper=upper,
                work=work,
                merged=merged,
                resumed_from=run_id,
                lowerdirs=lowerdirs,
            )
        return None

    async def import_changes(
        self,
        chapter: WorkUnitLike,
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
                transaction = f"{os.getpid()}-{uuid4().hex}"
                staged: dict[str, Path] = {}
                backups: dict[str, Path] = {}
                committed: list[str] = []
                committed_all = False
                try:
                    # Complete every allocation and content verification before
                    # the first live path changes. In particular, ENOSPC while
                    # copying cannot leave a partially imported scope.
                    for relative in changed:
                        fingerprint = merged_manifest.get(relative)
                        if fingerprint is None:
                            continue
                        destination = self.settings.repo / relative
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        temporary = destination.with_name(
                            f".{destination.name}.paf-stage-{transaction}"
                        )
                        staged[relative] = temporary
                        shutil.copy2(merged_root / relative, temporary)
                        if _fingerprint(temporary) != fingerprint:
                            raise OSError(f"staged source verification failed: {relative}")

                    for relative in changed:
                        destination = self.settings.repo / relative
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        if os.path.lexists(destination):
                            backup = destination.with_name(
                                f".{destination.name}.paf-backup-{transaction}"
                            )
                            os.replace(destination, backup)
                            backups[relative] = backup
                        committed.append(relative)
                        if relative in staged:
                            os.replace(staged[relative], destination)
                    committed_all = True
                except BaseException:
                    for relative in reversed(committed):
                        destination = self.settings.repo / relative
                        if os.path.lexists(destination):
                            destination.unlink()
                        if backup := backups.get(relative):
                            os.replace(backup, destination)
                    raise
                finally:
                    for temporary in staged.values():
                        temporary.unlink(missing_ok=True)
                    if committed_all:
                        for backup in backups.values():
                            backup.unlink(missing_ok=True)
                if changed:
                    self._revision += 1
                    self._pending_source_changes.update(changed)
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
        if workspace.resumed_from:
            try:
                if workspace.root in _mount_points() or os.path.ismount(workspace.root):
                    await self._unmount(workspace.root)
                slot_root = workspace.root.parent
                (slot_root / "resume.json").unlink(missing_ok=True)
                await asyncio.to_thread(shutil.rmtree, slot_root)
                stale_root = workspace.root.parents[2]
                if not any(stale_root.glob("slots/*/resume.json")):
                    self._schedule_remove_tree(stale_root)
            finally:
                self._available.put_nowait(workspace.slot)
            return
        try:
            if os.path.ismount(workspace.root):
                await self._unmount(workspace.root)
            slot_root = workspace.root.parent
            # The detached upper may contain a large private Lake cache. Slot
            # paths include the run id, so the numeric slot can be reused while
            # serialized background cleanup reclaims this unique directory.
            self._schedule_remove_tree(slot_root)
        finally:
            async with self._lock:
                self._generation_references[workspace.generation] -= 1
                if (
                    workspace.generation != self._revision
                    and self._generation_references[workspace.generation] == 0
                    and workspace.generation != max(self._generation_paths)
                ):
                    path = self._generation_paths.pop(workspace.generation)
                    self._generation_references.pop(workspace.generation)
                    self._schedule_remove_tree(path)
                self._cache_generations[workspace.cache_generation].references -= 1
                self._drop_unused_cache_generations_locked()
            self._available.put_nowait(workspace.slot)

    async def close(self, *, preserve: bool = False) -> None:
        if preserve:
            return
        if self._available.qsize() != self.settings.max_agents:
            raise RuntimeError("cannot close isolation while workspaces are active")
        if self._active_build_layers:
            raise RuntimeError("cannot close isolation while a coordinator build is active")
        error: BaseException | None = None
        try:
            if self._compaction_task is not None:
                await self._compaction_task
            if self._cleanup_tasks:
                await asyncio.gather(*self._cleanup_tasks)
            if self._cleanup_errors:
                raise self._cleanup_errors[0]
        except BaseException as caught:
            error = caught
        finally:
            if self.root.exists():
                await asyncio.to_thread(shutil.rmtree, self.root)
        if error is not None:
            raise error


IsolationManager = SharedIsolation | FuseOverlayIsolation
Workspace = SharedWorkspace | FuseWorkspace
BuildWorkspace = SharedBuildWorkspace | FuseBuildWorkspace


def create_isolation(settings: SwarmSettings, *, resume: bool = False) -> IsolationManager:
    backend = settings.isolation
    if backend == "auto":
        backend = "fuse-overlay" if fuse_overlay_available() else "shared"
    if backend == "fuse-overlay":
        return FuseOverlayIsolation(settings, resume=resume)
    if backend == "shared":
        return SharedIsolation(settings)
    raise ValueError(f"unknown isolation backend: {backend}")
