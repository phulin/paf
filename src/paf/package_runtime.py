from __future__ import annotations

import fcntl
import re
import subprocess
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from paf.package_model import (
    CapabilityPackage,
    IntegrationJournal,
    IntegrationPhase,
    PackageRecovery,
    RelevantReadInterface,
    StewardLease,
)
from paf.state_db import StateDatabase


class PackageGitError(RuntimeError):
    pass


@dataclass(frozen=True)
class WorktreeSnapshot:
    path: Path
    branch: str
    head: str
    status: str
    dirty_paths: tuple[str, ...]
    dirty_digest: str

    @property
    def clean(self) -> bool:
        return not self.dirty_paths


@dataclass(frozen=True)
class InterfaceChange:
    interface_id: str
    expected_digest: str
    actual_digest: str | None


@dataclass(frozen=True)
class IntegrationResult:
    integrated: bool
    package_id: str
    candidate_revision: str
    canonical_revision: str
    validation_digest: str = ""
    stale_reason: str = ""
    journal_id: str = ""


def _slug(value: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-.")
    if not clean:
        raise ValueError("package id cannot form a Git branch name")
    return clean


class _Git:
    def __init__(self, repo: Path) -> None:
        self.repo = repo.resolve()

    def run(self, *arguments: str, cwd: Path | None = None, check: bool = True) -> str:
        result = subprocess.run(
            ("git", "--literal-pathspecs", *arguments),
            cwd=cwd or self.repo,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        output = result.stdout.decode(errors="replace")
        if check and result.returncode:
            raise PackageGitError(
                f"git {' '.join(arguments[:2])} failed ({result.returncode}): "
                f"{output[-4000:].strip()}"
            )
        return output

    def head(self, cwd: Path | None = None) -> str:
        return self.run("rev-parse", "HEAD", cwd=cwd).strip()

    def branch(self, cwd: Path | None = None) -> str:
        return self.run("branch", "--show-current", cwd=cwd).strip()

    def status(self, cwd: Path | None = None) -> str:
        return self.run("status", "--porcelain=v1", "-z", cwd=cwd)

    def dirty_paths(self, cwd: Path | None = None) -> tuple[str, ...]:
        status = self.status(cwd)
        paths: set[str] = set()
        records = status.split("\0")
        index = 0
        while index < len(records):
            record = records[index]
            index += 1
            if not record:
                continue
            path = record[3:]
            if record[:2].strip().startswith(("R", "C")) and index < len(records):
                path = records[index]
                index += 1
            paths.add(path)
        return tuple(sorted(paths))


class PackageWorktreeManager:
    """Create and recover fenced package worktrees without discarding dirty state."""

    def __init__(self, repo: Path, state_dir: Path, database: StateDatabase) -> None:
        self.repo = repo.resolve()
        self.state_dir = state_dir.resolve()
        self.database = database
        self.git = _Git(self.repo)
        self.worktrees = self.state_dir / "worktrees"

    def inspect(self, path: Path) -> WorktreeSnapshot:
        resolved = path.resolve()
        status = self.git.status(resolved)
        dirty_paths = self.git.dirty_paths(resolved)
        return WorktreeSnapshot(
            resolved,
            self.git.branch(resolved),
            self.git.head(resolved),
            status,
            dirty_paths,
            sha256(status.encode()).hexdigest(),
        )

    def create(
        self,
        package_id: str,
        lease_generation: int,
        *,
        expected_revision: int,
        base_revision: str | None = None,
    ) -> CapabilityPackage:
        slug = _slug(package_id)
        path = self.worktrees / f"package-{slug}"
        branch = f"paf/package-{slug}/generation-{lease_generation}"
        base = base_revision or self.git.head()
        self.database.assert_live_steward_lease(package_id, lease_generation)
        self.worktrees.mkdir(parents=True, exist_ok=True)
        if path.exists():
            snapshot = self.inspect(path)
            if snapshot.branch != branch:
                raise PackageGitError(
                    f"existing package worktree {path} is on {snapshot.branch}, expected {branch}"
                )
        else:
            existing = self.git.run("branch", "--list", branch)
            if existing:
                self.git.run("worktree", "add", str(path), branch)
            else:
                self.git.run("worktree", "add", "-b", branch, str(path), base)
        return self.database.update_package_workspace(
            package_id,
            expected_revision=expected_revision,
            lease_generation=lease_generation,
            base_revision=base,
            branch=branch,
            worktree=str(path),
        )

    def recover(
        self,
        package: CapabilityPackage,
        agent_id: str,
        *,
        expected_revision: int,
        ttl_seconds: float,
        active_child_workers: tuple[str, ...] = (),
        now: str | None = None,
    ) -> tuple[StewardLease, PackageRecovery, WorktreeSnapshot | None]:
        snapshot = None
        if package.worktree:
            path = Path(package.worktree)
            if path.exists():
                snapshot = self.inspect(path)
        journal = tuple(
            value
            for value in self.database.load_package_state().integration_journal.values()
            if value.package_id == package.id
        )
        latest_phase = max(journal, key=lambda value: value.updated_at).phase if journal else None
        lease, recovery = self.database.recover_steward_lease(
            package.id,
            agent_id,
            expected_revision=expected_revision,
            ttl_seconds=ttl_seconds,
            worktree_head=snapshot.head if snapshot else "",
            worktree_status=snapshot.status if snapshot else "",
            dirty_digest=snapshot.dirty_digest if snapshot else sha256(b"").hexdigest(),
            active_child_workers=active_child_workers,
            journal_phase=latest_phase,
            now=now,
        )
        recovered_package = self.database.load_package_state().packages[package.id]
        if snapshot is None:
            recovered_package = self.create(
                package.id,
                lease.generation,
                expected_revision=recovered_package.revision,
                base_revision=package.base_revision or None,
            )
            snapshot = self.inspect(Path(recovered_package.worktree))
        elif snapshot.clean:
            branch = f"paf/package-{_slug(package.id)}/generation-{lease.generation}"
            if snapshot.branch != branch:
                self.git.run("branch", "-m", branch, cwd=snapshot.path)
                recovered_package = self.database.update_package_workspace(
                    package.id,
                    expected_revision=recovered_package.revision,
                    lease_generation=lease.generation,
                    base_revision=package.base_revision,
                    branch=branch,
                    worktree=str(snapshot.path),
                )
                snapshot = self.inspect(Path(recovered_package.worktree))
        return lease, recovery, snapshot

    @contextmanager
    def sequential_worker(
        self, package_id: str, lease_generation: int, worker_id: str
    ) -> Iterator[Path]:
        """Serialize workers in the package worktree while preserving fenced authority."""

        del worker_id
        self.database.assert_live_steward_lease(package_id, lease_generation)
        locks = self.state_dir / "locks"
        locks.mkdir(parents=True, exist_ok=True)
        lock_path = locks / f"package-worker-{_slug(package_id)}.lock"
        with lock_path.open("a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                self.database.assert_live_steward_lease(package_id, lease_generation)
                package = self.database.load_package_state().packages[package_id]
                if not package.worktree:
                    raise ValueError(f"package {package_id} has no worktree")
                yield Path(package.worktree)
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class RelevantInterfaceGuard:
    def __init__(self, database: StateDatabase) -> None:
        self.database = database

    def capture(
        self,
        package_id: str,
        lease_generation: int,
        *,
        expected_revision: int,
        interface_ids: tuple[str, ...],
        source_revision: str,
        digest: Callable[[str], str],
    ) -> CapabilityPackage:
        interfaces = tuple(
            RelevantReadInterface(package_id, interface_id, digest(interface_id), source_revision)
            for interface_id in sorted(dict.fromkeys(interface_ids))
        )
        return self.database.replace_relevant_read_interfaces(
            package_id,
            interfaces,
            expected_revision=expected_revision,
            lease_generation=lease_generation,
        )

    def check(
        self, package_id: str, digest: Callable[[str], str | None]
    ) -> tuple[InterfaceChange, ...]:
        recorded = (
            item
            for item in self.database.load_package_state().relevant_read_interfaces
            if item.package_id == package_id
        )
        return tuple(
            InterfaceChange(item.interface_id, item.digest, actual)
            for item in recorded
            if (actual := digest(item.interface_id)) != item.digest
        )


class PackageIntegrator:
    """Optimistic two-phase Git integration with a restart-safe durable journal."""

    def __init__(self, repo: Path, state_dir: Path, database: StateDatabase) -> None:
        self.repo = repo.resolve()
        self.state_dir = state_dir.resolve()
        self.database = database
        self.git = _Git(self.repo)
        self.interfaces = RelevantInterfaceGuard(database)
        self.lock_path = self.state_dir / "locks" / "canonical-integration.lock"

    @contextmanager
    def _canonical_lock(self) -> Iterator[None]:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _require_clean(self, path: Path, label: str) -> None:
        dirty = self.git.dirty_paths(path)
        if path.resolve() == self.repo:
            try:
                state_prefix = self.state_dir.relative_to(self.repo).as_posix()
            except ValueError:
                state_prefix = ""
            if state_prefix:
                dirty = tuple(
                    value
                    for value in dirty
                    if value != state_prefix and not value.startswith(f"{state_prefix}/")
                )
        if dirty:
            raise PackageGitError(f"{label} worktree is dirty: {', '.join(dirty)}")

    def _record(self, journal: IntegrationJournal) -> CapabilityPackage:
        package = self.database.load_package_state().packages[journal.package_id]
        return self.database.record_integration_journal(journal, expected_revision=package.revision)

    def integrate(
        self,
        package_id: str,
        lease_generation: int,
        *,
        validate: Callable[[Path], str],
        interface_digest: Callable[[str], str | None],
        max_stale_retries: int = 2,
    ) -> IntegrationResult:
        for attempt in range(max_stale_retries + 1):
            package = self.database.load_package_state().packages[package_id]
            if not package.worktree:
                raise ValueError(f"package {package_id} has no worktree")
            worktree = Path(package.worktree)
            with self._canonical_lock():
                self.database.assert_live_steward_lease(package_id, lease_generation)
                self._require_clean(self.repo, "canonical")
                self._require_clean(worktree, "package")
                canonical_before = self.git.head()
                if canonical_before != package.base_revision:
                    self.git.run("rebase", canonical_before, cwd=worktree)
                    package = self.database.update_package_workspace(
                        package_id,
                        expected_revision=package.revision,
                        lease_generation=lease_generation,
                        base_revision=canonical_before,
                        branch=package.branch,
                        worktree=package.worktree,
                    )
                candidate = self.git.head(worktree)
                journal = IntegrationJournal(
                    f"integration-{_slug(package_id)}-{lease_generation}-{uuid4().hex}",
                    package_id,
                    lease_generation,
                    canonical_before,
                    candidate,
                    canonical_before,
                    IntegrationPhase.PREPARED,
                )
                self._record(journal)

            journal = replace(journal, phase=IntegrationPhase.VALIDATING)
            self._record(journal)
            validation_digest = validate(worktree)
            if not validation_digest:
                raise ValueError("package validation must return a non-empty digest")
            journal = replace(
                journal,
                phase=IntegrationPhase.VALIDATED,
                validation_digest=validation_digest,
            )
            self._record(journal)

            with self._canonical_lock():
                self.database.assert_live_steward_lease(package_id, lease_generation)
                self._require_clean(self.repo, "canonical")
                current = self.git.head()
                changes = self.interfaces.check(package_id, interface_digest)
                if current != canonical_before:
                    journal = replace(journal, phase=IntegrationPhase.ABORTED)
                    self._record(journal)
                    if attempt < max_stale_retries and not changes:
                        continue
                    return IntegrationResult(
                        False,
                        package_id,
                        candidate,
                        current,
                        validation_digest,
                        "canonical revision changed during validation",
                        journal.id,
                    )
                if changes:
                    journal = replace(journal, phase=IntegrationPhase.ABORTED)
                    self._record(journal)
                    return IntegrationResult(
                        False,
                        package_id,
                        candidate,
                        current,
                        validation_digest,
                        "relevant read interface changed during validation: "
                        + ", ".join(change.interface_id for change in changes),
                        journal.id,
                    )
                journal = replace(journal, phase=IntegrationPhase.IMPORTING)
                current_package = self._record(journal)
                self.git.run("merge", "--ff-only", candidate)
                canonical_after = self.git.head()
                finalized = self.database.finalize_package_integration(
                    journal.id,
                    expected_revision=current_package.revision,
                    lease_generation=lease_generation,
                    canonical_revision_after=canonical_after,
                )
                return IntegrationResult(
                    True,
                    finalized.id,
                    candidate,
                    canonical_after,
                    validation_digest,
                    journal_id=journal.id,
                )
        raise AssertionError("stale integration retry loop did not terminate")

    def reconcile(self) -> tuple[IntegrationResult, ...]:
        results: list[IntegrationResult] = []
        journals = tuple(
            journal
            for journal in self.database.load_package_state().integration_journal.values()
            if journal.phase is IntegrationPhase.IMPORTING
        )
        for journal in journals:
            with self._canonical_lock():
                self._require_clean(self.repo, "canonical")
                canonical = self.git.head()
                included = (
                    subprocess.run(
                        (
                            "git",
                            "merge-base",
                            "--is-ancestor",
                            journal.candidate_revision,
                            canonical,
                        ),
                        cwd=self.repo,
                        check=False,
                    ).returncode
                    == 0
                )
                if not included and canonical == journal.canonical_revision_before:
                    self.git.run("merge", "--ff-only", journal.candidate_revision)
                    canonical = self.git.head()
                    included = True
                if not included:
                    self.database.abort_integration_reconciliation(journal.id)
                    results.append(
                        IntegrationResult(
                            False,
                            journal.package_id,
                            journal.candidate_revision,
                            canonical,
                            journal.validation_digest,
                            "canonical history diverged from interrupted import",
                            journal.id,
                        )
                    )
                    continue
                package = self.database.reconcile_imported_integration(
                    journal.id,
                    canonical_revision_after=journal.candidate_revision,
                    validation_digest=journal.validation_digest,
                )
                results.append(
                    IntegrationResult(
                        True,
                        package.id,
                        journal.candidate_revision,
                        canonical,
                        journal.validation_digest,
                        journal_id=journal.id,
                    )
                )
        return tuple(results)
