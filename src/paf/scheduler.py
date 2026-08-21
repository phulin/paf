from __future__ import annotations

import asyncio
import hashlib
import re
from collections import deque
from collections.abc import Callable, Coroutine, Iterable
from contextlib import suppress
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from threading import Lock
from typing import Any
from uuid import uuid4

from paf import json_codec as json
from paf.codex import (
    DIAGNOSTIC_REVIEW_ROLE,
    DIAGNOSTIC_REVIEW_ROLES,
    PACKAGE_STEWARD_ROLE,
    PACKAGE_WORKER_ROLE,
    PROOF_REVIEW_ROLE,
    WARNING_REVIEW_ROLE,
    AgentResult,
    CodexExecutor,
    ValidationResult,
    ValidationStatus,
    count_placeholders,
    declaration_uses_placeholder,
    migrate_scope_digests,
    proof_target_chunk,
    proof_target_spans,
    proof_targets,
    report_schema_key,
    scope_digest,
    scoped_files,
    validate,
)
from paf.coordination import CoordinatorBuildQueue, PriorityLimiter
from paf.corpus import (
    WorkUnitImportGraph,
    build_compiled_import_graph,
    build_corpus_schedule,
    build_source_dependency_graph,
    scheduling_snapshot,
)
from paf.diagnostics import (
    LEAN_DIAGNOSTIC_RE,
    LeanDiagnostic,
    failed_lean_modules,
    lean_diagnostics,
)
from paf.git import (
    GitCommitError,
    GitCommitter,
    deterministic_warning_commit_subject,
    deterministic_warning_revert_subject,
)
from paf.hashing import is_legacy_digest, migrate_digest_text, tagged_digest_text
from paf.interface_fingerprint import (
    FingerprintCollection,
    InterfaceFingerprintError,
    collect_interface_fingerprints,
)
from paf.isolation import FuseWorkspace, IsolationResult, create_isolation
from paf.models import PipelineConfig, ProofTarget, Stage, WorkUnit, WorkUnitLike
from paf.package_model import (
    CapabilityPackage,
    EvidenceKind,
    PackageConsumer,
    PackageEvidence,
    PackageStep,
    ReservationMode,
    ReservationSpec,
)
from paf.package_runtime import (
    ConsumerValidation,
    PackageExecutionLayer,
    PackageExecutionResult,
    PackageImport,
    PackageReportError,
    PackageValidation,
    PackageWorkspace,
)
from paf.scope import ScopeMatcher
from paf.state import (
    BUILD_WARNING_REVIEW_KIND,
    ProofBlockerStatus,
    Requirement,
    RequirementKind,
    RunRecord,
    StateStore,
    TaskPhase,
    TaskStatus,
)
from paf.warning_cleanup import (
    WarningCleanupResult,
    WarningDiagnostic,
    apply_deterministic_warning_cleanup,
    revert_deterministic_warning_cleanup,
)

MAXIMUM_COORDINATOR_BUILD_TARGETS = 500
MAXIMUM_STALE_REVIEW_SNAPSHOT_RETRIES = 3
REVIEW_REPORT_RETRY_PROMPT = """Your previous review turn did not satisfy the report contract:

{error}

Continue the same review assignment in the current files. Re-read the assignment's definition of
done and structured-output instructions, finish any incomplete review work, and return exactly one
complete structured report. Account for every supplied finding id when the assignment includes
proof findings."""
LIVE_AGENT_RETRY_PROMPT = """An operator requested a targeted retry of this live agent. Continue
the same assignment from the current files and conversation. Re-read the assignment instructions,
correct the issue that prevented the prior turn from completing, and return the required structured
report only after the work is stable."""
PROOF_VALIDATION_RETRY_PROMPT = """PAF's authoritative build rejected the proof changes. Continue
the same proof assignment, use the attached validation diagnostics as required work, and repair the
cause within your assigned proof edits, focused helpers, or imports. Do not edit an unrelated
declaration merely because it reports a downstream symptom. Return only after validation is clean or
you have precise new blocker evidence."""


async def _gather_cancel_on_error[T](
    operations: Iterable[Coroutine[Any, Any, T]],
) -> list[T]:
    """Cancel and drain sibling operations when any one raises."""

    tasks = [asyncio.create_task(operation) for operation in operations]
    try:
        return list(await asyncio.gather(*tasks))
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


@dataclass(frozen=True)
class Attempt:
    agent: AgentResult
    validation: ValidationResult
    run: RunRecord

    def feedback(self, *, validation_output: str | None = None) -> str:
        parts = []
        if self.agent.error:
            parts.append(self.agent.error)
        summary = self.agent.report.get("summary")
        if isinstance(summary, str) and summary:
            parts.append(f"Agent summary: {summary}")
        issues = self.agent.report.get("issues")
        if isinstance(issues, list) and issues:
            parts.append("Reported issues:\n" + "\n".join(f"- {issue}" for issue in issues))
        parts.append(
            f"Scoped changes: {self.agent.changed}; remaining proof placeholders: "
            f"{self.agent.placeholders}."
        )
        output = self.validation.output if validation_output is None else validation_output
        if not self.validation.succeeded and output:
            parts.append("Validation failed:\n" + output)
        return "\n\n".join(parts)


class ExecutionDisposition(StrEnum):
    SUCCEEDED = "succeeded"
    WAITING = "waiting"
    FAILED = "failed"


@dataclass(frozen=True)
class StageOutcome:
    disposition: ExecutionDisposition
    waiting_on: tuple[Requirement, ...] = ()
    changed: bool = False
    complete: bool = True
    run_id: str = ""
    report_error: str = ""
    retry_fresh: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.disposition, ExecutionDisposition):
            raise TypeError("StageOutcome requires an explicit ExecutionDisposition")

    @property
    def succeeded(self) -> bool:
        return self.disposition is ExecutionDisposition.SUCCEEDED

    @property
    def failed(self) -> bool:
        return self.disposition is ExecutionDisposition.FAILED

    @property
    def waiting(self) -> bool:
        return self.disposition is ExecutionDisposition.WAITING

    def __bool__(self) -> bool:
        return self.succeeded


@dataclass(frozen=True)
class WarningCleanupOutcome:
    clean: bool
    changed: bool = False


@dataclass(frozen=True)
class DeterministicWarningCleanupOutcome:
    attempted: bool = False
    clean: bool = False
    changed: bool = False


@dataclass(frozen=True)
class BuildDiagnostics:
    actionable: dict[str, str]
    deferred_owner_ids: tuple[str, ...]


@dataclass(frozen=True)
class ValidatedBuildSnapshot:
    graph: WorkUnitImportGraph
    source_digests: dict[str, str]
    source_generations: dict[str, int] = field(default_factory=dict)
    fingerprint: dict[str, Any] | None = None
    import_dependencies: tuple[str, ...] = ()
    fingerprint_error: str = ""


@dataclass(frozen=True)
class PendingBuildRequest:
    chapters: tuple[WorkUnitLike, ...]
    publish_if_clean: bool
    mode: str
    iteration: int
    maximum_iterations: int
    stage: Stage
    snapshots: dict[str, ValidatedBuildSnapshot] | None
    future: asyncio.Future[dict[str, ValidationResult]]
    routed_results: dict[str, ValidationResult] = field(default_factory=dict)
    routed_snapshots: dict[str, ValidatedBuildSnapshot] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokenBuild:
    source_generations: dict[str, int]
    result: ValidationResult


@dataclass(frozen=True)
class PendingDiscovery:
    chapter: WorkUnitLike
    dependencies: tuple[str, ...]
    summary: str
    issues: tuple[Any, ...]
    future: asyncio.Future[None]


@dataclass(frozen=True)
class RunningFormalizeStage:
    task: asyncio.Task[bool]
    progress: asyncio.Event
    idle: asyncio.Event
    target_ids: frozenset[str]


@dataclass(frozen=True)
class RunningReview:
    task: asyncio.Task[StageOutcome]
    dependencies: frozenset[str]
    proof_request_ids: tuple[str, ...] = ()
    auxiliary: bool = False


LEAN_LOCATION_RE = re.compile(r"^(?P<path>.+?\.lean):(?P<line>\d+):(?P<column>\d+):(?:[ \t]|$)")
COORDINATOR_DIAGNOSTIC_SUMMARY_RE = re.compile(
    r"(?:\n\nCoordinator found \d+ Lean diagnostic\(s\) relevant to [^\n]+\.)+\s*$"
)
PROOF_FEEDBACK_MAX_CHARS = 12_000
PROOF_FEEDBACK_ROUNDS = 3
DISCOVERY_BATCH_SECONDS = 0.025
DISCOVERY_BATCH_MAXIMUM = 256
DIAGNOSTIC_OWNER_CACHE_MAXIMUM = 16_384
PROOF_FINDING_REVIEW_KIND = "proof_finding"
BUILD_ERROR_REVIEW_KIND = "build_error"
DETERMINISTIC_WARNING_CLEANUP_ROLE = "deterministic_warning_cleanup"
LEGACY_DIAGNOSTIC_REVIEW_KIND = "diagnostic"
COORDINATOR_VERIFICATION_RETRY_DETAIL = "coordinator verification retry queued"
DIAGNOSTIC_REVIEW_KINDS = frozenset(
    {
        BUILD_ERROR_REVIEW_KIND,
        BUILD_WARNING_REVIEW_KIND,
        LEGACY_DIAGNOSTIC_REVIEW_KIND,
    }
)


@dataclass
class _IdentifierTrieNode:
    children: dict[str, _IdentifierTrieNode] = field(default_factory=dict)
    owner_ids: tuple[str, ...] = ()


def _is_identifier_character(character: str) -> bool:
    return character == "_" or (character.isascii() and character.isalnum())


def _bounded_proof_feedback(blocks: Iterable[str]) -> str:
    text = "\n\n".join(blocks)
    if len(text) <= PROOF_FEEDBACK_MAX_CHARS:
        return text
    marker = "\n\n... older proof feedback omitted ...\n\n"
    available = PROOF_FEEDBACK_MAX_CHARS - len(marker)
    head = available // 3
    return text[:head] + marker + text[-(available - head) :]


def _lean_diagnostics(output: str) -> tuple[LeanDiagnostic, ...]:
    """Compatibility wrapper around the shared full-transcript parser."""

    return lean_diagnostics(output)


def _result_diagnostics(result: ValidationResult) -> tuple[LeanDiagnostic, ...]:
    """Use authoritative full-stream evidence with legacy text fallback."""

    return result.diagnostics or _lean_diagnostics(result.output)


def _result_failed_modules(result: ValidationResult) -> tuple[str, ...]:
    """Use authoritative full-stream failed targets with legacy text fallback."""

    return result.failed_modules or _failed_modules(result.output)


def _diagnostic_output_for_target(diagnostics: Iterable[LeanDiagnostic], target_id: str) -> str:
    """Render diagnostics with provenance for the target receiving the result."""

    selected = tuple(diagnostics)
    output = "\n\n".join(
        COORDINATOR_DIAGNOSTIC_SUMMARY_RE.sub("", diagnostic.text).rstrip()
        for diagnostic in selected
    )
    output += f"\n\nCoordinator found {len(selected)} Lean diagnostic(s) relevant to {target_id}."
    return output[-20_000:]


def _deterministic_warning_diagnostics(
    output: str,
    *,
    lean_project: Path,
) -> tuple[WarningDiagnostic, ...]:
    """Convert complete, located warning blocks into deterministic cleanup inputs."""

    converted: list[WarningDiagnostic] = []
    project_prefix = lean_project.as_posix().strip("/") + "/"
    for diagnostic in _lean_diagnostics(output):
        if diagnostic.severity != "warning":
            return ()
        header = LEAN_DIAGNOSTIC_RE.match(diagnostic.header)
        if header is None:
            return ()
        location = LEAN_LOCATION_RE.match(header.group("message"))
        if location is None:
            return ()
        path = location.group("path").removeprefix(project_prefix)
        converted.append(
            WarningDiagnostic(
                path=path,
                line=int(location.group("line")),
                column=int(location.group("column")),
                message=header.group("message")[location.end() :].strip(),
                text=diagnostic.text,
            )
        )
    return tuple(converted)


def _failed_modules(output: str) -> tuple[str, ...]:
    return failed_lean_modules(output)


class RunControl:
    """Cooperative pause/stop control checked between chapter attempts."""

    def __init__(self) -> None:
        self._gate = asyncio.Event()
        self._gate.set()
        self.paused = False
        self.stopping = False

    async def checkpoint(self) -> None:
        await self._gate.wait()
        if self.stopping:
            raise asyncio.CancelledError

    def pause(self) -> None:
        if not self.stopping:
            self.paused = True
            self._gate.clear()

    def resume(self) -> None:
        if not self.stopping:
            self.paused = False
            self._gate.set()

    def stop(self) -> None:
        self.stopping = True
        self.paused = False
        self._gate.set()


def scaffold_directories(
    config: PipelineConfig, chapters: Iterable[WorkUnitLike]
) -> tuple[str, ...]:
    """Create chapter directories deterministically without creating Lean files."""

    created: list[str] = []
    for chapter in chapters:
        paths = (
            config.backend.scaffold_paths(chapter)
            if config.backend is not None and isinstance(chapter, WorkUnit)
            else (chapter.lean_root / chapter.chapter_path,)
        )
        for relative in paths:
            directory = config.settings.repo / relative
            if not directory.is_dir():
                directory.mkdir(parents=True, exist_ok=True)
                created.append(directory.relative_to(config.settings.repo).as_posix())
    return tuple(created)


def _with_build_command(work_unit: WorkUnitLike, command: str) -> WorkUnitLike:
    if isinstance(work_unit, WorkUnit):
        return replace(
            work_unit,
            target=replace(work_unit._target(), build_command=command),
        )
    return replace(work_unit, build_command=command)


def _with_scope(work_unit: WorkUnitLike, scope: tuple[str, ...]) -> WorkUnitLike:
    """Create a package assignment whose digest and sandbox cover all reserved paths."""

    if isinstance(work_unit, WorkUnit):
        return replace(work_unit, target=replace(work_unit._target(), scope=scope))
    return replace(work_unit, scope=scope)


def _scope_digests(
    root: Path,
    by_id: dict[str, WorkUnitLike],
    work_unit_ids: Iterable[str],
) -> dict[str, str]:
    return {work_unit_id: scope_digest(root, by_id[work_unit_id]) for work_unit_id in work_unit_ids}


def _mutation_reservation_specs(work_unit: WorkUnitLike) -> tuple[ReservationSpec, ...]:
    specs: list[ReservationSpec] = []
    for pattern in work_unit.scope:
        wildcard = min(
            (index for marker in "*[?" if (index := pattern.find(marker)) >= 0),
            default=-1,
        )
        if wildcard < 0:
            specs.append(ReservationSpec(pattern, ReservationMode.EXCLUSIVE_FILE))
            continue
        prefix = pattern[:wildcard].rstrip("/")
        if not prefix:
            raise ValueError(f"mutating scope is too broad to reserve safely: {pattern}")
        if "/" not in prefix or not pattern[:wildcard].endswith("/"):
            prefix = prefix.rsplit("/", 1)[0] if "/" in prefix else "."
        specs.append(ReservationSpec(prefix, ReservationMode.EXCLUSIVE_SUBTREE))
    unique = tuple(dict.fromkeys(specs))
    subtrees = tuple(
        item.normalized_path for item in unique if item.mode is ReservationMode.EXCLUSIVE_SUBTREE
    )
    return tuple(
        item
        for item in unique
        if item.mode is ReservationMode.EXCLUSIVE_SUBTREE
        or not any(
            item.normalized_path == subtree or item.normalized_path.startswith(f"{subtree}/")
            for subtree in subtrees
        )
    )


class Orchestrator:
    def __init__(
        self,
        config: PipelineConfig,
        state: StateStore,
        *,
        work_units: Iterable[WorkUnitLike] | None = None,
        chapters: Iterable[WorkUnitLike] | None = None,
        force: bool = False,
        resume_agents: bool = False,
        control: RunControl | None = None,
    ) -> None:
        self.config = config
        self.state = state
        if work_units is not None and chapters is not None:
            raise ValueError("pass work_units or legacy chapters, not both")
        selected_units = work_units if work_units is not None else chapters
        self.work_units = tuple(selected_units if selected_units is not None else config.work_units)
        self._work_units_by_id = {chapter.id: chapter for chapter in self.work_units}
        self._work_unit_order = {chapter.id: index for index, chapter in enumerate(self.work_units)}
        self._path_owners: dict[str, list[str]] = {}
        self._scope_matchers = {
            chapter.id: ScopeMatcher(chapter.scope) for chapter in self.work_units
        }
        self._source_generations = {chapter.id: 0 for chapter in self.work_units}
        self._broken_builds: dict[str, BrokenBuild] = {}
        self._module_owners: dict[str, list[str]] = {}
        self._identifier_trie = _IdentifierTrieNode()
        self._diagnostic_owner_cache: dict[LeanDiagnostic, tuple[str, ...]] = {}
        self._diagnostic_owner_cache_lock = Lock()
        self._observed_graph_source: dict[str, Any] | None = None
        self._observed_graph_cache: WorkUnitImportGraph | None = None
        self._interface_graph_key: tuple[int, int, int, int] | None = None
        self._interface_graph_cache: WorkUnitImportGraph | None = None
        self._interface_stale_cache: set[str] = set()
        self._compiled_interface_state: dict[str, Any] | None = None
        self._compiled_interface_imports: object | None = None
        self._compiled_interface_fallback: WorkUnitImportGraph | None = None
        self._compiled_interface_graph: WorkUnitImportGraph | None = None
        self._saved_dependency_state: dict[str, Any] | None = None
        self._saved_dependency_edges: object | None = None
        self._saved_dependency_graph: WorkUnitImportGraph | None = None
        self._build_diagnostic_indexes()
        self.force = force
        self.resume_agents = resume_agents
        self.control = control or RunControl()
        self.executor = CodexExecutor(config, state, resume_agents=resume_agents)
        self.isolation = create_isolation(config.settings, resume=resume_agents)
        self.git = GitCommitter(config.settings.repo)
        self.state.isolation = {
            "configured": config.settings.isolation,
            "backend": self.isolation.name,
            "codex_access": (
                "full" if config.settings.bypass_approvals_and_sandbox else config.settings.sandbox
            ),
            "enforcement": (
                "best-effort" if config.settings.bypass_approvals_and_sandbox else "sandboxed"
            ),
            "lake_cache": (
                "immutable-dependency-and-coordinator-delta-layers"
                if self.isolation.name == "fuse-overlay"
                else "coordinator-owned-shared-worktree"
            ),
        }
        selected_documents = {work_unit.document_id for work_unit in self.work_units}
        self.statement_schedule = build_corpus_schedule(
            config.documents,
            self.work_units,
            phase="statements",
            selected_documents=selected_documents,
        )
        self.proof_schedule = build_corpus_schedule(
            config.documents,
            self.work_units,
            phase="proofs",
            selected_documents=selected_documents,
        )
        self.state.scheduling = self.scheduling_snapshot()
        self.agent_slots = PriorityLimiter(config.settings.max_agents)
        discovery_max_agents = config.stages[Stage.DISCOVER].max_agents
        assert discovery_max_agents is not None
        self.discovery_slots = PriorityLimiter(discovery_max_agents)
        self.build_queue = CoordinatorBuildQueue()
        self._pending_build_requests: list[PendingBuildRequest] = []
        self._build_dispatch_task: asyncio.Task[None] | None = None
        # Snapshot creation and scoped source integration need a short
        # consistency barrier with main-worktree builds. Unlike build_queue,
        # this lock is never held for an overlay agent's editing lifetime or
        # for a proof validation outside a coordinator build.
        self.source_lock = asyncio.Lock()
        self._formalize_graph_lock = asyncio.Lock()
        self._discovery_lock = asyncio.Lock()
        self._pending_discoveries: deque[PendingDiscovery] = deque()
        self._discovery_batch_task: asyncio.Task[None] | None = None
        self._invalidated_reviews: set[str] = set()
        self._proof_rechecks: set[str] = set()
        self._review_invalidation_generations: dict[str, int] = {}
        self._review_generation_lock = asyncio.Lock()
        self._chapter_agent_locks = {chapter.id: asyncio.Lock() for chapter in self.work_units}
        self._live_agent_tasks: dict[
            tuple[str, Stage], tuple[RunRecord, asyncio.Task[AgentResult]]
        ] = {}
        self._live_agent_retry_requests: set[str] = set()
        self._package_tasks: dict[str, asyncio.Task[PackageExecutionResult]] = {}
        self.package_execution = PackageExecutionLayer(
            config.settings.repo,
            config.settings.state_dir,
            state._database,
            run_steward=self._run_package_steward_agent,
            run_worker=self._run_package_worker_agent,
            validate_step=self._validate_package_step,
            validate_package=self._validate_capability_package,
            validate_consumer=self._validate_package_consumer,
            acquire_workspace=self._acquire_package_workspace,
            interface_digest=self._package_interface_digest,
            wake_consumers=self._wake_package_consumers,
            lease_ttl_seconds=config.steward.lease_ttl_seconds,
            maximum_worker_steps=config.steward.maximum_worker_steps,
        )

    @property
    def chapters(self) -> tuple[WorkUnitLike, ...]:
        """Compatibility view for callers using the previous domain name."""

        return self.work_units

    def scheduling_snapshot(self) -> dict[str, object]:
        return scheduling_snapshot(self.statement_schedule, self.proof_schedule)

    def _package_anchor(self, package: CapabilityPackage) -> WorkUnitLike:
        consumers = self.state._database.load_package_state().consumers_for(package.id)
        anchor_id = next(
            (
                consumer.work_unit_id
                for consumer in consumers
                if consumer.work_unit_id in self._work_units_by_id
            ),
            self.work_units[0].id,
        )
        return _with_scope(
            self._work_units_by_id[anchor_id],
            self._repository_package_scope(package.write_scope),
        )

    async def _package_heartbeat(
        self, package_id: str, agent_id: str, generation: int, stop: asyncio.Event
    ) -> None:
        interval = max(30.0, self.config.settings.agent_timeout_seconds / 4)
        while True:
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return
            except TimeoutError:
                await self.state.heartbeat_steward_lease(
                    package_id,
                    agent_id,
                    generation,
                    ttl_seconds=(
                        self.config.settings.agent_timeout_seconds
                        + self.config.settings.validation_timeout_seconds
                        + 600
                    ),
                )

    async def _run_package_steward_agent(
        self, package: CapabilityPackage, dossier: dict[str, Any], worktree: Path
    ) -> dict[str, Any]:
        anchor = self._package_anchor(package)
        lease = self.state._database.load_package_state().leases[package.id]
        run = await self.state.start_auxiliary_run(
            anchor.id,
            Stage.PROVE,
            role=PACKAGE_STEWARD_ROLE,
            request_ids=(package.id,),
            model=self.config.steward.model,
        )
        stop = asyncio.Event()
        heartbeat = asyncio.create_task(
            self._package_heartbeat(package.id, lease.agent_id, lease.generation, stop)
        )
        await self.agent_slots.acquire(self.proof_schedule.priority(anchor.document_id))
        try:
            result = await self.executor.run_package_steward(
                anchor, run, dossier, workspace_root=worktree
            )
        finally:
            self.agent_slots.release()
            stop.set()
            await asyncio.gather(heartbeat, return_exceptions=True)
        if not result.succeeded:
            raise PackageReportError(result.error or "package Steward agent failed")
        return result.report

    def _repository_package_scope(self, scope: tuple[str, ...]) -> tuple[str, ...]:
        """Resolve legacy Lean-root paths to repository-root overlay paths."""

        resolved: list[str] = []
        for path in scope:
            direct = self.config.settings.repo / path
            first_component = self.config.settings.repo / Path(path).parts[0]
            if direct.exists() or first_component.is_dir():
                resolved.append(path)
                continue
            prefixed = self.config.settings.lean_project / path
            target = self.config.settings.repo / prefixed
            resolved.append(prefixed.as_posix() if target.parent.exists() else path)
        return tuple(dict.fromkeys(resolved))

    async def _acquire_package_workspace(
        self, package: CapabilityPackage, generation: int, scope: tuple[str, ...]
    ) -> PackageWorkspace:
        """Acquire one ordinary overlay-backed agent workspace for a package turn."""

        if self.isolation.name != "fuse-overlay":
            raise RuntimeError("capability packages require fuse-overlay isolation")
        anchor = _with_scope(self._package_anchor(package), self._repository_package_scope(scope))
        async with self.source_lock:
            await self.git.ensure_clean(anchor)
            workspace = await self.isolation.acquire(
                f"package-{package.id}-generation-{generation}-{uuid4().hex[:12]}"
            )
        assert isinstance(workspace, FuseWorkspace)

        async def integrate(paths: tuple[str, ...], message: str) -> PackageImport:
            current_anchor = _with_scope(
                self._package_anchor(package), self._repository_package_scope(paths)
            )
            async with self.source_lock:
                await self.git.ensure_clean(current_anchor)
                isolated = await workspace.collect(current_anchor, integration_lock=None)
                if not isolated.accepted:
                    detail = isolated.error
                    if isolated.out_of_scope_paths:
                        detail += ": " + ", ".join(isolated.out_of_scope_paths)
                    raise PackageReportError(detail)
                commit = await self.git.commit(
                    current_anchor,
                    Stage.PROVE,
                    summary=message,
                    changed_paths=isolated.changed_paths,
                    subject=message,
                )
                if isolated.changed_paths:
                    self._mark_source_changed(
                        unit.id for unit in self._package_owner_units(isolated.changed_paths)
                    )
                return PackageImport(
                    isolated.changed_paths,
                    commit,
                    commit or await self.git.head(),
                )

        return PackageWorkspace(workspace.root, integrate, workspace.close)

    async def _run_package_worker_agent(
        self,
        package: CapabilityPackage,
        step: PackageStep,
        packet: dict[str, Any],
        worktree: Path,
    ) -> dict[str, Any]:
        anchor = _with_scope(
            self._package_anchor(package),
            self._repository_package_scope(step.intended_paths),
        )
        run = await self.state.start_auxiliary_run(
            anchor.id,
            Stage.PROVE,
            role=PACKAGE_WORKER_ROLE,
            request_ids=(package.id, step.id),
            model=self.config.steward.worker_model,
        )
        await self.agent_slots.acquire(self.proof_schedule.priority(anchor.document_id))
        try:
            result = await self.executor.run_package_worker(
                anchor, run, packet, workspace_root=worktree
            )
        finally:
            self.agent_slots.release()
        if not result.succeeded:
            raise PackageReportError(result.error or f"package worker {step.id} failed")
        return result.report

    def _package_owner_units(self, paths: Iterable[str]) -> tuple[WorkUnitLike, ...]:
        resolved = self._repository_package_scope(tuple(str(path) for path in paths))
        owner_ids = {owner_id for path in resolved for owner_id in self._path_owner_ids(path)}
        return tuple(
            self._work_units_by_id[owner_id]
            for owner_id in sorted(owner_ids, key=self._work_unit_order.__getitem__)
        )

    async def _validate_package_units(
        self, worktree: Path, units: Iterable[WorkUnitLike]
    ) -> PackageValidation:
        outputs: list[str] = []
        succeeded = True
        units_by_id = {unit.id: unit for unit in units}
        for unit in units_by_id.values():
            result = await validate(self.config, unit, workspace_root=worktree)
            outputs.append(f"{unit.id}:\n{result.output}")
            succeeded = succeeded and result.succeeded
        evidence = "\n\n".join(outputs) or "No configured owner build was required."
        return PackageValidation(
            succeeded,
            hashlib.sha256(evidence.encode()).hexdigest(),
            evidence,
        )

    async def _validate_package_step(self, worktree: Path, step: PackageStep) -> PackageValidation:
        return await self._validate_package_units(
            worktree, self._package_owner_units(step.intended_paths)
        )

    async def _validate_capability_package(
        self, worktree: Path, package: CapabilityPackage
    ) -> PackageValidation:
        return await self._validate_package_units(
            worktree, self._package_owner_units(package.write_scope)
        )

    async def _validate_package_consumer(
        self, worktree: Path, consumer: PackageConsumer
    ) -> ConsumerValidation:
        unit = self._work_units_by_id.get(consumer.work_unit_id)
        if unit is None:
            return ConsumerValidation(
                False,
                hashlib.sha256(b"unknown consumer").hexdigest(),
                f"consumer work unit {consumer.work_unit_id} is outside this scheduler",
                consumer.id,
            )
        validation = await self._validate_package_units(worktree, (unit,))
        consumer_path = self._repository_package_scope((consumer.path,))[0]
        placeholder = declaration_uses_placeholder(worktree, consumer_path, consumer.declaration)
        accepted = validation.succeeded and placeholder is False
        evidence = validation.evidence
        if placeholder is not False:
            evidence += "\n\nThe consumer declaration still contains a placeholder."
        return ConsumerValidation(
            accepted,
            hashlib.sha256(evidence.encode()).hexdigest(),
            evidence,
            consumer.id,
            (consumer.work_unit_id,),
        )

    def _package_interface_digest(self, interface_id: str) -> str | None:
        path = self.config.settings.repo / interface_id
        if path.is_file():
            return hashlib.sha256(path.read_bytes()).hexdigest()
        return None

    async def _wake_package_consumers(self, work_unit_ids: tuple[str, ...]) -> None:
        for work_unit_id in work_unit_ids:
            if work_unit_id not in self._work_units_by_id:
                continue
            await self.state.set_task(
                work_unit_id,
                Stage.PROVE,
                TaskStatus.PENDING,
                "accepted capability package changed this consumer",
            )
            self._proof_rechecks.add(work_unit_id)

    async def _schedule_ready_packages(self, *, exclude: Iterable[str] = ()) -> bool:
        if not self.git.enabled:
            return False
        excluded = set(exclude)
        launched = False
        for package in self.package_execution.ready_packages():
            if package.id in self._package_tasks or package.id in excluded:
                continue
            task = asyncio.create_task(
                self._execute_package_task(package.id),
                name=f"paf-package-{package.id}",
            )
            self._package_tasks[package.id] = task
            launched = True
        return launched

    async def _execute_package_task(self, package_id: str) -> PackageExecutionResult:
        try:
            result = await self.package_execution.execute(package_id)
            if result.integrated_revision:
                owners = self._package_owner_units(
                    self.state._database.load_package_state().packages[package_id].write_scope
                )
                self._mark_source_changed(unit.id for unit in owners)
            return result
        finally:
            await self.state.refresh_package_state()

    async def _drain_active_packages(self) -> tuple[PackageExecutionResult, ...]:
        results: list[PackageExecutionResult] = []
        attempted: set[str] = set()
        while self._package_tasks:
            active = dict(self._package_tasks)
            attempted.update(active)
            try:
                results.extend(await asyncio.gather(*active.values()))
            finally:
                for package_id, task in active.items():
                    if self._package_tasks.get(package_id) is task:
                        self._package_tasks.pop(package_id, None)
            if self.config.steward.enabled:
                await self._schedule_ready_packages(exclude=attempted)
        return tuple(results)

    async def prepare(
        self,
        progress: Callable[[str, int, int], None] | None = None,
    ) -> None:
        total = 7

        def report(phase: str, completed: int) -> None:
            if progress is not None:
                progress(phase, completed, total)

        report("Preparing the Lean project", 0)
        if self.config.backend is not None:
            await asyncio.to_thread(
                self.config.backend.prepare_project,
                self.config.settings.repo,
                timeout_seconds=self.config.settings.validation_timeout_seconds,
            )
        report("Loading orchestration state", 1)
        await self.state.load_or_create()
        report("Recovering interrupted work", 2)
        await self.state.requeue_interrupted(resume_agents=self.resume_agents)
        report("Scaffolding work-unit directories", 3)
        self.scaffold()
        report("Migrating persisted workflow state", 4)
        await self._migrate_persisted_content_digests()
        migrated = await self.state.migrate_post_review_fixups()
        if migrated:
            # The normal review scheduler reports an invalid import graph.
            with suppress(ValueError):
                await self._invalidate_reviews(
                    migrated,
                    detail="recovered post-review findings",
                )
        await self.state.migrate_stale_snapshot_review_requests()
        report("Preparing agent execution", 5)
        await self.executor.prepare()
        report("Preparing isolated workspaces and Lean caches", 6)
        await self.isolation.prepare()
        report("Checking the Git worktree", 7)
        await self.git.prepare()
        await self.state.refresh_package_state()
        if self.config.steward.enabled:
            await self._schedule_ready_packages()
        report("Preparation complete", 7)

    async def _commit_agent_changes(
        self,
        chapter: WorkUnitLike,
        stage: Stage,
        agent: AgentResult | None,
        isolated: IsolationResult,
    ) -> IsolationResult:
        if not isolated.accepted or not isolated.changed_paths:
            return isolated
        summary = (
            agent.report.get("summary", "")
            if agent is not None
            else (
                "Integrated partial scoped changes recovered after the agent was interrupted "
                "before returning its final summary."
            )
        )
        commit = await self.git.commit(
            chapter,
            stage,
            summary=summary if isinstance(summary, str) else "",
            changed_paths=isolated.changed_paths,
        )
        return replace(isolated, commit=commit)

    def _mark_source_changed(self, chapter_ids: Iterable[str]) -> None:
        for chapter_id in chapter_ids:
            self._source_generations[chapter_id] = self._source_generations.get(chapter_id, 0) + 1

    def _current_broken_build(
        self,
        owner_id: str,
        graph: WorkUnitImportGraph,
    ) -> BrokenBuild | None:
        broken = self._broken_builds.get(owner_id)
        if broken is None:
            return None
        required = self._dependency_closure(graph, (owner_id,))
        if required != set(broken.source_generations) or any(
            self._source_generations.get(chapter_id, 0) != broken.source_generations[chapter_id]
            for chapter_id in required
        ):
            self._broken_builds.pop(owner_id, None)
            return None
        return broken

    async def _possibly_modified_scope_ids(
        self,
        chapter_ids: Iterable[str],
        baseline: dict[str, int],
    ) -> set[str]:
        """Find scopes that changed internally or have uncommitted external edits."""

        selected = set(chapter_ids)
        changed = {
            chapter_id
            for chapter_id in selected
            if self._source_generations.get(chapter_id, 0) != baseline.get(chapter_id, 0)
        }
        if not self.git.enabled:
            return selected
        dirty_paths = await self.git.working_tree_paths()
        if dirty_paths:
            changed.update(
                chapter_id
                for chapter_id in selected.difference(changed)
                if any(self._scope_matchers[chapter_id].matches(path) for path in dirty_paths)
            )
        return changed

    async def shutdown(self) -> None:
        try:
            package_tasks = tuple(self._package_tasks.values())
            for task in package_tasks:
                task.cancel()
            await asyncio.gather(*package_tasks, return_exceptions=True)
            self._package_tasks.clear()
            if self._discovery_batch_task is not None:
                self._discovery_batch_task.cancel()
                await asyncio.gather(self._discovery_batch_task, return_exceptions=True)
                self._discovery_batch_task = None
            for pending in self._pending_discoveries:
                if not pending.future.done():
                    pending.future.cancel()
            self._pending_discoveries.clear()
            if self._build_dispatch_task is not None:
                self._build_dispatch_task.cancel()
                await asyncio.gather(self._build_dispatch_task, return_exceptions=True)
                self._build_dispatch_task = None
            for request in self._pending_build_requests:
                if not request.future.done():
                    request.future.cancel()
            self._pending_build_requests.clear()
            await self.isolation.close(preserve=self.control.stopping)
        finally:
            await self.state.close()

    def _already_done(self, chapter: WorkUnitLike, stage: Stage) -> bool:
        return not self.force and self.state.task(chapter.id, stage).status == TaskStatus.SUCCEEDED

    def scaffold(self) -> None:
        """Create configured chapter directories without creating Lean files."""

        scaffold_directories(self.config, self.work_units)

    def resolve_work_unit_id(self, selector: str) -> str:
        """Resolve one complete id or unambiguous ordinal in this invocation."""

        matches = [unit for unit in self.work_units if unit.id == selector]
        if not matches and selector.isdigit():
            matches = [unit for unit in self.work_units if unit.ordinal == int(selector)]
        if not matches:
            raise ValueError(f"work-unit selector matched nothing: {selector}")
        if len(matches) > 1:
            raise ValueError(f"work-unit selector {selector!r} is ambiguous; pass a complete id")
        return matches[0].id

    def retry_live_agent(self, chapter_selector: str) -> dict[str, object]:
        """Interrupt and continue the single agent currently executing for one chapter."""

        chapter_id = self.resolve_work_unit_id(chapter_selector)
        live = [
            (stage, active)
            for (active_chapter_id, stage), active in self._live_agent_tasks.items()
            if active_chapter_id == chapter_id and not active[1].done()
        ]
        if not live:
            raise ValueError(f"no live agent for {chapter_id}")
        if len(live) > 1:
            raise ValueError(f"multiple live agents unexpectedly found for {chapter_id}")
        stage, active = live[0]
        run, task = active
        if run.id in self._live_agent_retry_requests:
            raise ValueError(f"retry already requested for live run {run.id}")
        self._live_agent_retry_requests.add(run.id)
        task.cancel()
        return {
            "accepted": True,
            "chapter_id": chapter_id,
            "stage": stage.value,
            "interrupted_run_id": run.id,
        }

    def _observed_work_unit_graph(self) -> WorkUnitImportGraph:
        source = self.state.source_dependency_tree
        if source is self._observed_graph_source and self._observed_graph_cache is not None:
            return self._observed_graph_cache
        nodes = source.get("nodes", {})
        graph = build_source_dependency_graph(
            self.work_units,
            nodes if isinstance(nodes, dict) else {},
        )
        self._observed_graph_source = source
        self._observed_graph_cache = graph
        return graph

    def _source_input_digests(
        self,
        chapters: Iterable[WorkUnitLike],
    ) -> dict[str, str]:
        """Hash source excerpts while reading each shared document only once."""

        source_lines: dict[Path, list[str]] = {}
        digests: dict[str, str] = {}
        for chapter in chapters:
            path = self.config.settings.repo / chapter.source
            lines = source_lines.get(path)
            if lines is None:
                lines = path.read_text(encoding="utf-8").splitlines()
                source_lines[path] = lines
            selected = "\n".join(
                lines[chapter.source_span.start_line - 1 : chapter.source_span.end_line]
            )
            digests[chapter.id] = tagged_digest_text(f"{chapter.id}\0{selected}")
        return digests

    def _source_input_digest(self, chapter: WorkUnitLike) -> str:
        return self._source_input_digests((chapter,))[chapter.id]

    async def _migrate_persisted_content_digests(self) -> None:
        """Replace verified SHA/unversioned cache digests with tagged XXH digests."""

        by_id = {chapter.id: chapter for chapter in self.work_units}

        def migrate() -> tuple[bool, dict[str, str], set[str]]:
            changed = False
            current_discoveries: set[str] = set()
            source_lines: dict[Path, list[str]] = {}
            raw_nodes = self.state.source_dependency_tree.get("nodes", {})
            nodes = raw_nodes if isinstance(raw_nodes, dict) else {}
            for chapter_id, raw_record in nodes.items():
                chapter = by_id.get(chapter_id)
                if chapter is None or not isinstance(raw_record, dict):
                    continue
                stored = raw_record.get("source_digest")
                if not isinstance(stored, str):
                    continue
                path = self.config.settings.repo / chapter.source
                lines = source_lines.get(path)
                if lines is None:
                    lines = path.read_text(encoding="utf-8").splitlines()
                    source_lines[path] = lines
                selected = "\n".join(
                    lines[chapter.source_span.start_line - 1 : chapter.source_span.end_line]
                )
                migrated = migrate_digest_text(stored, f"{chapter.id}\0{selected}")
                if migrated is not None and migrated != stored:
                    raw_record["source_digest"] = migrated
                    changed = True
                    if is_legacy_digest(stored):
                        current_discoveries.add(chapter_id)

            raw_clean = self.state.formalize_graph.get("clean", {})
            clean = raw_clean if isinstance(raw_clean, dict) else {}
            proof_migrations: dict[str, str] = {}
            for chapter_id, chapter in by_id.items():
                raw_record = clean.get(chapter_id)
                record = raw_record if isinstance(raw_record, dict) else None
                proof_digest = self.state.task(chapter_id, Stage.PROVE).source_digest
                stored = {
                    value
                    for value in (
                        record.get("source_digest") if record is not None else None,
                        proof_digest,
                    )
                    if isinstance(value, str)
                }
                migrations = migrate_scope_digests(
                    self.config.settings.repo,
                    chapter,
                    stored,
                )
                if record is not None:
                    old = record.get("source_digest")
                    migrated = migrations.get(old) if isinstance(old, str) else None
                    if migrated is not None and migrated != old:
                        record["source_digest"] = migrated
                        changed = True
                if isinstance(proof_digest, str):
                    migrated = migrations.get(proof_digest)
                    if migrated is not None and migrated != proof_digest:
                        proof_migrations[chapter_id] = migrated
            return changed, proof_migrations, current_discoveries

        changed, proof_migrations, current_discoveries = await asyncio.to_thread(migrate)
        if changed or proof_migrations or current_discoveries:
            await self.state.save_digest_migration(
                proof_migrations,
                current_discoveries=current_discoveries,
            )

    def _discovery_is_current(
        self,
        chapter: WorkUnitLike,
        *,
        source_digest: str | None = None,
    ) -> bool:
        nodes = self.state.source_dependency_tree.get("nodes", {})
        record = nodes.get(chapter.id, {}) if isinstance(nodes, dict) else {}
        return (
            self.state.task(chapter.id, Stage.DISCOVER).status == TaskStatus.SUCCEEDED
            and isinstance(record, dict)
            and record.get("source_digest")
            == (source_digest if source_digest is not None else self._source_input_digest(chapter))
        )

    async def _persist_source_dependencies(
        self,
        chapter: WorkUnitLike,
        dependencies: Iterable[str],
        report: dict[str, Any],
    ) -> None:
        """Queue one discovery result for a coalesced graph transaction."""

        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        raw_issues = report.get("issues", ())
        self._pending_discoveries.append(
            PendingDiscovery(
                chapter=chapter,
                dependencies=tuple(sorted(set(dependencies))),
                summary=str(report.get("summary", "")),
                issues=tuple(raw_issues) if isinstance(raw_issues, list) else (),
                future=future,
            )
        )
        if self._discovery_batch_task is None or self._discovery_batch_task.done():
            self._discovery_batch_task = asyncio.create_task(
                self._drain_discovery_batches(), name="paf-discovery-persistence"
            )
        await future

    async def _drain_discovery_batches(self) -> None:
        try:
            while self._pending_discoveries:
                await asyncio.sleep(DISCOVERY_BATCH_SECONDS)
                batch = tuple(
                    self._pending_discoveries.popleft()
                    for _ in range(min(len(self._pending_discoveries), DISCOVERY_BATCH_MAXIMUM))
                )
                try:
                    await self._persist_discovery_batch(batch)
                except asyncio.CancelledError:
                    for pending in batch:
                        if not pending.future.done():
                            pending.future.cancel()
                    raise
                except BaseException as error:
                    for pending in batch:
                        if not pending.future.done():
                            pending.future.set_exception(error)
        finally:
            self._discovery_batch_task = None

    async def _persist_discovery_batch(self, batch: tuple[PendingDiscovery, ...]) -> None:
        """Merge a completion burst and promote every valid task atomically."""

        async with self._discovery_lock:
            previous = self.state.source_dependency_tree
            raw_nodes = previous.get("nodes", {}) if isinstance(previous, dict) else {}
            base_nodes: dict[str, object] = (
                {
                    str(chapter_id): dict(node)
                    for chapter_id, node in raw_nodes.items()
                    if isinstance(chapter_id, str) and isinstance(node, dict)
                }
                if isinstance(raw_nodes, dict)
                else {}
            )
            digests = await asyncio.to_thread(
                lambda: {
                    pending.chapter.id: self._source_input_digest(pending.chapter)
                    for pending in batch
                }
            )
            updates: dict[str, object] = {
                pending.chapter.id: {
                    "dependencies": list(pending.dependencies),
                    "source_digest": digests[pending.chapter.id],
                    "summary": pending.summary,
                    "issues": list(pending.issues),
                }
                for pending in batch
            }
            nodes: dict[str, object] = base_nodes | updates
            valid = list(batch)
            rejected: dict[str, ValueError] = {}
            try:
                graph = build_source_dependency_graph(self.work_units, nodes)
            except ValueError:
                # Cycles are exceptional. Identify only the offending results
                # while preserving batching for the normal, valid path.
                nodes = dict(base_nodes)
                valid = []
                graph = build_source_dependency_graph(self.work_units, nodes)
                for pending in batch:
                    trial = nodes | {pending.chapter.id: updates[pending.chapter.id]}
                    try:
                        candidate = build_source_dependency_graph(self.work_units, trial)
                    except ValueError as error:
                        rejected[pending.chapter.id] = error
                        continue
                    nodes = trial
                    graph = candidate
                    valid.append(pending)
            for chapter_id, node in nodes.items():
                if chapter_id in graph.dependencies and isinstance(node, dict):
                    node["dependencies"] = sorted(graph.dependencies[chapter_id])
            previous_edges = previous.get("edges") if isinstance(previous, dict) else None
            edges = [list(edge) for edge in graph.edges]
            revision = int(previous.get("revision", 0)) if isinstance(previous, dict) else 0
            if previous_edges != edges:
                revision += 1
            self.state.source_dependency_tree = {
                "algorithm": "source-discovery",
                "revision": revision,
                "order": list(graph.order),
                "edges": edges,
                "dependencies": {
                    chapter_id: sorted(required)
                    for chapter_id, required in sorted(graph.dependencies.items())
                },
                "nodes": nodes,
            }
            async with self.state.batch():
                await self.state.save("source_dependency_tree")
                await self.state.set_tasks(
                    (pending.chapter.id for pending in valid),
                    Stage.DISCOVER,
                    TaskStatus.SUCCEEDED,
                    "source dependency tree persisted",
                )
            for pending in valid:
                if not pending.future.done():
                    pending.future.set_result(None)
            for pending in batch:
                error = rejected.get(pending.chapter.id)
                if error is not None and not pending.future.done():
                    pending.future.set_exception(error)

    async def _retain_formalize_clean(
        self,
        graph: WorkUnitImportGraph,
        records: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        """Retain records whose own source is unchanged.

        Dependency freshness is represented separately.  A proof-body change
        a dependency must not erase a descendant's locally validated build fact.
        """

        def retain() -> dict[str, dict[str, Any]]:
            by_id = {chapter.id: chapter for chapter in self.work_units}
            retained: dict[str, dict[str, Any]] = {}
            for chapter_id in graph.order:
                record = records.get(chapter_id)
                if not isinstance(record, dict):
                    continue
                source = scope_digest(self.config.settings.repo, by_id[chapter_id])
                if record.get("source_digest") == source:
                    retained[chapter_id] = dict(record) | {
                        "source_digest": source,
                        "build_generation": int(record.get("build_generation", 0)),
                    }
            return retained

        return await asyncio.to_thread(retain)

    @staticmethod
    def _copy_formalize_clean(records: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """Copy persisted clean records without re-reading the whole corpus.

        Source edits integrated by the orchestrator explicitly invalidate their owners, while
        every consumer checks its own digest before trusting a record.  Graph transactions can
        therefore preserve unrelated records instead of globbing and hashing every clean work
        unit after each completed build.
        """

        return {
            str(chapter_id): dict(record)
            for chapter_id, record in records.items()
            if isinstance(chapter_id, str) and isinstance(record, dict)
        }

    async def _retained_formalize_record(
        self,
        chapter: WorkUnitLike,
        records: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Validate only the clean record a scheduler decision is about to consume."""

        record = records.get(chapter.id)
        if not isinstance(record, dict):
            return None
        source = await asyncio.to_thread(scope_digest, self.config.settings.repo, chapter)
        if record.get("source_digest") != source:
            return None
        return dict(record) | {
            "source_digest": source,
            "build_generation": int(record.get("build_generation", 0)),
        }

    @staticmethod
    def _dependency_closure(graph: WorkUnitImportGraph, chapter_ids: Iterable[str]) -> set[str]:
        required = set(chapter_ids)
        pending = list(required)
        while pending:
            chapter_id = pending.pop()
            for dependency in graph.dependencies.get(chapter_id, frozenset()):
                if dependency not in required:
                    required.add(dependency)
                    pending.append(dependency)
        return required

    @staticmethod
    def _successor_closure(graph: WorkUnitImportGraph, chapter_ids: Iterable[str]) -> set[str]:
        affected = set(chapter_ids)
        pending = list(affected)
        while pending:
            chapter_id = pending.pop()
            for successor in graph.successors.get(chapter_id, frozenset()):
                if successor not in affected:
                    affected.add(successor)
                    pending.append(successor)
        return affected

    @staticmethod
    def _dependency_frontiers_ready(
        graph: WorkUnitImportGraph,
        completed: set[str],
    ) -> set[str]:
        """Return nodes whose entire transitive prerequisite frontier is complete."""

        ready: set[str] = set()
        for chapter_id in graph.order:
            if all(
                dependency in completed and dependency in ready
                for dependency in graph.dependencies.get(chapter_id, frozenset())
            ):
                ready.add(chapter_id)
        return ready

    async def _publish_validated_builds(
        self,
        snapshots: dict[str, ValidatedBuildSnapshot],
    ) -> bool:
        """Publish only if the exact source snapshot built is still current."""

        if not snapshots:
            return False
        async with self.source_lock:
            graph = self._observed_work_unit_graph()
            captured: dict[str, str] = {}
            captured_generations: dict[str, int] = {}
            required: set[str] = set()
            for chapter_id, snapshot in snapshots.items():
                # Discovery is scheduling guidance, not part of the Lean build
                # certificate.  A concurrently refined discovery graph must not
                # invalidate a build of an unchanged source snapshot.
                required.update(self._dependency_closure(snapshot.graph, (chapter_id,)))
                for chapter_id, digest in snapshot.source_digests.items():
                    existing = captured.setdefault(chapter_id, digest)
                    if existing != digest:
                        return False
                for chapter_id, generation in snapshot.source_generations.items():
                    existing_generation = captured_generations.setdefault(chapter_id, generation)
                    if existing_generation != generation:
                        return False
            if not required.issubset(captured):
                return False
            by_id = self._work_units_by_id
            if required.issubset(captured_generations):
                possibly_modified = await self._possibly_modified_scope_ids(
                    required, captured_generations
                )
            else:
                possibly_modified = required
            current = await asyncio.to_thread(
                _scope_digests,
                self.config.settings.repo,
                by_id,
                possibly_modified,
            )
            if any(captured[chapter_id] != digest for chapter_id, digest in current.items()):
                return False

            persisted = self.state.formalize_graph.get("clean", {})
            records = persisted if isinstance(persisted, dict) else {}
            clean = self._copy_formalize_clean(records)
            raw_interfaces = self.state.formalize_graph.get("interfaces", {})
            interfaces = raw_interfaces if isinstance(raw_interfaces, dict) else {}
            raw_imports = self.state.formalize_graph.get("interface_imports", {})
            compiled_imports = (
                {
                    str(chapter_id): tuple(item for item in dependencies if isinstance(item, str))
                    for chapter_id, dependencies in raw_imports.items()
                    if isinstance(chapter_id, str) and isinstance(dependencies, list)
                }
                if isinstance(raw_imports, dict)
                else {}
            )
            interface_updates = {
                chapter_id: snapshot.fingerprint
                for chapter_id, snapshot in snapshots.items()
                if snapshot.fingerprint is not None
            }
            import_updates = {
                chapter_id: snapshot.import_dependencies
                for chapter_id, snapshot in snapshots.items()
                if snapshot.fingerprint is not None
            }
            imports_changed = any(
                compiled_imports.get(chapter_id) != dependencies
                for chapter_id, dependencies in import_updates.items()
            )
            invalidation_graph = (
                self._interface_invalidation_graph(graph, compiled_imports | import_updates)
                if imports_changed
                else self._persisted_interface_invalidation_graph(graph, compiled_imports)
            )
            invalidated: set[str] = set()
            previous_dirty = set(self.state.formalize_graph.get("dirty", ()))
            previous_stale = set(self.state.formalize_graph.get("interface_stale", ()))
            metric_updates: dict[str, int] = {}
            invalidation_events: list[tuple[str, str, str | None, str | None, tuple[str, ...]]] = []
            for chapter_id, snapshot in snapshots.items():
                old_files = self._file_interface_digests(interfaces.get(chapter_id))
                new_files = self._file_interface_digests(snapshot.fingerprint)
                if new_files is None:
                    metric_updates["fingerprint_failures"] = (
                        metric_updates.get("fingerprint_failures", 0) + 1
                    )
                    continue
                if old_files is None:
                    metric_updates["interface_baselines_initialized"] = metric_updates.get(
                        "interface_baselines_initialized", 0
                    ) + len(new_files)
                    continue
                # A file enters invalidation only after PAF has observed a successful
                # fingerprint for it. Newly observed files establish their golden
                # baseline; changed and deleted observed files invalidate successors.
                added_files = set(new_files).difference(old_files)
                if added_files:
                    metric_updates["interface_baselines_initialized"] = metric_updates.get(
                        "interface_baselines_initialized", 0
                    ) + len(added_files)
                changed_files = [
                    (chapter_id, source, old_files[source], new_files.get(source))
                    for source in sorted(old_files)
                    if old_files[source] != new_files.get(source)
                ]
                if changed_files:
                    changed_successors = self._successor_closure(
                        invalidation_graph, (chapter_id,)
                    ) - set(snapshots)
                    invalidation_events.extend(
                        (*changed_file, tuple(sorted(changed_successors)))
                        for changed_file in changed_files
                    )
                    invalidated.update(changed_successors)
                    metric_updates["interface_changing_edits"] = (
                        metric_updates.get("interface_changing_edits", 0) + 1
                    )
                    metric_updates["descendants_queued"] = metric_updates.get(
                        "descendants_queued", 0
                    ) + len(changed_successors)
                elif chapter_id in previous_dirty and chapter_id not in previous_stale:
                    metric_updates["interface_preserving_edits"] = (
                        metric_updates.get("interface_preserving_edits", 0) + 1
                    )
            for chapter_id in invalidated:
                clean.pop(chapter_id, None)
            build_generation = int(self.state.formalize_graph.get("build_generation", 0))
            for chapter_id in graph.order:
                if chapter_id not in required:
                    continue
                source = captured[chapter_id]
                record = clean.get(chapter_id)
                if isinstance(record, dict) and record.get("source_digest") == source:
                    if fingerprint := interface_updates.get(chapter_id):
                        record.update(fingerprint)
                    continue
                build_generation += 1
                clean[chapter_id] = {
                    "source_digest": source,
                    "build_generation": build_generation,
                } | interface_updates.get(chapter_id, {})
            fingerprint_errors = tuple(
                snapshot.fingerprint_error
                for snapshot in snapshots.values()
                if snapshot.fingerprint_error
            )
            automatically_rechecked = len(previous_stale & required)
            if automatically_rechecked:
                metric_updates["automatic_successful_rechecks"] = automatically_rechecked
            await self._save_formalize_graph(
                graph,
                clean,
                build_generation=build_generation,
                invalidated=invalidated,
                validated=required,
                interface_updates=interface_updates,
                import_updates=import_updates,
                fingerprint_error="\n\n".join(fingerprint_errors)[-4000:],
                interface_invalidated=invalidated,
                interface_validated=required,
                metric_updates=metric_updates,
            )
            for (
                chapter_id,
                source_file,
                old_digest,
                new_digest,
                affected_ids,
            ) in invalidation_events:
                with suppress(Exception):
                    await self.state.record_interface_invalidation(
                        work_unit_id=chapter_id,
                        source_file=source_file,
                        old_digest=old_digest,
                        new_digest=new_digest,
                        invalidated_work_unit_ids=affected_ids,
                    )
            return True

    async def _publish_validated_build(
        self,
        chapter: WorkUnitLike,
        snapshot: ValidatedBuildSnapshot,
    ) -> bool:
        return await self._publish_validated_builds({chapter.id: snapshot})

    @staticmethod
    def _file_interface_digests(record: Any) -> dict[str, str] | None:
        """Return the persisted file/signature pairs from one fingerprint record."""

        if not isinstance(record, dict) or not isinstance(record.get("modules"), list):
            return None
        pairs: dict[str, str] = {}
        for module in record["modules"]:
            if not isinstance(module, dict):
                return None
            source = module.get("source")
            digest = module.get("interface_digest")
            if not isinstance(source, str) or not isinstance(digest, str) or not digest:
                return None
            pairs[source] = digest
        return pairs or None

    async def _invalidate_build_records(self, chapter_ids: Iterable[str]) -> set[str]:
        """Mark edited sources stale while retaining known downstream interfaces."""

        graph = self._observed_work_unit_graph()
        targets = set(chapter_ids)
        persisted = self.state.formalize_graph.get("clean", {})
        clean = self._copy_formalize_clean(persisted if isinstance(persisted, dict) else {})
        for chapter_id in targets:
            clean.pop(chapter_id, None)
        await self._save_formalize_graph(
            graph,
            clean,
            build_generation=int(self.state.formalize_graph.get("build_generation", 0)),
            invalidated=targets,
        )
        return targets

    async def _save_formalize_graph(
        self,
        graph: WorkUnitImportGraph,
        clean: dict[str, dict[str, Any]],
        *,
        build_generation: int,
        invalidated: Iterable[str] = (),
        validated: Iterable[str] = (),
        interface_updates: dict[str, dict[str, Any]] | None = None,
        import_updates: dict[str, tuple[str, ...]] | None = None,
        fingerprint_error: str = "",
        interface_invalidated: Iterable[str] = (),
        interface_validated: Iterable[str] = (),
        metric_updates: dict[str, int] | None = None,
    ) -> int:
        async with self._formalize_graph_lock:
            explicitly_invalidated = set(invalidated)
            explicitly_validated = set(validated)
            previous = self.state.formalize_graph
            graph_unchanged = (
                previous is self._saved_dependency_state
                and previous.get("edges") is self._saved_dependency_edges
                and graph is self._saved_dependency_graph
            )
            if graph_unchanged:
                dependency_snapshot = {
                    key: previous[key]
                    for key in ("algorithm", "order", "edges", "dependencies")
                    if key in previous
                }
            else:
                dependency_snapshot = graph.snapshot()
                previous_edges = previous.get("edges") if isinstance(previous, dict) else None
                graph_unchanged = (
                    previous.get("algorithm") == "source-dependency-tree"
                    and previous_edges == dependency_snapshot["edges"]
                )
            current = previous.get("clean", {}) if isinstance(previous, dict) else {}
            if isinstance(current, dict):
                for chapter_id, record in current.items():
                    if not isinstance(record, dict):
                        continue
                    if chapter_id in explicitly_invalidated:
                        continue
                    local = clean.get(chapter_id)
                    local_generation = (
                        int(local.get("build_generation", 0)) if isinstance(local, dict) else -1
                    )
                    # Merge concurrently published exact builds without
                    # resurrecting records this caller explicitly invalidated.
                    if int(record.get("build_generation", 0)) >= local_generation:
                        clean[chapter_id] = dict(record)
            revision = int(previous.get("revision", 0)) if isinstance(previous, dict) else 0
            if not graph_unchanged:
                revision += 1
            build_generation = max(
                build_generation,
                int(previous.get("build_generation", 0)) if isinstance(previous, dict) else 0,
            )
            dirty = set(previous.get("dirty", ())) if isinstance(previous, dict) else set()
            dirty.update(explicitly_invalidated)
            dirty.difference_update(explicitly_validated)
            raw_interfaces = previous.get("interfaces", {})
            interfaces = (
                {
                    str(chapter_id): dict(record)
                    for chapter_id, record in raw_interfaces.items()
                    if isinstance(chapter_id, str) and isinstance(record, dict)
                }
                if isinstance(raw_interfaces, dict)
                else {}
            )
            interfaces.update(interface_updates or {})
            for chapter_id, record in clean.items():
                if record.get("interface_digest") and chapter_id not in interfaces:
                    interfaces[chapter_id] = {
                        key: value
                        for key, value in record.items()
                        if key
                        in {
                            "artifact_digest",
                            "interface_digest",
                            "fingerprint_schema",
                            "lean_version",
                            "modules",
                        }
                    }
            raw_imports = previous.get("interface_imports", {})
            interface_imports = (
                {
                    str(chapter_id): tuple(item for item in required if isinstance(item, str))
                    for chapter_id, required in raw_imports.items()
                    if isinstance(chapter_id, str) and isinstance(required, list)
                }
                if isinstance(raw_imports, dict)
                else {}
            )
            imports_changed = any(
                interface_imports.get(chapter_id) != dependencies
                for chapter_id, dependencies in (import_updates or {}).items()
            )
            interface_imports.update(import_updates or {})
            invalidation_graph = (
                self._interface_invalidation_graph(graph, interface_imports)
                if imports_changed
                else self._persisted_interface_invalidation_graph(graph, interface_imports)
            )
            interface_stale = set(previous.get("interface_stale", ()))
            interface_stale.update(interface_invalidated)
            interface_stale.difference_update(interface_validated)
            raw_metrics = previous.get("fingerprint_metrics", {})
            fingerprint_metrics = (
                {
                    str(name): int(value)
                    for name, value in raw_metrics.items()
                    if isinstance(name, str) and isinstance(value, int)
                }
                if isinstance(raw_metrics, dict)
                else {}
            )
            for name, increment in (metric_updates or {}).items():
                fingerprint_metrics[name] = fingerprint_metrics.get(name, 0) + increment
            previous_interface_graph = previous.get("interface_import_graph")
            interface_graph_snapshot = (
                previous_interface_graph
                if graph_unchanged
                and not imports_changed
                and isinstance(previous_interface_graph, dict)
                else invalidation_graph.snapshot() | {"coverage": len(interface_imports)}
            )
            self.state.formalize_graph = dependency_snapshot | {
                "algorithm": "source-dependency-tree",
                "revision": revision,
                "build_generation": build_generation,
                "clean": clean,
                "dirty": sorted(dirty),
                "interfaces": interfaces,
                "interface_imports": {
                    chapter_id: list(required)
                    for chapter_id, required in sorted(interface_imports.items())
                },
                "interface_import_graph": interface_graph_snapshot,
                "fingerprint_schema": "olean-proof-erased-v1",
                "last_fingerprint_error": fingerprint_error,
                "interface_stale": sorted(interface_stale),
                "fingerprint_metrics": fingerprint_metrics,
            }
            self._saved_dependency_state = self.state.formalize_graph
            self._saved_dependency_edges = self.state.formalize_graph.get("edges")
            self._saved_dependency_graph = graph
            self._compiled_interface_state = self.state.formalize_graph
            self._compiled_interface_imports = self.state.formalize_graph.get("interface_imports")
            self._compiled_interface_fallback = graph
            self._compiled_interface_graph = invalidation_graph
            # Parallel builds often publish in bursts.  The scheduler consumes this graph from
            # memory, so coalesce its expensive normalized database projection without delaying
            # downstream scheduling; StateStore.close still provides a durability barrier.
            self.state.save_deferred("formalize_graph")
            return revision

    def _persisted_interface_invalidation_graph(
        self,
        fallback: WorkUnitImportGraph,
        compiled: dict[str, tuple[str, ...]],
    ) -> WorkUnitImportGraph:
        if (
            self.state.formalize_graph is self._compiled_interface_state
            and self.state.formalize_graph.get("interface_imports")
            is self._compiled_interface_imports
            and fallback is self._compiled_interface_fallback
            and self._compiled_interface_graph is not None
        ):
            return self._compiled_interface_graph
        return self._interface_invalidation_graph(fallback, compiled)

    def _interface_invalidation_graph(
        self,
        fallback: WorkUnitImportGraph,
        compiled: dict[str, tuple[str, ...]],
    ) -> WorkUnitImportGraph:
        dependencies = {
            chapter.id: set(compiled.get(chapter.id, fallback.dependencies[chapter.id]))
            for chapter in self.work_units
        }
        try:
            return build_compiled_import_graph(self.work_units, dependencies)
        except ValueError:
            # Partial migration can temporarily combine two individually valid
            # graph revisions into a work-unit cycle. Stay conservative until
            # the remaining compiled records replace the fallback edges.
            return fallback

    def _interface_dependencies_are_current(
        self,
        graph: WorkUnitImportGraph,
        chapter_id: str,
    ) -> bool:
        stale_value = self.state.formalize_graph.get("interface_stale", ())
        stale = set(stale_value) if isinstance(stale_value, list) else set()
        if not stale:
            return True
        raw_imports = self.state.formalize_graph.get("interface_imports", {})
        cache_key = (
            id(self.state.formalize_graph),
            id(raw_imports),
            id(stale_value),
            id(graph),
        )
        if cache_key == self._interface_graph_key and self._interface_graph_cache is not None:
            return chapter_id not in self._interface_stale_cache
        compiled = (
            {
                str(owner): tuple(item for item in dependencies if isinstance(item, str))
                for owner, dependencies in raw_imports.items()
                if isinstance(owner, str) and isinstance(dependencies, list)
            }
            if isinstance(raw_imports, dict)
            else {}
        )
        interface_graph = self._persisted_interface_invalidation_graph(graph, compiled)
        self._interface_graph_key = cache_key
        self._interface_graph_cache = interface_graph
        self._interface_stale_cache = self._successor_closure(interface_graph, stale)
        return chapter_id not in self._interface_stale_cache

    async def _scope_exists(self, chapter: WorkUnitLike) -> bool:
        return await asyncio.to_thread(
            ScopeMatcher(chapter.scope).has_match_for_primary_pattern,
            self.config.settings.repo,
        )

    async def _proof_build_is_fresh(self, chapter: WorkUnitLike) -> bool:
        """Whether the current chapter source belongs to a retained clean build."""

        graph = self._observed_work_unit_graph()
        persisted = self.state.formalize_graph.get("clean", {})
        records = persisted if isinstance(persisted, dict) else {}
        record = await self._retained_formalize_record(chapter, records)
        return record is not None and self._interface_dependencies_are_current(graph, chapter.id)

    async def _attempt(
        self,
        chapter: WorkUnitLike,
        stage: Stage,
        *,
        feedback: str = "",
        queue_detail: str = "",
        role: str = "",
        request_ids: Iterable[str] = (),
        proof_targets: Iterable[ProofTarget] = (),
        priority_override: float | None = None,
        resume_thread_id: str | None = None,
        resume_run_id: str = "",
        resume_prompt: str = "",
    ) -> Attempt:
        auxiliary = role in {
            WARNING_REVIEW_ROLE,
            PROOF_REVIEW_ROLE,
        }
        selected_request_ids = tuple(dict.fromkeys(request_ids))
        selected_proof_targets = tuple(proof_targets)
        await self.control.checkpoint()
        schedule = (
            self.statement_schedule
            if stage in (Stage.DISCOVER, Stage.FORMALIZE, Stage.REVIEW)
            else self.proof_schedule
        )
        chapter_lock = self._chapter_agent_locks[chapter.id]
        await chapter_lock.acquire()
        chapter_lock_held = True
        slot_held = False
        reservation = None
        reservation_owner_id = (
            f"ordinary-{chapter.id}-{stage.value}-{uuid4().hex}"
            if stage is not Stage.DISCOVER
            else ""
        )
        slots = (
            self.discovery_slots if stage is Stage.DISCOVER and not auxiliary else self.agent_slots
        )
        try:
            await self.control.checkpoint()
            if not auxiliary:
                await self.state.set_task(
                    chapter.id,
                    stage,
                    TaskStatus.PENDING,
                    queue_detail or f"queued for {stage.value} agent",
                    queued=True,
                )
            if reservation_owner_id:
                specs = _mutation_reservation_specs(chapter)
                while True:
                    reservation = await self.state.claim_ordinary_path_reservations(
                        reservation_owner_id,
                        specs,
                        ttl_seconds=(
                            self.config.settings.agent_timeout_seconds
                            + self.config.settings.validation_timeout_seconds
                            + 300
                        ),
                    )
                    if reservation.granted:
                        break
                    await self.control.checkpoint()
                    await asyncio.sleep(0.1)
            await slots.acquire(
                priority_override
                if priority_override is not None
                else schedule.priority(chapter.document_id)
            )
            slot_held = True
        except BaseException:
            if reservation is not None:
                await self.state.release_ordinary_path_reservations(
                    reservation_owner_id, reservation.fence_generation
                )
            chapter_lock.release()
            task = self.state.task(chapter.id, stage)
            if not auxiliary and task.queued:
                await self.state.set_task(
                    chapter.id,
                    stage,
                    TaskStatus.PENDING,
                    "agent start interrupted while queued",
                )
            raise
        run = None
        workspace = None
        source_held = False
        isolated: IsolationResult | None = None
        live_discovery = not auxiliary and stage is Stage.DISCOVER

        async def start_agent_run() -> RunRecord:
            if auxiliary:
                started = await self.state.start_auxiliary_run(
                    chapter.id,
                    stage,
                    role=role,
                    request_ids=selected_request_ids,
                    model=None,
                )
            else:
                started = await self.state.start_run(chapter.id, stage)
                run_updates: dict[str, Any] = {
                    "prompt_kind": report_schema_key(stage, role=role, feedback=feedback),
                }
                if role:
                    run_updates["role"] = role
                if selected_request_ids:
                    run_updates["request_ids"] = list(selected_request_ids)
                if selected_proof_targets:
                    run_updates["proof_targets"] = [
                        target.as_dict() for target in selected_proof_targets
                    ]
                await self.state.update_run(started, **run_updates)
            return started

        try:
            run = await start_agent_run()
            if live_discovery:
                workspace_root = self.config.settings.repo
            elif self.isolation.name == "shared":
                await self.source_lock.acquire()
                source_held = True
                await self.git.ensure_clean(chapter)
                workspace = await self.isolation.acquire(run.id)
                snapshot = getattr(workspace, "snapshot", None)
                if snapshot is not None:
                    await snapshot(chapter)
                workspace_root = workspace.root
            else:
                async with self.source_lock:
                    await self.git.ensure_clean(chapter)
                interrupted_run = (
                    self.executor.interrupted_predecessor(run, stage)
                    if self.resume_agents
                    else None
                )
                workspace = await self.isolation.acquire(
                    run.id,
                    resume_run_id=interrupted_run.id if interrupted_run is not None else "",
                )
                workspace_root = workspace.root
            while True:
                if resume_thread_id is not None:
                    operation = self.executor.resume(
                        chapter,
                        stage,
                        run,
                        thread_id=resume_thread_id,
                        previous_run_id=resume_run_id,
                        reminder=resume_prompt,
                        feedback=feedback,
                        workspace_root=workspace_root,
                    )
                else:
                    operation = self.executor.run(
                        chapter,
                        stage,
                        run,
                        feedback=feedback,
                        workspace_root=workspace_root,
                    )
                agent_task = asyncio.create_task(operation)
                active_run = run
                live_key = (chapter.id, stage)
                self._live_agent_tasks[live_key] = (active_run, agent_task)
                try:
                    agent = await agent_task
                except asyncio.CancelledError:
                    current = asyncio.current_task()
                    operator_retry = active_run.id in self._live_agent_retry_requests
                    if (
                        not operator_retry
                        or self.control.stopping
                        or (current is not None and current.cancelling())
                    ):
                        self._live_agent_retry_requests.discard(active_run.id)
                        raise
                    self._live_agent_retry_requests.discard(active_run.id)
                    previous_run = active_run
                    if previous_run.status == TaskStatus.RUNNING:
                        await self.state.finish_run(
                            previous_run,
                            status=TaskStatus.INTERRUPTED,
                            thread_id=previous_run.thread_id,
                        )
                    run = await start_agent_run()
                    resume_thread_id = previous_run.thread_id
                    resume_run_id = previous_run.id
                    resume_prompt = LIVE_AGENT_RETRY_PROMPT
                    continue
                finally:
                    if self._live_agent_tasks.get(live_key) == (active_run, agent_task):
                        self._live_agent_tasks.pop(live_key, None)
                break
            slots.release()
            slot_held = False
            # Agent capacity covers live Codex processes, not integration or a
            # coordinator build queued after they exit.
            if not auxiliary:
                await self.state.set_task_phase(
                    chapter.id,
                    stage,
                    TaskPhase.POSTPROCESS,
                    f"postprocessing completed {stage.value} agent result",
                )
            if live_discovery:
                isolated = IsolationResult(accepted=True, generation=0)
            else:
                if not source_held:
                    await self.source_lock.acquire()
                    source_held = True
                assert workspace is not None
                isolated = await workspace.collect(chapter, integration_lock=None)
                isolated = await self._commit_agent_changes(chapter, stage, agent, isolated)
                if isolated.accepted and isolated.changed_paths:
                    self._mark_source_changed((chapter.id,))
                await workspace.close()
                workspace = None
                if source_held:
                    self.source_lock.release()
                    source_held = False
            if (
                isolated.accepted
                and agent.changed
                and stage
                in (
                    Stage.FORMALIZE,
                    Stage.REVIEW,
                    Stage.PROVE,
                )
            ):
                invalidated_builds = await self._invalidate_build_records((chapter.id,))
                if stage is Stage.REVIEW or auxiliary:
                    self._proof_rechecks.update(invalidated_builds)
            assigned_targets_satisfied = bool(selected_proof_targets) and all(
                declaration_uses_placeholder(
                    self.config.settings.repo,
                    target.path,
                    target.declaration,
                )
                is False
                for target in selected_proof_targets
            )
            if isolated.accepted:
                if stage is Stage.PROVE and (
                    agent.changed or self.force or assigned_targets_satisfied
                ):
                    snapshots: dict[str, ValidatedBuildSnapshot] = {}
                    validation = (
                        await self._build_chapters(
                            (chapter,),
                            publish_if_clean=True,
                            mode="proof-certification",
                            stage=Stage.PROVE,
                            snapshots=snapshots,
                        )
                    )[chapter.id]
                    if validation.succeeded and not await self._publish_validated_build(
                        chapter, snapshots[chapter.id]
                    ):
                        validation = ValidationResult(
                            False,
                            1,
                            "Source scope changed after the coordinator build; retry required.",
                            status=ValidationStatus.STALE_SNAPSHOT,
                        )
                    elif validation.warnings_only:
                        if await self._publish_validated_build(chapter, snapshots[chapter.id]):
                            warning_output = validation.output
                            await self._queue_warning_cleanup(
                                chapter,
                                validation,
                                stage=Stage.PROVE,
                            )
                            validation = ValidationResult(
                                True,
                                0,
                                "Lean build succeeded; warning cleanup queued.\n\n"
                                + warning_output,
                                process_exit_code=0,
                                status=ValidationStatus.CLEAN,
                            )
                        else:
                            validation = ValidationResult(
                                False,
                                1,
                                "Source scope changed after the coordinator build; retry required.",
                                status=ValidationStatus.STALE_SNAPSHOT,
                            )
                elif stage is Stage.PROVE:
                    build_fresh = await self._proof_build_is_fresh(chapter)
                    validation = (
                        ValidationResult(
                            True,
                            0,
                            "unchanged proof source reused the incoming clean build",
                            status=ValidationStatus.CLEAN,
                        )
                        if build_fresh
                        else await self._refresh_stale_proof_build(chapter)
                    )
                else:
                    validation = ValidationResult(
                        True,
                        0,
                        "validation deferred to the coordinator formalize loop",
                        status=ValidationStatus.DEFERRED,
                    )
            else:
                stale_snapshot = isolated.stale_scope
                validation = ValidationResult(
                    False,
                    1,
                    f"Isolation rejected the agent result: {isolated.error}",
                    status=(
                        ValidationStatus.STALE_SNAPSHOT
                        if stale_snapshot
                        else ValidationStatus.TARGET_FAILED
                    ),
                )
            if not isolated.accepted:
                detail = isolated.error
                if isolated.out_of_scope_paths:
                    detail += ": " + ", ".join(isolated.out_of_scope_paths)
                agent = replace(agent, succeeded=False, error=detail)
                validation = ValidationResult(
                    False,
                    1,
                    f"Isolation rejected the agent result: {detail}\n\n{validation.output}",
                    status=validation.status,
                )
                await self.state.update_run(run, status=TaskStatus.FAILED)
            await self.state.update_run(
                run,
                isolation=isolated.as_dict(),
                validation=validation.as_dict(),
            )
        except BaseException as error:
            interrupted_isolation: dict[str, object] | None = None
            if (
                isinstance(error, asyncio.CancelledError)
                and self.control.stopping
                and workspace is not None
                and self.isolation.name == "fuse-overlay"
            ):
                assert isinstance(workspace, FuseWorkspace)
                workspace.preserve(run.id if run is not None else "")
                interrupted_isolation = {
                    "accepted": False,
                    "interrupted": True,
                    "preserved": True,
                    "workspace": str(workspace.root),
                }
            if run is not None:
                detail = str(error) or type(error).__name__
                failure_isolation = interrupted_isolation
                if failure_isolation is None and isolated is not None:
                    failure_isolation = isolated.as_dict()
                    failure_isolation["error"] = (
                        f"changes integrated but orchestration failed before completion: {detail}"
                    )
                await self.state.finish_run(
                    run,
                    status=(
                        TaskStatus.INTERRUPTED
                        if isinstance(error, asyncio.CancelledError)
                        else TaskStatus.FAILED
                    ),
                    isolation=failure_isolation
                    or {
                        "accepted": False,
                        "error": f"orchestration failed before completion: {detail}",
                    },
                )
            raise
        finally:
            if slot_held:
                slots.release()
            if workspace is not None and not getattr(workspace, "preserved", False):
                await workspace.close()
            if source_held:
                self.source_lock.release()
            if reservation is not None:
                await self.state.release_ordinary_path_reservations(
                    reservation_owner_id, reservation.fence_generation
                )
            if chapter_lock_held:
                chapter_lock.release()
            task = self.state.task(chapter.id, stage)
            if not auxiliary and task.queued:
                await self.state.set_task(
                    chapter.id,
                    stage,
                    TaskStatus.PENDING,
                    "agent start interrupted while queued",
                )
        assert run is not None
        return Attempt(agent=agent, validation=validation, run=run)

    async def _discover(self, chapter: WorkUnitLike, *, rerun: bool = False) -> StageOutcome:
        if not rerun and not self.force and self._discovery_is_current(chapter):
            return StageOutcome(ExecutionDisposition.SUCCEEDED)
        attempt = await self._attempt(
            chapter,
            Stage.DISCOVER,
            queue_detail="reading source and discovering direct dependencies",
        )
        complete = bool(attempt.agent.report.get("complete"))
        raw_dependencies = attempt.agent.report.get("source_dependencies", ())
        dependencies = (
            tuple(dict.fromkeys(item for item in raw_dependencies if isinstance(item, str)))
            if isinstance(raw_dependencies, list)
            else ()
        )
        configured = tuple(chapter.depends_on)
        dependencies = tuple(dict.fromkeys((*configured, *dependencies)))
        known = {item.id for item in self.work_units}
        invalid = set(dependencies).difference(known)
        if chapter.id in dependencies:
            invalid.add(chapter.id)
        if attempt.agent.changed:
            await self.state.set_task(
                chapter.id,
                Stage.DISCOVER,
                TaskStatus.FAILED,
                "discovery must be read-only",
            )
            return StageOutcome(ExecutionDisposition.FAILED)
        if attempt.agent.succeeded and attempt.validation.succeeded and complete and not invalid:
            try:
                await self._persist_source_dependencies(chapter, dependencies, attempt.agent.report)
            except ValueError as error:
                await self.state.set_task(
                    chapter.id,
                    Stage.DISCOVER,
                    TaskStatus.FAILED,
                    str(error),
                )
                return StageOutcome(ExecutionDisposition.FAILED)
            return StageOutcome(ExecutionDisposition.SUCCEEDED)
        await self.state.set_task(
            chapter.id,
            Stage.DISCOVER,
            TaskStatus.FAILED,
            (
                "discovery reported unknown dependency ids: " + ", ".join(sorted(invalid))
                if invalid
                else "source dependency discovery was incomplete"
            ),
        )
        return StageOutcome(ExecutionDisposition.FAILED)

    async def _formalize(self, chapter: WorkUnitLike, *, rerun: bool = False) -> StageOutcome:
        if self._already_done(chapter, Stage.FORMALIZE):
            return StageOutcome(ExecutionDisposition.SUCCEEDED)
        maximum = self.config.stages[Stage.FORMALIZE].max_rounds
        feedback = ""
        last_attempt: Attempt | None = None
        for iteration in range(1, maximum + 1):
            if await self._scope_exists(chapter):
                snapshots: dict[str, ValidatedBuildSnapshot] = {}
                validation = (
                    await self._build_chapters(
                        (chapter,),
                        publish_if_clean=True,
                        mode="source-tree-formalize",
                        iteration=iteration,
                        maximum_iterations=maximum,
                        stage=Stage.FORMALIZE,
                        snapshots=snapshots,
                    )
                )[chapter.id]
                if (
                    last_attempt is not None
                    and (previous_run := getattr(last_attempt, "run", None)) is not None
                ):
                    await self.state.update_run(previous_run, validation=validation.as_dict())
                if validation.succeeded or validation.warnings_only:
                    return await self._complete_formalize_build(
                        chapter, validation, snapshots[chapter.id]
                    )
                if validation.status is ValidationStatus.DEPENDENCY_FAILED:
                    return await self._block_on_dependency_diagnostics(chapter, validation)
                if validation.blocked_by:
                    await self._block_on_dependency_diagnostics(
                        chapter, validation, block_consumer=False
                    )
                diagnostics = await self._build_feedback_async({chapter.id: validation})
                feedback = diagnostics.actionable.get(chapter.id, validation.output)

            attempt = await self._attempt(
                chapter,
                Stage.FORMALIZE,
                feedback=feedback,
                queue_detail=f"dependency-ready formalization pass {iteration}/{maximum}",
            )
            last_attempt = attempt
            complete = bool(attempt.agent.report.get("complete"))
            if attempt.agent.capacity_exhausted:
                await self.state.set_task(
                    chapter.id,
                    Stage.FORMALIZE,
                    TaskStatus.FAILED,
                    "model capacity remained unavailable after the configured retries",
                )
                return StageOutcome(ExecutionDisposition.FAILED)
            if not attempt.agent.succeeded or not attempt.validation.succeeded:
                feedback = attempt.feedback()
                continue
            if not complete:
                feedback = attempt.feedback()
                continue

        if await self._scope_exists(chapter):
            snapshots = {}
            validation = (
                await self._build_chapters(
                    (chapter,),
                    publish_if_clean=True,
                    mode="source-tree-formalize-final",
                    iteration=maximum,
                    maximum_iterations=maximum,
                    stage=Stage.FORMALIZE,
                    snapshots=snapshots,
                )
            )[chapter.id]
            if (
                last_attempt is not None
                and (previous_run := getattr(last_attempt, "run", None)) is not None
            ):
                await self.state.update_run(previous_run, validation=validation.as_dict())
            if validation.succeeded or validation.warnings_only:
                return await self._complete_formalize_build(
                    chapter, validation, snapshots[chapter.id]
                )
            if validation.status is ValidationStatus.DEPENDENCY_FAILED:
                return await self._block_on_dependency_diagnostics(chapter, validation)
            if validation.blocked_by:
                await self._block_on_dependency_diagnostics(
                    chapter, validation, block_consumer=False
                )

        await self.state.set_task(
            chapter.id,
            Stage.FORMALIZE,
            TaskStatus.FAILED,
            f"formalization did not reach clean diagnostics in {maximum} attempts",
        )
        return StageOutcome(ExecutionDisposition.FAILED)

    async def _complete_formalize_build(
        self,
        chapter: WorkUnitLike,
        validation: ValidationResult,
        snapshot: ValidatedBuildSnapshot,
    ) -> StageOutcome:
        """Publish a clean formalize build, independently of discovery refinements."""

        if not await self._publish_validated_build(chapter, snapshot):
            return await self._defer_formalize_publication(chapter)
        if validation.warnings_only:
            await self._queue_warning_cleanup(chapter, validation, stage=Stage.FORMALIZE)
            detail = "coordinator build succeeded; warning cleanup queued"
        else:
            detail = "clean diagnostics and coordinator build in source dependency order"
        await self.state.set_task(
            chapter.id,
            Stage.FORMALIZE,
            TaskStatus.SUCCEEDED,
            detail,
        )
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

    async def _defer_formalize_publication(self, chapter: WorkUnitLike) -> StageOutcome:
        """Requeue a clean build whose source snapshot changed before publication."""

        await self.state.set_task(
            chapter.id,
            Stage.FORMALIZE,
            TaskStatus.PENDING,
            "source changed after the clean coordinator build; revalidation queued",
        )
        return StageOutcome(ExecutionDisposition.WAITING)

    async def _block_on_dependency_diagnostics(
        self,
        consumer: WorkUnitLike,
        validation: ValidationResult,
        *,
        block_consumer: bool = True,
    ) -> StageOutcome:
        """Route a coordinator failure to its owners without spending a consumer round."""

        owners = tuple(
            owner_id
            for owner_id in dict.fromkeys(validation.blocked_by)
            if owner_id in self._work_units_by_id and owner_id != consumer.id
        )
        if not owners:
            return StageOutcome(ExecutionDisposition.FAILED)
        await self._invalidate_build_records(owners)
        await self._invalidate_reviews(
            owners,
            detail="review invalidated by reopened formalization",
        )
        requirements = tuple(
            Requirement(
                RequirementKind.COORDINATOR_OWNER,
                owner_task_key=self.state.key(owner_id, Stage.FORMALIZE),
                detail=f"coordinator diagnostic owned by {owner_id}",
            )
            for owner_id in owners
        )
        async with self.state.batch():
            await self.state.set_tasks(
                owners,
                Stage.PROVE,
                TaskStatus.PENDING,
                "proof invalidated by reopened formalization",
            )
            for owner_id in owners:
                owner_task = self.state.task(owner_id, Stage.FORMALIZE)
                if owner_task.status == TaskStatus.RUNNING:
                    continue
                owner_task.recovering_failure = True
                await self.state.set_task(
                    owner_id,
                    Stage.FORMALIZE,
                    TaskStatus.PENDING,
                    f"requeued as owner of coordinator diagnostics blocking {consumer.id}",
                )
            if block_consumer:
                await self.state.set_task_waiting(
                    consumer.id,
                    Stage.FORMALIZE,
                    requirements,
                    "waiting for coordinator diagnostic dependency owners",
                )
        return StageOutcome(
            ExecutionDisposition.WAITING,
            requirements,
        )

    def _chapter_identifiers(self, chapter: WorkUnitLike) -> tuple[str, ...]:
        root = (chapter.lean_root / chapter.chapter_path).as_posix()
        lean_prefix = self.config.settings.lean_project.as_posix().rstrip("/") + "/"
        without_project = root.removeprefix(lean_prefix)
        return tuple(
            dict.fromkeys(
                (
                    root,
                    without_project,
                    chapter.chapter_module,
                    chapter.chapter_module.replace(".", "/"),
                )
            )
        )

    def _build_diagnostic_indexes(self) -> None:
        """Index immutable work-unit names used to route coordinator diagnostics."""

        lean_prefix = self.config.settings.lean_project.as_posix().rstrip("/") + "/"
        for chapter in self.work_units:
            root = (chapter.lean_root / chapter.chapter_path).as_posix()
            for path_root in dict.fromkeys((root, root.removeprefix(lean_prefix))):
                self._path_owners.setdefault(path_root, []).append(chapter.id)
            self._module_owners.setdefault(chapter.chapter_module, []).append(chapter.id)
            for identifier in self._chapter_identifiers(chapter):
                node = self._identifier_trie
                for character in identifier:
                    node = node.children.setdefault(character, _IdentifierTrieNode())
                if chapter.id not in node.owner_ids:
                    node.owner_ids += (chapter.id,)

    def _ordered_owner_ids(self, owner_ids: Iterable[str]) -> tuple[str, ...]:
        return tuple(sorted(set(owner_ids), key=self._work_unit_order.__getitem__))

    def _path_owner_ids(self, path: str) -> tuple[str, ...]:
        normalized = path.replace("\\", "/")
        repo_prefix = self.config.settings.repo.as_posix().rstrip("/") + "/"
        normalized = normalized.removeprefix(repo_prefix).removeprefix("./")
        owners: list[str] = []
        if normalized.endswith(".lean"):
            owners.extend(self._path_owners.get(normalized.removesuffix(".lean"), ()))
        parent = normalized
        while "/" in parent:
            parent = parent.rsplit("/", 1)[0]
            owners.extend(self._path_owners.get(parent, ()))
        return self._ordered_owner_ids(owners)

    @staticmethod
    @staticmethod
    def _failed_attempt_feedback(report: dict[str, Any]) -> str:
        """Render structured proof failures for an independent chapter review."""

        raw_attempts = report.get("failed_attempts")
        if not isinstance(raw_attempts, list):
            return ""
        blocks: list[str] = []
        for raw in raw_attempts:
            if not isinstance(raw, dict):
                continue
            path = str(raw.get("path", "")).strip()
            declaration = str(raw.get("declaration", "")).strip()
            remaining_goal = str(raw.get("remaining_goal", "")).strip()
            obstruction = str(raw.get("obstruction", "")).strip()
            attempts = raw.get("attempts")
            checked = (
                [str(item).strip() for item in attempts if str(item).strip()]
                if isinstance(attempts, list)
                else []
            )
            if not (path and declaration and remaining_goal and obstruction and checked):
                continue
            blocks.append(
                f"Failed proof `{declaration}` in `{path}`:\n"
                + "Checked attempts:\n"
                + "\n".join(f"- {item}" for item in checked)
                + f"\nRemaining goal:\n{remaining_goal}\nObserved obstruction:\n{obstruction}"
            )
        return "\n\n".join(blocks)

    @staticmethod
    def _obsolete_dependency_blocker(blocker: dict[str, Any]) -> bool:
        text = " ".join(
            str(blocker.get(field, "")) for field in ("remaining_goal", "obstruction")
        ).casefold()
        return ("dependency" in text or "imported" in text or "prerequisite" in text) and (
            "fail" in text or "before" in text or "could not" in text
        )

    @staticmethod
    def _normalized_blocker_goal(blocker: dict[str, Any]) -> str:
        goal = re.sub(r"\s+", " ", str(blocker.get("remaining_goal", ""))).strip()
        goal = goal.removeprefix("⊢ ")
        if goal.startswith("Nonempty (") and goal.endswith(")"):
            goal = goal[len("Nonempty (") : -1]
        goal = goal.replace("BaseChangeData.leftSquare B", "B.leftSquare")
        return goal.replace("BaseChangeData.outerSquare B", "B.outerSquare")

    async def _resolve_obsolete_dependency_blockers(self, chapter_id: str) -> None:
        stale = (
            str(blocker["id"])
            for blocker in self.state.proof_blockers_for_consumer(chapter_id)
            if self._obsolete_dependency_blocker(blocker)
        )
        await self.state.set_proof_blocker_status(stale, ProofBlockerStatus.RESOLVED)

    def _durable_blocker_feedback(
        self,
        chapter_id: str,
        targets: Iterable[ProofTarget] = (),
    ) -> str:
        """Render deduplicated blockers relevant to the current proof assignment."""

        selected = tuple(targets)

        def matches(blocker: dict[str, Any]) -> bool:
            if not selected:
                return True
            path = str(blocker.get("path", ""))
            declaration = str(blocker.get("declaration", ""))
            return any(
                path == target.path
                and (
                    declaration == target.declaration
                    or declaration.endswith("." + target.declaration)
                )
                for target in selected
            )

        deduplicated: dict[tuple[str, str, str], dict[str, Any]] = {}
        for blocker in self.state.proof_blockers_for_consumer(chapter_id):
            if not matches(blocker):
                continue
            key = (
                str(blocker.get("path", "")),
                str(blocker.get("declaration", "")).rsplit(".", 1)[-1],
                self._normalized_blocker_goal(blocker),
            )
            previous = deduplicated.get(key)
            if previous is None or str(blocker.get("updated_at", "")) >= str(
                previous.get("updated_at", "")
            ):
                deduplicated[key] = blocker

        blocks = []
        for blocker in sorted(deduplicated.values(), key=lambda value: str(value.get("id", ""))):
            responses = blocker.get("review_responses")
            review_advice = (
                "\nLatest reviewer response: " + str(responses[-1])[:2000]
                if isinstance(responses, list) and responses
                else ""
            )
            blocks.append(
                f"{blocker['id']} — `{blocker.get('declaration', '')}` in "
                f"`{blocker.get('path', '')}` (seen {blocker.get('sightings', 1)} time(s))\n"
                f"Residual goal: {str(blocker.get('remaining_goal', ''))[:2000]}\n"
                f"Obstruction: {str(blocker.get('obstruction', ''))[:1200]}"
                f"{review_advice}"
            )
        if not blocks:
            return ""
        return _bounded_proof_feedback(
            (
                "Durable blocker ledger. Do not repeat its evidence. Put unchanged IDs in "
                "`blocker_refs`; use `failed_attempts` only for new or materially changed "
                "blockers:\n\n" + "\n\n".join(blocks),
            )
        )

    async def _record_proof_blocker_deltas(
        self,
        chapter: WorkUnitLike,
        run: RunRecord,
        report: dict[str, Any],
    ) -> tuple[dict[str, Any], ...]:
        attempts = report.get("failed_attempts")
        refs = report.get("blocker_refs")
        capabilities = (
            [
                value.get("capability")
                for value in attempts
                if isinstance(value, dict) and isinstance(value.get("capability"), dict)
            ]
            if isinstance(attempts, list)
            else []
        )
        return await self.state.record_proof_blockers(
            chapter.id,
            origin_run_id=run.id,
            failed_attempts=(attempts if isinstance(attempts, list) else ()),
            unchanged_ids=(refs if isinstance(refs, list) else ()),
            capability_candidates=capabilities,
        )

    @staticmethod
    def _package_owned_blocker(blocker: dict[str, Any]) -> bool:
        """Recognize structural, repeated, or cross-file work before legacy routing."""

        candidate = blocker.get("capability")
        cross_file = isinstance(candidate, dict) and any(
            str(path).strip() != str(blocker.get("path", "")).strip()
            for path in candidate.get("owner_paths", ())
            if isinstance(path, str)
        )
        return (
            cross_file
            or int(blocker.get("sightings", 0)) > 1
            or str(blocker.get("disposition", ""))
            in {
                "missing_capability",
                "statement_review",
                "interface_review",
                "genuine_blocker",
            }
        )

    async def _attach_structural_blockers_to_packages(
        self,
        chapter: WorkUnitLike,
        blockers: Iterable[dict[str, Any]],
    ) -> tuple[str, ...]:
        package_ids: list[str] = []
        for blocker in blockers:
            if not self._package_owned_blocker(blocker):
                continue
            blocker_id = str(blocker.get("id", ""))
            candidate = blocker.get("capability")
            raw_key = ""
            owner_paths: list[str] = []
            if isinstance(candidate, dict):
                raw_key = str(candidate.get("capability_key", "")).strip()
                owner_paths = [
                    str(path).strip()
                    for path in candidate.get("owner_paths", ())
                    if isinstance(path, str)
                    and path.strip()
                    and self._path_owner_ids(str(path).strip())
                ]
            declaration = str(blocker.get("declaration", "")).strip()
            residual_goal = self._normalized_blocker_goal(blocker)
            capability_key = raw_key or f"proof-capability:{declaration}:{residual_goal}"
            digest = hashlib.sha256(capability_key.casefold().encode()).hexdigest()[:20]
            package_id = f"package-{digest}"
            consumer_path = str(blocker.get("path", "")).strip()
            if chapter.id not in self._path_owner_ids(consumer_path):
                continue
            paths = tuple(dict.fromkeys([*owner_paths, consumer_path]))
            if not consumer_path or not paths:
                continue
            package = CapabilityPackage(
                package_id,
                capability_key,
                f"Capability for {declaration or residual_goal[:80]}",
                str(
                    candidate.get("needed_result", "")
                    if isinstance(candidate, dict)
                    else blocker.get("obstruction", "")
                ).strip()
                or f"Resolve the structural proof obstruction for {declaration}",
                write_scope=paths,
                expansion_scope=paths,
            )
            source_digest = await asyncio.to_thread(
                scope_digest, self.config.settings.repo, chapter
            )
            consumer = PackageConsumer(
                id=f"consumer-{blocker_id}",
                package_id=package_id,
                work_unit_id=chapter.id,
                path=consumer_path,
                declaration=declaration,
                stage=Stage.PROVE.value,
                residual_goal=str(blocker.get("remaining_goal", "")),
                source_digest=source_digest,
                blocker_ids=(blocker_id,),
                attempted_routes=tuple(str(value) for value in blocker.get("attempts", ())),
                acceptance_contract={
                    "build_command": chapter.build_command,
                    "declaration": declaration,
                },
            )
            evidence = PackageEvidence(
                id=f"evidence-{blocker_id}-{str(blocker.get('fingerprint', ''))[:16]}",
                package_id=package_id,
                producer="proof-blocker-router",
                kind=EvidenceKind.RESIDUAL_GOAL,
                source_revision=source_digest,
                paths=(consumer_path,),
                declarations=(declaration,) if declaration else (),
                payload={"blocker": blocker},
                digest=str(blocker.get("fingerprint", "")),
            )
            attached, _created = await self.state.create_or_attach_capability_package(
                package,
                consumer=consumer,
                evidence=(evidence,),
            )
            await self.state.attach_proof_blockers_to_package((blocker_id,), attached.id)
            package_ids.append(attached.id)
        return tuple(dict.fromkeys(package_ids))

    async def _resolve_satisfied_proof_blockers(self, chapter: WorkUnitLike) -> None:
        resolved = []
        for blocker in self.state.proof_blockers_for_consumer(chapter.id, active_only=False):
            if (
                declaration_uses_placeholder(
                    self.config.settings.repo,
                    str(blocker.get("path", "")),
                    str(blocker.get("declaration", "")),
                )
                is False
            ):
                resolved.append(str(blocker["id"]))
        await self.state.set_proof_blocker_status(resolved, ProofBlockerStatus.RESOLVED)

    @staticmethod
    def _blocker_needs_review(blocker: dict[str, Any]) -> bool:
        disposition = str(blocker.get("disposition", ""))
        if disposition in {"statement_review", "interface_review"}:
            return True
        evidence = (
            str(blocker.get("obstruction", "")) + " " + str(blocker.get("remaining_goal", ""))
        ).lower()
        return any(
            marker in evidence
            for marker in (
                "statement is false",
                "missing hypothesis",
                "missing assumption",
                "statement/interface",
                "interface mismatch",
            )
        )

    @staticmethod
    def _blocker_report(blocker: dict[str, Any]) -> dict[str, Any]:
        return {
            "failed_attempts": [
                {
                    "path": blocker.get("path", ""),
                    "declaration": blocker.get("declaration", ""),
                    "attempts": list(blocker.get("attempts", ()))
                    or [
                        "Durable proof attempts are recorded in the blocker ledger.",
                        "The residual obstruction was unchanged on a later proof cycle.",
                    ],
                    "remaining_goal": blocker.get("remaining_goal", ""),
                    "obstruction": blocker.get("obstruction", ""),
                }
            ]
        }

    def _proof_review_feedback(
        self,
        chapter_id: str,
    ) -> tuple[str, tuple[str, ...]]:
        entries: list[tuple[str, str, str]] = []
        for request_id, value in self.state.proof_review_requests.items():
            feedback = value.get("feedback") if isinstance(value, dict) else None
            block = feedback.get(chapter_id) if isinstance(feedback, dict) else None
            if not isinstance(block, str) or not block.strip():
                continue
            kind = str(value.get("kind", PROOF_FINDING_REVIEW_KIND))
            if kind == BUILD_WARNING_REVIEW_KIND:
                # Warning cleanup is an auxiliary obligation. It must not reopen
                # or delay the semantic review dependency frontier.
                continue
            entries.append((request_id, kind, block))
        diagnostic_entries = [entry for entry in entries if entry[1] in DIAGNOSTIC_REVIEW_KINDS]
        selected = diagnostic_entries or entries
        blocks: dict[str, None] = {}
        request_ids: list[str] = []
        for request_id, kind, block in selected:
            request_ids.append(request_id)
            rendered = (
                block
                if kind in DIAGNOSTIC_REVIEW_KINDS
                else self._tag_proof_findings(request_id, block)
            )
            blocks[rendered] = None
        return "\n\n".join(blocks), tuple(request_ids)

    def _proof_review_role(self, request_ids: Iterable[str]) -> str:
        """Select a reason-specific re-review role for one homogeneous request batch."""

        kinds = {
            str(value.get("kind", PROOF_FINDING_REVIEW_KIND))
            for request_id in request_ids
            for value in (self.state.proof_review_requests.get(request_id),)
            if isinstance(value, dict)
        }
        if kinds == {BUILD_WARNING_REVIEW_KIND}:
            return WARNING_REVIEW_ROLE
        if kinds and kinds.issubset(DIAGNOSTIC_REVIEW_KINDS):
            return DIAGNOSTIC_REVIEW_ROLE
        return PROOF_REVIEW_ROLE if kinds else ""

    @staticmethod
    def _proof_finding_ids(request_id: str, feedback: str) -> tuple[str, ...]:
        count = len(re.findall(r"(?m)^Failed proof `", feedback))
        return tuple(f"{request_id}:{index}" for index in range(1, count + 1))

    @classmethod
    def _tag_proof_findings(cls, request_id: str, feedback: str) -> str:
        finding_ids = iter(cls._proof_finding_ids(request_id, feedback))
        return re.sub(
            r"(?m)^Failed proof `",
            lambda _match: f"Finding ID: `{next(finding_ids)}`\nFailed proof `",
            feedback,
        )

    def _expected_proof_finding_ids(
        self,
        chapter_id: str,
        request_ids: Iterable[str],
    ) -> tuple[str, ...]:
        expected: list[str] = []
        for request_id in request_ids:
            value = self.state.proof_review_requests.get(request_id)
            feedback = value.get("feedback") if isinstance(value, dict) else None
            block = feedback.get(chapter_id) if isinstance(feedback, dict) else None
            if isinstance(block, str):
                expected.extend(self._proof_finding_ids(request_id, block))
        return tuple(expected)

    def _proof_review_assessment_error(
        self,
        chapter_id: str,
        run_id: str,
        expected_ids: Iterable[str],
    ) -> str:
        expected = tuple(expected_ids)
        if not expected:
            return ""
        run = next(
            (item for item in self.state.task(chapter_id, Stage.REVIEW).runs if item.id == run_id),
            None,
        )
        if run is None:
            return "proof-review run disappeared before finding assessments were validated"
        self.state.load_run_details(run)
        report = run.report if isinstance(run.report, dict) else {}
        assessments = report.get("finding_assessments")
        received = (
            tuple(str(item.get("finding_id", "")) for item in assessments if isinstance(item, dict))
            if isinstance(assessments, list)
            else ()
        )
        if len(received) == len(expected) and set(received) == set(expected):
            return ""
        missing = sorted(set(expected).difference(received))
        unexpected = sorted(set(received).difference(expected))
        details = []
        if missing:
            details.append("missing: " + ", ".join(missing))
        if unexpected:
            details.append("unexpected: " + ", ".join(unexpected))
        if len(received) != len(set(received)):
            details.append("duplicate finding ids")
        return (
            "proof-review finding assessments did not match supplied findings ("
            + "; ".join(details)
            + ")"
        )

    async def _queue_proof_review(
        self,
        chapter: WorkUnitLike,
        report: dict[str, Any],
        *,
        origin_run_id: str,
    ) -> tuple[str, ...]:
        """Durably hand failed proof evidence to the auxiliary review service."""

        attempt_feedback = self._failed_attempt_feedback(report)
        if not attempt_feedback:
            return ()
        feedback = {
            chapter.id: (
                f"Proof work in `{chapter.id}` left checked failures. Evaluate this evidence while "
                "re-reviewing the complete assigned scope. If it makes logical sense to provide "
                "additional results in this module to satisfy the reported defect in the "
                "downstream module, do so:\n\n" + attempt_feedback
            )
        }
        attempts = report.get("failed_attempts")
        blocker_ids: list[str] = []
        for raw in attempts if isinstance(attempts, list) else ():
            if not isinstance(raw, dict):
                continue
            for blocker in self.state.proof_blockers_for_consumer(chapter.id, active_only=False):
                if (
                    str(blocker.get("path", "")) == str(raw.get("path", ""))
                    and str(blocker.get("declaration", "")).rsplit(".", 1)[-1]
                    == str(raw.get("declaration", "")).rsplit(".", 1)[-1]
                    and self._normalized_blocker_goal(blocker) == self._normalized_blocker_goal(raw)
                ):
                    blocker_ids.append(str(blocker["id"]))
                    break
        request_id, _ = await self.state.enqueue_proof_review_request(
            feedback,
            origin_run_id=origin_run_id,
            kind=PROOF_FINDING_REVIEW_KIND,
            blocker_ids=blocker_ids,
            source_digests={
                chapter.id: await asyncio.to_thread(
                    scope_digest,
                    self.config.settings.repo,
                    chapter,
                )
            },
        )
        return (request_id,)

    async def _discard_stale_proof_review_requests(self) -> set[str]:
        """Discard feedback whose owner scope changed after it was captured."""

        by_id = {chapter.id: chapter for chapter in self.work_units}
        owner_ids = {
            chapter_id
            for request in self.state.proof_review_requests.values()
            if isinstance(request, dict)
            for recorded in (request.get("source_digests"),)
            if isinstance(recorded, dict)
            for chapter_id, digest in recorded.items()
            if chapter_id in by_id and isinstance(digest, str) and digest
        }
        if not owner_ids:
            return set()
        current = await asyncio.to_thread(
            _scope_digests,
            self.config.settings.repo,
            by_id,
            owner_ids,
        )
        affected = await self.state.discard_stale_proof_review_requests(current)
        if not affected:
            return affected
        pending_owners = {
            chapter_id
            for request in self.state.proof_review_requests.values()
            if isinstance(request, dict)
            for feedback in (request.get("feedback"),)
            if isinstance(feedback, dict)
            for chapter_id in feedback
        }
        async with self.state.batch():
            for chapter_id in affected.difference(pending_owners):
                task_key = self.state.key(chapter_id, Stage.REVIEW)
                task = self.state.tasks.get(task_key)
                if task is not None and task.status in {TaskStatus.PENDING, TaskStatus.FAILED}:
                    await self.state.set_task(
                        chapter_id,
                        Stage.REVIEW,
                        TaskStatus.PENDING,
                        COORDINATOR_VERIFICATION_RETRY_DETAIL,
                    )
        return affected

    def _has_completed_green_review(self, chapter_id: str) -> bool:
        """Whether history contains a completed no-change review pass."""

        runs = self.state.task(chapter_id, Stage.REVIEW).runs
        for run in reversed(runs):
            if run.status != TaskStatus.SUCCEEDED or run.changed:
                continue
            self.state.load_run_details(run)
            report = run.report if isinstance(run.report, dict) else {}
            if report.get("complete") is True:
                return True
        return False

    async def _restore_review_successes_for_auxiliary_requests(
        self,
        pending_owners: set[str],
    ) -> None:
        """Keep canonical review green while auxiliary proof mail is pending."""

        restored_auxiliary: set[str] = set()
        restored_legacy: set[str] = set()
        synthetic_failures = {
            "formalization failed; quarantined from review",
            "formalization failed; quarantined from proof",
        }
        for chapter in self.work_units:
            task = self.state.task(chapter.id, Stage.REVIEW)
            recoverable = (
                task.status in (TaskStatus.PENDING, TaskStatus.BLOCKED)
                or (chapter.id in pending_owners and task.status == TaskStatus.FAILED)
                or (task.status == TaskStatus.FAILED and task.detail in synthetic_failures)
            )
            if recoverable and self._has_completed_green_review(chapter.id):
                target = restored_auxiliary if chapter.id in pending_owners else restored_legacy
                target.add(chapter.id)
        if restored_auxiliary:
            await self.state.set_tasks(
                restored_auxiliary,
                Stage.REVIEW,
                TaskStatus.SUCCEEDED,
                "canonical review remains green; proof-review request is auxiliary",
            )
        if restored_legacy:
            await self.state.set_tasks(
                restored_legacy,
                Stage.REVIEW,
                TaskStatus.SUCCEEDED,
                "durable review remains green; no pending findings for this chapter",
            )

    async def _recover_proof_review_requests(self) -> None:
        """Recover durable blockers and only escalate repeated review evidence."""

        await self._discard_stale_proof_review_requests()

        persisted_origins = {
            origin
            for value in self.state.proof_blockers.values()
            if isinstance(value, dict)
            for origin in value.get("origin_run_ids", ())
            if isinstance(origin, str)
        }
        for chapter in self.work_units:
            for run in self.state.task(chapter.id, Stage.PROVE).runs:
                if run.auxiliary or run.id in persisted_origins:
                    continue
                self.state.load_run_details(run)
                report = run.report if isinstance(run.report, dict) else {}
                if not report.get("failed_attempts") and not report.get("blocker_refs"):
                    continue
                blockers = await self._record_proof_blocker_deltas(chapter, run, report)
                for blocker in blockers:
                    if (
                        int(blocker.get("sightings", 0)) >= 2
                        and blocker.get("status") == ProofBlockerStatus.OPEN.value
                        and self._blocker_needs_review(blocker)
                    ):
                        await self._queue_proof_review(
                            chapter,
                            self._blocker_report(blocker),
                            origin_run_id=run.id,
                        )
                        await self.state.set_proof_blocker_status(
                            (str(blocker["id"]),),
                            ProofBlockerStatus.REVIEW_REQUESTED,
                        )
                persisted_origins.add(run.id)

        by_id = {chapter.id: chapter for chapter in self.work_units}
        pending_owners = {
            chapter_id
            for value in self.state.proof_review_requests.values()
            if isinstance(value, dict)
            for feedback in (value.get("feedback"),)
            if isinstance(feedback, dict)
            for chapter_id in feedback
            if chapter_id in by_id
        }
        await self._restore_review_successes_for_auxiliary_requests(pending_owners)

    def _module_owner_ids(self, module: str) -> tuple[str, ...]:
        owners: list[str] = []
        candidate = module
        while candidate:
            owners.extend(self._module_owners.get(candidate, ()))
            candidate = candidate.rpartition(".")[0]
        return self._ordered_owner_ids(owners)

    def _fingerprint_dependencies(
        self,
        work_unit_id: str,
        fingerprint: dict[str, Any],
    ) -> tuple[str, ...]:
        dependencies: set[str] = set()
        modules = fingerprint.get("modules", ())
        if not isinstance(modules, list):
            return ()
        for module in modules:
            if not isinstance(module, dict) or not isinstance(module.get("imports"), list):
                continue
            for imported in module["imports"]:
                if not isinstance(imported, str):
                    continue
                dependencies.update(self._module_owner_ids(imported))
        dependencies.discard(work_unit_id)
        return self._ordered_owner_ids(dependencies)

    def _identifier_owner_ids(self, text: str) -> tuple[str, ...]:
        owners: list[str] = []
        for start, character in enumerate(text):
            if start and _is_identifier_character(text[start - 1]):
                continue
            node = self._identifier_trie.children.get(character)
            if node is None:
                continue
            end = start + 1
            while True:
                if node.owner_ids and (end == len(text) or not _is_identifier_character(text[end])):
                    owners.extend(node.owner_ids)
                if end == len(text):
                    break
                node = node.children.get(text[end])
                if node is None:
                    break
                end += 1
        return self._ordered_owner_ids(owners)

    def _diagnostic_owner_ids(self, diagnostic: LeanDiagnostic) -> tuple[str, ...]:
        with self._diagnostic_owner_cache_lock:
            if diagnostic in self._diagnostic_owner_cache:
                return self._diagnostic_owner_cache[diagnostic]

        message = diagnostic.header.split(":", 1)[1].lstrip()
        if location := LEAN_LOCATION_RE.match(message):
            owners = self._path_owner_ids(location.group("path"))
        else:
            # Some orchestration failures have no line/column but do name one or
            # more modules or source roots. Matching only this diagnostic block is
            # intentional: matching the complete Lake transcript over-assigns every
            # replayed dependency and permitted `sorry` warning.
            owners = self._identifier_owner_ids(diagnostic.text)

        with self._diagnostic_owner_cache_lock:
            if len(self._diagnostic_owner_cache) >= DIAGNOSTIC_OWNER_CACHE_MAXIMUM:
                self._diagnostic_owner_cache.pop(next(iter(self._diagnostic_owner_cache)))
            self._diagnostic_owner_cache[diagnostic] = owners
        return owners

    def _partition_build_diagnostics(
        self,
        result: ValidationResult,
        target_ids: Iterable[str],
        graph: WorkUnitImportGraph,
    ) -> dict[str, ValidationResult]:
        """Project every combined Lake result onto each target's dependency closure."""

        ids = tuple(target_ids)
        if result.succeeded:
            return {
                target_id: replace(
                    result,
                    status=ValidationStatus.CLEAN,
                    blocked_by=(),
                )
                for target_id in ids
            }
        diagnostics = _result_diagnostics(result)
        owned = tuple(
            (diagnostic, set(self._diagnostic_owner_ids(diagnostic))) for diagnostic in diagnostics
        )
        unattributed = not diagnostics or any(not owners for _, owners in owned)

        partitioned: dict[str, ValidationResult] = {}
        for target_id in ids:
            closure = self._dependency_closure(graph, (target_id,))
            relevant = [
                (diagnostic, owners & closure) for diagnostic, owners in owned if closure & owners
            ]
            if relevant:
                errors = [item for item in relevant if item[0].severity == "error"]
                warnings = [item for item in relevant if item[0].severity == "warning"]
                error_owners = set().union(*(owners for _diagnostic, owners in errors), set())
                warning_owners = set().union(*(owners for _diagnostic, owners in warnings), set())
                blocked_by = tuple(sorted(error_owners - {target_id}))
                if target_id in error_owners:
                    status = ValidationStatus.TARGET_FAILED
                elif blocked_by:
                    status = ValidationStatus.DEPENDENCY_FAILED
                elif target_id in warning_owners:
                    status = ValidationStatus.TARGET_WARNINGS
                elif result.compiler_succeeded:
                    # Warnings are cleanup obligations of the source that emitted
                    # them. A consumer can use the successfully compiled artifact.
                    partitioned[target_id] = ValidationResult(
                        True,
                        0,
                        "Lake build succeeded; warnings belong to dependencies.",
                        process_exit_code=0,
                        status=ValidationStatus.CLEAN,
                        raw_log_path=result.raw_log_path,
                    )
                    continue
                else:
                    # The combined process failed, but this target's closure contains
                    # warnings only.  The actual error belongs to another batch target,
                    # and the process may have stopped before reaching this one.  Retry
                    # it separately instead of promoting a dependency warning to a build
                    # failure and reopening an already successful owner.
                    partitioned[target_id] = ValidationResult(
                        False,
                        result.exit_code,
                        result.output[-20_000:],
                        timed_out=result.timed_out,
                        process_exit_code=result.process_exit_code,
                        status=ValidationStatus.UNATTRIBUTED_BUILD_FAILURE,
                        diagnostics=result.diagnostics,
                        failed_modules=result.failed_modules,
                        raw_log_path=result.raw_log_path,
                    )
                    continue
                output = _diagnostic_output_for_target(
                    (diagnostic for diagnostic, _owners in relevant), target_id
                )
                partitioned[target_id] = ValidationResult(
                    False,
                    result.exit_code,
                    output[-20_000:],
                    timed_out=result.timed_out,
                    process_exit_code=result.process_exit_code,
                    status=status,
                    blocked_by=blocked_by,
                    diagnostics=tuple(diagnostic for diagnostic, _owners in relevant),
                    failed_modules=result.failed_modules,
                    raw_log_path=result.raw_log_path,
                )
            elif result.compiler_succeeded and not unattributed:
                partitioned[target_id] = ValidationResult(
                    True,
                    0,
                    "Lake build succeeded; diagnostics belonged to other batch targets.",
                    process_exit_code=0,
                    status=ValidationStatus.CLEAN,
                    raw_log_path=result.raw_log_path,
                )
            else:
                partitioned[target_id] = ValidationResult(
                    False,
                    result.exit_code,
                    result.output[-20_000:],
                    timed_out=result.timed_out,
                    process_exit_code=result.process_exit_code,
                    status=ValidationStatus.UNATTRIBUTED_BUILD_FAILURE,
                    diagnostics=result.diagnostics,
                    failed_modules=result.failed_modules,
                    raw_log_path=result.raw_log_path,
                )
        return partitioned

    def _remember_broken_builds(
        self,
        results: dict[str, ValidationResult],
        graph: WorkUnitImportGraph,
    ) -> None:
        """Cache attributable source failures until their input closure changes."""

        for target_id, result in results.items():
            if result.succeeded:
                self._broken_builds.pop(target_id, None)
                continue
            if result.status is ValidationStatus.TARGET_FAILED:
                owners = (target_id,)
            elif result.status is ValidationStatus.DEPENDENCY_FAILED:
                owners = result.blocked_by
            else:
                continue
            for owner_id in owners:
                if owner_id not in self._work_units_by_id:
                    continue
                required = self._dependency_closure(graph, (owner_id,))
                diagnostics = _result_diagnostics(result)
                owner_result = ValidationResult(
                    False,
                    result.exit_code,
                    (
                        _diagnostic_output_for_target(diagnostics, owner_id)
                        if diagnostics
                        else result.output
                    ),
                    timed_out=result.timed_out,
                    process_exit_code=result.process_exit_code,
                    status=ValidationStatus.TARGET_FAILED,
                    diagnostics=diagnostics,
                    failed_modules=result.failed_modules,
                    raw_log_path=result.raw_log_path,
                )
                self._broken_builds[owner_id] = BrokenBuild(
                    source_generations={
                        chapter_id: self._source_generations.get(chapter_id, 0)
                        for chapter_id in required
                    },
                    result=owner_result,
                )

    def _cached_broken_results(
        self,
        target_ids: Iterable[str],
        graph: WorkUnitImportGraph,
    ) -> dict[str, ValidationResult]:
        candidates = set(target_ids)
        results: dict[str, ValidationResult] = {}
        for owner_id in self._ordered_owner_ids(self._broken_builds):
            broken = self._current_broken_build(owner_id, graph)
            if broken is None:
                continue
            blocked = self._successor_closure(graph, (owner_id,)).intersection(candidates)
            blocked.discard(owner_id)
            for target_id in blocked.difference(results):
                diagnostics = _result_diagnostics(broken.result)
                results[target_id] = replace(
                    broken.result,
                    output=(
                        _diagnostic_output_for_target(diagnostics, target_id)
                        if diagnostics
                        else broken.result.output
                    ),
                    status=ValidationStatus.DEPENDENCY_FAILED,
                    blocked_by=(owner_id,),
                )
        return results

    @staticmethod
    def _proof_chunk_validation(
        result: ValidationResult,
        targets: Iterable[ProofTarget],
    ) -> ValidationResult:
        """Project whole-build diagnostics onto the assigned declaration spans.

        The coordinator still builds the chapter and its imported closure.  A proof
        worker, however, can only act on its assigned declarations, so located Lean
        errors and rejected warnings outside those declarations must not spend that
        chunk's retry budget.
        """

        selected = tuple(targets)
        if result.succeeded or not selected:
            return result
        diagnostics = _result_diagnostics(result)
        if not diagnostics:
            # Timeouts and process failures without a parsed Lean diagnostic do not
            # establish that the assigned declarations are clean.
            return result

        def same_path(left: str, right: str) -> bool:
            normalized_left = Path(left).as_posix().lstrip("./")
            normalized_right = Path(right).as_posix().lstrip("./")
            return (
                normalized_left == normalized_right
                or normalized_left.endswith("/" + normalized_right)
                or normalized_right.endswith("/" + normalized_left)
            )

        relevant: list[LeanDiagnostic] = []
        for diagnostic in diagnostics:
            message = diagnostic.header.split(":", 1)[1].lstrip()
            location = LEAN_LOCATION_RE.match(message)
            if location is None:
                continue
            line = int(location.group("line"))
            if any(
                same_path(location.group("path"), target.path)
                and target.line <= line <= target.end_line
                for target in selected
            ):
                relevant.append(diagnostic)

        if relevant:
            output = "\n\n".join(diagnostic.text for diagnostic in relevant)
            output += (
                f"\n\nCoordinator rejected {len(relevant)} Lean diagnostic(s) relevant "
                "to the assigned proof chunk."
            )
            return ValidationResult(
                False,
                result.exit_code,
                output[-20_000:],
                timed_out=result.timed_out,
                process_exit_code=result.process_exit_code,
                diagnostics=tuple(relevant),
                failed_modules=result.failed_modules,
                raw_log_path=result.raw_log_path,
            )

        return ValidationResult(
            True,
            0,
            "Whole-chapter build failed, but no located Lean errors or rejected warnings "
            "belonged to the assigned proof chunk.",
            process_exit_code=result.process_exit_code,
            raw_log_path=result.raw_log_path,
        )

    async def _build_chapters(
        self,
        chapters: Iterable[WorkUnitLike],
        *,
        publish_if_clean: bool,
        mode: str = "targeted",
        iteration: int = 1,
        maximum_iterations: int = 1,
        stage: Stage = Stage.FORMALIZE,
        snapshots: dict[str, ValidatedBuildSnapshot] | None = None,
    ) -> dict[str, ValidationResult]:
        """Coalesce pending coordinator requests and return this caller's target results."""

        selected = tuple(dict.fromkeys(chapter.id for chapter in chapters))
        if not selected:
            return {}
        by_id = {chapter.id: chapter for chapter in self.work_units}
        request_chapters = tuple(by_id[chapter_id] for chapter_id in selected)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, ValidationResult]] = loop.create_future()
        request = PendingBuildRequest(
            chapters=request_chapters,
            publish_if_clean=publish_if_clean,
            mode=mode,
            iteration=iteration,
            maximum_iterations=maximum_iterations,
            stage=stage,
            snapshots=snapshots,
            future=future,
        )
        self._pending_build_requests.append(request)
        if self._build_dispatch_task is None or self._build_dispatch_task.done():
            self._build_dispatch_task = asyncio.create_task(self._dispatch_build_requests())
        return await future

    async def _dispatch_build_requests(self) -> None:
        """Drain all build callers through one rolling, cross-stage batch lane."""

        try:
            while True:
                # Include every caller that became runnable in the current event-loop turn.
                await asyncio.sleep(0)
                requests = tuple(
                    request
                    for request in self._pending_build_requests
                    if not request.future.cancelled()
                )
                self._pending_build_requests.clear()
                if not requests:
                    return
                await self._run_build_batch(requests)
        finally:
            self._build_dispatch_task = None

    async def _run_build_batch(self, requests: tuple[PendingBuildRequest, ...]) -> None:
        """Execute and partition a coalesced batch until every caller has a precise result."""

        requested_ids = {
            chapter.id
            for request in requests
            for chapter in request.chapters
            if chapter.id not in request.routed_results
        }
        remaining = set(requested_ids)
        results_by_id: dict[str, ValidationResult] = {}
        snapshots_by_id: dict[str, ValidatedBuildSnapshot] = {}

        def requeue_unfinished() -> None:
            self._pending_build_requests[0:0] = [
                request for request in requests if not request.future.done()
            ]

        def finish_ready_requests() -> None:
            for request in requests:
                if request.future.done():
                    continue
                ids = tuple(chapter.id for chapter in request.chapters)
                request.routed_results.update(
                    {
                        chapter_id: results_by_id[chapter_id]
                        for chapter_id in ids
                        if chapter_id in results_by_id
                    }
                )
                request.routed_snapshots.update(
                    {
                        chapter_id: snapshots_by_id[chapter_id]
                        for chapter_id in ids
                        if chapter_id in snapshots_by_id
                    }
                )
                if not all(chapter_id in request.routed_results for chapter_id in ids):
                    continue
                if request.snapshots is not None:
                    request.snapshots.update(
                        {
                            chapter_id: request.routed_snapshots[chapter_id]
                            for chapter_id in ids
                            if chapter_id in request.routed_snapshots
                        }
                    )
                request.future.set_result(
                    {chapter_id: request.routed_results[chapter_id] for chapter_id in ids}
                )

        try:
            while remaining:
                active = tuple(
                    request
                    for request in requests
                    if not request.future.done()
                    and any(chapter.id in remaining for chapter in request.chapters)
                )
                candidate_ids = {
                    chapter.id
                    for request in active
                    for chapter in request.chapters
                    if chapter.id in remaining
                }
                if not candidate_ids:
                    break
                graph = self._observed_work_unit_graph()
                cached_failures = self._cached_broken_results(candidate_ids, graph)
                if cached_failures:
                    results_by_id.update(cached_failures)
                    remaining.difference_update(cached_failures)
                    finish_ready_requests()
                    candidate_ids.difference_update(cached_failures)
                    if not candidate_ids:
                        continue
                selected_by_id: dict[str, WorkUnitLike] = {}
                for request in active:
                    for chapter in request.chapters:
                        if chapter.id in candidate_ids:
                            selected_by_id.setdefault(chapter.id, chapter)
                candidates = tuple(selected_by_id.values())
                first_prefix = candidates[0].build_command.strip().rpartition(" ")[0]
                compatible = tuple(
                    chapter
                    for chapter in candidates
                    if chapter.build_command.strip().rpartition(" ")[0] == first_prefix
                )
                selected = compatible[:MAXIMUM_COORDINATOR_BUILD_TARGETS]
                active_ids = {chapter.id for chapter in selected}
                attempt_requests = tuple(
                    request
                    for request in active
                    if any(chapter.id in active_ids for chapter in request.chapters)
                )
                modes = {request.mode for request in attempt_requests}
                mode = next(iter(modes)) if len(modes) == 1 else "batched"
                owner = attempt_requests[0]
                attempt_snapshots: dict[str, ValidatedBuildSnapshot] = {}
                capture_snapshots = any(
                    request.snapshots is not None for request in attempt_requests
                )
                attempt_results = await self._execute_build_chapters(
                    selected,
                    publish_if_clean=any(request.publish_if_clean for request in attempt_requests),
                    mode=mode,
                    iteration=max(request.iteration for request in attempt_requests),
                    maximum_iterations=max(
                        request.maximum_iterations for request in attempt_requests
                    ),
                    stage=owner.stage,
                    snapshots=attempt_snapshots if capture_snapshots else None,
                )
                self._remember_broken_builds(attempt_results, graph)
                succeeded_ids = {
                    chapter_id for chapter_id, result in attempt_results.items() if result.succeeded
                }
                artifact_ids = {
                    chapter_id
                    for chapter_id, result in attempt_results.items()
                    if result.compiler_succeeded
                }
                snapshots_by_id.update(
                    (chapter_id, attempt_snapshots[chapter_id])
                    for chapter_id in artifact_ids
                    if chapter_id in attempt_snapshots
                )
                stale_ids = {
                    chapter_id
                    for chapter_id, result in attempt_results.items()
                    if result.status is ValidationStatus.STALE_SNAPSHOT
                }
                if succeeded_ids:
                    results_by_id.update(
                        (chapter_id, attempt_results[chapter_id]) for chapter_id in succeeded_ids
                    )
                    remaining.difference_update(succeeded_ids)
                    finish_ready_requests()
                # A stale immutable build is an orchestration retry, not a source finding.
                # Leave its callers unfinished so they re-enter the coordinator lane instead
                # of leaking a synthetic build error into review or proof work.
                failed_ids = active_ids.difference(succeeded_ids, stale_ids)
                if not failed_ids:
                    if remaining:
                        requeue_unfinished()
                        return
                    continue
                unattributed = {
                    chapter_id
                    for chapter_id in failed_ids
                    if attempt_results[chapter_id].status
                    is ValidationStatus.UNATTRIBUTED_BUILD_FAILURE
                }
                if unattributed and len(active_ids) > 1:
                    # A failed process may stop before compiling the other targets.
                    # Remove every attributed failure, then retry the uncertain
                    # targets together. A wholly unattributed process failure is
                    # shared by the batch below; probing subsets adds no evidence.
                    attributed = failed_ids.difference(unattributed)
                    if attributed:
                        results_by_id.update(
                            (chapter_id, attempt_results[chapter_id]) for chapter_id in attributed
                        )
                        remaining.difference_update(attributed)
                        finish_ready_requests()
                        # Yield the coordinator lane. Unreached targets retain
                        # their caller futures and join requests that accumulated
                        # while this command was running.
                        requeue_unfinished()
                        return
                affected = failed_ids
                results_by_id.update(
                    (chapter_id, attempt_results[chapter_id]) for chapter_id in affected
                )
                remaining.difference_update(affected)
                finish_ready_requests()
                if remaining:
                    requeue_unfinished()
                    return
        except asyncio.CancelledError:
            for request in requests:
                if not request.future.done():
                    request.future.cancel()
            raise
        except BaseException as error:
            for request in requests:
                if not request.future.done():
                    request.future.set_exception(error)
        finally:
            finish_ready_requests()

    async def _execute_build_chapters(
        self,
        chapters: Iterable[WorkUnitLike],
        *,
        publish_if_clean: bool,
        mode: str = "targeted",
        iteration: int = 1,
        maximum_iterations: int = 1,
        stage: Stage = Stage.FORMALIZE,
        snapshots: dict[str, ValidatedBuildSnapshot] | None = None,
    ) -> dict[str, ValidationResult]:
        """Execute one deterministic Lake invocation against the coordinator cache."""

        selected = tuple(chapters)
        if not selected:
            return {}
        ids = tuple(chapter.id for chapter in selected)
        label = f"{stage.value} {mode}: " + ", ".join(ids)

        await self.control.checkpoint()
        lease = await self.build_queue.acquire(label=label, stage=stage)
        try:
            results: dict[str, ValidationResult] = {}
            source_held = False
            build_workspace = None
            progress_flush: asyncio.Task[None] | None = None
            build_source_digests_task: asyncio.Task[dict[str, str]] | None = None

            async def flush_build_progress() -> None:
                await asyncio.sleep(0.25)
                await self.state.flush()

            try:
                await self.source_lock.acquire()
                source_held = True
                build_workspace = await self.isolation.acquire_build(label)
                async with self.state.batch():
                    await self.state.start_coordinator_build(
                        mode=mode,
                        stage=stage,
                        iteration=iteration,
                        maximum_iterations=maximum_iterations,
                        total=len(selected),
                        target_work_unit_ids=ids,
                    )
                build_graph = self._observed_work_unit_graph()
                by_id = {item.id: item for item in self.work_units}
                build_required_ids = self._dependency_closure(build_graph, ids)
                build_source_generations = {
                    chapter_id: self._source_generations.get(chapter_id, 0)
                    for chapter_id in build_required_ids
                }
                clean_value = self.state.formalize_graph.get("clean", {})
                clean_records = clean_value if isinstance(clean_value, dict) else {}
                reusable_digests = {
                    chapter_id: digest
                    for chapter_id in build_required_ids.difference(ids)
                    if isinstance((record := clean_records.get(chapter_id)), dict)
                    and isinstance((digest := record.get("source_digest")), str)
                }
                digest_ids = build_required_ids.difference(reusable_digests)
                build_root = build_workspace.root
                build_source_digests_task = asyncio.create_task(
                    asyncio.to_thread(
                        _scope_digests,
                        build_root,
                        by_id,
                        digest_ids,
                    )
                )
                # Digest capture reads the immutable build workspace. It can run
                # alongside Lake instead of delaying process startup, and a FUSE
                # build no longer needs the live-source barrier for this scan.
                if self.isolation.name != "shared":
                    self.source_lock.release()
                    source_held = False
                if len(selected) > 1:
                    targets = []
                    prefixes = []
                    for chapter in selected:
                        command = chapter.build_command.strip()
                        prefix, separator, target = command.rpartition(" ")
                        if not separator or not target.startswith("+"):
                            raise ValueError(
                                "cannot combine build command without a trailing "
                                f"Lake target: {command}"
                            )
                        prefixes.append(prefix.rstrip())
                        targets.append(target)
                    if len(set(prefixes)) != 1:
                        raise ValueError("build commands do not share a common prefix")
                    combined = f"{prefixes[0]} {' '.join(targets)}"
                    build_units = ((selected[0], combined, ids, len(selected)),)
                else:
                    chapter = selected[0]
                    build_units = ((chapter, chapter.build_command, (chapter.id,), 1),)
                for chapter, command, result_ids, _completed in build_units:
                    await self.state.advance_coordinator_build(
                        work_unit_id=chapter.id,
                        completed=0,
                        command=command,
                    )

                    def append_output(output: str) -> None:
                        nonlocal progress_flush
                        error_count = sum(
                            diagnostic.severity == "error"
                            and bool(set(self._diagnostic_owner_ids(diagnostic)) & set(ids))
                            for diagnostic in _lean_diagnostics(output)
                        )
                        self.state.append_coordinator_build_output(
                            output,
                            error_count=error_count,
                        )
                        if progress_flush is None or progress_flush.done():
                            progress_flush = asyncio.create_task(flush_build_progress())

                    validation = asyncio.create_task(
                        validate(
                            self.config,
                            _with_build_command(chapter, command),
                            workspace_root=build_workspace.root,
                            on_output=append_output,
                        )
                    )
                    result = await validation
                    results.update(
                        self._partition_build_diagnostics(result, result_ids, build_graph)
                    )
                    await self.state.advance_coordinator_build(
                        work_unit_id=chapter.id,
                        completed=self.state.coordinator_build.total,
                    )
                build_source_digests = reusable_digests | await build_source_digests_task
                artifacts_built = bool(results) and all(
                    result.compiler_succeeded for result in results.values()
                )
                # Lake records successful jobs with content traces even when another target makes
                # the combined command exit nonzero. Keep those private coordinator artifacts so
                # an uncertain-target retry does not rebuild the same dependency closure. Timed-out
                # or signal-terminated commands may leave incomplete writes and remain disposable.
                cache_reusable = bool(results) and all(
                    not result.timed_out
                    and (
                        result.process_exit_code
                        if result.process_exit_code is not None
                        else result.exit_code
                    )
                    >= 0
                    for result in results.values()
                )
                fingerprints: FingerprintCollection | None = None
                fingerprint_error = ""
                if artifacts_built and snapshots is not None:
                    cached = self.state.formalize_graph.get("interfaces", {})
                    try:
                        fingerprints = await asyncio.to_thread(
                            collect_interface_fingerprints,
                            self.config,
                            selected,
                            root=build_workspace.root,
                            cached_records=cached if isinstance(cached, dict) else {},
                        )
                    except InterfaceFingerprintError as error:
                        fingerprint_error = str(error)[-4000:]
                if artifacts_built:
                    if not source_held:
                        await self.source_lock.acquire()
                        source_held = True
                    possibly_modified = await self._possibly_modified_scope_ids(
                        build_required_ids,
                        build_source_generations,
                    )
                    current_source_digests = await asyncio.to_thread(
                        _scope_digests,
                        self.config.settings.repo,
                        by_id,
                        possibly_modified,
                    )
                    source_is_current = all(
                        build_source_digests[chapter_id] == digest
                        for chapter_id, digest in current_source_digests.items()
                    )
                    if not source_is_current:
                        stale = ValidationResult(
                            False,
                            1,
                            "Source dependency scope changed during the coordinator build; "
                            "retry required.",
                            status=ValidationStatus.STALE_SNAPSHOT,
                        )
                        results = {chapter_id: stale for chapter_id in results}
                        artifacts_built = False
                    elif snapshots is not None:
                        for chapter in selected:
                            required = self._dependency_closure(build_graph, (chapter.id,))
                            fingerprint = (
                                fingerprints.records[chapter.id].as_dict()
                                if fingerprints is not None and chapter.id in fingerprints.records
                                else None
                            )
                            snapshots[chapter.id] = ValidatedBuildSnapshot(
                                graph=build_graph,
                                source_digests={
                                    chapter_id: build_source_digests[chapter_id]
                                    for chapter_id in required
                                },
                                source_generations={
                                    chapter_id: build_source_generations[chapter_id]
                                    for chapter_id in required
                                },
                                fingerprint=fingerprint,
                                import_dependencies=(
                                    self._fingerprint_dependencies(chapter.id, fingerprint)
                                    if fingerprint is not None
                                    else ()
                                ),
                                fingerprint_error=fingerprint_error,
                            )
                await build_workspace.finish(
                    succeeded=artifacts_built,
                    publish=publish_if_clean and artifacts_built,
                    retain=cache_reusable,
                )
                build_workspace = None
            except BaseException:
                raise
        finally:
            try:
                if progress_flush is not None:
                    if not progress_flush.done():
                        progress_flush.cancel()
                    await asyncio.gather(progress_flush, return_exceptions=True)
                if build_source_digests_task is not None:
                    await asyncio.gather(build_source_digests_task, return_exceptions=True)
                if build_workspace is not None:
                    await build_workspace.close()
                if source_held:
                    await self.state.finish_coordinator_build()
            finally:
                if source_held:
                    self.source_lock.release()
                self.build_queue.release(lease)
        return results

    async def _build_all(
        self, *, iteration: int = 1, maximum_iterations: int = 1
    ) -> dict[str, ValidationResult]:
        """Build the full selection and publish only a globally clean cache snapshot."""

        return await self._build_chapters(
            self.work_units,
            publish_if_clean=True,
            mode="global",
            iteration=iteration,
            maximum_iterations=maximum_iterations,
        )

    def _build_feedback(
        self,
        results: dict[str, ValidationResult],
        *,
        blocked_owner_ids: set[str] | frozenset[str] = frozenset(),
    ) -> BuildDiagnostics:
        feedback: dict[str, dict[str, None]] = {}
        by_id = self._work_units_by_id

        def source_location(owner_id: str) -> str:
            owner = by_id[owner_id]
            return f"{owner.source}:{owner.source_span.start_line}-{owner.source_span.end_line}"

        targets_by_result: dict[ValidationResult, list[str]] = {}
        for target_id, result in results.items():
            if result.succeeded:
                continue
            targets_by_result.setdefault(result, []).append(target_id)

        for result, target_ids in targets_by_result.items():
            routed = False
            for diagnostic in _result_diagnostics(result):
                owners = self._diagnostic_owner_ids(diagnostic)
                if not owners:
                    continue
                routed = True
                for owner in owners:
                    block = (
                        f"Informal source: {source_location(owner)}\n"
                        f"Coordinator diagnostic:\n{diagnostic.text}"
                    )
                    feedback.setdefault(owner, {})[block] = None

            # Always route Lake's precise failed modules in addition to
            # source-located diagnostics.
            for module in _result_failed_modules(result):
                owners = self._module_owner_ids(module)
                if not owners:
                    continue
                routed = True
                for owner in owners:
                    block = (
                        f"Informal source: {source_location(owner)}\n"
                        f"Coordinator reported failed module `{module}`."
                    )
                    feedback.setdefault(owner, {})[block] = None

            if not routed:
                for target_id in target_ids:
                    block = (
                        f"Informal source: {source_location(target_id)}\n"
                        f"Coordinator build of {by_id[target_id].chapter_module} failed without a "
                        f"source-located diagnostic:\n{result.output[-12000:]}"
                    )
                    feedback.setdefault(target_id, {})[block] = None

        deferred = tuple(sorted(blocked_owner_ids.intersection(feedback)))
        return BuildDiagnostics(
            actionable={
                chapter_id: "\n\n".join(blocks)
                for chapter_id, blocks in feedback.items()
                if chapter_id not in blocked_owner_ids
            },
            deferred_owner_ids=deferred,
        )

    async def _build_feedback_async(
        self,
        results: dict[str, ValidationResult],
        *,
        blocked_owner_ids: set[str] | frozenset[str] = frozenset(),
    ) -> BuildDiagnostics:
        """Route build output without monopolizing the asyncio control thread."""

        return await asyncio.to_thread(
            self._build_feedback,
            results,
            blocked_owner_ids=blocked_owner_ids,
        )

    async def _discover_all(self) -> bool:
        """Discover inputs with bounded scheduling and batched promotion."""

        pending = deque(self.work_units)
        results: list[StageOutcome] = []
        maximum = self.config.stages[Stage.DISCOVER].max_agents
        assert maximum is not None

        async def worker() -> None:
            while pending:
                chapter = pending.popleft()
                results.append(await self._discover(chapter))

        workers = tuple(worker() for _ in range(min(len(pending), maximum * 2)))
        await _gather_cancel_on_error(workers)
        return all(result.succeeded for result in results)

    async def _discover_and_formalize(
        self,
        *,
        progress_event: asyncio.Event | None = None,
        idle_event: asyncio.Event | None = None,
        stop_event: asyncio.Event | None = None,
        discover: bool = True,
    ) -> bool:
        """Pipeline discovery into dependency-ready formalization without a stage gate."""

        if (idle_event is None) != (stop_event is None):
            raise ValueError("idle_event and stop_event must be provided together")

        by_id = {chapter.id: chapter for chapter in self.work_units}
        changes = self.state.change_bus.subscribe() if stop_event is not None else None
        pending_discoveries: deque[WorkUnitLike] = deque()
        discovery_tasks: dict[str, asyncio.Task[StageOutcome]] = {}
        if discover:
            discovery_digests = (
                {}
                if self.force
                else await asyncio.to_thread(self._source_input_digests, self.work_units)
            )
            for chapter in self.work_units:
                if self.force or not self._discovery_is_current(
                    chapter,
                    source_digest=discovery_digests[chapter.id],
                ):
                    pending_discoveries.append(chapter)
        discovery_maximum = self.config.stages[Stage.DISCOVER].max_agents
        assert discovery_maximum is not None
        discovery_window = discovery_maximum * 2

        def fill_discovery_window() -> None:
            while pending_discoveries and len(discovery_tasks) < discovery_window:
                chapter = pending_discoveries.popleft()
                discovery_tasks[chapter.id] = asyncio.create_task(
                    # This queue contains only chapters already classified as stale.
                    self._discover(chapter, rerun=True)
                )

        fill_discovery_window()
        formalize_tasks: dict[str, asyncio.Task[StageOutcome]] = {}

        async def cancel_all() -> None:
            tasks = [*discovery_tasks.values(), *formalize_tasks.values()]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        try:
            while True:
                if idle_event is not None:
                    idle_event.clear()
                fill_discovery_window()
                graph = self._observed_work_unit_graph()
                succeeded = {
                    chapter_id
                    for chapter_id in by_id
                    if self.state.task(chapter_id, Stage.FORMALIZE).status == TaskStatus.SUCCEEDED
                }
                for chapter_id in graph.order:
                    formalize_task = self.state.task(chapter_id, Stage.FORMALIZE)
                    if (
                        chapter_id in succeeded
                        or chapter_id in formalize_tasks
                        or formalize_task.status
                        in {TaskStatus.FAILED, TaskStatus.INTERRUPTED, TaskStatus.RUNNING}
                    ):
                        continue
                    if self.state.task(chapter_id, Stage.DISCOVER).status != TaskStatus.SUCCEEDED:
                        continue
                    dependencies = graph.dependencies[chapter_id]
                    if (
                        dependencies.issubset(succeeded)
                        and self.state.readiness(formalize_task).ready
                    ):
                        formalize_tasks[chapter_id] = asyncio.create_task(
                            self._formalize(by_id[chapter_id])
                        )

                live = [*discovery_tasks.values(), *formalize_tasks.values()]
                if not live:
                    if stop_event is None:
                        break
                    assert idle_event is not None
                    assert changes is not None
                    idle_event.set()
                    if progress_event is not None:
                        progress_event.set()

                    async def relevant_change() -> None:
                        while True:
                            change = await changes.get()
                            if (
                                change.full_resync
                                or Stage.DISCOVER.value in change.stages
                                or Stage.FORMALIZE.value in change.stages
                            ):
                                return

                    changed = asyncio.create_task(relevant_change())
                    stopped = asyncio.create_task(stop_event.wait())
                    done, pending = await asyncio.wait(
                        (changed, stopped), return_when=asyncio.FIRST_COMPLETED
                    )
                    for task in pending:
                        task.cancel()
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)
                    if stopped in done:
                        break
                    continue

                done, _ = await asyncio.wait(live, return_when=asyncio.FIRST_COMPLETED)
                for chapter_id, task in tuple(discovery_tasks.items()):
                    if task not in done:
                        continue
                    discovery_tasks.pop(chapter_id)
                    outcome = task.result()
                    if (
                        outcome.failed
                        and self.state.task(chapter_id, Stage.DISCOVER).status != TaskStatus.FAILED
                    ):
                        await self.state.set_task(
                            chapter_id,
                            Stage.DISCOVER,
                            TaskStatus.FAILED,
                            "source discovery failed",
                        )
                for chapter_id, task in tuple(formalize_tasks.items()):
                    if task not in done:
                        continue
                    formalize_tasks.pop(chapter_id)
                    outcome = task.result()
                    if (
                        outcome.failed
                        and self.state.task(chapter_id, Stage.FORMALIZE).status != TaskStatus.FAILED
                    ):
                        await self.state.set_task(
                            chapter_id,
                            Stage.FORMALIZE,
                            TaskStatus.FAILED,
                            "formalization execution failed",
                        )
                if progress_event is not None:
                    progress_event.set()
        except BaseException:
            await cancel_all()
            raise
        finally:
            if changes is not None:
                self.state.change_bus.unsubscribe(changes)

        return all(
            self.state.task(chapter_id, Stage.FORMALIZE).status == TaskStatus.SUCCEEDED
            for chapter_id in by_id
        )

    async def _formalize_all(self) -> bool:
        return await self._discover_and_formalize(discover=True)

    async def _review_once(
        self,
        chapter: WorkUnitLike,
        *,
        rerun: bool = False,
        feedback: str = "",
        role: str = "",
        request_ids: Iterable[str] = (),
        resume_thread_id: str | None = None,
        resume_run_id: str = "",
        resume_prompt: str = "",
    ) -> StageOutcome:
        auxiliary_request = role == PROOF_REVIEW_ROLE
        if not auxiliary_request and not rerun and self._already_done(chapter, Stage.REVIEW):
            return StageOutcome(ExecutionDisposition.SUCCEEDED, changed=False, complete=True)
        attempt = await self._attempt(
            chapter,
            Stage.REVIEW,
            feedback=feedback,
            role=role,
            request_ids=request_ids,
            queue_detail=(
                "targeted cleanup of coordinator warnings"
                if role == WARNING_REVIEW_ROLE
                else "targeted repair of coordinator diagnostics"
                if role == DIAGNOSTIC_REVIEW_ROLE
                else "full-scope review of failed-proof findings"
                if feedback
                else "source-faithful editing review"
            ),
            resume_thread_id=resume_thread_id,
            resume_run_id=resume_run_id,
            resume_prompt=resume_prompt,
        )
        if attempt.agent.capacity_exhausted:
            if not auxiliary_request:
                await self.state.set_task(
                    chapter.id,
                    Stage.REVIEW,
                    TaskStatus.FAILED,
                    "model capacity remained unavailable after the configured retries",
                )
            return StageOutcome(
                ExecutionDisposition.FAILED,
                changed=attempt.agent.changed,
                complete=False,
                run_id=attempt.run.id,
            )
        if attempt.validation.status is ValidationStatus.STALE_SNAPSHOT:
            if not auxiliary_request:
                await self.state.set_task(
                    chapter.id,
                    Stage.REVIEW,
                    TaskStatus.RUNNING,
                    "review snapshot became stale; fresh isolation retry queued",
                )
            return StageOutcome(
                ExecutionDisposition.WAITING,
                changed=False,
                complete=False,
                run_id=attempt.run.id,
                retry_fresh=True,
            )
        report_error = ""
        if not attempt.agent.report and not attempt.agent.capacity_exhausted:
            report_error = attempt.agent.error or "review returned no structured final report"
        succeeded = attempt.agent.succeeded and attempt.validation.succeeded
        complete = bool(attempt.agent.report.get("complete"))
        if succeeded and complete:
            if attempt.agent.changed and not auxiliary_request:
                await self.state.set_task(
                    chapter.id,
                    Stage.REVIEW,
                    TaskStatus.RUNNING,
                    "review changes merged; coordinator verification queued",
                )
            return StageOutcome(
                ExecutionDisposition.SUCCEEDED,
                changed=attempt.agent.changed,
                complete=True,
                run_id=attempt.run.id,
            )
        if report_error:
            if not auxiliary_request:
                await self.state.set_task(
                    chapter.id,
                    Stage.REVIEW,
                    TaskStatus.RUNNING,
                    "invalid review report; session retry queued",
                )
            return StageOutcome(
                ExecutionDisposition.WAITING,
                changed=attempt.agent.changed,
                complete=False,
                run_id=attempt.run.id,
                report_error=report_error,
            )
        if succeeded:
            if not auxiliary_request:
                await self.state.set_task(
                    chapter.id,
                    Stage.REVIEW,
                    TaskStatus.RUNNING,
                    "incomplete review report; session retry queued",
                )
            return StageOutcome(
                ExecutionDisposition.WAITING,
                changed=attempt.agent.changed,
                complete=False,
                run_id=attempt.run.id,
            )
        if not auxiliary_request:
            await self.state.set_task(
                chapter.id,
                Stage.REVIEW,
                TaskStatus.FAILED,
                "editing review failed",
            )
        return StageOutcome(
            ExecutionDisposition.SUCCEEDED if succeeded else ExecutionDisposition.FAILED,
            changed=attempt.agent.changed,
            complete=complete,
            run_id=attempt.run.id,
        )

    def _review_invalidation_generation(self, chapter_id: str) -> int:
        return self._review_invalidation_generations.get(chapter_id, 0)

    async def _complete_review(
        self,
        chapter: WorkUnitLike,
        detail: str,
        *,
        expected_generation: int | None = None,
        proof_request_ids: Iterable[str] = (),
    ) -> bool:
        async with self._review_generation_lock:
            if (
                expected_generation is not None
                and self._review_invalidation_generation(chapter.id) != expected_generation
            ):
                return False
            async with self.state.batch():
                await self._apply_proof_review_outcomes(
                    chapter,
                    proof_request_ids,
                )
                await self.state.finish_proof_review_requests(
                    chapter.id,
                    proof_request_ids,
                )
                await self.state.set_task(
                    chapter.id,
                    Stage.REVIEW,
                    TaskStatus.SUCCEEDED,
                    detail,
                )
            return True

    async def _apply_proof_review_outcomes(
        self,
        chapter: WorkUnitLike,
        request_ids: Iterable[str],
    ) -> None:
        """Resolve reviewed blockers without blindly scheduling the same proof again."""

        review_runs = self.state.task(chapter.id, Stage.REVIEW).runs
        if not review_runs:
            return
        run = review_runs[-1]
        self.state.load_run_details(run)
        report = run.report if isinstance(run.report, dict) else {}
        raw_assessments = report.get("finding_assessments")
        assessments = (
            {
                str(item.get("finding_id", "")): item
                for item in raw_assessments
                if isinstance(item, dict)
            }
            if isinstance(raw_assessments, list)
            else {}
        )
        routed_statuses: list[ProofBlockerStatus] = []
        waiting_dependencies: set[str] = set()
        for request_id in request_ids:
            request = self.state.proof_review_requests.get(request_id)
            if not isinstance(request, dict) or request.get("kind") != PROOF_FINDING_REVIEW_KIND:
                continue
            raw_ids = request.get("blocker_ids")
            blocker_ids = [str(value) for value in raw_ids] if isinstance(raw_ids, list) else []
            for index, blocker_id in enumerate(blocker_ids, start=1):
                blocker = self.state.proof_blockers.get(blocker_id)
                if not isinstance(blocker, dict):
                    continue
                placeholder = declaration_uses_placeholder(
                    self.config.settings.repo,
                    str(blocker.get("path", "")),
                    str(blocker.get("declaration", "")),
                )
                assessment = assessments.get(f"{request_id}:{index}", {})
                response = str(assessment.get("explanation", "")).strip()
                if response:
                    responses = blocker.setdefault("review_responses", [])
                    if isinstance(responses, list) and response not in responses:
                        responses.append(response)
                diagnosis, action = self._normalized_proof_review_resolution(
                    assessment,
                    source_changed=bool(run.changed),
                )
                blocker["review_diagnosis"] = diagnosis
                blocker["review_action"] = action
                self.state.record_routing_event(f"review_diagnosis.{diagnosis}")
                self.state.record_routing_event(f"review_action.{action}")
                blocker["review_resolution_digest"] = tagged_digest_text(
                    json.dumps(assessment, sort_keys=True)
                )
                if action != "wait_for_dependency":
                    blocker["review_exchange_count"] = (
                        int(blocker.get("review_exchange_count", 0)) + 1
                    )

                if placeholder is False:
                    status = ProofBlockerStatus.RESOLVED
                elif action == "drop_stale_target" and placeholder is None:
                    self.state.record_routing_event("stale_target_dropped")
                    status = ProofBlockerStatus.RESOLVED
                elif action == "repair_and_retry" and run.changed:
                    status = ProofBlockerStatus.OPEN
                    blocker["retry_cause_digest"] = blocker["review_resolution_digest"]
                    blocker["retry_sighting_baseline"] = int(blocker.get("sightings", 0))
                elif action == "retry_with_route" and self._executable_retry_contract(
                    assessment.get("retry_contract")
                ):
                    contract = dict(assessment["retry_contract"])
                    blocker["retry_contract"] = contract
                    blocker["retry_cause_digest"] = tagged_digest_text(
                        json.dumps(contract, sort_keys=True)
                    )
                    blocker["retry_sighting_baseline"] = int(blocker.get("sightings", 0))
                    status = ProofBlockerStatus.OPEN
                elif action == "attach_package":
                    candidate = assessment.get("capability")
                    if candidate is not None:
                        blocker["capability"] = candidate
                        blocker["disposition"] = "missing_capability"
                    else:
                        blocker["disposition"] = "genuine_blocker"
                    package_ids = await self._attach_structural_blockers_to_packages(
                        chapter, (blocker,)
                    )
                    if package_ids:
                        blocker["package_id"] = package_ids[0]
                        status = ProofBlockerStatus.WAITING_DEPENDENCY
                    else:
                        status = ProofBlockerStatus.BLOCKED
                elif action == "wait_for_dependency":
                    raw_dependency_ids = assessment.get("dependency_ids")
                    dependency_ids = (
                        {
                            str(value)
                            for value in raw_dependency_ids
                            if str(value) in self._work_unit_order
                        }
                        if isinstance(raw_dependency_ids, list)
                        else set()
                    )
                    if dependency_ids:
                        waiting_dependencies.update(dependency_ids)
                        blocker["dependency_ids"] = sorted(dependency_ids)
                    status = (
                        ProofBlockerStatus.WAITING_DEPENDENCY
                        if dependency_ids
                        else ProofBlockerStatus.PARKED
                    )
                elif action == "park_external":
                    blocker["capability"] = {
                        "capability_key": (
                            f"external:{blocker.get('declaration', '')}:"
                            f"{self._normalized_blocker_goal(blocker)}"
                        ),
                        "owner_kind": "external",
                        "owner_paths": [str(blocker.get("path", ""))],
                        "needed_result": response or str(blocker.get("obstruction", "")),
                    }
                    blocker["disposition"] = "missing_capability"
                    package_ids = await self._attach_structural_blockers_to_packages(
                        chapter, (blocker,)
                    )
                    status = (
                        ProofBlockerStatus.WAITING_DEPENDENCY
                        if package_ids
                        else ProofBlockerStatus.PARKED
                    )
                else:
                    # A review that neither changed source nor supplied a checked route is terminal
                    # evidence, not permission to rerun the same proof.
                    status = ProofBlockerStatus.BLOCKED
                await self.state.set_proof_blocker_status((blocker_id,), status)
                routed_statuses.append(status)

        if ProofBlockerStatus.PACKAGE_REQUIRED in routed_statuses:
            await self.state.set_task(
                chapter.id,
                Stage.PROVE,
                TaskStatus.FAILED,
                "proof review routed unresolved mathematics to package work",
            )
        elif ProofBlockerStatus.OPEN in routed_statuses:
            # Keep making source-local progress when some independent blockers are parked. The
            # durable blocker ledger prevents the proof agent from losing those terminal routes.
            await self.state.set_task(
                chapter.id,
                Stage.PROVE,
                TaskStatus.PENDING,
                "proof review supplied changed source or an executable retry contract",
            )
        elif ProofBlockerStatus.WAITING_DEPENDENCY in routed_statuses and waiting_dependencies:
            await self.state.set_task_waiting(
                chapter.id,
                Stage.PROVE,
                (
                    Requirement(
                        RequirementKind.STAGE_DEPENDENCY,
                        owner_task_key=self.state.key(dependency_id, Stage.PROVE),
                        detail=f"waiting for dependency proof {dependency_id}",
                    )
                    for dependency_id in sorted(waiting_dependencies)
                ),
                "proof review deferred validation until dependencies are repaired",
            )
        elif ProofBlockerStatus.WAITING_DEPENDENCY in routed_statuses:
            package_ids = sorted(
                {
                    str(blocker.get("package_id", ""))
                    for blocker in self.state.proof_blockers.values()
                    if blocker.get("consumer_chapter_id") == chapter.id
                    and blocker.get("status") == ProofBlockerStatus.WAITING_DEPENDENCY.value
                    and blocker.get("package_id")
                }
            )
            await self.state.set_task_waiting(
                chapter.id,
                Stage.PROVE,
                (
                    Requirement(
                        RequirementKind.CAPABILITY_PACKAGE,
                        request_id=package_id,
                        detail="package Steward owns the reviewed structural proof work",
                    )
                    for package_id in package_ids
                ),
                "proof review attached structural work to capability package(s): "
                + ", ".join(package_ids),
            )
        elif any(status is ProofBlockerStatus.PARKED for status in routed_statuses):
            await self.state.set_task(
                chapter.id,
                Stage.PROVE,
                TaskStatus.BLOCKED,
                "proof review parked the blocker pending external or dependency work",
            )
        elif ProofBlockerStatus.BLOCKED in routed_statuses:
            await self.state.set_task(
                chapter.id,
                Stage.PROVE,
                TaskStatus.BLOCKED,
                "proof review found no changed source or executable retry route",
            )

    @staticmethod
    def _executable_retry_contract(raw: Any) -> bool:
        if not isinstance(raw, dict) or str(raw.get("known_remaining_gap", "")).strip():
            return False
        return all(
            (
                isinstance(raw.get("new_information"), str)
                and bool(str(raw["new_information"]).strip()),
                isinstance(raw.get("declarations"), list)
                and bool(raw["declarations"])
                and all(
                    isinstance(value, str) and bool(value.strip()) for value in raw["declarations"]
                ),
                isinstance(raw.get("intermediate_claims"), list)
                and bool(raw["intermediate_claims"]),
                isinstance(raw.get("critical_probe"), str)
                and bool(str(raw["critical_probe"]).strip()),
            )
        )

    @staticmethod
    def _normalized_proof_review_resolution(
        assessment: dict[str, Any],
        *,
        source_changed: bool,
    ) -> tuple[str, str]:
        """Read the routing contract and conservatively migrate legacy review reports."""

        diagnosis = str(assessment.get("diagnosis", "")).strip()
        action = str(assessment.get("action", "")).strip()
        if diagnosis and action:
            return diagnosis, action
        legacy = str(assessment.get("assessment", "")).strip()
        if source_changed and legacy == "confirmed":
            return "interface_defect", "repair_and_retry"
        # Legacy prose has no checked retry contract. Parking it is safer than recreating the
        # review/proof loop; an operator can reopen it when evidence changes.
        return "genuine_blocker", "attach_package"

    async def _invalidate_reviews(
        self,
        chapter_ids: Iterable[str],
        *,
        exclude: Iterable[str] = (),
        detail: str,
    ) -> set[str]:
        """Invalidate only reviews that received new findings."""

        targets = set(chapter_ids).difference(exclude)
        if not targets:
            return set()
        async with self._review_generation_lock:
            for chapter_id in targets:
                self._review_invalidation_generations[chapter_id] = (
                    self._review_invalidation_generation(chapter_id) + 1
                )
            self._invalidated_reviews.update(targets)
            changed_targets = {
                chapter_id
                for chapter_id in targets
                if self.state.task(chapter_id, Stage.REVIEW).status != TaskStatus.PENDING
            }
            if changed_targets:
                await self.state.set_tasks(
                    changed_targets,
                    Stage.REVIEW,
                    TaskStatus.PENDING,
                    detail,
                )
        return targets

    async def _review_build(self, chapter: WorkUnitLike) -> dict[str, str]:
        """Build review output, retrying stale snapshots before returning diagnostics."""

        while True:
            snapshots: dict[str, ValidatedBuildSnapshot] = {}
            result = (
                await self._build_chapters(
                    (chapter,),
                    publish_if_clean=True,
                    mode="review-verification",
                    stage=Stage.REVIEW,
                    snapshots=snapshots,
                )
            )[chapter.id]
            if result.status is ValidationStatus.STALE_SNAPSHOT:
                await self.state.set_task(
                    chapter.id,
                    Stage.REVIEW,
                    TaskStatus.RUNNING,
                    COORDINATOR_VERIFICATION_RETRY_DETAIL,
                )
                continue
            if result.succeeded:
                if await self._publish_validated_build(chapter, snapshots[chapter.id]):
                    await self.state.set_task(
                        chapter.id,
                        Stage.REVIEW,
                        TaskStatus.RUNNING,
                        "coordinator verification clean; continuing editing review",
                    )
                    return {}
                await self.state.set_task(
                    chapter.id,
                    Stage.REVIEW,
                    TaskStatus.RUNNING,
                    COORDINATOR_VERIFICATION_RETRY_DETAIL,
                )
                continue
            if result.warnings_only:
                if await self._publish_validated_build(chapter, snapshots[chapter.id]):
                    await self._queue_warning_cleanup(
                        chapter,
                        result,
                        stage=Stage.REVIEW,
                    )
                    await self.state.set_task(
                        chapter.id,
                        Stage.REVIEW,
                        TaskStatus.RUNNING,
                        "coordinator build succeeded; warning cleanup queued",
                    )
                    return {}
                await self.state.set_task(
                    chapter.id,
                    Stage.REVIEW,
                    TaskStatus.RUNNING,
                    COORDINATOR_VERIFICATION_RETRY_DETAIL,
                )
                continue
            feedback = (await self._build_feedback_async({chapter.id: result})).actionable
            return feedback or {chapter.id: result.output}

    async def _queue_review_feedback(
        self,
        feedback: dict[str, str],
        *,
        origin: str,
        stage: Stage = Stage.REVIEW,
        exclude_from_invalidation: Iterable[str] = (),
    ) -> tuple[str, set[str]]:
        """Persist follow-up work and reopen only its direct owners."""

        diagnostics = tuple(
            diagnostic for block in feedback.values() for diagnostic in _lean_diagnostics(block)
        )
        kind = (
            BUILD_WARNING_REVIEW_KIND
            if diagnostics and all(item.severity == "warning" for item in diagnostics)
            else BUILD_ERROR_REVIEW_KIND
        )
        if diagnostics:
            fingerprint_input = "\0".join(
                (*sorted(feedback), *(sorted(item.text for item in diagnostics)))
            )
            origin = f"{kind}:{hashlib.sha256(fingerprint_input.encode()).hexdigest()}"
        by_id = {chapter.id: chapter for chapter in self.work_units}
        digest_owner_ids = set(feedback).intersection(by_id)
        source_digests = await asyncio.to_thread(
            _scope_digests,
            self.config.settings.repo,
            by_id,
            digest_owner_ids,
        )
        request_id, created = await self.state.enqueue_proof_review_request(
            feedback,
            origin_run_id=origin,
            kind=kind,
            stage=stage,
            source_digests=source_digests,
        )
        if not created:
            return request_id, set()
        if kind == BUILD_WARNING_REVIEW_KIND:
            # Warning-only builds produced usable artifacts. Preserve the stage
            # certificate and clean the diagnostics through an auxiliary worker.
            return request_id, set()
        invalidated = await self._invalidate_reviews(
            feedback,
            exclude=exclude_from_invalidation,
            detail="review invalidated by follow-up findings",
        )
        return request_id, invalidated

    async def _queue_warning_cleanup(
        self,
        chapter: WorkUnitLike,
        validation: ValidationResult,
        *,
        stage: Stage,
    ) -> str:
        feedback = (await self._build_feedback_async({chapter.id: validation})).actionable
        owned = feedback or {chapter.id: validation.output}
        request_id, _ = await self._queue_review_feedback(
            owned,
            origin=f"warning:{stage.value}:{chapter.id}",
            stage=stage,
        )
        return request_id

    def _warning_cleanup_feedback(self, chapter_id: str) -> tuple[str, tuple[str, ...]]:
        blocks: dict[str, None] = {}
        request_ids: list[str] = []
        for request_id, value in self.state.proof_review_requests.items():
            if not isinstance(value, dict) or value.get("kind") != BUILD_WARNING_REVIEW_KIND:
                continue
            feedback = value.get("feedback")
            block = feedback.get(chapter_id) if isinstance(feedback, dict) else None
            if not isinstance(block, str) or not block.strip():
                continue
            request_ids.append(request_id)
            blocks[block] = None
        return "\n\n".join(blocks), tuple(request_ids)

    async def _revert_deterministic_warning_cleanup(
        self,
        chapter: WorkUnitLike,
        cleanup: WarningCleanupResult,
    ) -> str:
        async with self.source_lock:
            await self.git.ensure_clean(chapter)
            changed_paths = await asyncio.to_thread(
                revert_deterministic_warning_cleanup,
                repo_root=self.config.settings.repo,
                rewrites=cleanup.rewrites,
            )
            commit = await self.git.commit(
                chapter,
                Stage.REVIEW,
                summary="Restored the source after deterministic warning certification failed.",
                changed_paths=changed_paths,
                subject=deterministic_warning_revert_subject(chapter),
            )
            self._mark_source_changed((chapter.id,))
        invalidated_builds = await self._invalidate_build_records((chapter.id,))
        self._proof_rechecks.update(invalidated_builds)
        return commit

    async def _try_deterministic_warning_cleanup(
        self,
        chapter: WorkUnitLike,
        feedback: str,
        request_ids: tuple[str, ...],
    ) -> DeterministicWarningCleanupOutcome:
        diagnostics = _deterministic_warning_diagnostics(
            feedback,
            lean_project=self.config.settings.lean_project,
        )
        if not diagnostics:
            return DeterministicWarningCleanupOutcome()

        await self.control.checkpoint()
        chapter_lock = self._chapter_agent_locks[chapter.id]
        await chapter_lock.acquire()
        workspace = None
        source_held = False
        run: RunRecord | None = None
        isolated: IsolationResult | None = None
        cleanup_result = None
        try:
            if self.isolation.name == "shared":
                await self.source_lock.acquire()
                source_held = True
                await self.git.ensure_clean(chapter)
                workspace = await self.isolation.acquire(f"regex-{uuid4().hex[:12]}")
                snapshot = getattr(workspace, "snapshot", None)
                if snapshot is not None:
                    await snapshot(chapter)
            else:
                async with self.source_lock:
                    await self.git.ensure_clean(chapter)
                workspace = await self.isolation.acquire(f"regex-{uuid4().hex[:12]}")

            cleanup_result = await asyncio.to_thread(
                apply_deterministic_warning_cleanup,
                repo_root=workspace.root,
                lean_root=workspace.root / self.config.settings.lean_project,
                scope=chapter.scope,
                diagnostics=diagnostics,
            )
            if not cleanup_result.applied:
                return DeterministicWarningCleanupOutcome()

            run = await self.state.start_auxiliary_run(
                chapter.id,
                Stage.REVIEW,
                role=DETERMINISTIC_WARNING_CLEANUP_ROLE,
                request_ids=request_ids,
                model="deterministic-regex",
            )
            if not source_held:
                await self.source_lock.acquire()
                source_held = True
            isolated = await workspace.collect(chapter, integration_lock=None)
            if isolated.accepted and isolated.changed_paths:
                summary = (
                    f"Resolved {cleanup_result.warning_count} allowlisted Lean warning(s) "
                    "with location-bound edits."
                )
                commit = await self.git.commit(
                    chapter,
                    Stage.REVIEW,
                    summary=summary,
                    changed_paths=isolated.changed_paths,
                    subject=deterministic_warning_commit_subject(chapter),
                )
                isolated = replace(isolated, commit=commit)
                self._mark_source_changed((chapter.id,))
            await workspace.close()
            workspace = None
            self.source_lock.release()
            source_held = False

            if not isolated.accepted or not isolated.changed_paths:
                detail = isolated.error or "deterministic cleanup produced no scoped source change"
                validation = ValidationResult(
                    False,
                    1,
                    f"Deterministic warning cleanup was not integrated: {detail}",
                )
                await self.state.finish_run(
                    run,
                    status=TaskStatus.FAILED,
                    changed=False,
                    report={"complete": False, "summary": "", "issues": [detail]},
                    isolation=isolated.as_dict(),
                    validation=validation.as_dict(),
                )
                return DeterministicWarningCleanupOutcome(
                    attempted=True,
                )

            invalidated_builds = await self._invalidate_build_records((chapter.id,))
            self._proof_rechecks.update(invalidated_builds)
            snapshots: dict[str, ValidatedBuildSnapshot] = {}
            validation = (
                await self._build_chapters(
                    (chapter,),
                    publish_if_clean=True,
                    mode="deterministic-warning-cleanup-certification",
                    stage=Stage.REVIEW,
                    snapshots=snapshots,
                )
            )[chapter.id]
            if validation.succeeded and not await self._publish_validated_build(
                chapter, snapshots[chapter.id]
            ):
                validation = ValidationResult(
                    False,
                    1,
                    "Source scope changed after deterministic warning cleanup; retry required.",
                    status=ValidationStatus.STALE_SNAPSHOT,
                )

            complete = validation.succeeded
            revert_commit = ""
            if not complete:
                revert_commit = await self._revert_deterministic_warning_cleanup(
                    chapter,
                    cleanup_result,
                )
            issues = [] if complete else [validation.output[-4000:]]
            isolation_payload = isolated.as_dict()
            if revert_commit:
                isolation_payload["revert_commit"] = revert_commit
            await self.state.finish_run(
                run,
                status=TaskStatus.SUCCEEDED if complete else TaskStatus.FAILED,
                changed=complete,
                report={
                    "complete": complete,
                    "summary": (
                        f"Resolved {cleanup_result.warning_count} warning(s) deterministically."
                        if complete
                        else "Deterministic warning edits require agent follow-up."
                    ),
                    "issues": issues,
                },
                isolation=isolation_payload,
                validation=validation.as_dict(),
            )
            if complete:
                await self.state.finish_proof_review_requests(chapter.id, request_ids)
            return DeterministicWarningCleanupOutcome(
                attempted=True,
                clean=complete,
                changed=complete,
            )
        except BaseException as error:
            if run is not None and run.status == TaskStatus.RUNNING:
                detail = str(error) or type(error).__name__
                await self.state.finish_run(
                    run,
                    status=(
                        TaskStatus.INTERRUPTED
                        if isinstance(error, asyncio.CancelledError)
                        else TaskStatus.FAILED
                    ),
                    changed=bool(isolated and isolated.changed_paths),
                    report={"complete": False, "summary": "", "issues": [detail]},
                    isolation=(
                        isolated.as_dict()
                        if isolated is not None
                        else {"accepted": False, "error": detail}
                    ),
                )
            raise
        finally:
            if workspace is not None:
                await workspace.close()
            if source_held:
                self.source_lock.release()
            chapter_lock.release()

    async def _clean_warnings_for_chapter(
        self,
        chapter: WorkUnitLike,
        feedback: str,
        request_ids: tuple[str, ...],
    ) -> WarningCleanupOutcome:
        deterministic = await self._try_deterministic_warning_cleanup(
            chapter,
            feedback,
            request_ids,
        )
        if deterministic.clean:
            return WarningCleanupOutcome(True, deterministic.changed)
        attempt = await self._attempt(
            chapter,
            Stage.REVIEW,
            feedback=feedback,
            role=WARNING_REVIEW_ROLE,
            request_ids=request_ids,
            queue_detail="auxiliary warning cleanup queued",
        )
        changed = deterministic.changed or attempt.agent.changed
        complete = bool(attempt.agent.report.get("complete"))
        snapshots: dict[str, ValidatedBuildSnapshot] = {}
        validation = (
            await self._build_chapters(
                (chapter,),
                publish_if_clean=True,
                mode="warning-cleanup-certification",
                stage=Stage.REVIEW,
                snapshots=snapshots,
            )
        )[chapter.id]
        await self.state.update_run(attempt.run, validation=validation.as_dict())
        if validation.succeeded:
            if not await self._publish_validated_build(chapter, snapshots[chapter.id]):
                return WarningCleanupOutcome(False, changed)
            if attempt.agent.succeeded and complete:
                await self.state.finish_proof_review_requests(chapter.id, request_ids)
                return WarningCleanupOutcome(True, changed)
            return WarningCleanupOutcome(False, changed)
        if not validation.warnings_only:
            routed = (await self._build_feedback_async({chapter.id: validation})).actionable
            await self._queue_review_feedback(
                routed or {chapter.id: validation.output},
                origin=f"warning-cleanup-error:{attempt.run.id}",
                stage=Stage.REVIEW,
            )
        return WarningCleanupOutcome(False, changed)

    async def _drain_warning_cleanups(self) -> WarningCleanupOutcome:
        await self._discard_stale_proof_review_requests()
        work: list[Coroutine[Any, Any, WarningCleanupOutcome]] = []
        for chapter in self.work_units:
            feedback, request_ids = self._warning_cleanup_feedback(chapter.id)
            if request_ids:
                work.append(self._clean_warnings_for_chapter(chapter, feedback, request_ids))
        if not work:
            return WarningCleanupOutcome(True)
        outcomes = await _gather_cancel_on_error(work)
        return WarningCleanupOutcome(
            clean=all(outcome.clean for outcome in outcomes)
            and not any(
                isinstance(value, dict) and value.get("kind") == BUILD_WARNING_REVIEW_KIND
                for value in self.state.proof_review_requests.values()
            ),
            changed=any(outcome.changed for outcome in outcomes),
        )

    async def _review_chapter_to_clean(
        self,
        chapter: WorkUnitLike,
        rounds_used: dict[str, int],
        *,
        rerun: bool = False,
        feedback: str = "",
        role: str = "",
        proof_request_ids: tuple[str, ...] = (),
        verification_retry: bool = False,
    ) -> StageOutcome:
        """Run at most five edit/rebuild cycles for one reviewable chapter."""

        auxiliary_request = role == PROOF_REVIEW_ROLE
        review_generation = self._review_invalidation_generation(chapter.id)
        if (
            not auxiliary_request
            and self.state.task(chapter.id, Stage.REVIEW).status == TaskStatus.SUCCEEDED
        ):
            return StageOutcome(ExecutionDisposition.SUCCEEDED)
        request_ids = list(proof_request_ids)
        review_feedback = feedback

        async def set_review_task(status: TaskStatus, detail: str) -> None:
            if not auxiliary_request:
                await self.state.set_task(chapter.id, Stage.REVIEW, status, detail)

        async def complete_review(detail: str) -> bool:
            if not auxiliary_request:
                return await self._complete_review(
                    chapter,
                    detail,
                    expected_generation=review_generation,
                    proof_request_ids=request_ids,
                )
            async with self.state.batch():
                await self._apply_proof_review_outcomes(chapter, request_ids)
                await self.state.finish_proof_review_requests(chapter.id, request_ids)
            return True

        async def route_feedback(items: dict[str, str], *, origin: str) -> bool:
            nonlocal request_ids, review_feedback, role
            if not items:
                return True
            routed_items = (
                {owner_id: block for owner_id, block in items.items() if owner_id != chapter.id}
                if auxiliary_request
                else items
            )
            request_id = ""
            if routed_items:
                request_id, _ = await self._queue_review_feedback(
                    routed_items,
                    origin=origin,
                    exclude_from_invalidation={chapter.id},
                )
            if chapter.id in items:
                block = items[chapter.id]
                if auxiliary_request:
                    if block not in review_feedback:
                        review_feedback = (
                            f"{review_feedback}\n\n{block}" if review_feedback else block
                        )
                    return True
                if role in DIAGNOSTIC_REVIEW_ROLES:
                    if request_id not in request_ids:
                        request_ids.append(request_id)
                    if block not in review_feedback:
                        review_feedback = (
                            f"{review_feedback}\n\n{block}" if review_feedback else block
                        )
                else:
                    # A newly discovered coordinator diagnostic takes precedence
                    # over initial review or proof-finding work. Keep those older
                    # requests durable for their own clean follow-up pass.
                    request_ids = [request_id]
                    review_feedback = block
                role = self._proof_review_role(request_ids)
            if chapter.id in items:
                await set_review_task(TaskStatus.RUNNING, "review follow-up queued")
            return True

        if verification_retry:
            build_feedback = await self._review_build(chapter)
            if build_feedback:
                if not await route_feedback(
                    build_feedback,
                    origin=f"review-build:{chapter.id}:{uuid4().hex[:12]}",
                ):
                    return StageOutcome(ExecutionDisposition.FAILED)
            else:
                completed = await complete_review(
                    "coordinator verification completed after stale snapshot"
                )
                return StageOutcome(
                    ExecutionDisposition.SUCCEEDED if completed else ExecutionDisposition.WAITING
                )

        persisted = self.state.formalize_graph.get("clean", {})
        records = persisted if isinstance(persisted, dict) else {}
        was_clean = await self._retained_formalize_record(chapter, records) is not None
        # Diagnostic reports and coordinator certification are separate facts. Remember a clean
        # verification obtained during this operation so a later dependency invalidation does not
        # turn a valid report into an agent-report retry.
        coordinator_verified = was_clean
        if not was_clean and not rerun:
            build_feedback = await self._review_build(chapter)
            if build_feedback and not await route_feedback(
                build_feedback,
                origin=f"review-build:{chapter.id}:{uuid4().hex[:12]}",
            ):
                return StageOutcome(ExecutionDisposition.FAILED)
            coordinator_verified = not build_feedback

        maximum = min(self.config.stages[Stage.REVIEW].max_rounds, 5)
        resume_thread_id: str | None = None
        resume_run_id = ""
        resume_prompt = ""
        zero_work_report_retries = 0
        stale_snapshot_retries = 0

        async def queue_report_retry(outcome: StageOutcome, error: str) -> bool:
            nonlocal resume_thread_id, resume_run_id, resume_prompt, zero_work_report_retries
            if rounds_used[chapter.id] >= maximum:
                await set_review_task(
                    TaskStatus.FAILED,
                    f"{error}; review report retry cap reached after {maximum} cycles",
                )
                return False
            run = next(
                (
                    item
                    for item in self.state.task(chapter.id, Stage.REVIEW).runs
                    if item.id == outcome.run_id
                ),
                None,
            )
            zero_work = run is not None and run.usage.total_tokens == 0
            if zero_work:
                zero_work_report_retries += 1
                self.state.record_routing_event("zero_work_review_report")
                resume_thread_id = None
                resume_run_id = ""
                resume_prompt = ""
                # A continuation that produced no invocation work is infrastructure noise, not a
                # semantic review exchange. Retry once in a fresh session.
                rounds_used[chapter.id] = max(0, rounds_used[chapter.id] - 1)
                if zero_work_report_retries > 1:
                    await set_review_task(
                        TaskStatus.FAILED,
                        "review infrastructure failed after repeated zero-work reports",
                    )
                    return False
            elif run is not None and run.thread_id:
                resume_thread_id = run.thread_id
                resume_run_id = run.id
                resume_prompt = REVIEW_REPORT_RETRY_PROMPT.format(error=error)
            await set_review_task(
                TaskStatus.RUNNING,
                (
                    "resuming review session after invalid report "
                    if resume_thread_id is not None
                    else "retrying review after invalid report "
                )
                + f"({rounds_used[chapter.id]}/{maximum})",
            )
            return True

        while rounds_used[chapter.id] < maximum:
            rounds_used[chapter.id] += 1
            review_rerun = rerun or rounds_used[chapter.id] > 1
            finding_guided = bool(review_feedback)
            attempt_feedback = review_feedback
            source_digest_before = await asyncio.to_thread(
                scope_digest, self.config.settings.repo, chapter
            )
            review_options: dict[str, Any] = {"rerun": review_rerun}
            if review_feedback:
                review_options.update(
                    feedback=review_feedback,
                    role=role,
                    request_ids=request_ids,
                )
            if resume_thread_id is not None:
                review_options.update(
                    resume_thread_id=resume_thread_id,
                    resume_run_id=resume_run_id,
                    resume_prompt=resume_prompt,
                )
            outcome = await self._review_once(chapter, **review_options)
            source_digest_after = await asyncio.to_thread(
                scope_digest, self.config.settings.repo, chapter
            )
            source_changed = source_digest_after != source_digest_before
            if source_changed:
                coordinator_verified = False
            resume_thread_id = None
            resume_run_id = ""
            resume_prompt = ""
            review_feedback = ""
            if outcome.retry_fresh:
                stale_snapshot_retries += 1
                # A concurrent integration invalidating an isolation snapshot is infrastructure
                # contention, not a semantic review cycle. Retry from the new live scope without
                # charging the chapter's review budget or resuming the stale agent conversation.
                rounds_used[chapter.id] = max(0, rounds_used[chapter.id] - 1)
                if stale_snapshot_retries > MAXIMUM_STALE_REVIEW_SNAPSHOT_RETRIES:
                    await set_review_task(
                        TaskStatus.FAILED,
                        "review scope remained unstable after repeated fresh isolation retries",
                    )
                    return StageOutcome(ExecutionDisposition.FAILED)
                await set_review_task(
                    TaskStatus.RUNNING,
                    "review snapshot became stale; retrying from the current scope "
                    f"({stale_snapshot_retries}/{MAXIMUM_STALE_REVIEW_SNAPSHOT_RETRIES})",
                )
                continue
            stale_snapshot_retries = 0
            if outcome.report_error:
                review_feedback = attempt_feedback
                if source_changed:
                    malformed_build_feedback = await self._review_build(chapter)
                    if malformed_build_feedback and not await route_feedback(
                        malformed_build_feedback,
                        origin=f"review-build:{outcome.run_id or uuid4().hex[:12]}",
                    ):
                        return StageOutcome(ExecutionDisposition.FAILED)
                    coordinator_verified = not malformed_build_feedback
                if await queue_report_retry(outcome, outcome.report_error):
                    continue
            if outcome.failed:
                return StageOutcome(ExecutionDisposition.FAILED)
            expected_finding_ids = self._expected_proof_finding_ids(chapter.id, request_ids)
            if assessment_error := self._proof_review_assessment_error(
                chapter.id,
                outcome.run_id,
                expected_finding_ids,
            ):
                review_feedback = attempt_feedback
                if await queue_report_retry(outcome, assessment_error):
                    continue
                return StageOutcome(ExecutionDisposition.FAILED)
            build_feedback: dict[str, str] = {}
            if source_changed:
                build_feedback = await self._review_build(chapter)
                if build_feedback and not await route_feedback(
                    build_feedback,
                    origin=f"review-build:{outcome.run_id or uuid4().hex[:12]}",
                ):
                    return StageOutcome(ExecutionDisposition.FAILED)
                coordinator_verified = not build_feedback
            if review_feedback:
                if rounds_used[chapter.id] >= maximum:
                    await set_review_task(
                        TaskStatus.FAILED,
                        f"review follow-up remained unresolved after {maximum} cycles",
                    )
                    return StageOutcome(ExecutionDisposition.FAILED)
                continue
            if not outcome.complete:
                review_feedback = attempt_feedback
                if await queue_report_retry(
                    outcome, "review report marked the assignment incomplete"
                ):
                    continue
                return StageOutcome(ExecutionDisposition.FAILED)
            if role in DIAGNOSTIC_REVIEW_ROLES and not coordinator_verified:
                certification_feedback = await self._review_build(chapter)
                if certification_feedback:
                    if not await route_feedback(
                        certification_feedback,
                        origin=f"review-certification:{outcome.run_id or uuid4().hex[:12]}",
                    ):
                        return StageOutcome(ExecutionDisposition.FAILED)
                    if review_feedback:
                        if rounds_used[chapter.id] < maximum:
                            continue
                        await set_review_task(
                            TaskStatus.FAILED,
                            f"review follow-up remained unresolved after {maximum} cycles",
                        )
                        return StageOutcome(ExecutionDisposition.FAILED)
                    owner_ids = tuple(
                        owner_id for owner_id in certification_feedback if owner_id != chapter.id
                    )
                    if owner_ids:
                        requirements = tuple(
                            Requirement(
                                RequirementKind.COORDINATOR_OWNER,
                                owner_task_key=self.state.key(owner_id, Stage.REVIEW),
                                detail=f"coordinator diagnostic owned by {owner_id}",
                            )
                            for owner_id in owner_ids
                        )
                        await self.state.set_task_waiting(
                            chapter.id,
                            Stage.REVIEW,
                            requirements,
                            "waiting for coordinator diagnostic dependency owners",
                        )
                        return StageOutcome(ExecutionDisposition.WAITING, requirements)
                    await set_review_task(
                        TaskStatus.FAILED,
                        "coordinator verification failed without actionable feedback",
                    )
                    return StageOutcome(ExecutionDisposition.FAILED)
                coordinator_verified = True
            if finding_guided:
                completed = await complete_review(
                    "targeted review completed with no pending findings"
                )
                return StageOutcome(
                    ExecutionDisposition.SUCCEEDED if completed else ExecutionDisposition.WAITING
                )
            if not source_changed:
                completed = await complete_review("editing review found no actionable issues")
                return StageOutcome(
                    ExecutionDisposition.SUCCEEDED if completed else ExecutionDisposition.WAITING
                )
        completed = await complete_review(f"review/rebuild cap reached after {maximum} cycles")
        return StageOutcome(
            ExecutionDisposition.SUCCEEDED if completed else ExecutionDisposition.WAITING
        )

    async def _review_tree(
        self,
        *,
        rerun: bool = False,
        prove: bool = False,
        quarantined: Iterable[str] = (),
        formalize: RunningFormalizeStage | None = None,
    ) -> bool:
        """Release each review and proof when its local dependency frontier is done."""

        await self._recover_proof_review_requests()
        by_id = {chapter.id: chapter for chapter in self.work_units}
        quarantined_ids = set(quarantined).intersection(by_id)
        try:
            initial_graph = self._observed_work_unit_graph()
        except ValueError as error:
            self.state.scheduling["graph_failure"] = {
                "kind": "source_dependency_graph",
                "detail": str(error),
                "members": sorted(by_id),
                "updated_at": datetime.now(UTC).isoformat(),
            }
            await self.state.save("state")
            return False
        if self.force:
            await self._invalidate_reviews(
                by_id,
                detail="review explicitly forced by this invocation",
            )
        reviewed = {
            chapter.id
            for chapter in self.work_units
            if chapter.id not in quarantined_ids
            and self.state.task(chapter.id, Stage.REVIEW).status == TaskStatus.SUCCEEDED
        }
        review_failures: set[str] = set(quarantined_ids)
        review_blocked: set[str] = set()
        attempted: set[str] = set()
        rounds_used = {chapter_id: 0 for chapter_id in by_id}
        review_tasks: dict[str, RunningReview] = {}
        rebuild_tasks: dict[str, asyncio.Task[bool]] = {}
        proof_tasks: dict[str, asyncio.Task[StageOutcome]] = {}
        failed_rebuilds: set[str] = set()
        persisted_clean = self.state.formalize_graph.get("clean", {})
        clean = await self._retain_formalize_clean(
            initial_graph,
            persisted_clean if isinstance(persisted_clean, dict) else {},
        )
        stale_proof_builds = {
            chapter_id
            for chapter_id in reviewed
            if (
                self.state.task(chapter_id, Stage.PROVE).status == TaskStatus.SUCCEEDED
                and (
                    not isinstance(clean.get(chapter_id), dict)
                    or self.state.task(chapter_id, Stage.PROVE).source_digest
                    != clean[chapter_id].get("source_digest")
                )
            )
        }
        proof_reviews = {chapter_id: 0 for chapter_id in by_id}
        proof_review_rounds = {chapter_id: 0 for chapter_id in by_id}
        formalize_failures_applied = False
        formalize_failure_ids: set[str] = set()

        def formalize_ready(chapter_id: str) -> bool:
            return self.state.task(chapter_id, Stage.FORMALIZE).status == TaskStatus.SUCCEEDED

        async def cancel_all() -> None:
            tasks: list[asyncio.Task[Any]] = [handle.task for handle in review_tasks.values()]
            tasks.extend(rebuild_tasks.values())
            tasks.extend(proof_tasks.values())
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        try:
            while True:
                await self._discard_stale_proof_review_requests()
                if formalize is not None:
                    # Clear before inspecting state so a subsequent formalize
                    # transition cannot be lost between the scan and wait.
                    formalize.progress.clear()
                    recovered = {
                        chapter_id
                        for chapter_id in formalize_failure_ids
                        if self.state.task(chapter_id, Stage.FORMALIZE).status
                        not in {TaskStatus.FAILED, TaskStatus.INTERRUPTED}
                    }
                    formalize_failure_ids.difference_update(recovered)
                    review_failures.difference_update(recovered)
                    if recovered or not formalize.idle.is_set():
                        formalize_failures_applied = False
                try:
                    graph = self._observed_work_unit_graph()
                except ValueError as error:
                    self.state.scheduling["graph_failure"] = {
                        "kind": "source_dependency_graph",
                        "detail": str(error),
                        "members": sorted(by_id),
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                    await self.state.save("state")
                    return False

                if (
                    formalize is not None
                    and (formalize.task.done() or formalize.idle.is_set())
                    and not formalize_failures_applied
                ):
                    # Propagate orchestration failures, then quarantine only
                    # chapters whose own formalization did not succeed. Independent
                    # clean branches remain eligible for review and proof.
                    if formalize.task.done():
                        formalize.task.result()
                    failed_formalizations = {
                        chapter_id
                        for chapter_id in formalize.target_ids
                        if self.state.task(chapter_id, Stage.FORMALIZE).status
                        != TaskStatus.SUCCEEDED
                    }
                    if failed_formalizations:
                        reviewed.difference_update(failed_formalizations)
                        review_failures.update(failed_formalizations)
                        cancelled_proofs = [
                            proof_tasks.pop(chapter_id)
                            for chapter_id in failed_formalizations
                            if chapter_id in proof_tasks
                        ]
                        for task in cancelled_proofs:
                            task.cancel()
                        await asyncio.gather(*cancelled_proofs, return_exceptions=True)
                        await self._invalidate_reviews(
                            failed_formalizations,
                            detail="review blocked by failed formalization",
                        )
                    formalize_failure_ids = failed_formalizations
                    formalize_failures_applied = True

                # Pull durable successes into readiness and remove reviews with
                # direct findings. Source edits separately trigger build rechecks.
                reviewed.difference_update(self._invalidated_reviews)
                self._invalidated_reviews.clear()
                new_rechecks = set(self._proof_rechecks)
                self._proof_rechecks.clear()
                failed_rebuilds.difference_update(new_rechecks)
                dirty_value = self.state.formalize_graph.get("dirty", ())
                dirty_builds = (
                    set(dirty_value) if isinstance(dirty_value, list) else set()
                ) | stale_proof_builds
                reviewed.update(
                    chapter_id
                    for chapter_id in by_id
                    if self.state.task(chapter_id, Stage.REVIEW).status == TaskStatus.SUCCEEDED
                )
                # Findings reopen only their direct owners. Descendant agents
                # that were already released against the previous clean build
                # remain pinned to that snapshot and are allowed to drain.
                stale_reviews = [
                    chapter_id
                    for chapter_id, handle in review_tasks.items()
                    if self.state.task(chapter_id, Stage.REVIEW).status
                    not in {TaskStatus.RUNNING, TaskStatus.SUCCEEDED}
                ]
                cancelled_reviews: list[asyncio.Task[Any]] = []
                for chapter_id in stale_reviews:
                    handle = review_tasks.pop(chapter_id)
                    handle.task.cancel()
                    cancelled_reviews.append(handle.task)
                if cancelled_reviews:
                    await asyncio.gather(*cancelled_reviews, return_exceptions=True)
                    async with self.state.batch():
                        for chapter_id in stale_reviews:
                            if (
                                self.state.task(chapter_id, Stage.REVIEW).status
                                == TaskStatus.RUNNING
                            ):
                                await self.state.set_task(
                                    chapter_id,
                                    Stage.REVIEW,
                                    TaskStatus.PENDING,
                                    "prerequisite review was invalidated during review",
                                )

                review_frontiers_ready = self._dependency_frontiers_ready(graph, reviewed)
                for chapter_id in graph.order:
                    proof_feedback, proof_request_ids = self._proof_review_feedback(chapter_id)
                    proof_review_role = self._proof_review_role(proof_request_ids)
                    if (
                        proof_review_role == PROOF_REVIEW_ROLE
                        and chapter_id in reviewed
                        and chapter_id not in review_tasks
                        and chapter_id not in proof_tasks
                        and chapter_id not in rebuild_tasks
                        and formalize_ready(chapter_id)
                    ):
                        review_tasks[chapter_id] = RunningReview(
                            task=asyncio.create_task(
                                self._review_chapter_to_clean(
                                    by_id[chapter_id],
                                    proof_review_rounds,
                                    rerun=True,
                                    feedback=proof_feedback,
                                    role=PROOF_REVIEW_ROLE,
                                    proof_request_ids=proof_request_ids,
                                )
                            ),
                            dependencies=graph.dependencies[chapter_id],
                            proof_request_ids=proof_request_ids,
                            auxiliary=True,
                        )

                for chapter_id in graph.order:
                    review_task = self.state.task(chapter_id, Stage.REVIEW)
                    # A forced pipeline run is not itself evidence that this node has
                    # already been reviewed. Only actual prior/active review work may
                    # bypass dependency-review ordering.
                    rereview = chapter_id in attempted or review_task.rounds > 0
                    if (
                        chapter_id not in reviewed
                        and chapter_id not in review_failures
                        and chapter_id not in review_blocked
                        and chapter_id not in review_tasks
                        and chapter_id not in proof_tasks
                        and chapter_id not in rebuild_tasks
                        and formalize_ready(chapter_id)
                        and self.state.readiness(review_task).ready
                        and (rereview or chapter_id in review_frontiers_ready)
                    ):
                        dependencies = graph.dependencies[chapter_id]
                        proof_feedback, proof_request_ids = self._proof_review_feedback(chapter_id)
                        proof_review_role = self._proof_review_role(proof_request_ids)
                        verification_retry = (
                            review_task.detail == COORDINATOR_VERIFICATION_RETRY_DETAIL
                        )
                        review_rerun = (
                            rerun or chapter_id in attempted or review_task.rounds > 0
                        ) and not verification_retry
                        await self.state.set_task(
                            chapter_id,
                            Stage.REVIEW,
                            TaskStatus.RUNNING,
                            (
                                "non-sorry warning cleanup queued"
                                if proof_review_role == WARNING_REVIEW_ROLE
                                else "coordinator diagnostic repair queued"
                                if proof_review_role == DIAGNOSTIC_REVIEW_ROLE
                                else "failed-proof statement re-review queued"
                                if proof_feedback
                                else COORDINATOR_VERIFICATION_RETRY_DETAIL
                                if verification_retry
                                else "targeted re-review queued"
                                if rereview
                                else "waiting for dependency-ordered coordinator build"
                            ),
                        )
                        review_feedback_options: dict[str, Any] = {
                            "rerun": review_rerun,
                            "feedback": proof_feedback,
                            "proof_request_ids": proof_request_ids,
                        }
                        if proof_review_role:
                            review_feedback_options["role"] = proof_review_role
                        if verification_retry:
                            review_feedback_options["verification_retry"] = True
                        review_operation = (
                            self._review_chapter_to_clean(
                                by_id[chapter_id],
                                rounds_used,
                                **review_feedback_options,
                            )
                            if proof_feedback
                            else self._review_chapter_to_clean(
                                by_id[chapter_id],
                                rounds_used,
                                rerun=review_rerun,
                                **({"verification_retry": True} if verification_retry else {}),
                            )
                        )
                        review_tasks[chapter_id] = RunningReview(
                            task=asyncio.create_task(review_operation),
                            dependencies=dependencies,
                            proof_request_ids=proof_request_ids,
                        )
                        attempted.add(chapter_id)

                for chapter_id in graph.order:
                    if (
                        chapter_id in dirty_builds
                        and chapter_id not in failed_rebuilds
                        and chapter_id not in review_tasks
                        and chapter_id not in proof_tasks
                        and chapter_id not in rebuild_tasks
                        and formalize_ready(chapter_id)
                    ):
                        rebuild_tasks[chapter_id] = asyncio.create_task(
                            self._rebuild_dirty_chapter(by_id[chapter_id])
                        )

                if prove:
                    for chapter_id in graph.order:
                        if (
                            chapter_id in reviewed
                            and self.state.task(chapter_id, Stage.PROVE).status
                            not in {TaskStatus.SUCCEEDED, TaskStatus.FAILED}
                            and chapter_id not in proof_tasks
                            and chapter_id not in review_tasks
                            and chapter_id not in rebuild_tasks
                            and formalize_ready(chapter_id)
                            and self.state.readiness(self.state.task(chapter_id, Stage.PROVE)).ready
                        ):
                            proof_tasks[chapter_id] = asyncio.create_task(
                                self._prove(by_id[chapter_id], defer_review=True)
                            )

                live_tasks: list[asyncio.Task[Any]] = [
                    handle.task for handle in review_tasks.values()
                ]
                live_tasks.extend(rebuild_tasks.values())
                live_tasks.extend(proof_tasks.values())
                progress_waiter: asyncio.Task[bool] | None = None
                if (
                    formalize is not None
                    and not formalize.task.done()
                    and not formalize.idle.is_set()
                ):
                    progress_waiter = asyncio.create_task(formalize.progress.wait())
                    live_tasks.extend((formalize.task, progress_waiter))
                if not live_tasks:
                    unresolved = set(by_id).difference(reviewed | review_failures | review_blocked)
                    if unresolved:
                        if not review_failures:
                            raise RuntimeError(
                                "review scheduler has unresolved chapters but no runnable tasks"
                            )
                        review_blocked.update(unresolved)
                        break
                    if not prove or all(
                        self.state.task(chapter_id, Stage.PROVE).status
                        in {TaskStatus.SUCCEEDED, TaskStatus.FAILED}
                        for chapter_id in reviewed
                    ):
                        break
                    raise RuntimeError(
                        "review scheduler has unfinished proofs but no runnable tasks"
                    )
                done, _ = await asyncio.wait(live_tasks, return_when=asyncio.FIRST_COMPLETED)
                if progress_waiter is not None and progress_waiter not in done:
                    progress_waiter.cancel()
                    await asyncio.gather(progress_waiter, return_exceptions=True)

                completed_reviews = [
                    chapter_id for chapter_id, handle in review_tasks.items() if handle.task in done
                ]
                for chapter_id in completed_reviews:
                    handle = review_tasks.pop(chapter_id)
                    outcome = handle.task.result()
                    if handle.auxiliary:
                        if outcome.waiting:
                            proof_review_rounds[chapter_id] = 0
                            continue
                        proof_review_rounds[chapter_id] = 0
                        if outcome.failed:
                            blocker_ids = {
                                str(blocker_id)
                                for request_id in handle.proof_request_ids
                                for request in (self.state.proof_review_requests.get(request_id),)
                                if isinstance(request, dict)
                                for blocker_id in request.get("blocker_ids", ())
                            }
                            await self.state.set_proof_blocker_status(
                                blocker_ids, ProofBlockerStatus.BLOCKED
                            )
                            await self.state.finish_proof_review_requests(
                                chapter_id, handle.proof_request_ids
                            )
                            await self.state.set_task(
                                chapter_id,
                                Stage.PROVE,
                                TaskStatus.FAILED,
                                "proof-review correspondence exhausted without a usable response",
                            )
                        continue
                    if outcome.waiting:
                        rounds_used[chapter_id] = 0
                        continue
                    if outcome.failed:
                        task_record = self.state.task(chapter_id, Stage.REVIEW)
                        if task_record.status == TaskStatus.PENDING:
                            rounds_used[chapter_id] = 0
                            continue
                        review_failures.add(chapter_id)
                        if task_record.status != TaskStatus.FAILED:
                            await self.state.set_task(
                                chapter_id,
                                Stage.REVIEW,
                                TaskStatus.FAILED,
                                "chapter-local review failed; unrelated branches continue",
                            )
                        continue
                    current_graph = self._observed_work_unit_graph()
                    current_dependencies = current_graph.dependencies[chapter_id]
                    if not current_dependencies.issubset(handle.dependencies):
                        continue
                    if self.state.task(chapter_id, Stage.REVIEW).status != TaskStatus.SUCCEEDED:
                        await self._complete_review(
                            by_id[chapter_id],
                            "editing review completed",
                            proof_request_ids=handle.proof_request_ids,
                        )
                    remaining_feedback, _ = self._proof_review_feedback(chapter_id)
                    if remaining_feedback:
                        await self.state.set_task(
                            chapter_id,
                            Stage.REVIEW,
                            TaskStatus.PENDING,
                            "additional reason-specific re-review queued",
                        )
                        continue
                    reviewed.add(chapter_id)

                completed_rebuilds = [
                    chapter_id for chapter_id, task in rebuild_tasks.items() if task in done
                ]
                for chapter_id in completed_rebuilds:
                    succeeded = rebuild_tasks.pop(chapter_id).result()
                    stale_proof_builds.discard(chapter_id)
                    if not succeeded:
                        failed_rebuilds.add(chapter_id)

                completed_proofs = [
                    chapter_id for chapter_id, task in proof_tasks.items() if task in done
                ]
                for chapter_id in completed_proofs:
                    if chapter_id not in proof_tasks:
                        continue
                    proof_outcome = proof_tasks.pop(chapter_id).result()
                    if proof_outcome.succeeded:
                        proof_task = self.state.task(chapter_id, Stage.PROVE)
                        if proof_task.status != TaskStatus.SUCCEEDED:
                            source_digest = await asyncio.to_thread(
                                scope_digest, self.config.settings.repo, by_id[chapter_id]
                            )
                            await self.state.set_task(
                                chapter_id,
                                Stage.PROVE,
                                TaskStatus.SUCCEEDED,
                                "proof completed",
                                source_digest=source_digest,
                            )
                        continue
                    if proof_outcome.waiting:
                        continue

                    proof_task = self.state.task(chapter_id, Stage.PROVE)
                    primary_runs = [run for run in proof_task.runs if not run.auxiliary]
                    report = primary_runs[-1].report if primary_runs else None
                    if not isinstance(report, dict) or not report.get("failed_attempts"):
                        continue
                    if (
                        proof_reviews[chapter_id]
                        >= self.config.stages[Stage.PROVE].unchanged_retry_limit
                    ):
                        await self.state.set_task(
                            chapter_id,
                            Stage.PROVE,
                            TaskStatus.FAILED,
                            "proof findings persisted after the review retry cap",
                        )
                        continue
                    proof_reviews[chapter_id] += 1

                    chapter = by_id[chapter_id]
                    proof_run = primary_runs[-1]
                    await self._queue_proof_review(
                        chapter,
                        report,
                        origin_run_id=proof_run.id,
                    )
                    proof_request_ids = self._proof_review_feedback(chapter_id)[1]
                    if proof_request_ids:
                        await self.state.set_task_waiting(
                            chapter_id,
                            Stage.PROVE,
                            (
                                Requirement(
                                    RequirementKind.PROOF_REVIEW_REQUEST,
                                    owner_task_key=self.state.key(chapter_id, Stage.REVIEW),
                                    request_id=request_id,
                                    detail="waiting for proof-review findings",
                                )
                                for request_id in proof_request_ids
                            ),
                            "waiting for proof-review findings to be resolved",
                        )

        finally:
            await cancel_all()

        # Proofs are terminal leaves in the task graph. Their durable FAILED status
        # remains visible and retryable, but cannot block any downstream work
        # or turn an otherwise drained review tree into an orchestration failure.
        return not review_failures and not review_blocked

    async def _review_until_clean(self, *, rerun: bool = False) -> bool:
        return await self._review_tree(rerun=rerun)

    async def _refresh_stale_proof_build(
        self,
        chapter: WorkUnitLike,
    ) -> ValidationResult:
        """Refresh an exact coordinator build before launching a proof agent."""

        await self.control.checkpoint()
        snapshots: dict[str, ValidatedBuildSnapshot] = {}
        validation = (
            await self._build_chapters(
                (chapter,),
                publish_if_clean=True,
                mode="proof-refresh",
                stage=Stage.PROVE,
                snapshots=snapshots,
            )
        )[chapter.id]
        if validation.succeeded and not await self._publish_validated_build(
            chapter, snapshots[chapter.id]
        ):
            return ValidationResult(
                False,
                1,
                "Source scope changed after the coordinator build; retry required.",
                status=ValidationStatus.STALE_SNAPSHOT,
            )
        if validation.warnings_only:
            if not await self._publish_validated_build(chapter, snapshots[chapter.id]):
                return ValidationResult(
                    False,
                    1,
                    "Source scope changed after the coordinator build; retry required.",
                    status=ValidationStatus.STALE_SNAPSHOT,
                )
            warning_output = validation.output
            await self._queue_warning_cleanup(chapter, validation, stage=Stage.PROVE)
            return ValidationResult(
                True,
                0,
                "Lean build succeeded; warning cleanup queued.\n\n" + warning_output,
                process_exit_code=0,
                status=ValidationStatus.CLEAN,
            )
        return validation

    async def _rebuild_dirty_chapter(self, chapter: WorkUnitLike) -> bool:
        """Refresh one invalidated exact build while its chapter has no agent."""

        proof = self.state.task(chapter.id, Stage.PROVE)
        was_proved = proof.status == TaskStatus.SUCCEEDED
        validation = await self._refresh_stale_proof_build(chapter)
        if was_proved:
            if validation.succeeded:
                source_digest = await asyncio.to_thread(
                    scope_digest, self.config.settings.repo, chapter
                )
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.SUCCEEDED,
                    proof.detail,
                    source_digest=source_digest,
                )
            else:
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.PENDING,
                    "stale proof no longer builds",
                )
        return validation.succeeded

    async def _prove(self, chapter: WorkUnitLike, *, defer_review: bool = False) -> StageOutcome:
        build_fresh = False
        if not self.force:
            graph = self._observed_work_unit_graph()
            persisted = self.state.formalize_graph.get("clean", {})
            records = persisted if isinstance(persisted, dict) else {}
            proof_task = self.state.task(chapter.id, Stage.PROVE)
            record = await self._retained_formalize_record(chapter, records)
            dependencies_current = self._interface_dependencies_are_current(graph, chapter.id)
            if (
                proof_task.status == TaskStatus.SUCCEEDED
                and isinstance(record, dict)
                and proof_task.source_digest == record.get("source_digest")
                and dependencies_current
            ):
                placeholders = await asyncio.to_thread(
                    count_placeholders, self.config.settings.repo, chapter
                )
                await self.state.record_sorry_count(chapter.id, placeholders)
                return StageOutcome(ExecutionDisposition.SUCCEEDED)
            files = scoped_files(self.config.settings.repo, chapter)
            if files:
                placeholders = await asyncio.to_thread(
                    count_placeholders, self.config.settings.repo, chapter
                )
                await self.state.record_sorry_count(chapter.id, placeholders)
                build_fresh = isinstance(record, dict) and dependencies_current
                if not build_fresh:
                    revalidation = await self._refresh_stale_proof_build(chapter)
                    if not revalidation.succeeded:
                        routed = (
                            await self._build_feedback_async({chapter.id: revalidation})
                        ).actionable
                        diagnostic_feedback = routed or {
                            chapter.id: (
                                "Coordinator validation of the current sources failed before proof "
                                "work:\n" + revalidation.output
                            )
                        }
                        origin = (
                            f"proof-refresh:{chapter.id}:"
                            + hashlib.sha256(revalidation.output.encode()).hexdigest()[:12]
                        )
                        await self._queue_review_feedback(
                            diagnostic_feedback,
                            origin=origin,
                        )
                        await self.state.set_task(
                            chapter.id,
                            Stage.PROVE,
                            TaskStatus.PENDING,
                            "pre-existing coordinator diagnostics routed before proof work",
                        )
                        return StageOutcome(
                            ExecutionDisposition.WAITING,
                            (
                                Requirement(
                                    RequirementKind.PROOF_REVIEW_REQUEST,
                                    detail="coordinator diagnostics require review",
                                ),
                            ),
                        )
                    else:
                        build_fresh = True
                        if placeholders > 0:
                            await self.state.set_task(
                                chapter.id,
                                Stage.PROVE,
                                TaskStatus.PENDING,
                                f"fresh exact build confirmed; {placeholders} placeholders remain",
                            )
                if placeholders == 0 and build_fresh:
                    source_digest = await asyncio.to_thread(
                        scope_digest, self.config.settings.repo, chapter
                    )
                    await self.state.set_task(
                        chapter.id,
                        Stage.PROVE,
                        TaskStatus.SUCCEEDED,
                        "placeholder-free sources validated without an agent",
                        source_digest=source_digest,
                    )
                    return StageOutcome(ExecutionDisposition.SUCCEEDED)
        proof_maximum = self.config.stages[Stage.PROVE].max_rounds
        proof_chunk_size = self.config.stages[Stage.PROVE].chunk_size or 6
        discovered_targets = await asyncio.to_thread(
            proof_targets, self.config.settings.repo, chapter
        )
        if build_fresh:
            await self._resolve_obsolete_dependency_blockers(chapter.id)
        chunked_proofs = bool(discovered_targets)
        assigned_targets: tuple[ProofTarget, ...] = ()
        chunk_round = 0
        skipped_target_ids: set[str] = set()
        blocking_package_ids: set[str] = set()

        def matches_target(value: dict[str, Any], target: ProofTarget) -> bool:
            path = str(value.get("path", value.get("consumer_path", "")))
            declaration = str(value.get("declaration", value.get("blocked_declaration", "")))
            same_declaration = declaration == target.declaration or declaration.endswith(
                "." + target.declaration
            )
            return path == target.path and same_declaration

        def unavailable_target_ids(targets: Iterable[ProofTarget]) -> set[str]:
            blockers = self.state.proof_blockers_for_consumer(chapter.id, active_only=False)
            return {
                target.fingerprint
                for target in targets
                if any(
                    blocker.get("status") != ProofBlockerStatus.OPEN.value
                    and matches_target(blocker, target)
                    for blocker in blockers
                )
            }

        # A completed review must explicitly reopen or resolve its blockers. Merely consuming the
        # request is not evidence that another proof attempt has become possible.
        feedback = ""
        feedback_ledger: deque[str] = deque(maxlen=PROOF_FEEDBACK_ROUNDS)
        stalled_rounds = 0
        previous_placeholders: int | None = None
        proof_round = 0
        proof_resume_thread_id: str | None = None
        proof_resume_run_id = ""
        proof_resume_prompt = ""
        while chunked_proofs or proof_round < proof_maximum:
            if chunked_proofs and not assigned_targets:
                discovered_targets = await asyncio.to_thread(
                    proof_targets, self.config.settings.repo, chapter
                )
                if not discovered_targets:
                    remaining_placeholders = await asyncio.to_thread(
                        count_placeholders, self.config.settings.repo, chapter
                    )
                    if remaining_placeholders == 0:
                        source_digest = await asyncio.to_thread(
                            scope_digest, self.config.settings.repo, chapter
                        )
                        await self._resolve_satisfied_proof_blockers(chapter)
                        await self.state.set_task(
                            chapter.id,
                            Stage.PROVE,
                            TaskStatus.SUCCEEDED,
                            "all proof chunks completed and chapter elaborates",
                            source_digest=source_digest,
                        )
                        return StageOutcome(ExecutionDisposition.SUCCEEDED)
                    # Keep unusual declaration syntax safe: after all recognized chunks, hand any
                    # remaining raw placeholders to the established whole-chapter fallback.
                    chunked_proofs = False
                    proof_round = 0
                    assigned_targets = ()
                if chunked_proofs:
                    unavailable = skipped_target_ids | unavailable_target_ids(discovered_targets)
                    candidates = tuple(
                        target
                        for target in discovered_targets
                        if target.fingerprint not in unavailable
                    )
                    if not candidates:
                        break
                    blocked_candidates = tuple(
                        target
                        for target in candidates
                        if any(
                            matches_target(blocker, target)
                            for blocker in self.state.proof_blockers_for_consumer(chapter.id)
                        )
                    )
                    # Once a declaration has durable failure evidence, isolate it from otherwise
                    # productive holes so a hard residual cannot consume the whole chunk budget.
                    assigned_targets = (
                        blocked_candidates[:1]
                        if blocked_candidates
                        else proof_target_chunk(candidates, proof_chunk_size)
                    )
                    chunk_round = 0
                    if not feedback and (
                        durable_feedback := self._durable_blocker_feedback(
                            chapter.id, assigned_targets
                        )
                    ):
                        feedback_ledger.append(durable_feedback)
                        feedback = _bounded_proof_feedback(feedback_ledger)
            try:
                attempt = await self._attempt(
                    chapter,
                    Stage.PROVE,
                    feedback=feedback,
                    queue_detail=(
                        f"proof chunk retry {chunk_round + 1}/{proof_maximum}: "
                        + ", ".join(target.declaration for target in assigned_targets)
                        if chunked_proofs
                        else f"proof round {proof_round + 1}/{proof_maximum}"
                    ),
                    proof_targets=assigned_targets,
                    resume_thread_id=proof_resume_thread_id,
                    resume_run_id=proof_resume_run_id,
                    resume_prompt=proof_resume_prompt,
                )
            except GitCommitError as error:
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.PENDING,
                    f"proof retry deferred until dirty exclusive scope is reconciled: {error}",
                )
                return StageOutcome(
                    ExecutionDisposition.WAITING,
                    (
                        Requirement(
                            RequirementKind.BUILD_FRESHNESS,
                            detail="dirty exclusive scope must be reconciled",
                        ),
                    ),
                )
            if attempt.agent.capacity_exhausted:
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.FAILED,
                    "model capacity remained unavailable after the configured retries",
                )
                return StageOutcome(ExecutionDisposition.FAILED)
            coordinator_validation = attempt.validation
            if chunked_proofs and assigned_targets:
                validation_targets = await asyncio.to_thread(
                    proof_target_spans,
                    self.config.settings.repo,
                    chapter,
                    assigned_targets,
                )
                attempt = replace(
                    attempt,
                    validation=self._proof_chunk_validation(
                        coordinator_validation,
                        validation_targets,
                    ),
                )
            proof_resume_thread_id = None
            proof_resume_run_id = ""
            proof_resume_prompt = ""
            proof_round += 1
            if chunked_proofs:
                chunk_round += 1
            routed_validation_output = attempt.validation.output
            foreign_validation_feedback: dict[str, str] = {}
            if not coordinator_validation.succeeded:
                routed_validation = (
                    await self._build_feedback_async({chapter.id: coordinator_validation})
                ).actionable
                local_validation_output = routed_validation.pop(chapter.id, "")
                foreign_validation_feedback = routed_validation
                if not attempt.validation.succeeded:
                    routed_validation_output = local_validation_output or attempt.validation.output
                else:
                    # Mainline scopes chunk retries to their assigned declarations.
                    # A same-chapter diagnostic outside those spans must not spend
                    # this chunk's budget, though diagnostics owned by other work
                    # units still need to be routed below.
                    routed_validation_output = ""
                if (
                    not routed_validation_output
                    and not foreign_validation_feedback
                    and not attempt.validation.succeeded
                ):
                    # Keep unattributed compiler failures with the originating
                    # proof assignment instead of silently dropping them.
                    routed_validation_output = attempt.validation.output
                if foreign_validation_feedback:
                    await self._queue_review_feedback(
                        foreign_validation_feedback,
                        origin=f"proof-validation:{attempt.run.id}",
                    )
                if (
                    not attempt.validation.succeeded
                    and routed_validation_output
                    and attempt.agent.thread_id
                ):
                    proof_resume_thread_id = attempt.agent.thread_id
                    proof_resume_run_id = attempt.run.id
                    proof_resume_prompt = PROOF_VALIDATION_RETRY_PROMPT
            if (
                not coordinator_validation.succeeded
                and attempt.validation.succeeded
                and not routed_validation_output
                and foreign_validation_feedback
            ):
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.PENDING,
                    "proof validation diagnostics routed to their source owners",
                )
                return StageOutcome(
                    ExecutionDisposition.WAITING,
                    (
                        Requirement(
                            RequirementKind.PROOF_REVIEW_REQUEST,
                            detail="validation diagnostics were routed to source owners",
                        ),
                    ),
                )
            remaining_targets = (
                await asyncio.to_thread(proof_targets, self.config.settings.repo, chapter)
                if chunked_proofs
                else ()
            )
            remaining_placeholder_count = await asyncio.to_thread(
                count_placeholders, self.config.settings.repo, chapter
            )
            assigned_ids = {target.fingerprint for target in assigned_targets}
            remaining_assigned = tuple(
                target for target in remaining_targets if target.fingerprint in assigned_ids
            )
            if (
                attempt.agent.succeeded
                and coordinator_validation.succeeded
                and (
                    remaining_placeholder_count == 0
                    if chunked_proofs
                    else attempt.agent.placeholders == 0
                )
            ):
                await self._resolve_satisfied_proof_blockers(chapter)
                source_digest = await asyncio.to_thread(
                    scope_digest, self.config.settings.repo, chapter
                )
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.SUCCEEDED,
                    "no placeholders and chapter elaborates",
                    source_digest=source_digest,
                )
                return StageOutcome(ExecutionDisposition.SUCCEEDED)
            if (
                chunked_proofs
                and assigned_targets
                and not remaining_assigned
                and remaining_placeholder_count == 0
                and attempt.agent.succeeded
                and attempt.validation.succeeded
                and not coordinator_validation.succeeded
            ):
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.PENDING,
                    "all proof chunks completed; coordinator diagnostics remain outside the "
                    "assigned declarations",
                )
                return StageOutcome(
                    ExecutionDisposition.WAITING,
                    (
                        Requirement(
                            RequirementKind.BUILD_FRESHNESS,
                            detail="coordinator diagnostics remain outside the proof chunk",
                        ),
                    ),
                )
            if (
                chunked_proofs
                and assigned_targets
                and not remaining_assigned
                and attempt.agent.succeeded
                and attempt.validation.succeeded
            ):
                await self._resolve_satisfied_proof_blockers(chapter)
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.PENDING,
                    f"completed proof chunk with {len(remaining_targets)} declaration(s) remaining",
                )
                assigned_targets = ()
                chunk_round = 0
                feedback_ledger.clear()
                feedback = ""
                continue

            report_disposition = str(attempt.agent.report.get("disposition", ""))
            if report_disposition == "validation_inconsistency" and not attempt.agent.changed:
                exact_validation = await self._refresh_stale_proof_build(chapter)
                if not exact_validation.succeeded:
                    routed = (
                        await self._build_feedback_async({chapter.id: exact_validation})
                    ).actionable
                    await self._queue_review_feedback(
                        routed
                        or {
                            chapter.id: (
                                "Exact coordinator revalidation after an agent-reported validation "
                                "inconsistency failed:\n" + exact_validation.output
                            )
                        },
                        origin=f"proof-validation-inconsistency:{attempt.run.id}",
                    )
                    await self.state.set_task(
                        chapter.id,
                        Stage.PROVE,
                        TaskStatus.PENDING,
                        "exact coordinator diagnostics routed after validation inconsistency",
                    )
                    return StageOutcome(
                        ExecutionDisposition.WAITING,
                        (
                            Requirement(
                                RequirementKind.PROOF_REVIEW_REQUEST,
                                detail="exact coordinator diagnostics require repair",
                            ),
                        ),
                    )
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.FAILED,
                    "agent-reported validation inconsistency was not reproduced by an exact build",
                )
                return StageOutcome(ExecutionDisposition.FAILED)

            feedback_ledger.clear()
            feedback_ledger.append(
                f"Proof attempt {proof_round}:\n"
                + attempt.feedback(validation_output=routed_validation_output)
            )
            blockers = await self._record_proof_blocker_deltas(
                chapter, attempt.run, attempt.agent.report
            )
            package_ids = await self._attach_structural_blockers_to_packages(chapter, blockers)
            if package_ids:
                if chunked_proofs:
                    blocking_package_ids.update(package_ids)
                    skipped_target_ids.update(target.fingerprint for target in assigned_targets)
                    assigned_targets = ()
                    chunk_round = 0
                    feedback_ledger.clear()
                    feedback = ""
                    continue
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.BLOCKED,
                    "structural proof work attached to capability package(s): "
                    + ", ".join(package_ids),
                )
                return StageOutcome(
                    ExecutionDisposition.WAITING,
                    tuple(
                        Requirement(
                            RequirementKind.CAPABILITY_PACKAGE,
                            request_id=package_id,
                            detail="package Steward owns the structural proof work",
                        )
                        for package_id in package_ids
                    ),
                )
            if durable_feedback := self._durable_blocker_feedback(chapter.id, assigned_targets):
                feedback_ledger.append(durable_feedback)
            feedback = _bounded_proof_feedback(feedback_ledger)
            if report_disposition == "statement_defect" and not blockers:
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.FAILED,
                    "statement defect report lacked target-specific failed-attempt evidence",
                )
                return StageOutcome(ExecutionDisposition.FAILED)
            if report_disposition == "structural_blocked" and not blockers:
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.FAILED,
                    "structural-blocked report lacked package evidence",
                )
                return StageOutcome(ExecutionDisposition.FAILED)
            if report_disposition == "partial" and not attempt.agent.changed:
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.FAILED,
                    "partial proof report retained no scoped source change",
                )
                return StageOutcome(ExecutionDisposition.FAILED)
            review_queued = False
            terminal_blockers: list[str] = []
            for blocker in blockers:
                blocker_id = str(blocker.get("id", ""))
                if blocker.get("status") in {
                    ProofBlockerStatus.PACKAGE_REQUIRED.value,
                    ProofBlockerStatus.PARKED.value,
                    ProofBlockerStatus.WAITING_DEPENDENCY.value,
                }:
                    terminal_blockers.append(blocker_id)
                    continue
                sightings = int(blocker.get("sightings", 0))
                retry_baseline = int(blocker.get("retry_sighting_baseline", 0))
                immediate_handoff = (
                    report_disposition in {"statement_defect", "structural_blocked"}
                    or isinstance(blocker.get("capability"), dict)
                    or self._blocker_needs_review(blocker)
                )
                retry_limit = (
                    1
                    if immediate_handoff
                    else self.config.stages[Stage.PROVE].unchanged_retry_limit
                )
                if sightings - retry_baseline < retry_limit:
                    continue
                if self._blocker_needs_review(blocker):
                    if (
                        int(blocker.get("review_exchange_count", 0))
                        >= self.config.stages[Stage.PROVE].unchanged_retry_limit
                    ):
                        await self.state.set_proof_blocker_status(
                            (blocker_id,), ProofBlockerStatus.BLOCKED
                        )
                        terminal_blockers.append(blocker_id)
                        continue
                    report = self._blocker_report(blocker)
                    await self._queue_proof_review(
                        chapter,
                        report,
                        origin_run_id=attempt.run.id,
                    )
                    await self.state.set_proof_blocker_status(
                        (blocker_id,), ProofBlockerStatus.REVIEW_REQUESTED
                    )
                    review_queued = True
                else:
                    await self.state.set_proof_blocker_status(
                        (blocker_id,), ProofBlockerStatus.BLOCKED
                    )
                    terminal_blockers.append(blocker_id)
            if review_queued:
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.PENDING,
                    "unchanged statement/interface blocker queued for focused review",
                )
                return StageOutcome(
                    ExecutionDisposition.WAITING,
                    tuple(
                        Requirement(
                            RequirementKind.PROOF_REVIEW_REQUEST,
                            request_id=request_id,
                            detail="proof blocker queued for focused review",
                        )
                        for request_id in self._proof_review_feedback(chapter.id)[1]
                    ),
                )
            if (
                chunked_proofs
                and terminal_blockers
                and not self.state.proof_blockers_for_consumer(chapter.id)
            ):
                skipped_target_ids.update(target.fingerprint for target in assigned_targets)
                assigned_targets = ()
                chunk_round = 0
                feedback_ledger.clear()
                feedback = ""
                continue
            if terminal_blockers and not self.state.proof_blockers_for_consumer(chapter.id):
                routed_statuses = {
                    str(self.state.proof_blockers.get(blocker_id, {}).get("status", ""))
                    for blocker_id in terminal_blockers
                }
                if routed_statuses.intersection(
                    {
                        ProofBlockerStatus.PACKAGE_REQUIRED.value,
                        ProofBlockerStatus.PARKED.value,
                        ProofBlockerStatus.WAITING_DEPENDENCY.value,
                    }
                ):
                    package_required = ProofBlockerStatus.PACKAGE_REQUIRED.value in routed_statuses
                    await self.state.set_task(
                        chapter.id,
                        Stage.PROVE,
                        TaskStatus.FAILED if package_required else TaskStatus.BLOCKED,
                        "proof blocker routed without unchanged retry: "
                        + ", ".join(terminal_blockers),
                    )
                    return StageOutcome(ExecutionDisposition.FAILED)
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.FAILED,
                    "unchanged proof blocker(s): " + ", ".join(terminal_blockers),
                )
                return StageOutcome(ExecutionDisposition.FAILED)
            placeholders = attempt.agent.placeholders
            validation_retry_exhausted = (
                not attempt.validation.succeeded
                and bool(routed_validation_output)
                and (
                    chunk_round >= proof_maximum if chunked_proofs else proof_round >= proof_maximum
                )
            )
            if validation_retry_exhausted:
                await self._queue_review_feedback(
                    {chapter.id: routed_validation_output},
                    origin=f"proof-validation-exhausted:{attempt.run.id}",
                )
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.PENDING,
                    "proof-local validation retries exhausted; diagnostic repair queued",
                )
                return StageOutcome(
                    ExecutionDisposition.WAITING,
                    (
                        Requirement(
                            RequirementKind.PROOF_REVIEW_REQUEST,
                            detail="proof-local diagnostic repair queued",
                        ),
                    ),
                )
            if chunked_proofs:
                if chunk_round >= proof_maximum:
                    skipped_target_ids.update(target.fingerprint for target in assigned_targets)
                    assigned_targets = ()
                    chunk_round = 0
                    feedback_ledger.clear()
                    feedback = ""
                elif remaining_assigned:
                    # Refresh line numbers and placeholder counts after partial progress while
                    # retaining the retry budget for this logical chunk.
                    assigned_targets = remaining_assigned
                continue
            if previous_placeholders is not None and placeholders >= previous_placeholders:
                stalled_rounds += 1
            else:
                stalled_rounds = 0
            previous_placeholders = placeholders
            if stalled_rounds >= 2:
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.FAILED,
                    f"proof pass stalled with {placeholders} placeholders",
                )
                return StageOutcome(ExecutionDisposition.FAILED)

        unresolved_placeholders = await asyncio.to_thread(
            count_placeholders, self.config.settings.repo, chapter
        )
        if blocking_package_ids:
            await self.state.set_task(
                chapter.id,
                Stage.PROVE,
                TaskStatus.BLOCKED,
                "independent proof chunks exhausted; structural work remains in package(s): "
                + ", ".join(sorted(blocking_package_ids)),
            )
            return StageOutcome(
                ExecutionDisposition.WAITING,
                tuple(
                    Requirement(
                        RequirementKind.CAPABILITY_PACKAGE,
                        request_id=package_id,
                        detail="package Steward owns the remaining structural proof work",
                    )
                    for package_id in sorted(blocking_package_ids)
                ),
            )
        await self.state.set_task(
            chapter.id,
            Stage.PROVE,
            TaskStatus.FAILED,
            (
                "proof chunks exhausted retries with "
                f"{unresolved_placeholders} "
                "placeholder(s) remaining; durable blockers retained"
                if chunked_proofs
                else (
                    f"proof pass did not converge in {proof_maximum} rounds; "
                    "durable blockers retained"
                )
            ),
        )
        return StageOutcome(ExecutionDisposition.FAILED)

    async def _run_stage_once(self, stage: Stage) -> bool:
        if stage is Stage.DISCOVER:
            return await self._discover_all()
        if stage is Stage.FORMALIZE:
            return await self._discover_and_formalize(discover=True)
        if stage is Stage.REVIEW:
            return await self._review_until_clean()
        return await self._review_tree(prove=True)

    async def run_stage(self, stage: Stage) -> bool:
        result = await self._run_stage_once(stage)
        if stage is Stage.DISCOVER:
            return result
        cleanup = await self._drain_warning_cleanups()
        if cleanup.clean and cleanup.changed:
            # Re-evaluate stage certificates after the cleanup edit. Interface
            # fingerprints decide which completed dependents actually reopen.
            result = await self._run_stage_once(stage)
            cleanup = await self._drain_warning_cleanups()
        return result and cleanup.clean

    async def _run_pipeline_once(self) -> bool:
        progress = asyncio.Event()
        idle = asyncio.Event()
        stop = asyncio.Event()
        formalize_task = asyncio.create_task(
            self._discover_and_formalize(
                progress_event=progress,
                idle_event=idle,
                stop_event=stop,
                discover=True,
            )
        )
        handle = RunningFormalizeStage(
            task=formalize_task,
            progress=progress,
            idle=idle,
            target_ids=frozenset(chapter.id for chapter in self.work_units),
        )
        try:
            reviewed = await self._review_tree(prove=True, formalize=handle)
            stop.set()
            formalized = await formalize_task
            return formalized and reviewed
        except BaseException:
            formalize_task.cancel()
            await asyncio.gather(formalize_task, return_exceptions=True)
            raise

    async def run_pipeline(self) -> bool:
        if self.config.steward.enabled:
            await self._schedule_ready_packages()
        result = await self._run_pipeline_once()
        await self._drain_active_packages()
        cleanup = await self._drain_warning_cleanups()
        if cleanup.clean and cleanup.changed:
            result = await self._run_pipeline_once()
            cleanup = await self._drain_warning_cleanups()
        return result and cleanup.clean
