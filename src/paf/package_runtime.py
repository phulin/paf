from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import uuid4

from paf import json_codec as json
from paf.package_model import (
    CapabilityPackage,
    ConsumerStatus,
    EvidenceKind,
    PackageConsumer,
    PackageDependency,
    PackageDisposition,
    PackageEvidence,
    PackageStatus,
    PackageStep,
    PackageStepKind,
    PackageStepStatus,
    RelevantReadInterface,
    ReservationMode,
    ReservationOwnerKind,
    ReservationSpec,
    StewardLease,
    normalize_capability_key,
    normalize_repository_path,
)
from paf.state_db import StateDatabase


class PackageReportError(ValueError):
    """A model report requested a mutation outside its fenced package authority."""


class PackageReservationWaiting(RuntimeError):
    """A package mutation is durably queued behind another path owner."""


@dataclass
class PackageWorkspace:
    """One private, in-flight package overlay owned by the isolation layer."""

    root: Path
    integrate: Callable[[tuple[str, ...], str], Awaitable[PackageImport]]
    close: Callable[[], Awaitable[None]]


@dataclass(frozen=True)
class PackageImport:
    """One accepted overlay delta imported and committed on canonical source."""

    changed_paths: tuple[str, ...]
    commit: str
    canonical_revision: str


@dataclass(frozen=True)
class InterfaceChange:
    interface_id: str
    expected_digest: str
    actual_digest: str | None


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


@dataclass(frozen=True)
class PackageValidation:
    """Authoritative validation evidence produced outside the model report."""

    succeeded: bool
    digest: str
    evidence: str


@dataclass(frozen=True)
class ConsumerValidation(PackageValidation):
    consumer_id: str = ""
    affected_work_unit_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class PackageExecutionResult:
    package_id: str
    generation: int
    status: PackageStatus
    integrated_revision: str | None = None
    worker_ids: tuple[str, ...] = ()
    accepted_consumer_ids: tuple[str, ...] = ()
    woken_work_unit_ids: tuple[str, ...] = ()
    detail: str = ""


StewardRunner = Callable[[CapabilityPackage, dict[str, Any], Path], Awaitable[dict[str, Any]]]
WorkerRunner = Callable[
    [CapabilityPackage, PackageStep, dict[str, Any], Path], Awaitable[dict[str, Any]]
]
StepValidator = Callable[[Path, PackageStep], PackageValidation | Awaitable[PackageValidation]]
PackageValidator = Callable[
    [Path, CapabilityPackage], PackageValidation | Awaitable[PackageValidation]
]
ConsumerValidator = Callable[
    [Path, PackageConsumer], ConsumerValidation | Awaitable[ConsumerValidation]
]
WorkspaceProvider = Callable[[CapabilityPackage, int, tuple[str, ...]], Awaitable[PackageWorkspace]]


async def _await_validation(
    value: PackageValidation | Awaitable[PackageValidation],
) -> PackageValidation:
    return await value if isinstance(value, Awaitable) else value


async def _await_consumer_validation(
    value: ConsumerValidation | Awaitable[ConsumerValidation],
) -> ConsumerValidation:
    return await value if isinstance(value, Awaitable) else value


_TERMINAL_PACKAGE_STATUSES = frozenset(
    {
        PackageStatus.COMPLETE,
        PackageStatus.DECOMPOSED,
        PackageStatus.EXTERNAL,
        PackageStatus.STATEMENT_REVISION_REQUIRED,
        PackageStatus.PARKED,
        PackageStatus.SUPERSEDED,
    }
)


class PackageExecutionLayer:
    """Run one fenced writable Steward and its bounded sequential worker steps."""

    def __init__(
        self,
        repo: Path,
        state_dir: Path,
        database: StateDatabase,
        *,
        run_steward: StewardRunner,
        run_worker: WorkerRunner,
        validate_step: StepValidator,
        validate_package: PackageValidator,
        validate_consumer: ConsumerValidator,
        acquire_workspace: WorkspaceProvider,
        interface_digest: Callable[[str], str | None] = lambda _interface: None,
        wake_consumers: Callable[[tuple[str, ...]], Awaitable[None]] | None = None,
        lease_ttl_seconds: float = 14_400,
        maximum_worker_steps: int = 8,
    ) -> None:
        if lease_ttl_seconds <= 0 or maximum_worker_steps < 0:
            raise ValueError("package lease ttl must be positive and worker bound nonnegative")
        self.repo = repo.resolve()
        del state_dir
        self.database = database
        self.run_steward = run_steward
        self.run_worker = run_worker
        self.validate_step = validate_step
        self.validate_package = validate_package
        self.validate_consumer = validate_consumer
        self.acquire_workspace = acquire_workspace
        self.interface_digest = interface_digest
        self.wake_consumers = wake_consumers
        self.lease_ttl_seconds = lease_ttl_seconds
        self.maximum_worker_steps = maximum_worker_steps
        self.interfaces = RelevantInterfaceGuard(database)
        self._running: set[str] = set()
        self._running_lock = asyncio.Lock()

    def ready_packages(self) -> tuple[CapabilityPackage, ...]:
        self.database.wake_waiting_reservation_packages()
        state = self.database.load_package_state()
        ready: list[CapabilityPackage] = []
        for package in state.packages.values():
            if (
                package.status in _TERMINAL_PACKAGE_STATUSES
                or package.status is PackageStatus.WAITING_RESERVATION
                or package.id in self._running
            ):
                continue
            dependencies = state.dependencies_of(package.id)
            if any(
                (dependency_package := state.packages.get(edge.depends_on_package_id)) is None
                or dependency_package.status is not PackageStatus.COMPLETE
                or (
                    edge.required_revision is not None
                    and dependency_package.integrated_revision != edge.required_revision
                )
                for edge in dependencies
            ):
                continue
            ready.append(package)
        return tuple(sorted(ready, key=lambda value: (value.created_at, value.id)))

    async def run_ready(self) -> tuple[PackageExecutionResult, ...]:
        """Schedule at most one Steward turn for each currently ready package."""

        results: list[PackageExecutionResult] = []
        for package in self.ready_packages():
            results.append(await self.execute(package.id))
        return tuple(results)

    async def execute(self, package_id: str) -> PackageExecutionResult:
        async with self._running_lock:
            if package_id in self._running:
                raise ValueError(f"package {package_id} already has a running Steward")
            self._running.add(package_id)
        try:
            return await self._execute(package_id)
        finally:
            async with self._running_lock:
                self._running.discard(package_id)

    def _current(self, package_id: str) -> CapabilityPackage:
        return self.database.load_package_state().packages[package_id]

    def _append_evidence(
        self,
        package_id: str,
        generation: int,
        *,
        producer: str,
        kind: EvidenceKind,
        payload: dict[str, Any],
        paths: tuple[str, ...] = (),
        declarations: tuple[str, ...] = (),
    ) -> CapabilityPackage:
        package = self._current(package_id)
        serialized = json.dumps(payload, sort_keys=True)
        evidence = PackageEvidence(
            id=f"evidence-{package_id}-{uuid4().hex}",
            package_id=package_id,
            producer=producer,
            kind=kind,
            source_revision=package.integrated_revision or "",
            paths=paths,
            declarations=declarations,
            payload=payload,
            digest=sha256(serialized.encode()).hexdigest(),
        )
        return self.database.append_package_evidence(
            evidence,
            expected_revision=package.revision,
            lease_generation=generation,
        )

    def _claim(self, package: CapabilityPackage) -> tuple[StewardLease, CapabilityPackage]:
        state = self.database.load_package_state()
        existing = state.leases.get(package.id)
        agent_id = f"steward-{package.id}-{uuid4().hex[:12]}"
        if existing is None:
            lease = self.database.claim_steward_lease(
                package.id,
                agent_id,
                expected_revision=package.revision,
                ttl_seconds=self.lease_ttl_seconds,
            )
            return lease, self._current(package.id)
        if existing is not None:
            expires = datetime.fromisoformat(existing.expires_at.replace("Z", "+00:00"))
            if expires > datetime.now(UTC):
                raise ValueError(f"package {package.id} already has a live Steward")
        lease, recovery = self.database.recover_steward_lease(
            package.id,
            agent_id,
            expected_revision=package.revision,
            ttl_seconds=self.lease_ttl_seconds,
        )
        self._append_evidence(
            package.id,
            lease.generation,
            producer=agent_id,
            kind=EvidenceKind.LEASE_RECOVERY,
            payload={
                "prior_generation": recovery.prior_generation,
                "discarded_in_flight_overlay": True,
            },
        )
        return lease, self._current(package.id)

    async def _heartbeat_lease(self, lease: StewardLease, stop: asyncio.Event) -> None:
        """Renew package authority through workers, validation, and integration."""

        interval = max(0.01, min(60.0, self.lease_ttl_seconds / 3))
        while True:
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return
            except TimeoutError:
                await asyncio.to_thread(
                    self.database.heartbeat_steward_lease,
                    lease.package_id,
                    lease.agent_id,
                    lease.generation,
                    ttl_seconds=self.lease_ttl_seconds,
                )

    def _reserve_initial_scope(
        self, package: CapabilityPackage, generation: int
    ) -> CapabilityPackage:
        if not package.write_scope:
            return package
        result = self.database.acquire_path_reservations(
            ReservationOwnerKind.PACKAGE,
            package.id,
            generation,
            tuple(ReservationSpec(path) for path in package.write_scope),
            expected_revision=package.revision,
            queue_on_conflict=True,
        )
        if not result.granted:
            owners = ", ".join(sorted({value.owner_id for value in result.conflicts}))
            raise PackageReservationWaiting(
                f"initial package scope is queued behind path owner(s): {owners}"
            )
        return self._current(package.id)

    def _dossier(self, package: CapabilityPackage, generation: int) -> dict[str, Any]:
        state = self.database.load_package_state()
        return {
            "package": package.__dict__,
            "lease_generation": generation,
            "consumers": [value.__dict__ for value in state.consumers_for(package.id)],
            "evidence": [value.__dict__ for value in state.evidence_for(package.id)],
            "steps": [value.__dict__ for value in state.steps_for(package.id)],
            "dependencies": [value.__dict__ for value in state.dependencies_of(package.id)],
            "reservations": [
                value.__dict__
                for value in state.reservations.values()
                if value.package_id == package.id
            ],
            "rules": {
                "sequential_workers": True,
                "maximum_worker_steps": self.maximum_worker_steps,
                "package_owns_structural_work": True,
            },
        }

    @staticmethod
    def _items(report: dict[str, Any], key: str) -> tuple[dict[str, Any], ...]:
        value = report.get(key)
        if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
            raise PackageReportError(f"package report field {key} must be an object array")
        return tuple(value)

    @staticmethod
    def _path_is_covered(path: str, scopes: tuple[str, ...]) -> bool:
        return any(path == scope or path.startswith(f"{scope.rstrip('/')}/") for scope in scopes)

    def _validate_decomposition(
        self,
        package: CapabilityPackage,
        raw_children: tuple[dict[str, Any], ...],
    ) -> None:
        """Reject malformed splits before any report-driven state is applied."""

        state = self.database.load_package_state()
        open_consumers = {
            value.id: value
            for value in state.consumers_for(package.id)
            if value.status is ConsumerStatus.OPEN
        }
        existing_keys = {value.capability_key for value in state.packages.values()}
        child_keys: set[str] = set()
        child_scopes: dict[str, tuple[str, ...]] = {}
        assignments: dict[str, str] = {}
        parent_scope = package.write_scope
        for raw in raw_children:
            key = normalize_capability_key(str(raw.get("capability_key", "")))
            if not key or key in child_keys:
                raise PackageReportError("decomposition child capability keys must be distinct")
            if key in existing_keys:
                raise PackageReportError(f"decomposition child capability already exists: {key}")
            child_keys.add(key)
            try:
                scope = tuple(
                    dict.fromkeys(
                        normalize_repository_path(str(value))
                        for value in raw.get("write_scope", ())
                    )
                )
            except (TypeError, ValueError) as error:
                raise PackageReportError(f"invalid decomposition child scope: {error}") from error
            if not scope:
                raise PackageReportError("decomposition children require a non-empty write scope")
            if any(not self._path_is_covered(path, parent_scope) for path in scope):
                raise PackageReportError("decomposed child requests path outside parent scope")
            child_scopes[key] = scope
            raw_consumer_ids = raw.get("consumer_ids", ())
            if not isinstance(raw_consumer_ids, list):
                raise PackageReportError("decomposition child consumer_ids must be an array")
            for value in raw_consumer_ids:
                consumer_id = str(value)
                if consumer_id in assignments:
                    raise PackageReportError("a consumer was assigned to multiple child packages")
                assignments[consumer_id] = key

        scopes = tuple((key, scope) for key, values in child_scopes.items() for scope in values)
        for index, (left_key, left) in enumerate(scopes):
            for right_key, right in scopes[index + 1 :]:
                if left_key != right_key and (
                    self._path_is_covered(left, (right,)) or self._path_is_covered(right, (left,))
                ):
                    raise PackageReportError(
                        f"decomposition child scopes overlap: {left} and {right}"
                    )

        if set(assignments) != set(open_consumers):
            raise PackageReportError(
                "decomposition must assign every and only open package consumer"
            )
        for consumer_id, child_key in assignments.items():
            consumer = open_consumers[consumer_id]
            if consumer.path and not self._path_is_covered(
                normalize_repository_path(consumer.path), child_scopes[child_key]
            ):
                raise PackageReportError(
                    f"consumer {consumer_id} path is outside its decomposition child scope"
                )

        for reservation in state.reservations.values():
            if reservation.package_id != package.id:
                continue
            owners = [
                key
                for key, scope in child_scopes.items()
                if self._path_is_covered(reservation.normalized_path, scope)
            ]
            if len(owners) > 1:
                raise PackageReportError(
                    f"reservation {reservation.normalized_path} belongs to multiple child scopes"
                )

    def _validate_report(
        self, package: CapabilityPackage, report: dict[str, Any]
    ) -> dict[str, Any]:
        required = {
            "complete",
            "summary",
            "issues",
            "diagnosis",
            "placement_decision",
            "scope_expansion_requests",
            "plan_revision",
            "completed_step_assessments",
            "worker_assignments",
            "package_dependency_requests",
            "child_packages",
            "consumer_assessments",
            "disposition",
            "remaining_work",
        }
        if set(report) != required:
            raise PackageReportError(
                "package Steward report fields differ from the strict contract: "
                + ", ".join(sorted(set(report) ^ required))
            )
        if not isinstance(report["diagnosis"], str) or not report["diagnosis"].strip():
            raise PackageReportError("package diagnosis must be non-empty")
        plan = report.get("plan_revision")
        if (
            not isinstance(plan, dict)
            or int(plan.get("base_revision", -1)) != package.plan_revision
        ):
            raise PackageReportError("package plan revision is stale")
        expansions = self._items(report, "scope_expansion_requests")
        expansion_paths = {normalize_repository_path(item.get("path", "")) for item in expansions}
        if not expansion_paths.issubset(set(package.expansion_scope)):
            # Subpaths are admitted by the database only when the configured root covers them.
            for path in expansion_paths:
                if not any(
                    path.startswith(f"{root.rstrip('/')}/") for root in package.expansion_scope
                ):
                    raise PackageReportError(f"model requested invalid package path: {path}")
        placement = report.get("placement_decision")
        if not isinstance(placement, dict):
            raise PackageReportError("placement decision must be an object")
        bounded_paths = set(package.write_scope) | set(package.expansion_scope)
        for raw_path in placement.get("paths", ()):
            path = normalize_repository_path(raw_path)
            if not any(
                path == root or path.startswith(f"{root.rstrip('/')}/") for root in bounded_paths
            ):
                raise PackageReportError(f"placement decision names invalid package path: {path}")
        state = self.database.load_package_state()
        consumer_ids = {value.id for value in state.consumers_for(package.id)}
        assessed = {
            str(value.get("consumer_id", ""))
            for value in self._items(report, "consumer_assessments")
        }
        if not assessed.issubset(consumer_ids):
            raise PackageReportError("consumer assessment names a consumer outside the package")
        dependency_ids = {
            str(value.get("package_id", ""))
            for value in self._items(report, "package_dependency_requests")
        }
        if package.id in dependency_ids or not dependency_ids.issubset(set(state.packages)):
            raise PackageReportError("package dependency request names an invalid package")
        children = self._items(report, "child_packages")
        if report.get("disposition") == "decomposed" and not children:
            raise PackageReportError("decomposed disposition requires child packages")
        if report.get("disposition") != "decomposed" and children:
            raise PackageReportError("child packages require the decomposed disposition")
        if children:
            self._validate_decomposition(package, children)
        return report

    def _expand_scope(self, package_id: str, generation: int, report: dict[str, Any]) -> bool:
        requests = self._items(report, "scope_expansion_requests")
        if not requests:
            return True
        specs = tuple(
            ReservationSpec(
                str(item["path"]),
                ReservationMode(str(item.get("mode", ReservationMode.EXCLUSIVE_FILE))),
            )
            for item in requests
        )
        package = self._current(package_id)
        result = self.database.expand_package_write_scope(
            package_id,
            generation,
            specs,
            expected_revision=package.revision,
            queue_on_conflict=True,
        )
        if not result.granted:
            owners = ", ".join(sorted({value.owner_id for value in result.conflicts}))
            raise PackageReservationWaiting(
                f"package scope expansion is queued behind path owner(s): {owners}"
            )
        return True

    def _apply_plan(self, package_id: str, generation: int, report: dict[str, Any]) -> None:
        raw_plan = report["plan_revision"]
        assert isinstance(raw_plan, dict)
        raw_steps = raw_plan.get("steps")
        if not isinstance(raw_steps, list):
            raise PackageReportError("package plan steps must be an array")
        next_revision = self._current(package_id).plan_revision + 1
        state = self.database.load_package_state()
        existing_steps = state.steps_for(package_id)
        reported_ids = tuple(
            str(value.get("step_id", "")) for value in raw_steps if isinstance(value, dict)
        )
        if len(set(reported_ids)) != len(reported_ids):
            raise PackageReportError("package plan repeats a step id")
        known_ids = {
            value.id for value in existing_steps if value.status is PackageStepStatus.COMPLETE
        } | set(reported_ids)
        reservations = tuple(
            value
            for value in self.database.load_package_state().reservations.values()
            if value.package_id == package_id and value.lease_generation == generation
        )

        def reserved(path: str) -> bool:
            return any(
                path == value.normalized_path
                or (
                    value.mode is ReservationMode.EXCLUSIVE_SUBTREE
                    and path.startswith(f"{value.normalized_path.rstrip('/')}/")
                )
                for value in reservations
            )

        replacement_steps: list[PackageStep] = []
        for raw in raw_steps:
            if not isinstance(raw, dict):
                raise PackageReportError("package plan step must be an object")
            step_id = str(raw.get("step_id", ""))
            dependencies = tuple(str(value) for value in raw.get("depends_on_step_ids", ()))
            paths = tuple(
                normalize_repository_path(value) for value in raw.get("intended_paths", ())
            )
            if not step_id or not set(dependencies).issubset(known_ids):
                raise PackageReportError(f"step {step_id or '<empty>'} has invalid dependencies")
            if not all(reserved(path) for path in paths):
                raise PackageReportError(f"step {step_id} names an unreserved path")
            existing = state.get_step(package_id, step_id)
            status = (
                existing.status
                if existing is not None and existing.status is PackageStepStatus.COMPLETE
                else PackageStepStatus.READY
                if not dependencies
                else PackageStepStatus.PENDING
            )
            replacement_steps.append(
                PackageStep(
                    id=step_id,
                    package_id=package_id,
                    objective=str(raw.get("objective", "")),
                    kind=PackageStepKind(str(raw.get("kind", "investigation"))),
                    status=status,
                    intended_declarations=tuple(
                        str(value) for value in raw.get("intended_declarations", ())
                    ),
                    intended_paths=paths,
                    depends_on_step_ids=dependencies,
                    commit_ids=existing.commit_ids if existing is not None else (),
                    validation_contract={
                        "commands": [str(value) for value in raw.get("validation_commands", ())]
                    },
                    remaining_gap=existing.remaining_gap if existing is not None else "",
                    plan_revision=next_revision,
                    created_at=existing.created_at if existing is not None else "",
                )
            )
        package = self._current(package_id)
        self.database.replace_package_plan(
            package_id,
            tuple(replacement_steps),
            plan_revision=next_revision,
            expected_revision=package.revision,
            lease_generation=generation,
        )

    async def _commit_overlay_edits(
        self,
        package: CapabilityPackage,
        generation: int,
        workspace: PackageWorkspace,
        *,
        message: str,
    ) -> PackageImport:
        reservations = tuple(
            value
            for value in self.database.load_package_state().reservations.values()
            if value.package_id == package.id and value.lease_generation == generation
        )
        if not reservations:
            raise PackageReportError(f"package {package.id} has no reserved source paths")
        self.database.assert_live_steward_lease(package.id, generation)
        return await workspace.integrate(
            tuple(value.normalized_path for value in reservations), message
        )

    def _apply_dependencies(self, package_id: str, generation: int, report: dict[str, Any]) -> bool:
        requested = self._items(report, "package_dependency_requests")
        for raw in requested:
            package = self._current(package_id)
            self.database.add_package_dependency(
                PackageDependency(
                    package_id,
                    str(raw["package_id"]),
                    str(raw["required_revision"])
                    if raw.get("required_revision") is not None
                    else None,
                ),
                expected_revision=package.revision,
                lease_generation=generation,
            )
        state = self.database.load_package_state()
        return any(
            (dependency := state.packages.get(edge.depends_on_package_id)) is None
            or dependency.status is not PackageStatus.COMPLETE
            or (
                edge.required_revision is not None
                and dependency.integrated_revision != edge.required_revision
            )
            for edge in state.dependencies_of(package_id)
        )

    async def _assess_steward_steps(
        self,
        package_id: str,
        generation: int,
        report: dict[str, Any],
        worktree: Path,
    ) -> None:
        for raw in self._items(report, "completed_step_assessments"):
            step_id = str(raw.get("step_id", ""))
            state = self.database.load_package_state()
            step = state.get_step(package_id, step_id)
            if step is None:
                raise PackageReportError(f"completed assessment names unknown step {step_id}")
            validation = await _await_validation(self.validate_step(worktree, step))
            accepted = bool(raw.get("accepted")) and validation.succeeded
            package = state.packages[package_id]
            self.database.upsert_package_step(
                replace(
                    step,
                    status=(PackageStepStatus.COMPLETE if accepted else PackageStepStatus.BLOCKED),
                    commit_ids=tuple(str(value) for value in raw.get("commit_ids", ())),
                    remaining_gap=str(raw.get("remaining_gap", "")),
                ),
                expected_revision=package.revision,
                lease_generation=generation,
            )
            self._append_evidence(
                package_id,
                generation,
                producer="package-validator",
                kind=EvidenceKind.VALIDATION,
                payload={
                    "step_id": step_id,
                    "model_assessment": raw,
                    "validation": validation.__dict__,
                },
                paths=step.intended_paths,
                declarations=step.intended_declarations,
            )

    def _apply_consumer_classifications(
        self, package_id: str, generation: int, report: dict[str, Any]
    ) -> None:
        state = self.database.load_package_state()
        package_ids = set(state.packages)
        consumers = {value.id: value for value in state.consumers_for(package_id)}
        for raw in self._items(report, "consumer_assessments"):
            consumer = consumers[str(raw["consumer_id"])]
            disposition = str(raw.get("disposition", "open"))
            if disposition in {"open", "accepted"}:
                # Acceptance is only granted by the configured consumer validator.
                continue
            if disposition == "detached":
                detached = raw.get("detached_package_id")
                if not isinstance(detached, str) or detached not in package_ids:
                    raise PackageReportError(
                        f"consumer {consumer.id} has no valid detached package"
                    )
                status = ConsumerStatus.DETACHED
            else:
                if not str(raw.get("acceptance_evidence", "")).strip():
                    raise PackageReportError(
                        f"terminal consumer {consumer.id} lacks exact evidence"
                    )
                detached = None
                status = ConsumerStatus.TERMINAL
            package = self._current(package_id)
            self.database.update_package_consumer(
                replace(
                    consumer,
                    status=status,
                    detached_package_id=detached,
                    residual_goal=str(raw.get("remaining_obstruction", "")),
                ),
                expected_revision=package.revision,
                lease_generation=generation,
            )

    def _ready_step(self, package_id: str, step_id: str) -> PackageStep:
        state = self.database.load_package_state()
        step = state.get_step(package_id, step_id)
        if step is None:
            raise PackageReportError(f"worker assignment names unknown step {step_id}")
        incomplete = []
        for dependency_id in step.depends_on_step_ids:
            dependency = state.get_step(package_id, dependency_id)
            if dependency is None or dependency.status is not PackageStepStatus.COMPLETE:
                incomplete.append(dependency_id)
        if incomplete:
            raise PackageReportError(
                f"worker step {step_id} has incomplete dependencies: {', '.join(incomplete)}"
            )
        return step

    async def _run_workers(
        self,
        package_id: str,
        generation: int,
        report: dict[str, Any],
        canonical_revision: str,
    ) -> tuple[tuple[str, ...], str]:
        assignments = self._items(report, "worker_assignments")
        if len(assignments) > self.maximum_worker_steps:
            raise PackageReportError("Steward exceeded the bounded worker-step limit")
        completed_workers: list[str] = []
        for assignment in assignments:
            worker_id = str(assignment.get("worker_id", ""))
            step = self._ready_step(package_id, str(assignment.get("step_id", "")))
            package = self._current(package_id)
            package = self.database.upsert_package_step(
                replace(
                    step,
                    status=PackageStepStatus.IMPLEMENTING,
                    assigned_worker_id=worker_id,
                ),
                expected_revision=package.revision,
                lease_generation=generation,
            )
            packet = {
                "package_id": package_id,
                "lease_generation": generation,
                "step": step.__dict__,
                "objective": str(assignment.get("objective", step.objective)),
                "completed_prerequisites": list(step.depends_on_step_ids),
                "known_evidence": [
                    item.__dict__
                    for item in self.database.load_package_state().evidence_for(package_id)
                ],
            }
            workspace = await self.acquire_workspace(package, generation, step.intended_paths)
            try:
                self.database.assert_live_steward_lease(package_id, generation)
                worker_report = await self.run_worker(package, step, packet, workspace.root)
                if str(worker_report.get("step_id", "")) != step.id:
                    raise PackageReportError("worker report names the wrong step")
                imported = await workspace.integrate(
                    step.intended_paths,
                    f"feat(packages): implement {step.objective}",
                )
                canonical_revision = imported.canonical_revision
            finally:
                await workspace.close()
            validation = await _await_validation(self.validate_step(self.repo, step))
            accepted = bool(worker_report.get("complete")) and validation.succeeded
            package = self._current(package_id)
            self.database.upsert_package_step(
                replace(
                    step,
                    status=(PackageStepStatus.COMPLETE if accepted else PackageStepStatus.BLOCKED),
                    assigned_worker_id=worker_id,
                    commit_ids=(
                        (*step.commit_ids, imported.commit) if imported.commit else step.commit_ids
                    ),
                    remaining_gap=str(worker_report.get("remaining_gap", "")),
                ),
                expected_revision=package.revision,
                lease_generation=generation,
            )
            self._append_evidence(
                package_id,
                generation,
                producer=worker_id,
                kind=EvidenceKind.WORKER_REPORT,
                payload={"report": worker_report, "validation": validation.__dict__},
                paths=imported.changed_paths,
                declarations=tuple(
                    str(value) for value in worker_report.get("changed_declarations", ())
                ),
            )
            completed_workers.append(worker_id)
        return tuple(completed_workers), canonical_revision

    async def _validate_consumers(
        self, package_id: str, generation: int, worktree: Path
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        accepted: list[str] = []
        affected: set[str] = set()
        state = self.database.load_package_state()
        for consumer in state.consumers_for(package_id):
            if consumer.status is not ConsumerStatus.OPEN:
                continue
            validation = await _await_consumer_validation(
                self.validate_consumer(worktree, consumer)
            )
            self._append_evidence(
                package_id,
                generation,
                producer="package-validator",
                kind=EvidenceKind.CONSUMER_ACCEPTANCE,
                payload=validation.__dict__,
                paths=(consumer.path,) if consumer.path else (),
                declarations=(consumer.declaration,) if consumer.declaration else (),
            )
            if not validation.succeeded:
                continue
            accepted.append(consumer.id)
            affected.update(validation.affected_work_unit_ids or (consumer.work_unit_id,))
        return tuple(accepted), tuple(sorted(affected))

    def _terminal_status(
        self, package_id: str, report: dict[str, Any], *, has_dependencies: bool
    ) -> tuple[PackageStatus, PackageDisposition | None]:
        disposition = str(report.get("disposition", "continue"))
        mapping = {
            "decomposed": (PackageStatus.DECOMPOSED, PackageDisposition.DECOMPOSED),
            "external": (PackageStatus.EXTERNAL, PackageDisposition.EXTERNAL),
            "statement_revision_required": (
                PackageStatus.STATEMENT_REVISION_REQUIRED,
                PackageDisposition.STATEMENT_REVISION,
            ),
            "parked": (PackageStatus.PARKED, PackageDisposition.PARKED),
        }
        if disposition in mapping:
            return mapping[disposition]
        if has_dependencies or disposition == "waiting_dependency":
            return PackageStatus.WAITING_DEPENDENCY, None
        state = self.database.load_package_state()
        steps = state.steps_for(package_id)
        consumers = state.consumers_for(package_id)
        if (
            disposition == "complete"
            and report.get("complete") is True
            and all(
                value.status in {PackageStepStatus.COMPLETE, PackageStepStatus.SUPERSEDED}
                for value in steps
            )
            and all(value.status is not ConsumerStatus.OPEN for value in consumers)
        ):
            return PackageStatus.COMPLETE, PackageDisposition.IMPLEMENTED
        return PackageStatus.IMPLEMENTING, None

    def _decompose(
        self, package_id: str, generation: int, report: dict[str, Any]
    ) -> tuple[CapabilityPackage, ...]:
        raw_children = self._items(report, "child_packages")
        state = self.database.load_package_state()
        self._validate_decomposition(state.packages[package_id], raw_children)
        open_consumers = {
            value.id
            for value in state.consumers_for(package_id)
            if value.status is ConsumerStatus.OPEN
        }
        children: list[CapabilityPackage] = []
        assignments: dict[str, tuple[str, ...]] = {}
        assigned: set[str] = set()
        for raw in raw_children:
            key = str(raw["capability_key"])
            child_id = f"package-{sha256(key.casefold().encode()).hexdigest()[:20]}"
            consumer_ids = tuple(str(value) for value in raw.get("consumer_ids", ()))
            if assigned.intersection(consumer_ids):
                raise PackageReportError("a consumer was assigned to multiple child packages")
            assigned.update(consumer_ids)
            scope = tuple(
                dict.fromkeys(
                    normalize_repository_path(str(value)) for value in raw.get("write_scope", ())
                )
            )
            children.append(
                CapabilityPackage(
                    child_id,
                    key,
                    str(raw["title"]),
                    str(raw["mathematical_objective"]),
                    write_scope=scope,
                    expansion_scope=scope,
                    parent_package_id=package_id,
                )
            )
            assignments[child_id] = consumer_ids
        if assigned != open_consumers:
            raise PackageReportError(
                "decomposition must assign every and only open package consumer"
            )
        package = state.packages[package_id]
        return self.database.split_capability_package(
            package_id,
            tuple(children),
            assignments,
            expected_revision=package.revision,
            lease_generation=generation,
        )

    async def _execute(self, package_id: str) -> PackageExecutionResult:
        package = self._current(package_id)
        if package.status is PackageStatus.WAITING_RESERVATION:
            self.database.wake_waiting_reservation_packages()
            package = self._current(package_id)
            if package.status is PackageStatus.WAITING_RESERVATION:
                return PackageExecutionResult(
                    package_id, 0, package.status, detail="path reservation remains queued"
                )
        if package.status in _TERMINAL_PACKAGE_STATUSES:
            return PackageExecutionResult(package_id, 0, package.status, detail="already terminal")
        lease, package = self._claim(package)
        generation = lease.generation
        heartbeat_stop = asyncio.Event()
        heartbeat = asyncio.create_task(
            self._heartbeat_lease(lease, heartbeat_stop),
            name=f"paf-package-heartbeat-{package_id}",
        )
        workspace: PackageWorkspace | None = None
        try:
            package = self._reserve_initial_scope(package, generation)
            workspace = await self.acquire_workspace(package, generation, package.write_scope)
            package = self.database.update_package_lifecycle(
                package_id,
                PackageStatus.INVESTIGATING,
                expected_revision=package.revision,
                lease_generation=generation,
            )
            report = self._validate_report(
                package,
                await self.run_steward(package, self._dossier(package, generation), workspace.root),
            )
            self._append_evidence(
                package_id,
                generation,
                producer=lease.agent_id,
                kind=EvidenceKind.STEWARD_REPORT,
                payload=report,
            )
            placement = report.get("placement_decision")
            assert isinstance(placement, dict)
            self._append_evidence(
                package_id,
                generation,
                producer=lease.agent_id,
                kind=EvidenceKind.PLACEMENT_DECISION,
                payload=placement,
                paths=tuple(str(value) for value in placement.get("paths", ())),
                declarations=tuple(str(value) for value in placement.get("declarations", ())),
            )
            self._expand_scope(package_id, generation, report)
            self._apply_plan(package_id, generation, report)
            package = self._current(package_id)
            steward_import = await self._commit_overlay_edits(
                package,
                generation,
                workspace,
                message=f"feat(packages): implement {package.title}",
            )
            await workspace.close()
            workspace = None
            canonical_revision = steward_import.canonical_revision
            if steward_import.changed_paths:
                self._append_evidence(
                    package_id,
                    generation,
                    producer=lease.agent_id,
                    kind=EvidenceKind.COMMIT,
                    payload={"commit_id": steward_import.commit, "owner": "Steward"},
                    paths=steward_import.changed_paths,
                )
            await self._assess_steward_steps(package_id, generation, report, self.repo)
            self._apply_consumer_classifications(package_id, generation, report)
            has_dependencies = self._apply_dependencies(package_id, generation, report)
            workers, canonical_revision = await self._run_workers(
                package_id, generation, report, canonical_revision
            )
            package = self._current(package_id)
            package = self.database.update_package_lifecycle(
                package_id,
                PackageStatus.VALIDATING,
                expected_revision=package.revision,
                lease_generation=generation,
            )
            package_validation = await _await_validation(self.validate_package(self.repo, package))
            self._append_evidence(
                package_id,
                generation,
                producer="package-validator",
                kind=EvidenceKind.VALIDATION,
                payload=package_validation.__dict__,
            )
            if not package_validation.succeeded:
                current = self._current(package_id)
                failed = self.database.update_package_lifecycle(
                    package_id,
                    PackageStatus.IMPLEMENTING,
                    expected_revision=current.revision,
                    integrated_revision=canonical_revision,
                    lease_generation=generation,
                )
                return PackageExecutionResult(
                    package_id,
                    generation,
                    failed.status,
                    canonical_revision,
                    worker_ids=workers,
                    detail=package_validation.evidence,
                )
            interface_changes = self.interfaces.check(package_id, self.interface_digest)
            if interface_changes:
                current = self._current(package_id)
                stale = self.database.update_package_lifecycle(
                    package_id,
                    PackageStatus.INVESTIGATING,
                    expected_revision=current.revision,
                    integrated_revision=canonical_revision,
                    lease_generation=generation,
                )
                detail = "relevant read interface changed during validation: " + ", ".join(
                    change.interface_id for change in interface_changes
                )
                return PackageExecutionResult(
                    package_id,
                    generation,
                    stale.status,
                    canonical_revision,
                    worker_ids=workers,
                    detail=detail,
                )
            accepted, affected = await self._validate_consumers(package_id, generation, self.repo)
            for consumer_id in accepted:
                state = self.database.load_package_state()
                consumer = state.consumers[consumer_id]
                self.database.update_package_consumer(
                    replace(
                        consumer,
                        status=ConsumerStatus.ACCEPTED,
                        accepted_revision=canonical_revision,
                        residual_goal="",
                    ),
                    expected_revision=state.packages[package_id].revision,
                    lease_generation=generation,
                )
            if report.get("disposition") == "decomposed":
                self._decompose(package_id, generation, report)
                if affected and self.wake_consumers is not None:
                    await self.wake_consumers(affected)
                return PackageExecutionResult(
                    package_id,
                    generation,
                    PackageStatus.DECOMPOSED,
                    canonical_revision,
                    workers,
                    accepted,
                    affected,
                    "remaining consumers transferred to child packages",
                )
            status, disposition = self._terminal_status(
                package_id, report, has_dependencies=has_dependencies
            )
            current = self._current(package_id)
            final = self.database.update_package_lifecycle(
                package_id,
                status,
                disposition=disposition,
                expected_revision=current.revision,
                integrated_revision=canonical_revision,
                lease_generation=generation,
            )
            if affected and self.wake_consumers is not None:
                await self.wake_consumers(affected)
            return PackageExecutionResult(
                package_id,
                generation,
                final.status,
                canonical_revision,
                workers,
                accepted,
                affected,
            )
        except PackageReservationWaiting as error:
            self._append_evidence(
                package_id,
                generation,
                producer=lease.agent_id,
                kind=EvidenceKind.DIAGNOSTIC,
                payload={"waiting_on_reservation": str(error)},
            )
            current = self._current(package_id)
            waiting = self.database.update_package_lifecycle(
                package_id,
                PackageStatus.WAITING_RESERVATION,
                expected_revision=current.revision,
                lease_generation=generation,
            )
            return PackageExecutionResult(package_id, generation, waiting.status, detail=str(error))
        except PackageReportError as error:
            # Freeze all edits and retain ownership. A later fenced Steward sees the evidence.
            self._append_evidence(
                package_id,
                generation,
                producer=lease.agent_id,
                kind=EvidenceKind.DIAGNOSTIC,
                payload={"invalid_report_or_paths": str(error)},
            )
            current = self._current(package_id)
            parked = self.database.update_package_lifecycle(
                package_id,
                PackageStatus.PARKED,
                disposition=PackageDisposition.PARKED,
                expected_revision=current.revision,
                lease_generation=generation,
            )
            return PackageExecutionResult(package_id, generation, parked.status, detail=str(error))
        finally:
            if workspace is not None:
                await workspace.close()
            heartbeat_stop.set()
            await asyncio.gather(heartbeat, return_exceptions=True)
            release_reservations = self._current(package_id).status in _TERMINAL_PACKAGE_STATUSES
            with suppress(ValueError):
                self.database.release_steward_lease(
                    package_id,
                    lease.agent_id,
                    generation,
                    release_reservations=release_reservations,
                )
