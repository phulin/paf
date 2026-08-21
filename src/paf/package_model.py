from __future__ import annotations

import os
import re
import unicodedata
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Any


class PackageStatus(StrEnum):
    OBSERVED = "observed"
    INVESTIGATING = "investigating"
    PLANNED = "planned"
    IMPLEMENTING = "implementing"
    VALIDATING = "validating"
    INTEGRATING = "integrating"
    COMPLETE = "complete"
    WAITING_DEPENDENCY = "waiting_dependency"
    WAITING_RESERVATION = "waiting_reservation"
    DECOMPOSED = "decomposed"
    EXTERNAL = "external"
    STATEMENT_REVISION_REQUIRED = "statement_revision_required"
    PARKED = "parked"
    SUPERSEDED = "superseded"


class PackageDisposition(StrEnum):
    IMPLEMENTED = "implemented"
    DECOMPOSED = "decomposed"
    EXTERNAL = "external"
    STATEMENT_REVISION = "statement_revision"
    PARKED = "parked"
    SUPERSEDED = "superseded"


class ConsumerStatus(StrEnum):
    OPEN = "open"
    ACCEPTED = "accepted"
    DETACHED = "detached"
    TERMINAL = "terminal"


class PackageStepKind(StrEnum):
    INVESTIGATION = "investigation"
    INTERFACE = "interface"
    SUPPORTING_LEMMA = "supporting_lemma"
    CONSUMER_INTEGRATION = "consumer_integration"
    STATEMENT_REVISION = "statement_revision"
    VALIDATION = "validation"


class PackageStepStatus(StrEnum):
    PENDING = "pending"
    READY = "ready"
    ASSIGNED = "assigned"
    IMPLEMENTING = "implementing"
    VALIDATING = "validating"
    COMPLETE = "complete"
    BLOCKED = "blocked"
    SUPERSEDED = "superseded"


class EvidenceKind(StrEnum):
    RESIDUAL_GOAL = "residual_goal"
    DIAGNOSTIC = "diagnostic"
    FAILED_APPROACH = "failed_approach"
    DECLARATION_SEARCH = "declaration_search"
    REVIEW_FINDING = "review_finding"
    LEAN_PROBE = "lean_probe"
    VALIDATION = "validation"
    COMMIT = "commit"
    EXTERNAL_DEPENDENCY = "external_dependency"
    MIGRATION = "migration"
    STEWARD_REPORT = "steward_report"
    WORKER_REPORT = "worker_report"
    PLACEMENT_DECISION = "placement_decision"
    CONSUMER_ACCEPTANCE = "consumer_acceptance"
    LEASE_RECOVERY = "lease_recovery"
    OPERATOR_DECISION = "operator_decision"


class ReservationMode(StrEnum):
    EXCLUSIVE_FILE = "exclusive_file"
    EXCLUSIVE_SUBTREE = "exclusive_subtree"


class ReservationOwnerKind(StrEnum):
    PACKAGE = "package"
    ORDINARY_TASK = "ordinary_task"


class ReservationDecision(StrEnum):
    GRANTED = "granted"
    QUEUED = "queued"
    CONFLICT = "conflict"


class IntegrationPhase(StrEnum):
    PREPARED = "prepared"
    VALIDATING = "validating"
    VALIDATED = "validated"
    IMPORTING = "importing"
    FINALIZED = "finalized"
    ABORTED = "aborted"


def normalize_capability_key(value: str) -> str:
    """Return the stable comparison form used by capability ownership."""

    normalized = unicodedata.normalize("NFKC", value)
    return re.sub(r"\s+", " ", normalized).strip().casefold()


def normalize_repository_path(value: str | Path) -> str:
    """Normalize a repository-relative path without consulting a checkout."""

    raw = str(value).replace("\\", "/").strip()
    path = PurePosixPath(raw)
    if not raw or path.is_absolute() or ".." in path.parts:
        raise ValueError(f"package path must be repository-relative: {value}")
    normalized = path.as_posix()
    return os.path.normcase(normalized)


@dataclass(frozen=True)
class CapabilityPackage:
    id: str
    capability_key: str
    title: str
    mathematical_objective: str
    status: PackageStatus = PackageStatus.OBSERVED
    disposition: PackageDisposition | None = None
    aliases: tuple[str, ...] = ()
    textbook_refs: tuple[str, ...] = ()
    write_scope: tuple[str, ...] = ()
    expansion_scope: tuple[str, ...] = ()
    base_revision: str = ""
    branch: str = ""
    worktree: str = ""
    parent_package_id: str | None = None
    plan_revision: int = 0
    integrated_revision: str | None = None
    revision: int = 0
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self) -> None:
        if not self.id:
            raise ValueError("package id must not be empty")
        normalized = normalize_capability_key(self.capability_key)
        if not normalized:
            raise ValueError("capability key must not be empty")
        object.__setattr__(self, "capability_key", normalized)
        object.__setattr__(
            self,
            "aliases",
            tuple(
                dict.fromkeys(normalize_capability_key(value) for value in self.aliases if value)
            ),
        )
        object.__setattr__(
            self,
            "write_scope",
            tuple(dict.fromkeys(normalize_repository_path(value) for value in self.write_scope)),
        )
        object.__setattr__(
            self,
            "expansion_scope",
            tuple(
                dict.fromkeys(normalize_repository_path(value) for value in self.expansion_scope)
            ),
        )


@dataclass(frozen=True)
class PackageConsumer:
    id: str
    package_id: str
    work_unit_id: str
    path: str
    declaration: str
    stage: str
    residual_goal: str = ""
    source_digest: str | None = None
    blocker_ids: tuple[str, ...] = ()
    attempted_routes: tuple[str, ...] = ()
    acceptance_contract: dict[str, Any] = field(default_factory=dict)
    status: ConsumerStatus = ConsumerStatus.OPEN
    accepted_revision: str | None = None
    detached_package_id: str | None = None
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self) -> None:
        if not self.id or not self.package_id:
            raise ValueError("consumer and package ids must not be empty")
        if self.path:
            object.__setattr__(self, "path", normalize_repository_path(self.path))


@dataclass(frozen=True)
class PackageStep:
    id: str
    package_id: str
    objective: str
    kind: PackageStepKind
    status: PackageStepStatus = PackageStepStatus.PENDING
    intended_declarations: tuple[str, ...] = ()
    intended_paths: tuple[str, ...] = ()
    depends_on_step_ids: tuple[str, ...] = ()
    assigned_worker_id: str | None = None
    commit_ids: tuple[str, ...] = ()
    validation_contract: dict[str, Any] = field(default_factory=dict)
    remaining_gap: str = ""
    plan_revision: int = 0
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "intended_paths",
            tuple(dict.fromkeys(normalize_repository_path(value) for value in self.intended_paths)),
        )


@dataclass(frozen=True)
class PackageEvidence:
    id: str
    package_id: str
    producer: str
    kind: EvidenceKind
    source_revision: str = ""
    paths: tuple[str, ...] = ()
    declarations: tuple[str, ...] = ()
    payload: dict[str, Any] = field(default_factory=dict)
    digest: str = ""
    created_at: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "paths",
            tuple(dict.fromkeys(normalize_repository_path(value) for value in self.paths)),
        )


@dataclass(frozen=True)
class StewardLease:
    package_id: str
    agent_id: str
    generation: int
    acquired_at: str
    heartbeat_at: str
    expires_at: str


@dataclass(frozen=True)
class PathReservation:
    normalized_path: str
    mode: ReservationMode
    package_id: str
    lease_generation: int
    acquired_at: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "normalized_path", normalize_repository_path(self.normalized_path))


@dataclass(frozen=True)
class ReservationSpec:
    normalized_path: str
    mode: ReservationMode = ReservationMode.EXCLUSIVE_FILE

    def __post_init__(self) -> None:
        object.__setattr__(self, "normalized_path", normalize_repository_path(self.normalized_path))


def canonical_reservation_specs(values: tuple[ReservationSpec, ...]) -> tuple[ReservationSpec, ...]:
    """Sort, deduplicate, and remove paths covered by a requested subtree."""

    by_path: dict[str, ReservationSpec] = {}
    for value in values:
        current = by_path.get(value.normalized_path)
        if current is None or value.mode is ReservationMode.EXCLUSIVE_SUBTREE:
            by_path[value.normalized_path] = value
    ordered = tuple(sorted(by_path.values(), key=lambda item: item.normalized_path))
    return tuple(
        item
        for item in ordered
        if not any(
            other.mode is ReservationMode.EXCLUSIVE_SUBTREE
            and item.normalized_path.startswith(f"{other.normalized_path}/")
            for other in ordered
        )
    )


@dataclass(frozen=True)
class GlobalPathReservation:
    normalized_path: str
    mode: ReservationMode
    owner_kind: ReservationOwnerKind
    owner_id: str
    fence_generation: int
    acquired_at: str
    expires_at: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "normalized_path", normalize_repository_path(self.normalized_path))


@dataclass(frozen=True)
class ReservationConflict:
    requested_path: str
    requested_mode: ReservationMode
    held_path: str
    held_mode: ReservationMode
    owner_kind: ReservationOwnerKind
    owner_id: str


@dataclass(frozen=True)
class ReservationResult:
    decision: ReservationDecision
    owner_kind: ReservationOwnerKind
    owner_id: str
    fence_generation: int
    requested: tuple[ReservationSpec, ...]
    conflicts: tuple[ReservationConflict, ...] = ()
    queue_id: str | None = None

    @property
    def granted(self) -> bool:
        return self.decision is ReservationDecision.GRANTED


@dataclass(frozen=True)
class PackageRecovery:
    package_id: str
    prior_generation: int
    recovered_generation: int
    worktree_head: str
    worktree_status: str
    dirty_digest: str
    active_child_workers: tuple[str, ...] = ()
    journal_phase: IntegrationPhase | None = None
    recovered_at: str = ""


@dataclass(frozen=True)
class PackageDependency:
    package_id: str
    depends_on_package_id: str
    required_revision: str | None = None
    created_at: str = ""


@dataclass(frozen=True)
class RelevantReadInterface:
    package_id: str
    interface_id: str
    digest: str
    source_revision: str = ""


@dataclass(frozen=True)
class IntegrationJournal:
    id: str
    package_id: str
    lease_generation: int
    base_revision: str
    candidate_revision: str
    canonical_revision_before: str
    phase: IntegrationPhase
    validation_digest: str = ""
    canonical_revision_after: str | None = None
    provisional_consumer_ids: tuple[str, ...] = ()
    created_at: str = ""
    updated_at: str = ""


@dataclass(frozen=True)
class PackageState:
    packages: dict[str, CapabilityPackage] = field(default_factory=dict)
    consumers: dict[str, PackageConsumer] = field(default_factory=dict)
    steps: dict[str, PackageStep] = field(default_factory=dict)
    evidence: dict[str, PackageEvidence] = field(default_factory=dict)
    leases: dict[str, StewardLease] = field(default_factory=dict)
    reservations: dict[str, PathReservation] = field(default_factory=dict)
    dependencies: tuple[PackageDependency, ...] = ()
    relevant_read_interfaces: tuple[RelevantReadInterface, ...] = ()
    integration_journal: dict[str, IntegrationJournal] = field(default_factory=dict)

    def package_for_capability(self, capability_key: str) -> CapabilityPackage | None:
        key = normalize_capability_key(capability_key)
        return next(
            (
                package
                for package in self.packages.values()
                if package.status is not PackageStatus.SUPERSEDED
                and (package.capability_key == key or key in package.aliases)
            ),
            None,
        )

    def consumers_for(self, package_id: str) -> tuple[PackageConsumer, ...]:
        return tuple(value for value in self.consumers.values() if value.package_id == package_id)

    def steps_for(self, package_id: str) -> tuple[PackageStep, ...]:
        return tuple(value for value in self.steps.values() if value.package_id == package_id)

    def evidence_for(self, package_id: str) -> tuple[PackageEvidence, ...]:
        return tuple(value for value in self.evidence.values() if value.package_id == package_id)

    def children_of(self, package_id: str) -> tuple[CapabilityPackage, ...]:
        return tuple(
            value for value in self.packages.values() if value.parent_package_id == package_id
        )

    def dependencies_of(self, package_id: str) -> tuple[PackageDependency, ...]:
        return tuple(value for value in self.dependencies if value.package_id == package_id)

    def as_dict(self) -> dict[str, Any]:
        def records(values: dict[str, Any]) -> dict[str, Any]:
            return {key: asdict(value) for key, value in sorted(values.items())}

        return {
            "capability_packages": records(self.packages),
            "package_consumers": records(self.consumers),
            "package_steps": records(self.steps),
            "package_evidence": records(self.evidence),
            "steward_leases": records(self.leases),
            "path_reservations": records(self.reservations),
            "package_dependencies": [asdict(value) for value in self.dependencies],
            "relevant_read_interfaces": [asdict(value) for value in self.relevant_read_interfaces],
            "integration_journal": records(self.integration_journal),
        }


PACKAGE_SNAPSHOT_KEYS = frozenset(PackageState().as_dict())
