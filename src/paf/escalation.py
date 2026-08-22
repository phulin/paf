from __future__ import annotations

import re
from collections import defaultdict
from enum import StrEnum
from itertools import islice
from pathlib import Path
from typing import Any

from paf.hashing import stable_digest_text
from paf.models import EscalationSettings, Stage
from paf.state import RunRecord, StateStore, TaskStatus, UpstreamRequestStatus


class CoordinationSignalKind(StrEnum):
    UPSTREAM_REQUEST = "upstream_request"
    SOURCE_ISSUE = "source_issue"
    PERSISTENT_FAILURE = "persistent_failure"


_VOLATILE_TEXT = re.compile(
    r"(?:[0-9a-f]{12,}|(?:run|thread|generation)[-_: ]+[A-Za-z0-9._:-]+|/tmp/[^\s,;]+)",
    re.IGNORECASE,
)
_SPACE = re.compile(r"\s+")


def _normalized(value: object, *, maximum: int = 2000) -> str:
    text = _SPACE.sub(" ", str(value or "")).strip().casefold()
    return _VOLATILE_TEXT.sub("<volatile>", text)[:maximum]


def _bounded_text(value: object, *, maximum: int = 4000) -> str:
    text = str(value or "").strip()
    if len(text) <= maximum:
        return text
    head = maximum // 2
    tail = maximum - head
    return f"{text[:head]}\n... <bounded evidence> ...\n{text[-tail:]}"


def _bounded_strings(values: object, *, maximum_items: int = 16) -> list[str]:
    if not isinstance(values, (list, tuple)):
        return []
    return [_bounded_text(value, maximum=1000) for value in values[:maximum_items]]


def _signal_id(kind: CoordinationSignalKind, identity: str) -> str:
    return f"signal-{kind.value}-{stable_digest_text(identity)[:16]}"


def _case_id(kind: str, group_key: str) -> str:
    return f"case-{kind}-{stable_digest_text(group_key)[:16]}"


def _run_failure_detail(state: StateStore, run: RunRecord) -> str:
    state.load_run_details(run)
    if run.error:
        return run.error
    for value in (run.isolation, run.validation, run.report):
        if not isinstance(value, dict):
            continue
        for key in ("error", "summary", "output"):
            if detail := str(value.get(key, "")).strip():
                return detail[-4000:]
        issues = value.get("issues")
        if isinstance(issues, list) and issues:
            return "\n".join(map(str, issues))[-4000:]
    return ""


def _upstream_signals(state: StateStore) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    live_statuses = {
        UpstreamRequestStatus.OPEN.value,
        UpstreamRequestStatus.EVALUATING.value,
    }
    for request_id, request in sorted(state.upstream_requests.items()):
        if request.get("status") not in live_statuses:
            continue
        capability = _normalized(request.get("capability_key") or request.get("needed_result"))
        owner_paths = sorted(_normalized(value) for value in request.get("owner_paths", ()))
        group_key = "upstream\0" + (capability or "\0".join(owner_paths) or request_id)
        payload = {
            "consumer_chapter_id": _bounded_text(request.get("consumer_chapter_id")),
            "consumer_path": _bounded_text(request.get("consumer_path")),
            "blocked_declaration": _bounded_text(request.get("blocked_declaration")),
            "residual_goal": _bounded_text(request.get("residual_goal")),
            "needed_result": _bounded_text(request.get("needed_result")),
            "capability_key": _bounded_text(request.get("capability_key")),
            "owner_paths": _bounded_strings(request.get("owner_paths")),
            "attempted_alternatives": _bounded_strings(request.get("attempted_alternatives")),
            "acceptance_tests": _bounded_strings(request.get("acceptance_tests")),
            "blocker_ids": _bounded_strings(request.get("blocker_ids")),
            "origin_run_ids": _bounded_strings(request.get("origin_run_ids")),
        }
        evidence_digest = stable_digest_text(repr(sorted(payload.items())))[:20]
        signals.append(
            {
                "id": _signal_id(CoordinationSignalKind.UPSTREAM_REQUEST, request_id),
                "kind": CoordinationSignalKind.UPSTREAM_REQUEST.value,
                "group_key": group_key,
                "subject_ids": [request_id],
                "work_unit_ids": [str(request.get("consumer_chapter_id", ""))],
                "evidence_digest": evidence_digest,
                "evidence": payload,
                "severity": "normal",
            }
        )
    return signals


def _source_issue_signals(
    state: StateStore,
    settings: EscalationSettings,
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    for issue_id, issue in sorted(state.source_issues.items()):
        if issue.status != "open" or issue.sightings < settings.source_issue_sighting_threshold:
            continue
        location_key = _normalized(issue.location) or _normalized(issue.source_excerpt)
        group_key = f"source\0{issue.source}\0{location_key}"
        source_path = Path(issue.source)
        if not source_path.is_absolute():
            source_path = state.config.settings.repo / source_path
        try:
            source_text = source_path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            source_text = ""
            source_check = "unavailable"
        else:
            excerpt = issue.source_excerpt.strip()
            if excerpt and excerpt in source_text:
                source_check = "exact"
            elif excerpt and _SPACE.sub(" ", excerpt).strip() in _SPACE.sub(" ", source_text):
                source_check = "normalized"
            else:
                source_check = "missing"
        evidence = {
            "source": issue.source,
            "source_digest": stable_digest_text(source_text)[:20] if source_text else "",
            "source_excerpt_check": source_check,
            "location": _bounded_text(issue.location),
            "source_excerpt": _bounded_text(issue.source_excerpt),
            "description": _bounded_text(issue.description),
            "suggested_correction": _bounded_text(issue.suggested_correction),
            "sightings": issue.sightings,
            "stages": list(issue.stages),
            "run_ids": list(issue.run_ids[-settings.recent_trace_runs :]),
        }
        signals.append(
            {
                "id": _signal_id(CoordinationSignalKind.SOURCE_ISSUE, issue_id),
                "kind": CoordinationSignalKind.SOURCE_ISSUE.value,
                "group_key": group_key,
                "subject_ids": [issue_id],
                "work_unit_ids": [issue.chapter_id],
                "evidence_digest": stable_digest_text(repr(evidence))[:20],
                "evidence": evidence,
                "severity": (
                    "high"
                    if issue.sightings >= 2 * settings.source_issue_sighting_threshold
                    else "normal"
                ),
            }
        )
    return signals


def _persistent_failure_signals(
    state: StateStore,
    settings: EscalationSettings,
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    failed_statuses = {TaskStatus.FAILED, TaskStatus.BLOCKED, TaskStatus.INTERRUPTED}
    for task in state.tasks.values():
        recent = list(
            islice(
                (
                    run
                    for run in reversed(state.chapter_runs(task.chapter_id))
                    if not run.auxiliary and run.stage == task.stage
                ),
                max(settings.recent_trace_runs, settings.persistent_failure_threshold),
            )
        )
        if len(recent) < settings.persistent_failure_threshold:
            continue
        selected = recent[: settings.persistent_failure_threshold]
        if any(run.status not in failed_statuses for run in selected):
            continue
        details = [_normalized(_run_failure_detail(state, run) or task.detail) for run in selected]
        signature = next((detail for detail in details if detail), _normalized(task.detail))
        if not signature or any(detail != signature for detail in details):
            continue
        identity = f"{task.chapter_id}\0{task.stage}\0{signature}"
        evidence = {
            "stage": str(task.stage),
            "task_status": str(task.status),
            "task_detail": _bounded_text(task.detail),
            "failure_signature": signature,
            "run_ids": [run.id for run in selected],
            "roles": list(dict.fromkeys(run.role for run in selected)),
        }
        signals.append(
            {
                "id": _signal_id(CoordinationSignalKind.PERSISTENT_FAILURE, identity),
                "kind": CoordinationSignalKind.PERSISTENT_FAILURE.value,
                "group_key": f"failure\0{task.chapter_id}\0{task.stage}\0{signature}",
                "subject_ids": [state.key(task.chapter_id, Stage(task.stage))],
                "work_unit_ids": [task.chapter_id],
                "evidence_digest": stable_digest_text(repr(evidence))[:20],
                "evidence": evidence,
                "severity": "high",
            }
        )
    return signals


def collect_coordination_signals(
    state: StateStore,
    settings: EscalationSettings,
) -> tuple[dict[str, Any], ...]:
    """Convert durable exceptional evidence into stable, bounded signal records."""

    if not settings.enabled:
        return ()
    return tuple(
        (
            *_upstream_signals(state),
            *_source_issue_signals(state, settings),
            *_persistent_failure_signals(state, settings),
        )
    )


def coordination_case_proposals(
    signals: tuple[dict[str, Any], ...] | list[dict[str, Any]],
) -> tuple[dict[str, Any], ...]:
    """Deterministically reduce related signals before any model sees a case."""

    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for signal in signals:
        kind = str(signal.get("kind", ""))
        group_key = str(signal.get("group_key", ""))
        if kind and group_key:
            groups[(kind, group_key)].append(signal)
    return tuple(
        {
            "id": _case_id(kind, group_key),
            "kind": kind,
            "group_key": group_key,
            "evidence_digest": stable_digest_text(
                "\0".join(
                    f"{signal['id']}:{signal.get('evidence_digest', '')}"
                    for signal in sorted(values, key=lambda value: str(value["id"]))
                )
            )[:20],
            "signal_ids": [str(signal["id"]) for signal in values],
            "work_unit_ids": list(
                dict.fromkeys(
                    str(work_unit_id)
                    for signal in values
                    for work_unit_id in signal.get("work_unit_ids", ())
                    if str(work_unit_id)
                )
            ),
            "severity": (
                "high" if any(signal.get("severity") == "high" for signal in values) else "normal"
            ),
        }
        for (kind, group_key), values in sorted(groups.items())
    )
