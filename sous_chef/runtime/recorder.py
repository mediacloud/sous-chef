"""
Runtime recording for recipe execution (wall + monotonic timing, semantic mark_step).

Used by kitchen and local runners. Flow code may call mark_step() inside an
active runtime_session; outside a session, mark_step is a no-op.
"""

from __future__ import annotations

import json
import sys
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

DEFAULT_MAX_EVENTS = 2000

_current: ContextVar[Optional["RuntimeRecorder"]] = ContextVar(
    "sous_chef_runtime_recorder", default=None
)


@dataclass
class _Event:
    sequence: int
    event_kind: str
    name: str
    offset_ms: float
    meta: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None


class RuntimeRecorder:
    """Collects recipe_start, mark_step, and recipe_end events for one run."""

    def __init__(
        self,
        recipe_name: str,
        run_id: Optional[str] = None,
        max_events: int = DEFAULT_MAX_EVENTS,
    ):
        self.recipe_name = recipe_name
        self.run_id = run_id
        self.max_events = max_events
        self._t0 = time.monotonic()
        self._seq = 0
        self._events: List[_Event] = []
        self.session_started_wall_iso = datetime.now(timezone.utc).isoformat()
        self.session_finished_wall_iso: Optional[str] = None
        self.total_duration_ms: Optional[float] = None
        self.final_status: Optional[str] = None

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _offset_ms(self) -> float:
        return (time.monotonic() - self._t0) * 1000.0

    def _at_capacity(self) -> bool:
        return len(self._events) >= self.max_events

    def _append(
        self,
        kind: str,
        name: str = "",
        meta: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
    ) -> None:
        if self._at_capacity():
            return
        self._events.append(
            _Event(
                sequence=self._next_seq(),
                event_kind=kind,
                name=name,
                offset_ms=self._offset_ms(),
                meta=meta,
                error_message=error_message,
            )
        )

    def record_recipe_start(self) -> None:
        self._append("recipe_start", name="")

    def mark_step(self, name: str, meta: Optional[Dict[str, Any]] = None) -> None:
        if self._at_capacity():
            return
        self._append("mark_step", name=name, meta=meta)

    def record_recipe_end(
        self,
        *,
        status: str,
        error_message: Optional[str] = None,
    ) -> None:
        self.session_finished_wall_iso = datetime.now(timezone.utc).isoformat()
        self.total_duration_ms = self._offset_ms()
        self.final_status = status
        self._append(
            "recipe_end",
            name="",
            error_message=error_message,
        )

    def to_timeline_artifact(self) -> "RuntimeTimelineArtifact":
        from sous_chef.artifacts.runtime_timeline import RuntimeTimelineArtifact

        rows: List[Dict[str, Any]] = []
        for e in self._events:
            meta_json: Optional[str] = None
            if e.meta:
                meta_json = json.dumps(e.meta, default=str)
            rows.append(
                {
                    "recipe_name": self.recipe_name,
                    "run_id": self.run_id,
                    "session_started_wall_iso": self.session_started_wall_iso,
                    "session_finished_wall_iso": self.session_finished_wall_iso,
                    "total_duration_ms": self.total_duration_ms,
                    "final_status": self.final_status,
                    "sequence": e.sequence,
                    "event_kind": e.event_kind,
                    "name": e.name,
                    "offset_ms": round(e.offset_ms, 3),
                    "meta_json": meta_json,
                    "error_message": e.error_message,
                }
            )
        return RuntimeTimelineArtifact(
            recipe_name=self.recipe_name,
            run_id=self.run_id,
            session_started_wall_iso=self.session_started_wall_iso,
            session_finished_wall_iso=self.session_finished_wall_iso,
            total_duration_ms=self.total_duration_ms,
            final_status=self.final_status,
            rows=rows,
        )


def mark_step(name: str, meta: Optional[Dict[str, Any]] = None) -> None:
    rec = _current.get()
    if rec is not None:
        rec.mark_step(name, meta)


@contextmanager
def runtime_session(
    recipe_name: str,
    run_id: Optional[str] = None,
    *,
    max_events: int = DEFAULT_MAX_EVENTS,
    enabled: bool = True,
) -> Iterator[RuntimeRecorder]:
    if not enabled:
        # No context var: mark_step stays no-op. Caller should skip artifact emission.
        yield RuntimeRecorder(recipe_name, run_id=run_id, max_events=max_events)
        return

    rec = RuntimeRecorder(recipe_name, run_id=run_id, max_events=max_events)
    token = _current.set(rec)
    try:
        rec.record_recipe_start()
        yield rec
    finally:
        err = sys.exc_info()[1]
        if err is not None:
            rec.record_recipe_end(status="error", error_message=str(err))
        else:
            rec.record_recipe_end(status="ok")
        _current.reset(token)
