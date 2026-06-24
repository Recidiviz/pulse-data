# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""The shared run loop for eOMIS writeback flows.

Everything every writeback needs to be a safe unattended job lives here:
login, the candidate loop, pacing, the volume circuit breaker, the
consecutive-failure stop, and attempt-then-result audit recording. Flows
inherit all of it for free.
"""
from __future__ import annotations

import csv
import logging
import random
import time
from abc import ABC, abstractmethod
from collections import Counter
from types import TracebackType
from typing import Sequence

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from recidiviz.tools.eomis.client import EomisClient
from recidiviz.tools.eomis.flow import (
    READ_ACTION,
    Candidate,
    CandidateT,
    EomisWritebackFlow,
    ResultStatus,
    WriteResult,
)

MAX_CONSECUTIVE_FAILURES = 3

AUDIT_FIELDNAMES = ["offender_id", "action", "result", "detail"]


class AuditRecorder(ABC):
    """Receives every attempt before it runs and every result after, so a
    crash mid-write still leaves a record of what was being attempted."""

    @abstractmethod
    def record_attempt(self, candidate: Candidate) -> None:
        ...

    @abstractmethod
    def record_result(self, result: WriteResult) -> None:
        ...


class CsvAuditRecorder(AuditRecorder):
    """Appends attempt/result rows to a local CSV, flushing after every row so
    an interrupted run still leaves a complete record."""

    def __init__(self, path: str) -> None:
        self.path = path
        # pylint: disable-next=consider-using-with
        self._file = open(path, "w", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._file, fieldnames=AUDIT_FIELDNAMES)
        self._writer.writeheader()
        self._file.flush()

    def record_attempt(self, candidate: Candidate) -> None:
        self._write_row(candidate, "attempt", candidate.reason)

    def record_result(self, result: WriteResult) -> None:
        self._write_row(result.candidate, result.status.value, result.detail)

    def _write_row(self, candidate: Candidate, result: str, detail: str) -> None:
        self._writer.writerow(
            {
                "offender_id": candidate.offender_id,
                "action": candidate.action,
                "result": result,
                "detail": detail,
            }
        )
        self._file.flush()

    def __enter__(self) -> "CsvAuditRecorder":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._file.close()


def run_writeback(
    *,
    flow: EomisWritebackFlow[CandidateT],
    client: EomisClient,
    candidates: Sequence[CandidateT],
    commit: bool,
    recorders: Sequence[AuditRecorder],
    max_writes: int,
    pause_min: float,
    pause_max: float,
) -> list[WriteResult]:
    """Runs every candidate through the flow with the shared production
    guardrails. Candidates must already be filtered to actionable ones."""
    if commit and len(candidates) > max_writes:
        raise RuntimeError(
            f"{flow.flow_name}: refusing to write {len(candidates)} rows in one "
            f"run (max {max_writes}). If the volume is expected, re-run with an "
            "explicit higher --max-writes."
        )

    client.login()

    results: list[WriteResult] = []
    consecutive_failures = 0
    for index, candidate in enumerate(candidates):
        for recorder in recorders:
            recorder.record_attempt(candidate)
        try:
            result = flow.run_candidate(client, candidate, commit=commit)
            consecutive_failures = 0
        except Exception as error:  # pylint: disable=broad-except
            consecutive_failures += 1
            result = WriteResult(candidate, ResultStatus.ERROR, str(error))
        results.append(result)
        for recorder in recorders:
            recorder.record_result(result)
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            logging.warning(
                "Stopping after %d consecutive failures.", consecutive_failures
            )
            break
        if index < len(candidates) - 1:
            time.sleep(random.uniform(pause_min, pause_max))
    return results


def render_plan(
    console: Console,
    candidates: Sequence[Candidate],
    selected: Sequence[Candidate],
    *,
    title: str,
    base_url: str,
    commit: bool,
) -> None:
    """Prints the run summary panel and the per-candidate plan table."""
    counts = Counter(candidate.action for candidate in candidates)
    mode = (
        "read-only"
        if selected and all(candidate.action == READ_ACTION for candidate in selected)
        else "COMMIT"
        if commit
        else "dry-run"
    )
    console.print(
        Panel.fit(
            "\n".join(
                [
                    f"Domain: {base_url}",
                    f"Mode: {mode}",
                    f"Loaded: {len(candidates)}",
                    f"Selected: {len(selected)}",
                    "  ".join(f"{action}: {count}" for action, count in counts.items()),
                ]
            ),
            title=title,
        )
    )

    display_columns = list(selected[0].display_fields()) if selected else []
    table = Table(title="Run plan")
    table.add_column("#", justify="right")
    table.add_column("OFFENDERID")
    table.add_column("Action")
    for column in display_columns:
        table.add_column(column)
    table.add_column("Reason")
    for index, candidate in enumerate(selected, start=1):
        fields = candidate.display_fields()
        table.add_row(
            str(index),
            candidate.offender_id,
            candidate.action,
            *(fields[column] for column in display_columns),
            candidate.reason,
        )
    console.print(table)


def render_results(
    console: Console, results: Sequence[WriteResult], out_path: str
) -> None:
    table = Table(title="Results")
    table.add_column("#", justify="right")
    table.add_column("OFFENDERID")
    table.add_column("Action")
    table.add_column("Result")
    table.add_column("Detail")
    for index, result in enumerate(results, start=1):
        table.add_row(
            str(index),
            result.candidate.offender_id,
            result.candidate.action,
            result.status.value,
            result.detail,
        )
    console.print(table)
    console.print(f"Results CSV: {out_path}")
