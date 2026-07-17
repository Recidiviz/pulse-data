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
"""Rich-console rendering for attended eOMIS writeback runs.

Interactive display only — the production package (recidiviz.eomis) must not
depend on rich, so everything console-facing lives here with the CLIs.
"""
from __future__ import annotations

from collections import Counter
from typing import Sequence

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from recidiviz.eomis.flow import READ_ACTION, Candidate, WriteResult


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
