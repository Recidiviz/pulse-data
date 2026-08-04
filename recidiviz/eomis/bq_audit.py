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
"""The durable BigQuery audit ledger for eOMIS writeback runs.

Streams append-only events to the `eomis_writeback_metadata` dataset: run
lifecycle events (started/terminal) and per-candidate events
(classified/attempting/result). This is the only record of an unattended run
that outlives the container — Cloud Run job logs are not exported to BigQuery.

Flow-agnostic: one recorder per run, built from any registered
EomisWritebackFlow. New flows get ledger coverage with no changes here.

Fail-closed by design: streaming errors propagate. A run that cannot record
what it is about to do must not write to eOMIS.
"""
from __future__ import annotations

import datetime
import logging
import uuid
from enum import Enum
from typing import Any, Sequence

import pytz

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_row_streamer import BigQueryRowStreamer
from recidiviz.common.constants.states import StateCode
from recidiviz.eomis.flow import (
    Candidate,
    EomisWritebackFlow,
    ResultStatus,
    WriteResult,
)
from recidiviz.eomis.runner import AuditRecorder
from recidiviz.source_tables.yaml_managed.collect_yaml_managed_source_table_configs import (
    build_source_table_repository_for_yaml_managed_tables,
)
from recidiviz.source_tables.yaml_managed.datasets import (
    EOMIS_WRITEBACK_METADATA_DATASET,
)
from recidiviz.utils import environment, metadata

RUNS_TABLE_ID = "eomis_writeback_runs"
CANDIDATES_TABLE_ID = "eomis_writeback_candidates"


class CandidateEventStatus(Enum):
    """Candidate event statuses emitted before a result exists. Result rows use
    ResultStatus values."""

    CLASSIFIED = "classified"
    ATTEMPTING = "attempting"


class RunStatus(Enum):
    STARTED = "started"
    SUCCESS = "success"
    COMPLETED_WITH_ERRORS = "completed_with_errors"
    FAILED = "failed"


def generate_run_id() -> str:
    """Returns a new unique identifier for one execution of a writeback job."""
    return uuid.uuid4().hex


class BigQueryAuditRecorder(AuditRecorder):
    """Streams run lifecycle and per-candidate events for one writeback run
    into the append-only `eomis_writeback_metadata` ledger.

    Rows are only streamed when running in GCP; local runs log and skip, so a
    developer dry-run cannot pollute the deployed ledger.
    """

    def __init__(
        self,
        *,
        # Unique identifier for this execution, from generate_run_id().
        run_id: str,
        # The registered flow this run executes.
        flow_name: str,
        # The state whose eOMIS instance this run writes to.
        state_code: StateCode,
        # True if the run may write to eOMIS; False for dry runs.
        commit: bool,
        # Streamer for the run lifecycle events table.
        runs_streamer: BigQueryRowStreamer,
        # Streamer for the per-candidate events table.
        candidates_streamer: BigQueryRowStreamer,
    ) -> None:
        self.run_id = run_id
        self.flow_name = flow_name
        self.state_code = state_code
        self.commit = commit
        self._runs_streamer = runs_streamer
        self._candidates_streamer = candidates_streamer

    @classmethod
    def for_run(
        cls,
        *,
        run_id: str,
        flow: EomisWritebackFlow,
        commit: bool,
    ) -> "BigQueryAuditRecorder":
        """Returns a recorder for one run of the given flow, wired to the
        ledger tables in the current project with streamers that validate rows
        against the deployed YAML schemas."""
        repository = build_source_table_repository_for_yaml_managed_tables(
            metadata.project_id()
        )
        bq_client = BigQueryClientImpl()

        def streamer_for(table_id: str) -> BigQueryRowStreamer:
            config = repository.get_config(
                BigQueryAddress(
                    dataset_id=EOMIS_WRITEBACK_METADATA_DATASET, table_id=table_id
                )
            )
            return BigQueryRowStreamer(
                bq_client=bq_client,
                table_address=config.address,
                expected_table_schema=config.schema_fields,
            )

        return cls(
            run_id=run_id,
            flow_name=flow.flow_name,
            state_code=flow.state_code,
            commit=commit,
            runs_streamer=streamer_for(RUNS_TABLE_ID),
            candidates_streamer=streamer_for(CANDIDATES_TABLE_ID),
        )

    def record_run_started(self) -> None:
        """Records that this run began. Emit before loading candidates so a
        crash at any later point still leaves a `started` row with no terminal
        row — the signature of an unconfirmed run."""
        self._stream_run_event(status=RunStatus.STARTED, error_detail=None)

    def record_run_finished(
        self, *, status: RunStatus, error_detail: str | None
    ) -> None:
        """Records this run's terminal state. Failure statuses must say what
        failed."""
        if status is RunStatus.STARTED:
            raise ValueError(
                f"[{RunStatus.STARTED.value}] is not a terminal run status."
            )
        if status is not RunStatus.SUCCESS and error_detail is None:
            raise ValueError(f"error_detail is required for a [{status.value}] run.")
        self._stream_run_event(status=status, error_detail=error_detail)

    def record_classified(self, candidates: Sequence[Candidate]) -> None:
        """Records every loaded candidate and its classification — including
        skips, which never reach the runner and would otherwise leave no
        record."""
        self._stream(
            self._candidates_streamer,
            [
                self._candidate_row(
                    candidate=candidate,
                    status=CandidateEventStatus.CLASSIFIED,
                    result_detail=None,
                )
                for candidate in candidates
            ],
        )

    def record_attempt(self, candidate: Candidate) -> None:
        self._stream(
            self._candidates_streamer,
            [
                self._candidate_row(
                    candidate=candidate,
                    status=CandidateEventStatus.ATTEMPTING,
                    result_detail=None,
                )
            ],
        )

    def record_result(self, result: WriteResult) -> None:
        self._stream(
            self._candidates_streamer,
            [
                self._candidate_row(
                    candidate=result.candidate,
                    status=result.status,
                    result_detail=result.detail,
                )
            ],
        )

    def _stream_run_event(self, *, status: RunStatus, error_detail: str | None) -> None:
        self._stream(
            self._runs_streamer,
            [
                {
                    "run_id": self.run_id,
                    "flow_name": self.flow_name,
                    "state_code": self.state_code.value,
                    "is_commit_run": self.commit,
                    "status": status.value,
                    "error_detail": error_detail,
                    "recorded_at": self._now(),
                }
            ],
        )

    def _candidate_row(
        self,
        *,
        candidate: Candidate,
        status: CandidateEventStatus | ResultStatus,
        result_detail: str | None,
    ) -> dict[str, str | None]:
        return {
            "run_id": self.run_id,
            "flow_name": self.flow_name,
            "state_code": self.state_code.value,
            "eomis_offender_id": candidate.offender_id,
            "action": candidate.action,
            "status": status.value,
            "candidate_reason": candidate.reason,
            "result_detail": result_detail,
            "recorded_at": self._now(),
        }

    def _stream(
        self, streamer: BigQueryRowStreamer, rows: Sequence[dict[str, Any]]
    ) -> None:
        if not environment.in_gcp():
            logging.info(
                "Not running in GCP - skipping streaming of [%d] audit rows to [%s].",
                len(rows),
                streamer.table_address.to_str(),
            )
            return
        streamer.stream_rows(rows)

    @staticmethod
    def _now() -> str:
        return datetime.datetime.now(tz=pytz.UTC).isoformat()
