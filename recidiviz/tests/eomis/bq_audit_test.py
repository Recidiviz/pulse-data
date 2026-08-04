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
"""Tests for the BigQuery audit ledger recorder for eOMIS writeback runs."""
import unittest
from typing import Any
from unittest import mock

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_row_streamer import BigQueryRowStreamer
from recidiviz.common.constants.states import StateCode
from recidiviz.eomis.bq_audit import (
    CANDIDATES_TABLE_ID,
    RUNS_TABLE_ID,
    BigQueryAuditRecorder,
    CandidateEventStatus,
    RunStatus,
    generate_run_id,
)
from recidiviz.eomis.flow import (
    SKIP_ACTION,
    Candidate,
    EomisWritebackFlow,
    ResultStatus,
    WriteResult,
)
from recidiviz.source_tables.yaml_managed.collect_yaml_managed_source_table_configs import (
    build_source_table_repository_for_yaml_managed_tables,
)
from recidiviz.source_tables.yaml_managed.datasets import (
    EOMIS_WRITEBACK_METADATA_DATASET,
)

_ACTIONABLE_CANDIDATE = Candidate(
    offender_id="123456", action="create", reason="certificate on file"
)
_SKIP_CANDIDATE = Candidate(
    offender_id="234567", action=SKIP_ACTION, reason="already completed in eOMIS"
)


class TestBigQueryAuditRecorder(unittest.TestCase):
    """Tests that the recorder emits schema-complete rows for every event type
    and never streams outside GCP."""

    def setUp(self) -> None:
        self.in_gcp_patcher = mock.patch(
            "recidiviz.eomis.bq_audit.environment.in_gcp", return_value=True
        )
        self.in_gcp_patcher.start()

        self.streamed: list[tuple[BigQueryAddress, list[dict[str, Any]]]] = []
        self.mock_bq_client = mock.MagicMock()
        self.mock_bq_client.stream_into_table.side_effect = (
            lambda address, rows: self.streamed.append((address, rows))
        )

        repository = build_source_table_repository_for_yaml_managed_tables(None)
        self.schemas_by_table_id: dict[str, list[bigquery.SchemaField]] = {}
        streamers = {}
        for table_id in [RUNS_TABLE_ID, CANDIDATES_TABLE_ID]:
            config = repository.get_config(
                BigQueryAddress(
                    dataset_id=EOMIS_WRITEBACK_METADATA_DATASET, table_id=table_id
                )
            )
            self.schemas_by_table_id[table_id] = config.schema_fields
            streamers[table_id] = BigQueryRowStreamer(
                bq_client=self.mock_bq_client,
                table_address=config.address,
                expected_table_schema=config.schema_fields,
            )

        self.run_id = generate_run_id()
        self.recorder = BigQueryAuditRecorder(
            run_id=self.run_id,
            flow_name="AR GED program referrals",
            state_code=StateCode.US_AR,
            commit=False,
            runs_streamer=streamers[RUNS_TABLE_ID],
            candidates_streamer=streamers[CANDIDATES_TABLE_ID],
        )

    def tearDown(self) -> None:
        self.in_gcp_patcher.stop()

    def _rows_for_table(self, table_id: str) -> list[dict[str, Any]]:
        return [
            row
            for address, rows in self.streamed
            for row in rows
            if address.table_id == table_id
        ]

    def test_every_row_carries_every_schema_column(self) -> None:
        self.recorder.record_run_started()
        self.recorder.record_classified([_ACTIONABLE_CANDIDATE, _SKIP_CANDIDATE])
        self.recorder.record_attempt(_ACTIONABLE_CANDIDATE)
        self.recorder.record_result(
            WriteResult(_ACTIONABLE_CANDIDATE, ResultStatus.DRY_RUN, "dry run only")
        )
        self.recorder.record_run_finished(status=RunStatus.SUCCESS, error_detail=None)

        for table_id, schema in self.schemas_by_table_id.items():
            expected_columns = {field.name for field in schema}
            rows = self._rows_for_table(table_id)
            self.assertTrue(rows)
            for row in rows:
                self.assertEqual(expected_columns, set(row.keys()))

    def test_run_lifecycle_rows(self) -> None:
        self.recorder.record_run_started()
        self.recorder.record_run_finished(
            status=RunStatus.FAILED, error_detail="login failed"
        )

        rows = self._rows_for_table(RUNS_TABLE_ID)
        self.assertEqual(
            [RunStatus.STARTED.value, RunStatus.FAILED.value],
            [row["status"] for row in rows],
        )
        for row in rows:
            self.assertEqual(self.run_id, row["run_id"])
            self.assertEqual("AR GED program referrals", row["flow_name"])
            self.assertEqual(StateCode.US_AR.value, row["state_code"])
            self.assertFalse(row["is_commit_run"])
        self.assertIsNone(rows[0]["error_detail"])
        self.assertEqual("login failed", rows[1]["error_detail"])

    def test_run_finished_rejects_non_terminal_status(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^\[started\] is not a terminal run status\.$"
        ):
            self.recorder.record_run_finished(
                status=RunStatus.STARTED, error_detail=None
            )

    def test_classified_streams_all_candidates_in_one_call(self) -> None:
        self.recorder.record_classified([_ACTIONABLE_CANDIDATE, _SKIP_CANDIDATE])

        self.assertEqual(1, len(self.streamed))
        rows = self._rows_for_table(CANDIDATES_TABLE_ID)
        self.assertEqual(
            [
                CandidateEventStatus.CLASSIFIED.value,
                CandidateEventStatus.CLASSIFIED.value,
            ],
            [row["status"] for row in rows],
        )
        skip_row = rows[1]
        self.assertEqual(SKIP_ACTION, skip_row["action"])
        self.assertEqual("already completed in eOMIS", skip_row["candidate_reason"])
        self.assertIsNone(skip_row["result_detail"])

    def test_attempt_then_result_rows(self) -> None:
        self.recorder.record_attempt(_ACTIONABLE_CANDIDATE)
        self.recorder.record_result(
            WriteResult(
                _ACTIONABLE_CANDIDATE, ResultStatus.ERROR, "no read-back confirmation"
            )
        )

        rows = self._rows_for_table(CANDIDATES_TABLE_ID)
        self.assertEqual(
            [CandidateEventStatus.ATTEMPTING.value, ResultStatus.ERROR.value],
            [row["status"] for row in rows],
        )
        self.assertEqual(
            ["certificate on file", "certificate on file"],
            [row["candidate_reason"] for row in rows],
        )
        self.assertEqual(
            [None, "no read-back confirmation"],
            [row["result_detail"] for row in rows],
        )
        for row in rows:
            self.assertEqual(
                _ACTIONABLE_CANDIDATE.offender_id, row["eomis_offender_id"]
            )
            self.assertEqual(_ACTIONABLE_CANDIDATE.action, row["action"])
            self.assertEqual("AR GED program referrals", row["flow_name"])
            self.assertEqual(StateCode.US_AR.value, row["state_code"])

    def test_error_detail_required_for_failure_statuses(self) -> None:
        for status in [RunStatus.FAILED, RunStatus.COMPLETED_WITH_ERRORS]:
            with self.assertRaisesRegex(
                ValueError,
                rf"^error_detail is required for a \[{status.value}\] run\.$",
            ):
                self.recorder.record_run_finished(status=status, error_detail=None)
        self.assertEqual([], self.streamed)

    def test_streaming_failure_propagates_before_write(self) -> None:
        self.mock_bq_client.stream_into_table.side_effect = RuntimeError(
            "streaming insert failed"
        )
        with self.assertRaisesRegex(RuntimeError, r"^streaming insert failed$"):
            self.recorder.record_attempt(_ACTIONABLE_CANDIDATE)

    def test_for_run_builds_streamers_for_the_ledger_tables(self) -> None:
        fake_flow = mock.create_autospec(EomisWritebackFlow, instance=True)
        fake_flow.flow_name = "AR GED program referrals"
        fake_flow.state_code = StateCode.US_AR

        with mock.patch(
            "recidiviz.eomis.bq_audit.BigQueryClientImpl"
        ) as mock_client_cls, mock.patch(
            "recidiviz.eomis.bq_audit.metadata.project_id",
            return_value="recidiviz-staging",
        ):
            recorder = BigQueryAuditRecorder.for_run(
                run_id=self.run_id, flow=fake_flow, commit=True
            )

        self.assertEqual("AR GED program referrals", recorder.flow_name)
        self.assertEqual(StateCode.US_AR, recorder.state_code)
        self.assertTrue(recorder.commit)

        recorder.record_run_started()
        recorder.record_attempt(_ACTIONABLE_CANDIDATE)

        streamed_addresses = [
            call.args[0]
            for call in mock_client_cls.return_value.stream_into_table.call_args_list
        ]
        self.assertEqual(
            [
                BigQueryAddress(
                    dataset_id=EOMIS_WRITEBACK_METADATA_DATASET,
                    table_id=RUNS_TABLE_ID,
                ),
                BigQueryAddress(
                    dataset_id=EOMIS_WRITEBACK_METADATA_DATASET,
                    table_id=CANDIDATES_TABLE_ID,
                ),
            ],
            streamed_addresses,
        )

    def test_local_run_streams_nothing(self) -> None:
        with mock.patch(
            "recidiviz.eomis.bq_audit.environment.in_gcp", return_value=False
        ):
            self.recorder.record_run_started()
            self.recorder.record_classified([_ACTIONABLE_CANDIDATE])
            self.recorder.record_run_finished(
                status=RunStatus.SUCCESS, error_detail=None
            )

        self.assertEqual([], self.streamed)
        self.mock_bq_client.stream_into_table.assert_not_called()
