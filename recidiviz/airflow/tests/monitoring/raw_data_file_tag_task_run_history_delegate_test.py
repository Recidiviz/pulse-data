# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Unit tests for RawDataFileTagTaskRunHistoryDelegate"""
import datetime
from unittest import TestCase
from unittest.mock import MagicMock, patch

from recidiviz.airflow.dags.monitoring.file_tag_import_run_summary import (
    BigQueryFailedFileImportRunSummary,
    FileTagImportRunSummary,
)
from recidiviz.airflow.dags.monitoring.job_run import JobRunState
from recidiviz.airflow.dags.monitoring.raw_data_file_tag_task_run_history_delegate import (
    RawDataFileTagTaskRunHistoryDelegate,
)
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

TEST_DAG = "test_dag"


class RawDataFileTagTaskRunHistoryDelegateTest(TestCase):
    """Unit tests for RawDataFileTagTaskRunHistoryDelegate"""

    def setUp(self) -> None:
        self.ti = MagicMock()
        self.context_patcher = patch(
            "recidiviz.airflow.dags.monitoring.raw_data_file_tag_task_run_history_delegate.get_current_context",
            return_value={"ti": self.ti},
        )
        self.context_patcher.start()
        self.delegate = RawDataFileTagTaskRunHistoryDelegate(dag_id=TEST_DAG)

    def tearDown(self) -> None:
        self.context_patcher.stop()

    def test_failed_upstream(self) -> None:
        self.ti.xcom_pull.return_value = None
        with self.assertLogs(level="ERROR") as logs:
            result = self.delegate.fetch_job_runs(lookback=datetime.timedelta(hours=1))

        assert result == []

        assert len(logs.output) == 1
        assert "Failed to fetch file tag import history" in logs.output[0]

    def test_empty(self) -> None:
        self.ti.xcom_pull.return_value = []

        with self.assertLogs(level="ERROR") as logs:
            result = self.delegate.fetch_job_runs(lookback=datetime.timedelta(hours=1))

        assert result == []

        assert len(logs.output) == 1
        assert "Found no import run summaries" in logs.output[0]

    def test_summaries(self) -> None:

        summaries = [
            FileTagImportRunSummary(
                import_run_start=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
                state_code=StateCode.US_XX,
                raw_data_instance=DirectIngestInstance.PRIMARY,
                file_tag="tag_a",
                file_tag_import_state=JobRunState.FAILED,
                failed_file_import_runs=[
                    BigQueryFailedFileImportRunSummary(
                        file_id=1,
                        update_datetime=datetime.datetime(
                            2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                        ),
                        file_import_status=DirectIngestRawFileImportStatus.FAILED_UNKNOWN,
                        error_message="ERROR\n\n\n\n\nERROR!",
                    ),
                    BigQueryFailedFileImportRunSummary(
                        file_id=2,
                        update_datetime=datetime.datetime(
                            2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC
                        ),
                        file_import_status=DirectIngestRawFileImportStatus.FAILED_UNKNOWN,
                        error_message="ERROR\n\n\n\n\nERROR!",
                    ),
                ],
            ),
            FileTagImportRunSummary(
                import_run_start=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
                state_code=StateCode.US_XX,
                raw_data_instance=DirectIngestInstance.PRIMARY,
                file_tag="tag_b",
                file_tag_import_state=JobRunState.SUCCESS,
                failed_file_import_runs=[
                    BigQueryFailedFileImportRunSummary(
                        file_id=3,
                        update_datetime=datetime.datetime(
                            2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                        ),
                        file_import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                        error_message=None,
                    ),
                    BigQueryFailedFileImportRunSummary(
                        file_id=4,
                        update_datetime=datetime.datetime(
                            2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC
                        ),
                        file_import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                        error_message=None,
                    ),
                ],
            ),
        ]

        self.ti.xcom_pull.return_value = [s.serialize() for s in summaries]

        job_runs = self.delegate.fetch_job_runs(lookback=datetime.timedelta(hours=1))

        assert len(job_runs) == 2
        assert job_runs == [s.as_job_run(TEST_DAG) for s in summaries]
