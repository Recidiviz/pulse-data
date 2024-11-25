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
"""Tests for file_tag_import_run_summary.py"""

import datetime
from typing import Any, Type
from unittest import TestCase

from recidiviz.airflow.dags.monitoring.file_tag_import_run_summary import (
    BigQueryFileImportRunSummary,
    FileTagImportRunSummary,
)
from recidiviz.airflow.dags.monitoring.job_run import JobRunState
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


class FileSummarySerializationTest(TestCase):
    """Tests serialization for classes in file_tag_import_run_summary that inherit from
    BaseResult.
    """

    def test_import_run_summary(self) -> None:
        summary = BigQueryFileImportRunSummary(
            file_id=1,
            update_datetime=datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC),
            file_import_status=DirectIngestRawFileImportStatus.FAILED_UNKNOWN,
            error_message="ERROR\n\n\n\n\nERROR!",
        )

        self._validate_serialization(summary, BigQueryFileImportRunSummary)

    def test_file_tag_import_run_summary(self) -> None:
        summary = FileTagImportRunSummary(
            import_run_start=datetime.datetime(
                2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
            ),
            state_code=StateCode.US_XX,
            raw_data_instance=DirectIngestInstance.PRIMARY,
            file_tag="tag_a",
            file_tag_import_state=JobRunState.FAILED,
            file_import_runs=[
                BigQueryFileImportRunSummary(
                    file_id=1,
                    update_datetime=datetime.datetime(
                        2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                    file_import_status=DirectIngestRawFileImportStatus.FAILED_UNKNOWN,
                    error_message="ERROR\n\n\n\n\nERROR!",
                ),
                BigQueryFileImportRunSummary(
                    file_id=2,
                    update_datetime=datetime.datetime(
                        2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                    file_import_status=DirectIngestRawFileImportStatus.FAILED_UNKNOWN,
                    error_message="ERROR\n\n\n\n\nERROR!",
                ),
            ],
        )

        self._validate_serialization(summary, FileTagImportRunSummary)

    def _validate_serialization(self, obj: Any, obj_type: Type) -> None:
        serialized = obj.serialize()
        deserialized = obj_type.deserialize(serialized)

        self.assertEqual(obj, deserialized)
