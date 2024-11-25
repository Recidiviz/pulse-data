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
"""Data model for file-tag level import runs."""

import datetime
import json
from typing import Any, Iterator

import attr

from recidiviz.airflow.dags.monitoring.job_run import JobRunState
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.airflow_types import BaseResult


@attr.define
class BigQueryFileImportRunSummary(BaseResult):
    """Metadata about a single "conceptual" file's import status."""

    file_id: int
    update_datetime: datetime.datetime
    file_import_status: DirectIngestRawFileImportStatus
    error_message: str | None

    def serialize(self) -> str:
        return json.dumps(
            [
                self.file_id,
                self.update_datetime.isoformat(),
                self.file_import_status.value,
                self.error_message,
            ]
        )

    @staticmethod
    def deserialize(json_str: str) -> "BigQueryFileImportRunSummary":
        json_obj = json.loads(json_str)
        return BigQueryFileImportRunSummary(
            file_id=json_obj[0],
            update_datetime=datetime.datetime.fromisoformat(json_obj[1]),
            file_import_status=DirectIngestRawFileImportStatus(json_obj[2]),
            error_message=json_obj[3],
        )

    @classmethod
    def from_db_json(cls, db_json: dict) -> "BigQueryFileImportRunSummary":
        return BigQueryFileImportRunSummary(
            file_id=db_json["file_id"],
            update_datetime=datetime.datetime.fromisoformat(db_json["update_datetime"]),
            file_import_status=DirectIngestRawFileImportStatus(
                db_json["file_import_status"]
            ),
            error_message=db_json["error_message"],
        )


@attr.define
class FileTagImportRunSummary(BaseResult):
    """A summary of all file imports for |file_tag| in the (|state_code|, |raw_data_instance|)
    infrastructure during a single run of the raw data import DAG. The value of
    |file_tag_import_state| will be failed if any file failed, pending if any file is
    still running, unknown if any file is unknown and success if all files were successfully
    imported.
    """

    import_run_start: datetime.datetime
    state_code: StateCode
    raw_data_instance: DirectIngestInstance
    file_tag: str
    file_tag_import_state: JobRunState
    file_import_runs: list[BigQueryFileImportRunSummary]

    def _format_file_import_run(
        self, file_import_run: BigQueryFileImportRunSummary
    ) -> str:
        return f"[{self.file_tag}] with update_datetime [{file_import_run.update_datetime.isoformat()}] and file_id [{file_import_run.file_id}] failed: \n{file_import_run.error_message}"

    def failed_import_runs(self) -> Iterator[BigQueryFileImportRunSummary]:
        return filter(
            lambda x: x.file_import_status == JobRunState.FAILED,
            self.file_import_runs,
        )

    def format_error_message(self) -> str:
        ascending_failures = sorted(
            self.failed_import_runs(), key=lambda x: x.update_datetime
        )

        return "\n".join(
            self._format_file_import_run(failure) for failure in ascending_failures
        )

    def serialize(self) -> str:
        return json.dumps(
            [
                self.import_run_start.isoformat(),
                self.state_code.value,
                self.raw_data_instance.value,
                self.file_tag,
                self.file_tag_import_state.value,
                [import_run.serialize() for import_run in self.file_import_runs],
            ]
        )

    @staticmethod
    def deserialize(json_str: str) -> "FileTagImportRunSummary":
        json_obj = json.loads(json_str)
        return FileTagImportRunSummary(
            import_run_start=datetime.datetime.fromisoformat(json_obj[0]),
            state_code=StateCode(json_obj[1]),
            raw_data_instance=DirectIngestInstance(json_obj[2]),
            file_tag=json_obj[3],
            file_tag_import_state=JobRunState(json_obj[4]),
            file_import_runs=[
                BigQueryFileImportRunSummary.deserialize(import_run)
                for import_run in json_obj[5]
            ],
        )

    @classmethod
    def from_db_row(
        cls,
        *,
        import_run_start: datetime.datetime,
        state_code_str: str,
        raw_data_instance_str: str,
        file_tag: str,
        file_tag_import_state_int: int,
        file_import_runs_json: list[dict[str, Any]],
    ) -> "FileTagImportRunSummary":
        return FileTagImportRunSummary(
            import_run_start=import_run_start,
            state_code=StateCode(state_code_str.upper()),
            raw_data_instance=DirectIngestInstance(raw_data_instance_str.upper()),
            file_tag=file_tag,
            file_tag_import_state=JobRunState(file_tag_import_state_int),
            file_import_runs=[
                BigQueryFileImportRunSummary.from_db_json(import_run_json)
                for import_run_json in file_import_runs_json
            ],
        )
