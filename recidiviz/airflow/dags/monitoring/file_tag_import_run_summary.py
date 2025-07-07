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
from typing import Any

import attr

from recidiviz.airflow.dags.monitoring.job_run import JobRun, JobRunState, JobRunType
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
    DirectIngestRawFileImportStatusBucket,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.airflow_types import BaseResult

SECTION_CHAR = "="
BETWEEN_FILE_CHAR = "-"
LINE_LENGTH = 120
SECTION_SEPARATOR = SECTION_CHAR * LINE_LENGTH
BETWEEN_FILE_SEPARATOR = BETWEEN_FILE_CHAR * LINE_LENGTH


@attr.define
class BigQueryFailedFileImportRunSummary(BaseResult):
    """Metadata about a single "conceptual" file's failed import status."""

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
    def deserialize(json_str: str) -> "BigQueryFailedFileImportRunSummary":
        json_obj = json.loads(json_str)
        return BigQueryFailedFileImportRunSummary(
            file_id=json_obj[0],
            update_datetime=datetime.datetime.fromisoformat(json_obj[1]),
            file_import_status=DirectIngestRawFileImportStatus(json_obj[2]),
            error_message=json_obj[3],
        )

    @classmethod
    def from_db_json(cls, db_json: dict) -> "BigQueryFailedFileImportRunSummary":
        return BigQueryFailedFileImportRunSummary(
            file_id=db_json["file_id"],
            update_datetime=datetime.datetime.fromisoformat(db_json["update_datetime"]),
            file_import_status=DirectIngestRawFileImportStatus(
                db_json["failed_file_import_status"]
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
    failed_file_import_runs: list[BigQueryFailedFileImportRunSummary]

    def _format_failed_file_import_run(
        self, file_import_run: BigQueryFailedFileImportRunSummary
    ) -> str:
        return f"[{self.file_tag}] with update_datetime [{file_import_run.update_datetime.isoformat()}] and file_id [{file_import_run.file_id}] failed: \n{file_import_run.error_message}"

    def filter_and_sort_failed_import_runs(
        self,
    ) -> tuple[
        list[BigQueryFailedFileImportRunSummary],
        list[BigQueryFailedFileImportRunSummary],
    ]:
        blocked_failures: list[BigQueryFailedFileImportRunSummary] = []
        non_blocked_failures: list[BigQueryFailedFileImportRunSummary] = []

        for import_run in self.failed_file_import_runs:
            if (
                import_run.file_import_status
                == DirectIngestRawFileImportStatus.FAILED_IMPORT_BLOCKED
            ):
                blocked_failures.append(import_run)
            elif (
                import_run.file_import_status
                in DirectIngestRawFileImportStatusBucket.failed_statuses()
            ):
                non_blocked_failures.append(import_run)

        return (
            list(sorted(non_blocked_failures, key=lambda x: x.update_datetime)),
            list(sorted(blocked_failures, key=lambda x: x.update_datetime)),
        )

    def _format_error_section(
        self,
        *,
        errors: list[BigQueryFailedFileImportRunSummary],
        header: str,
        max_errors: int,
    ) -> str | None:
        if not errors:
            return None

        first_n_errors = errors[:max_errors]
        next_n_errors = errors[max_errors:]

        first_n_errors_detail = f"\n{BETWEEN_FILE_SEPARATOR}\n".join(
            self._format_failed_file_import_run(error) for error in first_n_errors
        )

        next_n_errors_detail = (
            (
                f"\n{BETWEEN_FILE_SEPARATOR}\n... and {len(next_n_errors)} more not included in this alert; see airflow logs for more info"
            )
            if next_n_errors
            else ""
        )

        errors_detail = first_n_errors_detail + next_n_errors_detail

        header_full = f" {header} ({len(errors)}) "

        return f"{SECTION_SEPARATOR}\n{header_full.center(LINE_LENGTH, SECTION_CHAR)}\n{SECTION_SEPARATOR}\n{errors_detail}\n{SECTION_SEPARATOR}\n{SECTION_SEPARATOR}"

    def format_error_message(self, *, max_errors: int = 10) -> str:
        (
            non_blocked_failures,
            blocked_failures,
        ) = self.filter_and_sort_failed_import_runs()

        return "\n".join(
            filter(
                None,
                [
                    self._format_error_section(
                        errors=non_blocked_failures,
                        header="FAILURES",
                        max_errors=max_errors,
                    ),
                    self._format_error_section(
                        errors=blocked_failures,
                        header="IMPORT BLOCKED BY ABOVE FAILURES",
                        max_errors=max_errors,
                    ),
                ],
            )
        )

    def job_id(self) -> str:
        """Unique job_id: (state_code.file_tag) -- used to identify all files imported
        with the same file tag within a single run together. Since JobRun objects are
        unique based on job_id and dag_run_config, we do not need to include the
        raw_data_instance in the job_id.
        """
        return f"{self.state_code.value}.{self.file_tag}"

    def serialize(self) -> str:
        return json.dumps(
            [
                self.import_run_start.isoformat(),
                self.state_code.value,
                self.raw_data_instance.value,
                self.file_tag,
                self.file_tag_import_state.value,
                [import_run.serialize() for import_run in self.failed_file_import_runs],
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
            failed_file_import_runs=[
                BigQueryFailedFileImportRunSummary.deserialize(import_run)
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
        failed_file_import_runs_json: list[dict[str, Any]] | None,
    ) -> "FileTagImportRunSummary":
        return FileTagImportRunSummary(
            import_run_start=import_run_start,
            state_code=StateCode(state_code_str.upper()),
            raw_data_instance=DirectIngestInstance(raw_data_instance_str.upper()),
            file_tag=file_tag,
            file_tag_import_state=JobRunState(file_tag_import_state_int),
            failed_file_import_runs=(
                [
                    BigQueryFailedFileImportRunSummary.from_db_json(import_run_json)
                    for import_run_json in failed_file_import_runs_json
                ]
                if failed_file_import_runs_json is not None
                else []
            ),
        )

    def as_job_run(self, dag_id: str) -> JobRun:
        dag_run_config = (
            {}
            if self.raw_data_instance == DirectIngestInstance.PRIMARY
            else {"ingest_instance": self.raw_data_instance.value}
        )
        return JobRun(
            dag_id=dag_id,
            execution_date=self.import_run_start,
            dag_run_config=json.dumps(dag_run_config),
            job_id=self.job_id(),
            state=self.file_tag_import_state,
            error_message=self.format_error_message(),
            job_type=JobRunType.RAW_DATA_IMPORT,
            job_run_num=0,
        )
