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
"""Delegate that pulls FileTagImportRunSummary objects from XCom and builds JobRun
objects."""

import datetime
import logging

from airflow.operators.python import get_current_context

from recidiviz.airflow.dags.monitoring.file_tag_import_run_summary import (
    FileTagImportRunSummary,
)
from recidiviz.airflow.dags.monitoring.job_run import JobRun
from recidiviz.airflow.dags.monitoring.job_run_history_delegate import (
    JobRunHistoryDelegate,
)
from recidiviz.airflow.dags.monitoring.metadata import (
    FETCH_RAW_DATA_FILE_TAG_IMPORT_RUNS_TASK_ID,
)


class RawDataFileTagTaskRunHistoryDelegate(JobRunHistoryDelegate):
    """Pulls FileTagImportRunSummary objects from XCom to build JobRun objects."""

    def __init__(self, *, dag_id: str) -> None:
        self.dag_id = dag_id

    def fetch_job_runs(self, *, lookback: datetime.timedelta) -> list[JobRun]:
        context = get_current_context()
        file_tag_import_run_summary_strs = context["ti"].xcom_pull(
            task_ids=FETCH_RAW_DATA_FILE_TAG_IMPORT_RUNS_TASK_ID
        )

        if file_tag_import_run_summary_strs is None:
            # if this is None, that means that our upstream task failed; since there
            # will already be an alert associated with that failure, let's not fail hard
            # so as to block alerting on other dags; instead let's just log our failure
            # and continue on.
            logging.error("Failed to fetch file tag import history")
            return []

        if not file_tag_import_run_summary_strs:
            logging.error("Found no import run summaries")

        file_tag_import_run_summaries = [
            FileTagImportRunSummary.deserialize(file_tag_import_run_summary_str)
            for file_tag_import_run_summary_str in file_tag_import_run_summary_strs
        ]

        return [
            file_tag_import_run_summary.as_job_run(self.dag_id)
            for file_tag_import_run_summary in file_tag_import_run_summaries
        ]
