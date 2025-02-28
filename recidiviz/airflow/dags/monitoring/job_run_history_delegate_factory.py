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
"""Contains a factory class for creating JobRunHistoryDelegate objects."""

from recidiviz.airflow.dags.monitoring.airflow_task_run_history_delegate import (
    AirflowTaskRunHistoryDelegate,
)
from recidiviz.airflow.dags.monitoring.dag_registry import (
    get_calculation_dag_id,
    get_monitoring_dag_id,
    get_raw_data_import_dag_id,
    get_sftp_dag_id,
)
from recidiviz.airflow.dags.monitoring.job_run_history_delegate import (
    JobRunHistoryDelegate,
)
from recidiviz.airflow.dags.monitoring.raw_data_file_tag_task_run_history_delegate import (
    RawDataFileTagTaskRunHistoryDelegate,
)
from recidiviz.airflow.dags.utils.environment import get_project_id


class JobRunHistoryDelegateFactory:
    @classmethod
    def build(cls, *, dag_id: str) -> list[JobRunHistoryDelegate]:
        project_id = get_project_id()
        if dag_id == get_calculation_dag_id(project_id):
            return [AirflowTaskRunHistoryDelegate(dag_id=dag_id)]

        if dag_id == get_sftp_dag_id(project_id):
            return [AirflowTaskRunHistoryDelegate(dag_id=dag_id)]

        if dag_id == get_raw_data_import_dag_id(project_id):
            return [
                AirflowTaskRunHistoryDelegate(dag_id=dag_id),
                RawDataFileTagTaskRunHistoryDelegate(dag_id=dag_id),
            ]

        if dag_id == get_monitoring_dag_id(project_id):
            return [AirflowTaskRunHistoryDelegate(dag_id=dag_id)]

        raise ValueError(f"No configured builder delegate for: {dag_id}")
