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
from recidiviz.airflow.dags.monitoring.airflow_task_runtime_delegate import (
    AirflowAllTaskRuntimeConfig,
    AirflowExplicitTaskRuntimeAlertingConfig,
    AirflowTaskNameRegexRuntimeAlertingConfig,
    AirflowTaskRuntimeDelegate,
)
from recidiviz.airflow.dags.monitoring.dag_registry import (
    get_calculation_dag_id,
    get_metadata_maintenance_dag_id,
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
    """Factory for building the relevant JobRunHistoryDelegates for a particular dag."""

    @classmethod
    def build(cls, *, dag_id: str) -> list[JobRunHistoryDelegate]:
        """Returns a list of JobRunHistoryDelegates that will build JobRunHistory objects
        for the provided |dag_id|.
        """
        project_id = get_project_id()
        if dag_id == get_calculation_dag_id(project_id):
            return [
                AirflowTaskRunHistoryDelegate(dag_id=dag_id),
                AirflowTaskRuntimeDelegate(
                    dag_id=dag_id,
                    # the order that these are listed is the order they will be evaluated
                    # in -- in general, it's best to have the most restrictive configs
                    # first
                    configs=[
                        AirflowExplicitTaskRuntimeAlertingConfig(
                            dag_id=dag_id,
                            runtime_minutes=90,
                            task_names=["update_managed_views_all"],
                        ),
                        AirflowTaskNameRegexRuntimeAlertingConfig(
                            dag_id=dag_id,
                            runtime_minutes=55,
                            task_name_regex=".*ingest-pipeline%.run_pipeline",
                        ),
                        AirflowAllTaskRuntimeConfig(dag_id=dag_id, runtime_minutes=120),
                    ],
                ),
            ]

        if dag_id == get_sftp_dag_id(project_id):
            return [
                AirflowTaskRunHistoryDelegate(dag_id=dag_id),
                AirflowTaskRuntimeDelegate(
                    dag_id=dag_id,
                    configs=[
                        # TODO(#44467): consider reducing once nebraska uses a vpn tunnel
                        # set at 3 hours, as nebraska sometimes takes this long
                        AirflowAllTaskRuntimeConfig(dag_id=dag_id, runtime_minutes=180),
                    ],
                ),
            ]

        if dag_id == get_raw_data_import_dag_id(project_id):
            return [
                AirflowTaskRunHistoryDelegate(dag_id=dag_id),
                RawDataFileTagTaskRunHistoryDelegate(dag_id=dag_id),
                AirflowTaskRuntimeDelegate(
                    dag_id=dag_id,
                    configs=[
                        AirflowAllTaskRuntimeConfig(dag_id=dag_id, runtime_minutes=120),
                    ],
                ),
            ]

        if dag_id == get_monitoring_dag_id(project_id):
            return [
                AirflowTaskRunHistoryDelegate(dag_id=dag_id),
                AirflowTaskRuntimeDelegate(
                    dag_id=dag_id,
                    configs=[
                        AirflowAllTaskRuntimeConfig(dag_id=dag_id, runtime_minutes=30),
                    ],
                ),
            ]

        if dag_id == get_metadata_maintenance_dag_id(project_id):
            return [
                AirflowTaskRunHistoryDelegate(dag_id=dag_id),
                AirflowTaskRuntimeDelegate(
                    dag_id=dag_id,
                    configs=[
                        AirflowAllTaskRuntimeConfig(dag_id=dag_id, runtime_minutes=30),
                    ],
                ),
            ]

        raise ValueError(f"No configured builder delegate for: {dag_id}")
