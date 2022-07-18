# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Implementation of the BQResultSensorQueryGenerator that returns a query
that produces a row when the most cloud task queued by a particular
CloudTasksTaskCreateOperator has completed.
"""

from typing import Dict

from airflow.utils.log.logging_mixin import LoggingMixin
from google.cloud.tasks_v2 import CloudTasksClient

# Custom Airflow operators in the recidiviz.airflow.dags.operators package are imported
# into the Cloud Composer environment at the top-level. However, for unit tests, we
# still need to import the recidiviz-top-level.
try:
    from operators.bq_result_sensor import (  # type: ignore
        BQResultSensor,
        BQResultSensorQueryGenerator,
    )
except ImportError:
    from recidiviz.airflow.dags.operators.bq_result_sensor import (
        BQResultSensor,
        BQResultSensorQueryGenerator,
    )


class FinishedCloudTaskQueryGenerator(BQResultSensorQueryGenerator, LoggingMixin):
    """Implementation of the BQResultSensorQueryGenerator that returns a query
    that produces a row when the most cloud task queued by a particular
    CloudTasksTaskCreateOperator has completed.
    """

    def __init__(
        self,
        project_id: str,
        cloud_task_create_operator_task_id: str,
        tracker_dataset_id: str,
        tracker_table_id: str,
    ) -> None:
        super().__init__()
        self.project_id = project_id
        self.cloud_task_create_operator_task_id = cloud_task_create_operator_task_id
        self.tracker_dataset_id = tracker_dataset_id
        self.tracker_table_id = tracker_table_id

    def get_query(self, operator: BQResultSensor, context: Dict) -> str:
        task_path = self.get_last_queued_task_path(
            operator, context, self.cloud_task_create_operator_task_id
        )
        task_path_parts = CloudTasksClient.parse_task_path(task_path)
        cloud_task_id = task_path_parts["task"]

        return f"""SELECT *
FROM `{self.project_id}.{self.tracker_dataset_id}.{self.tracker_table_id}`
WHERE cloud_task_id = '{cloud_task_id}';"""

    def get_last_queued_task_path(
        self,
        operator: BQResultSensor,
        context: Dict,
        cloud_task_create_operator_task_id: str,
    ) -> str:
        self.log.info(
            "Fetching last task queued via the [%s] Airflow task.",
            cloud_task_create_operator_task_id,
        )
        create_cloud_task_result = operator.xcom_pull(
            context,
            key="return_value",
            task_ids=cloud_task_create_operator_task_id,
        )
        task_path = create_cloud_task_result["name"]
        self.log.info("Found last queued task [%s].", task_path)
        return task_path
