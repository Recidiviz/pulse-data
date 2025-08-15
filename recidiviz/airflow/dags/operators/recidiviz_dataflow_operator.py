# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""
A subclass of DataflowTemplateOperator to ensure that the operator does not add a hash or
unique id to the task_id name on Dataflow
"""
import logging
import time
from typing import Any, Dict

from airflow.providers.google.cloud.hooks.dataflow import (
    DataflowHook,
    DataflowJobStatus,
)
from airflow.providers.google.cloud.links.dataflow import DataflowJobLink
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator,
)
from airflow.utils.context import Context
from google.cloud.logging import Client as LoggingClient
from googleapiclient.discovery import build

from recidiviz.utils.string import StrictStringFormatter

DATAFLOW_ERROR_LOGS_QUERY = """
(log_id("dataflow.googleapis.com/job-message") OR log_id("dataflow.googleapis.com/launcher"))
resource.type="dataflow_step"
resource.labels.job_id="{job_id}"
timestamp >= "{job_creation_time}"
(severity >= ERROR OR "Error:")
"""

ZONAL_RESOURCES_EXHAUSTED = "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS"
QUOTA_EXCEEDED = "QUOTA_EXCEEDED"


def _collect_log_messages(
    project_id: str, job_id: str, job_creation_time: str
) -> list[str]:
    client = LoggingClient(project=project_id)
    return [
        entry.payload
        for entry in client.list_entries(
            resource_names=[f"projects/{project_id}"],
            filter_=StrictStringFormatter().format(
                DATAFLOW_ERROR_LOGS_QUERY,
                job_id=job_id,
                job_creation_time=job_creation_time,
            ),
        )
    ]


class RecidivizDataflowFlexTemplateOperator(DataflowStartFlexTemplateOperator):
    """A custom implementation of the DataflowStartFlexTemplateOperator for flex templates."""

    @property
    def job_name(self) -> str:
        return self.body["launchParameter"]["jobName"]

    def get_job(self) -> Dict[Any, Any]:
        """Retrieves the most recent Dataflow job with job_name.
        That job may either be currently running or completed (if another one has not yet started).
        Queries the Dataflow service directly because DataflowHook does not support retrieving jobs by name.
        """
        service = build("dataflow", "v1b3", cache_discovery=False)

        return (
            service.projects()
            .locations()
            .jobs()
            .list(
                projectId=self.project_id,
                location=self.location,
                name=self.job_name,
            )
            .execute()["jobs"][0]
        )

    def execute(
        self,
        # Some context about the context: https://bcb.github.io/airflow/execute-context
        context: Context,  # pylint: disable=unused-argument
    ) -> Dict[Any, Any]:
        """Checks if a Dataflow job is running (in case of task retry), otherwise starts
        the job. Polls the status of the job until it's finished or failed."""
        hook = DataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        # This adds a button in the Airflow UI that links directly to the Dataflow job.
        def set_current_job(current_job: Dict[Any, Any]) -> None:
            self.job = current_job
            DataflowJobLink.persist(
                self, context, self.project_id, self.location, self.job.get("id")
            )

        try:
            # If the operator is on a retry loop, we ignore the start operation by checking
            # if the job is running.
            if hook.is_job_dataflow_running(
                name=self.job_name, project_id=self.project_id, location=self.location
            ):
                hook.wait_for_done(
                    job_name=self.job_name,
                    project_id=self.project_id,
                    location=self.location,
                )
                return self.get_job()

            return hook.start_flex_template(
                location=self.location,
                project_id=self.project_id,
                body=self.body,
                on_new_job_callback=set_current_job,
            )
        # Dataflow `wait_for_done` methods do not raise an `Exception` subclass, they raise an `Exception` on failure
        except Exception as e:
            job = self.get_job()

            if job["currentState"] == DataflowJobStatus.JOB_STATE_FAILED:
                logging.info("Fetching logs for failed job...")
                log_messages = _collect_log_messages(
                    project_id=self.project_id,
                    job_id=job["id"],
                    job_creation_time=job["createTime"],
                )

                for message in log_messages:
                    logging.info(message)

                if any(
                    (ZONAL_RESOURCES_EXHAUSTED in message or QUOTA_EXCEEDED in message)
                    for message in log_messages
                ):
                    logging.info(
                        "Retrying once more in 5 minutes due to zonal resource exhaustion"
                    )

                    # Sleep in a loop to allow interrupts
                    for _ in range(300):
                        time.sleep(1)

                    return hook.start_flex_template(
                        location=self.location,
                        project_id=self.project_id,
                        body=self.body,
                        on_new_job_callback=set_current_job,
                    )

            raise e
