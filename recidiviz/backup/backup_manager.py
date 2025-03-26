# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Cron endpoint for managing long-term Cloud SQL backups

It uses the sqladmin client from the google api client.
The methods associated with backupRuns can be found here:
https://developers.google.com/resources/api-libraries/documentation/sqladmin/v1beta4/python/latest/sqladmin_v1beta4.backupRuns.html
"""
import logging
import sys
import time
from datetime import datetime, timedelta
from http import HTTPStatus
from typing import List, Tuple

import flask
import pytz

from recidiviz.persistence.database.sqladmin_client import sqladmin_client
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Approximately 6 months - with weekly backups, we will always have
# approximately 26 backups, but there may be extra ones triggered via the
# admin console.
_MAX_BACKUP_AGE_DAYS = 183


_SECONDS_BETWEEN_OPERATION_STATUS_CHECKS = 3


backup_manager_blueprint = flask.Blueprint("backup_manager", __name__)


@backup_manager_blueprint.route("/update_long_term_backups")
def update_long_term_backups() -> Tuple[str, HTTPStatus]:
    """Creates manual backups for all cloudsql instances and deletes
    manual backups for each instance that are older than _MAX_BACKUP_AGE_DAYS.

    It does this in three steps:
        1. Start a backup for each instance independently. Wait for all to complete.
        2. For each instance with a successful backup, get historic backups that are older than _MAX_BACKUP_AGE_DAYS.
        3. Start deletion of each historic backup that is older than _MAX_BACKUP_AGE_DAYS. Wait for all to complete.
    """
    project_id = metadata.project_id()
    instance_ids = SQLAlchemyEngineManager.get_all_stripped_cloudsql_instance_ids()
    success = make_new_and_remove_old_cloudsql_backups(project_id, instance_ids)
    return "", HTTPStatus.OK if success else HTTPStatus.INTERNAL_SERVER_ERROR


def make_new_and_remove_old_cloudsql_backups(
    project_id: str, instance_ids: List[str], run_locally: bool = False
) -> bool:
    """Creates manual backups for all cloudsql instances and deletes
    manual backups for each instance that are older than _MAX_BACKUP_AGE_DAYS.

    It does this in three steps:
        1. Start a backup for each instance independently. Wait for all to complete.
        2. For each instance with a successful backup, get historic backups that are older than _MAX_BACKUP_AGE_DAYS.
        3. Start deletion of each historic backup that is older than _MAX_BACKUP_AGE_DAYS. Wait for all to complete.
    """
    success = True
    # First we start the backup jobs for each instance independently so that they all start.
    logging.info("Starting backup of all cloudsql instances in [%s]", project_id)
    backup_job_map = {
        instance_id: begin_backup_job(project_id, instance_id, run_locally=run_locally)
        for instance_id in instance_ids
    }
    # Now we will check the status of these jobs and wait for them to complete.
    # Instances that had successful backups will move forward for historic cleanup.
    logging.info("Checking status of all backup jobs in [%s]", project_id)
    backup_job_statuses = {
        instance_id: await_operation(project_id, backup_job_map[instance_id])
        for instance_id in instance_ids
    }
    # Maps an instance_id -> list of historic backups to delete
    historic_backup_instance_map = {}
    for instance_id, status in backup_job_statuses.items():
        if status == "DONE":
            logging.info(
                "Backup for instance %s completed successfully. Getting historic backups to delete.",
                instance_id,
            )
            historic_backup_instance_map[instance_id] = get_historic_backups(
                project_id, instance_id
            )
        else:
            logging.error("Backup for instance %s had error %s", instance_id, status)
            success = False

    # Kick-off the historic deletions independently
    historic_deletion_jobs = []
    for instance_id, backup_ids in historic_backup_instance_map.items():
        for backup_id in backup_ids:
            historic_deletion_jobs.append(
                delete_backup_by_id(project_id, instance_id, backup_id)
            )
    # Now report out the success of all deletions
    for delete_job_id in historic_deletion_jobs:
        delete_job_status = await_operation(project_id, delete_job_id)
        if delete_job_status == "DONE":
            logging.info("Deletion job %s completed successfully.", delete_job_id)
        else:
            success = False
            logging.error(
                "Deletion job %s completed with status %s.",
                delete_job_id,
                delete_job_status,
            )
    return success


def begin_backup_job(
    project_id: str, instance_id: str, run_locally: bool = False
) -> str:
    """Starts a backup operation for the given instance and returns the operation ID."""
    description = (
        "ON_DEMAND run locally from backup_manager.py"
        if run_locally
        else "Weekly ON_DEMAND backup from /update_long_term_backups"
    )
    logging.info("Creating request for backup insert operation on [%s]", instance_id)
    insert_request = (
        sqladmin_client()
        .backupRuns()
        .insert(
            project=project_id,
            instance=instance_id,
            body={"description": description},
        )
    )
    logging.info("Beginning backup insert operation on [%s]", instance_id)
    insert_operation = insert_request.execute()
    return insert_operation["name"]


def get_historic_backups(project_id: str, instance_id: str) -> List[str]:
    """Returns a list of ON_DEMAND backups for the given instance OLDER than six months ago."""
    six_months_ago_date_str = (
        (datetime.now(tz=pytz.UTC) - timedelta(days=_MAX_BACKUP_AGE_DAYS))
        .date()
        .isoformat()
    )
    logging.info("Creating request for backup list operation on [%s]", instance_id)
    list_request = (
        sqladmin_client().backupRuns().list(project=project_id, instance=instance_id)
    )
    list_result = list_request.execute()
    # startTime is a string with format yyyy-mm-dd
    return [
        backup_run["id"]
        for backup_run in list_result["items"]
        if backup_run["type"] == "ON_DEMAND"
        and backup_run["startTime"] < six_months_ago_date_str
    ]


def delete_backup_by_id(project_id: str, instance_id: str, backup_id: str) -> str:
    """Begins deletion of the given backup and returns an operation ID to track success."""
    logging.info(
        "Creating request for backup delete operation for backup [%s] of [%s]",
        backup_id,
        instance_id,
    )
    delete_request = (
        sqladmin_client()
        .backupRuns()
        .delete(project=project_id, instance=instance_id, id=backup_id)
    )
    logging.info(
        "Beginning backup delete operation for backup [%s] of [%s]",
        backup_id,
        instance_id,
    )
    delete_operation = delete_request.execute()
    return delete_operation["name"]


def get_operation_status(project_id: str, operation_id: str) -> str:
    """Returns the operation status for the given operation."""
    operation = (
        sqladmin_client()
        .operations()
        .get(project=project_id, operation=operation_id)
        .execute()
    )
    if "error" in operation:
        errors = operation["error"].get("errors", [])
        error_messages = "\n".join(
            f"code: {error['code']}\n message: {error['message']}" for error in errors
        )
        return (
            f"ERROR: Operation finished with {len(errors)} errors:\n {error_messages}"
        )
    if operation["status"] not in {"PENDING", "RUNNING", "UNKNOWN", "DONE"}:
        return f"UNRECOGNIZED status: {operation['status']}"
    return operation["status"]


def await_operation(project_id: str, operation_id: str) -> str:
    """Awaits the given operation to complete with DONE, ERROR, or UNRECOGNIZED and returns the completed status."""
    while True:
        current_status = get_operation_status(project_id, operation_id)
        if current_status in {"PENDING", "RUNNING", "UNKNOWN"}:
            time.sleep(_SECONDS_BETWEEN_OPERATION_STATUS_CHECKS)
        elif (
            current_status == "DONE"
            or current_status.startswith("ERROR")
            or current_status.startswith("UNRECOGNIZED")
        ):
            return current_status
        else:
            # We should never get a status that we haven't marked in one of the above categories,
            # so ok to break if we somehow get to this point.
            raise RuntimeError(
                f"Returned an operation status '{current_status}' for '{operation_id}'"
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    with local_project_id_override(GCP_PROJECT_STAGING):
        project = metadata.project_id()
        instances = SQLAlchemyEngineManager.get_all_stripped_cloudsql_instance_ids()
        successful_run = make_new_and_remove_old_cloudsql_backups(
            project, instances, run_locally=True
        )
        sys.exit(not successful_run)
