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

"""Cron endpoint for managing long-term Cloud SQL backups"""
import datetime
import logging
import time
from http import HTTPStatus
from typing import Tuple

import flask
import pytz

from recidiviz.persistence.database.sqladmin_client import sqladmin_client
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.utils import metadata
from recidiviz.utils.auth.gae import requires_gae_auth

# Approximately 6 months - with weekly backups, we will always have
# approximately 26 backups, but there may be extra ones triggered via the
# admin console.
_MAX_BACKUP_AGE_DAYS = 183


_SECONDS_BETWEEN_OPERATION_STATUS_CHECKS = 3


backup_manager_blueprint = flask.Blueprint("backup_manager", __name__)


@backup_manager_blueprint.route("/update_long_term_backups")
@requires_gae_auth
def update_long_term_backups() -> Tuple[str, HTTPStatus]:
    """Create manual backups for all cloudsql instances and delete
    manual backups for each instance that are older than _MAX_BACKUP_AGE_DAYS.
    """
    project_id = metadata.project_id()
    logging.info("Starting backup of all cloudsql instances in [%s]", project_id)
    for instance_id in SQLAlchemyEngineManager.get_all_stripped_cloudsql_instance_ids():
        update_long_term_backups_for_cloudsql_instance(project_id, instance_id)

    logging.info("All backup operations completed successfully")
    return "", HTTPStatus.OK


def update_long_term_backups_for_cloudsql_instance(
    project_id: str, instance_id: str
) -> None:
    """Create a new manual backup for the given sqlalchemy instance
    and delete manual backups for that instance that are older than
    _MAX_BACKUP_AGE_DAYS.
    """

    logging.info("Creating request for backup insert operation on [%s]", instance_id)
    insert_request = (
        sqladmin_client()
        .backupRuns()
        .insert(project=project_id, instance=instance_id, body={})
    )

    logging.info("Beginning backup insert operation on [%s]", instance_id)
    insert_operation = insert_request.execute()
    _await_operation(project_id, insert_operation["name"])
    _throw_if_error(project_id, insert_operation["name"], "insert")
    logging.info("Backup insert operation on [%s] completed", instance_id)

    logging.info("Creating request for backup list operation on [%s]", instance_id)
    list_request = (
        sqladmin_client().backupRuns().list(project=project_id, instance=instance_id)
    )

    logging.info("Beginning backup list request")
    list_result = list_request.execute()
    backup_runs = list_result["items"]
    manual_backup_runs = [
        backup_run for backup_run in backup_runs if backup_run["type"] == "ON_DEMAND"
    ]
    logging.info(
        "Backup list request for [%s] completed with [%s] total backup"
        " runs and [%s] manual backup runs",
        instance_id,
        str(len(backup_runs)),
        str(len(manual_backup_runs)),
    )

    # startTime is a string with format yyyy-mm-dd, so sorting it as a
    # string will give the same result as converting it to a date and then
    # sorting by date
    manual_backup_runs.sort(key=lambda backup_run: backup_run["startTime"])

    six_months_ago_datetime = datetime.datetime.now(tz=pytz.UTC) - datetime.timedelta(
        days=_MAX_BACKUP_AGE_DAYS
    )
    six_months_ago_date_str = six_months_ago_datetime.date().isoformat()

    for backup_run in manual_backup_runs:
        backup_start_date_str = backup_run["startTime"]
        if backup_start_date_str > six_months_ago_date_str:
            break

        backup_id = backup_run["id"]

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
        _await_operation(project_id, delete_operation["name"])
        _throw_if_error(project_id, delete_operation["name"], "delete")
        logging.info(
            "Backup delete operation completed for backup [%s] of [%s]",
            backup_id,
            instance_id,
        )


def _await_operation(project_id: str, operation_id: str) -> None:
    done = False
    while True:
        if done:
            break

        operation = (
            sqladmin_client()
            .operations()
            .get(project=project_id, operation=operation_id)
            .execute()
        )
        current_status = operation["status"]

        if current_status in {"PENDING", "RUNNING", "UNKNOWN"}:
            time.sleep(_SECONDS_BETWEEN_OPERATION_STATUS_CHECKS)
        elif current_status == "DONE":
            done = True
        else:
            raise RuntimeError(f"Unrecognized operation status: {current_status}")


def _throw_if_error(project_id: str, operation_id: str, operation_type: str) -> None:
    operation = (
        sqladmin_client()
        .operations()
        .get(project=project_id, operation=operation_id)
        .execute()
    )

    if "error" in operation:
        errors = operation["error"].get("errors", [])
        error_messages = [
            f"code: {error['code']}\n message: {error['message']}" for error in errors
        ]
        raise RuntimeError(
            f"Backup {operation_type} operation finished with "
            f"{len(errors)} errors:\n"
            "\n".join(error_messages)
        )
