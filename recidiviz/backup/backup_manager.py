# Recidiviz - a platform for tracking granular recidivism metrics in real time
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

from http import HTTPStatus
import logging
import time
from typing import Tuple

import flask
import google.auth
from googleapiclient import discovery

from recidiviz.utils import metadata
from recidiviz.utils import secrets
from recidiviz.utils.auth import authenticate_request


# Weekly backups for 6 months: 26 weekly backups
_MAX_COUNT_MANUAL_BACKUPS = 26


_SECONDS_BETWEEN_OPERATION_STATUS_CHECKS = 3


backup_manager_blueprint = flask.Blueprint('backup_manager', __name__)


@backup_manager_blueprint.route('/update_long_term_backups')
@authenticate_request
def update_long_term_backups() -> Tuple[str, HTTPStatus]:
    """Create a new manual backup and delete the oldest manual backup once the
    maximum number has been reached
    """
    credentials, _ = google.auth.default()
    client = discovery.build('sqladmin', 'v1beta4', credentials=credentials)

    project_id = metadata.project_id()
    instance_id = _get_cloudsql_instance_id()

    logging.info('Creating request for backup insert operation')
    insert_request = client.backupRuns().insert(
        project=project_id, instance=instance_id, body={})

    logging.info('Beginning backup insert operation')
    insert_operation = insert_request.execute()
    _await_operation(client, project_id, insert_operation['name'])
    _throw_if_error(client, project_id, insert_operation['name'], 'insert')
    logging.info('Backup insert operation completed')

    logging.info('Creating request for backup list operation')
    list_request = client.backupRuns().list(
        project=project_id, instance=instance_id)

    logging.info('Beginning backup list request')
    list_result = list_request.execute()
    backup_runs = list_result['items']
    manual_backup_runs = [backup_run for backup_run in backup_runs
                          if backup_run['type'] == 'ON_DEMAND']
    logging.info('Backup list request completed with [%s] total backup runs '
                 'and [%s] manual backup runs',
                 str(len(backup_runs)),
                 str(len(manual_backup_runs)))

    if len(manual_backup_runs) > _MAX_COUNT_MANUAL_BACKUPS:
        # startTime is a string with format yyyy-mm-dd, so sorting it as a
        # string will give the same result as converting it to a date and then
        # sorting by date
        manual_backup_runs.sort(key=lambda backup_run: backup_run['startTime'])
        oldest_manual_backup = manual_backup_runs[0]
        oldest_manual_backup_id = oldest_manual_backup['id']

        logging.info('Creating request for backup delete operation')
        delete_request = client.backupRuns().delete(
            project=project_id,
            instance=instance_id,
            id=oldest_manual_backup_id)

        logging.info('Beginning backup delete operation')
        delete_operation = delete_request.execute()
        _await_operation(client, project_id, delete_operation['name'])
        _throw_if_error(client, project_id, delete_operation['name'], 'delete')
        logging.info('Backup delete operation completed')

    logging.info('All backup operations completed successfully')
    return ('', HTTPStatus.OK)


def _await_operation(client: discovery.Resource,
                     project_id: str,
                     operation_id: str) -> None:
    done = False
    while True:
        if done:
            break

        operation = client.operations().get(
            project=project_id, operation=operation_id).execute()
        current_status = operation['status']

        if current_status in {'PENDING', 'RUNNING', 'UNKNOWN'}:
            time.sleep(_SECONDS_BETWEEN_OPERATION_STATUS_CHECKS)
        elif current_status == 'DONE':
            done = True
        else:
            raise RuntimeError('Unrecognized operation status: {}'.format(
                current_status))


def _throw_if_error(client: discovery.Resource,
                    project_id: str,
                    operation_id: str,
                    operation_type: str) -> None:
    operation = client.operations().get(
        project=project_id, operation=operation_id).execute()

    if 'error' in operation:
        errors = operation['error'].get('errors', [])
        error_messages = ['code: {}\n message: {}'.format(
            error['code'], error['message'])
                          for error in errors]
        raise RuntimeError('Backup {} operation finished with '
                           '{} errors:\n{}'.format(
                               operation_type,
                               str(len(errors)),
                               '\n'.join(error_messages)))


def _get_cloudsql_instance_id() -> str:
    # Format <project ID>:<zone ID>:<instance ID>
    return secrets.get_secret('cloudsql_instance_id').split(':')[-1]
