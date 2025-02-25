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

"""Export Cloud SQL databases to CSV files on GCS."""

import logging
import time
from typing import Dict, Tuple

import google.auth
import googleapiclient.errors
from googleapiclient import discovery


# Importing only for typing.
import sqlalchemy

from recidiviz.calculator.bq import export_config
from recidiviz.utils import metadata
from recidiviz.utils import secrets


SECONDS_BETWEEN_OPERATION_STATUS_CHECKS = 3

_client = None
def client() -> discovery.Resource:
    global _client
    if not _client:
        credentials, _ = google.auth.default()
        _client = discovery.build(
            'sqladmin', 'v1beta4', credentials=credentials)
    return _client

# TODO(rasmi): If not in GAE, force user to prod or staging.
def cloudsql_instance_id() -> str:
    """Get the Cloud SQL instance ID."""
    instance_id_full = secrets.get_secret('cloudsql_instance_id')
    # Remove Project ID and Zone information from Cloud SQL instance ID.
    # Expected format "project_id:zone:instance_id"
    instance_id = instance_id_full.split(':')[-1]

    return instance_id


def cloudsql_db_name() -> str:
    """Get the Cloud SQL database name."""
    db_name = secrets.get_secret('sqlalchemy_db_name')
    return db_name


def create_export_context(export_uri: str, export_query: str) -> dict:
    """Creates the exportContext configuration for the export operation.

    See here for details:
    https://cloud.google.com/sql/docs/postgres/admin-api/v1beta4/instances/export

    Args:
        export_uri: GCS URI to write the exported CSV data to.
        export_query: SQL query defining the data to be exported.

    Returns:
        export_context dict which can be passed to client.instances.export().
    """

    export_context = {
        'exportContext': {
            'kind': 'sql#exportContext',
            'fileType': 'CSV',
            'uri': export_uri,
            'databases': [cloudsql_db_name()],
            'csvExportOptions': {
                'selectQuery': export_query
            }
        }
    }

    return export_context


def wait_until_operation_finished(operation_id: str) -> bool:
    """Monitor a Cloud SQL operation's progress and wait until it completes.

    We must wait until completion becuase only one Cloud SQL operation can run
    at a time.

    Args:
        operation_id: Cloud SQL Operation ID.
    Returns:
        True if operation succeeded without errors, False if not.

    See here for details:
    https://cloud.google.com/sql/docs/postgres/admin-api/v1beta4/operations/get
    """
    operation_in_progress = True
    operation_success = False

    while operation_in_progress:
        get_operation = client().operations().get(
            project=metadata.project_id(), operation=operation_id)
        operation = get_operation.execute()
        operation_status = operation['status']

        if operation_status in {'PENDING', 'RUNNING', 'UNKNOWN'}:
            time.sleep(SECONDS_BETWEEN_OPERATION_STATUS_CHECKS)
        elif operation_status == 'DONE':
            operation_in_progress = False

        logging.debug("Operation [%s] status: [%s]",
                      operation_id, operation_status)

    if 'error' in operation:
        errors = operation['error'].get('errors', [])
        for error in errors:
            logging.error(
                "Operation %s finished with error: %s, %s\n%s",
                operation_id,
                error.get('kind'),
                error.get('code'),
                error.get('message'))
    else:
        logging.info("Operation [%s] succeeded.", operation_id)
        operation_success = True

    return operation_success


def export_table(table_name: str, export_query: str) -> bool:
    """Export a Cloud SQL table to a CSV file on GCS.

    Given a table name and export_query, retrieve the export URI from
    export_config, then execute the export operation and wait until it
    completes.

    Args:
        table_name: Table to export.
        export_query: Corresponding query for the table.
    Returns:
        True if operation succeeded without errors, False if not.
    """

    export_uri = export_config.gcs_export_uri(table_name)
    export_context = create_export_context(
        export_uri, export_query)

    project_id = metadata.project_id()
    instance_id = cloudsql_instance_id()
    export_request = client().instances().export(
        project=project_id,
        instance=instance_id,
        body=export_context)

    logging.info("Starting export: [%s]", str(export_request.to_json()))
    try:
        response = export_request.execute()
    except googleapiclient.errors.HttpError:
        logging.exception("Failed to export table [%s]", table_name)
        return False

    # We need to block until the operation is done because
    # the Cloud SQL API only supports one operation at a time.
    operation_id = response['name']
    logging.info("Waiting for export operation [%s] to complete for table [%s] "
                 "in database [%s] in project [%s]",
                 operation_id, table_name, instance_id, project_id)
    operation_success = wait_until_operation_finished(operation_id)

    return operation_success


def export_all_tables(tables: Tuple[sqlalchemy.Table, ...],
                      export_queries: Dict[str, str]):
    for table in tables:
        try:
            export_query = export_queries[table.name]
        except KeyError:
            logging.exception(
                "Unknown table name [%s]. Is it listed in "
                "the TABLES_TO_EXPORT for this module?", table.name)
            return

        export_table(table.name, export_query)
