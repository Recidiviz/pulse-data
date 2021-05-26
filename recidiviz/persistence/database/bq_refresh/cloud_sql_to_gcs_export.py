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
from typing import Dict, Any
from http import HTTPStatus

import googleapiclient.errors

from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqladmin_client import sqladmin_client
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.utils import metadata


SECONDS_BETWEEN_OPERATION_STATUS_CHECKS = 3

# TODO(#7397): Delete this whole file once federated export ships to production.
def create_export_context(
    schema_type: SchemaType, export_uri: str, export_query: str
) -> dict:
    """Creates the exportContext configuration for the export operation.

    See here for details:
    https://cloud.google.com/sql/docs/postgres/admin-api/v1beta4/instances/export

    Args:
        schema_type: The SchemaType of the table being exported.
        export_uri: GCS URI to write the exported CSV data to.
        export_query: SQL query defining the data to be exported.

    Returns:
        export_context dict which can be passed to client.instances.export().
    """
    if schema_type == SchemaType.STATE:
        # TODO(#6078): Wire the state_code through to this function (or just pass a full
        #  SQLAlchemyDatabaseKey) so we can pick the non-legacy 'postgres' instance.
        database_key = SQLAlchemyDatabaseKey(SchemaType.STATE)
    else:
        database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)

    export_context = {
        "exportContext": {
            "kind": "sql#exportContext",
            "fileType": "CSV",
            "uri": export_uri,
            "databases": [database_key.db_name],
            "csvExportOptions": {"selectQuery": export_query},
        }
    }

    return export_context


def wait_until_operation_finished(operation_id: str) -> bool:
    """Monitor a Cloud SQL operation's progress and wait until it completes.

    We must wait until completion because only one Cloud SQL operation can run
    at a time.

    It's possible to get an HTTPError 404 from the `operations.get()` endpoint if the operation has been created, but
    not started yet. To mitigate this possible timing issue, we retry getting the operation instance up to 3 times
    before raising the error.

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
        operation = get_operation_with_retries(operation_id)
        operation_status = operation["status"]

        # See: https://cloud.google.com/sql/docs/postgres/admin-api/rest/v1beta4/operations#SqlOperationStatus
        if operation_status in {
            "PENDING",
            "RUNNING",
            "UNKNOWN",
            "SQL_OPERATION_STATUS_UNSPECIFIED",
        }:
            time.sleep(SECONDS_BETWEEN_OPERATION_STATUS_CHECKS)
        elif operation_status == "DONE":
            operation_in_progress = False

        logging.debug("Operation [%s] status: [%s]", operation_id, operation_status)

        # An operation object may also include an error property.
        if "error" in operation:
            errors = operation["error"].get("errors", [])
            for error in errors:
                logging.error(
                    "Operation %s finished with error: %s, %s\n%s",
                    operation_id,
                    error.get("kind"),
                    error.get("code"),
                    error.get("message"),
                )
        else:
            logging.info("Operation [%s] succeeded.", operation_id)
            operation_success = True

    return operation_success


def get_operation_with_retries(operation_id: str) -> Dict[str, Any]:
    num_retries = 3
    while num_retries > 0:
        # We need to guard here for possible 404 HttpErrors if the operation hasn't started yet
        try:
            get_operation = (
                sqladmin_client()
                .operations()
                .get(project=metadata.project_id(), operation=operation_id)
            )
            return get_operation.execute()
        except googleapiclient.errors.HttpError as error:
            # If we get a 404 HttpError, wait a few seconds and then retry getting the operation instance.
            if error.resp.status == HTTPStatus.NOT_FOUND and num_retries > 0:
                logging.debug(
                    "HttpError when requesting operation_id [%s]. Retrying request: %s",
                    operation_id,
                    num_retries,
                )
                time.sleep(SECONDS_BETWEEN_OPERATION_STATUS_CHECKS)
                num_retries -= 1
            else:
                raise
    raise ValueError("Operation not set, request for the operation failed.")


def export_table(table_name: str, cloud_sql_to_bq_config: CloudSqlToBQConfig) -> bool:
    """Export a Cloud SQL table to a CSV file on GCS.

    Given a table name and export_query, retrieve the export URI from
    cloud_sql_to_bq_config, then execute the export operation and wait until it
    completes.

    Args:
        table_name: Table to export.
        cloud_sql_to_bq_config: The export config class for the table's SchemaType.
    Returns:
        True if operation succeeded without errors, False if not.
    """
    schema_type = cloud_sql_to_bq_config.schema_type
    export_query = cloud_sql_to_bq_config.get_table_export_query(table_name)
    export_uri = cloud_sql_to_bq_config.get_gcs_export_uri_for_table(table_name)

    export_context = create_export_context(schema_type, export_uri, export_query)

    project_id = metadata.project_id()
    instance_id = SQLAlchemyEngineManager.get_stripped_cloudsql_instance_id(schema_type)
    export_request = (
        sqladmin_client()
        .instances()
        .export(project=project_id, instance=instance_id, body=export_context)
    )

    logging.info("GCS URI [%s] in project [%s]", export_uri, project_id)
    logging.info("Starting export: [%s]", str(export_request.to_json()))

    try:
        response = export_request.execute()
    except googleapiclient.errors.HttpError:
        logging.exception("Failed to export table [%s]", table_name)
        return False

    # We need to block until the operation is done because
    # the Cloud SQL API only supports one operation at a time.
    operation_id = response["name"]
    logging.info(
        "Waiting for export operation [%s] to complete for table [%s] "
        "in database [%s] in project [%s]",
        operation_id,
        table_name,
        instance_id,
        project_id,
    )
    operation_success = wait_until_operation_finished(operation_id)

    return operation_success


def export_all_tables(cloud_sql_to_bq_config: CloudSqlToBQConfig) -> None:
    tables = cloud_sql_to_bq_config.get_tables_to_export()
    for table in tables:
        export_table(table.name, cloud_sql_to_bq_config)
