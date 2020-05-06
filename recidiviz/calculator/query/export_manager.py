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

"""Export data from Cloud SQL and load it into BigQuery."""
from http import HTTPStatus
import json
import logging

import flask
from flask import request
# Importing only for typing.
from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.calculator.query import export_config, cloudsql_export, bq_load
from recidiviz.calculator.query.bq_export_cloud_task_manager import \
    BQExportCloudTaskManager
from recidiviz.calculator.query.county import dataset_config as county_dataset_config
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.persistence.database.sqlalchemy_engine_manager import SchemaType
from recidiviz.utils.auth import authenticate_request
from recidiviz.utils import pubsub_helper


def export_table_then_load_table(
        table: str,
        dataset_ref: bigquery.dataset.DatasetReference,
        schema_type: SchemaType) -> bool:
    """Exports a Cloud SQL table to CSV, then loads it into BigQuery.

    Waits until the BigQuery load is completed.

    Args:
        table: Table to export then import. Table must be defined
            in the TABLES_TO_EXPORT for its corresponding schema.
        dataset_ref: The BigQuery dataset to load the table into.
            Gets created if it does not already exist.
        schema_type: The schema, either SchemaType.COUNTY or SchemaType.STATE
            where this table lives.
    Returns:
        True if load succeeds, else False.
    """
    if schema_type == SchemaType.JAILS:
        export_queries = export_config.COUNTY_TABLE_EXPORT_QUERIES
    elif schema_type == SchemaType.STATE:
        export_queries = export_config.STATE_TABLE_EXPORT_QUERIES
    else:
        logging.error("Unknown schema_type: %s", schema_type)
        return False

    try:
        export_query = export_queries[table]
    except KeyError:
        logging.exception(
            "Unknown table name [%s]. Is it listed in "
            "the TABLES_TO_EXPORT for the %s schema_type?", table, schema_type)
        return False

    export_success = cloudsql_export.export_table(schema_type,
                                                  table,
                                                  export_query)
    if export_success: # pylint: disable=no-else-return
        load_success = bq_load.start_table_load_and_wait(dataset_ref, table,
                                                         schema_type)
        return load_success
    else:
        logging.error("Skipping BigQuery load of table [%s], "
                      "which failed to export.", table)
        return False


def export_then_load_all_sequentially(schema_type: SchemaType):
    """Exports then loads each table sequentially.

    No operations for a new table happen until all operations for
    the previous table have completed.

    For example, for Tables A, B, C:
    1. Export Table A
    2. Load Table A
    3. Export Table B
    4. Load Table B
    5. Export Table C
    6. Load Table C

    There is no reason to load sequentially, but we must export sequentially
    because Cloud SQL can only support one export operation at a time.
    """

    bq_client = BigQueryClientImpl()
    if schema_type == SchemaType.JAILS:
        tables_to_export = export_config.COUNTY_TABLES_TO_EXPORT
        dataset_ref = bq_client.dataset_ref_for_id(county_dataset_config.COUNTY_BASE_DATASET)
    elif schema_type == SchemaType.STATE:
        tables_to_export = export_config.STATE_TABLES_TO_EXPORT
        dataset_ref = bq_client.dataset_ref_for_id(state_dataset_config.STATE_BASE_DATASET)
    else:
        logging.error("Invalid schema_type requested. Must be either"
                      " SchemaType.JAILS or SchemaType.STATE.")
        return

    for table in tables_to_export:
        export_table_then_load_table(table.name, dataset_ref, schema_type)


def export_all_then_load_all(schema_type: SchemaType):
    """Export all tables from Cloud SQL in the given schema, then load all
    tables to BigQuery.

    Exports happen in sequence (one at a time),
    then once all exports are completed, the BigQuery loads happen in parallel.

    For example, for tables A, B, C:
    1. Export Table A
    2. Export Table B
    3. Export Table C
    4. Load Tables A, B, C in parallel.
    """

    bq_client = BigQueryClientImpl()
    if schema_type == SchemaType.JAILS:
        tables_to_export = export_config.COUNTY_TABLES_TO_EXPORT
        base_tables_dataset_ref = bq_client.dataset_ref_for_id(county_dataset_config.COUNTY_BASE_DATASET)
        export_queries = export_config.COUNTY_TABLE_EXPORT_QUERIES
    elif schema_type == SchemaType.STATE:
        tables_to_export = export_config.STATE_TABLES_TO_EXPORT
        base_tables_dataset_ref = bq_client.dataset_ref_for_id(state_dataset_config.STATE_BASE_DATASET)
        export_queries = export_config.STATE_TABLE_EXPORT_QUERIES
    else:
        logging.error("Invalid schema_type requested. Must be either"
                      " SchemaType.JAILS or SchemaType.STATE.")
        return

    logging.info("Beginning CloudSQL export")
    cloudsql_export.export_all_tables(schema_type,
                                      tables_to_export,
                                      export_queries)

    logging.info("Beginning BQ table load")
    bq_load.load_all_tables_concurrently(
        base_tables_dataset_ref, tables_to_export, schema_type)


export_manager_blueprint = flask.Blueprint('export_manager', __name__)


@export_manager_blueprint.route('/export', methods=['POST'])
@authenticate_request
def handle_bq_export_task():
    """Worker function to handle BQ export task requests.

    Form data must be a bytes-encoded JSON object with parameters listed below.

    URL Parameters:
        table_name: Table to export then import. Table must be defined
            in export_config.COUNTY_TABLES_TO_EXPORT.
    """
    json_data = request.get_data(as_text=True)
    data = json.loads(json_data)
    table_name = data['table_name']
    schema_type_str = data['schema_type']

    bq_client = BigQueryClientImpl()
    if schema_type_str == SchemaType.JAILS.value:
        schema_type = SchemaType.JAILS
        dataset_ref = bq_client.dataset_ref_for_id(county_dataset_config.COUNTY_BASE_DATASET)
    elif schema_type_str == SchemaType.STATE.value:
        schema_type = SchemaType.STATE
        dataset_ref = bq_client.dataset_ref_for_id(state_dataset_config.STATE_BASE_DATASET)
    else:
        return '', HTTPStatus.INTERNAL_SERVER_ERROR

    logging.info("Starting BQ export task for table: %s", table_name)

    success = export_table_then_load_table(table_name, dataset_ref, schema_type)

    return ('', HTTPStatus.OK if success else HTTPStatus.INTERNAL_SERVER_ERROR)


@export_manager_blueprint.route('/bq_monitor', methods=['POST'])
@authenticate_request
def handle_bq_monitor_task():
    """Worker function to publish a message to a Pub/Sub topic once all tasks in
    the BIGQUERY_QUEUE queue have completed.
    """
    json_data = request.get_data(as_text=True)
    data = json.loads(json_data)
    topic = data['topic']
    message = data['message']

    task_manager = BQExportCloudTaskManager()

    bq_tasks_in_queue = task_manager.get_bq_queue_info().size() > 0

    # If there are BQ tasks in the queue, then re-queue this task in a minute
    if bq_tasks_in_queue:
        logging.info("Tasks still in bigquery queue. Re-queuing bq monitor"
                     " task.")
        task_manager.create_bq_monitor_task(topic, message)
        return ('', HTTPStatus.OK)

    # Publish a message to the Pub/Sub topic once all BQ exports are complete
    pubsub_helper.publish_message_to_topic(message=message, topic=topic)

    return ('', HTTPStatus.OK)


@export_manager_blueprint.route('/create_export_tasks')
@authenticate_request
def create_all_bq_export_tasks():
    """Creates an export task for each table to be exported.

    A task is created for each table defined in
    export_config.COUNTY_TABLES_TO_EXPORT.

    Re-creates all tasks if any task fails to be created.
    """
    schema_type_str = SchemaType.JAILS.value

    logging.info("Beginning BQ export for jails schema tables.")

    task_manager = BQExportCloudTaskManager()
    for table in export_config.COUNTY_TABLES_TO_EXPORT:
        task_manager.create_bq_task(table.name, schema_type_str)
    return ('', HTTPStatus.OK)


@export_manager_blueprint.route('/create_state_export_tasks')
@authenticate_request
def create_all_state_bq_export_tasks():
    """Creates an export task for each table to be exported.

    A task is created for each table defined in
    export_config.STATE_TABLES_TO_EXPORT.

    Re-creates all tasks if any task fails to be created.
    """
    schema_type_str = SchemaType.STATE.value

    logging.info("Beginning BQ export for state schema tables.")

    task_manager = BQExportCloudTaskManager()
    for table in export_config.STATE_TABLES_TO_EXPORT:
        task_manager.create_bq_task(table.name, schema_type_str)

    pub_sub_topic = 'v1.calculator.recidivism'
    pub_sub_message = 'State export to BQ complete'
    task_manager.create_bq_monitor_task(pub_sub_topic, pub_sub_message)
    return ('', HTTPStatus.OK)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    local_export_schema_type = SchemaType.STATE

    export_all_then_load_all(local_export_schema_type)
