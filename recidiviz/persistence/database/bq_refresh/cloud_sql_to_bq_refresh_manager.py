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

"""Export data from Cloud SQL and load it into BigQuery.

Run this export locally with the following command:
    python -m recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_manager
        --project_id [PROJECT_ID]
        --schema_type [STATE, JAILS, OPERATIONS]

"""

import uuid
import json
import logging

from datetime import datetime
from http import HTTPStatus

from typing import Tuple

import flask
from flask import request

from recidiviz.big_query.big_query_client import BigQueryClientImpl, BigQueryClient
from recidiviz.cloud_storage.gcs_pseudo_lock_manager import GCSPseudoLockManager, \
    GCS_TO_POSTGRES_INGEST_RUNNING_LOCK_NAME, POSTGRES_TO_BQ_EXPORT_RUNNING_LOCK_NAME, GCSPseudoLockAlreadyExists
from recidiviz.ingest.direct.direct_ingest_control import kick_all_schedulers
from recidiviz.persistence.database.bq_refresh import bq_refresh, cloud_sql_to_gcs_export
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import CloudSqlToBQConfig

from recidiviz.persistence.database.bq_refresh.bq_refresh_cloud_task_manager import \
    BQRefreshCloudTaskManager
from recidiviz.persistence.database.sqlalchemy_engine_manager import SchemaType
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils import pubsub_helper


def export_table_then_load_table(
        big_query_client: BigQueryClient,
        table: str,
        cloud_sql_to_bq_config: CloudSqlToBQConfig) -> None:
    """Exports a Cloud SQL table to CSV, then loads it into BigQuery.

    If a table excludes some region codes, it first loads all the GCS and the excluded region's data to a temp table.
    See for details: load_table_with_excluded_regions

    Waits until the BigQuery load is completed.

    Args:
        big_query_client: A BigQueryClient.
        table: Table to export then import. Table must be defined
            in the metadata_base class for its corresponding SchemaType.
        cloud_sql_to_bq_config: The config class for the given SchemaType.
    Returns:
        True if load succeeds, else False.
    """
    export_success = cloud_sql_to_gcs_export.export_table(table, cloud_sql_to_bq_config)

    if not export_success:
        raise ValueError(f"Failure to export CloudSQL table to GCS, skipping BigQuery load of table [{table}].")

    bq_refresh.refresh_bq_table_from_gcs_export_synchronous(big_query_client, table, cloud_sql_to_bq_config)


cloud_sql_to_bq_blueprint = flask.Blueprint('export_manager', __name__)


@cloud_sql_to_bq_blueprint.route('/refresh_bq_table', methods=['POST'])
@requires_gae_auth
def refresh_bq_table() -> Tuple[str, int]:
    """Worker function to handle BQ export task requests.

    Form data must be a bytes-encoded JSON object with parameters listed below.

    URL Parameters:
        table_name: Table to export then import. Table must be defined
            in one of the base schema types.
    """
    json_data = request.get_data(as_text=True)
    data = json.loads(json_data)
    table_name = data['table_name']
    schema_type_str = data['schema_type']

    try:
        schema_type = SchemaType(schema_type_str)
    except ValueError:
        return (f'Unknown schema type [{schema_type_str}]', HTTPStatus.BAD_REQUEST)

    bq_client = BigQueryClientImpl()
    cloud_sql_to_bq_config = CloudSqlToBQConfig.for_schema_type(schema_type)

    if cloud_sql_to_bq_config is None:
        logging.info("Cloud SQL to BQ is disabled for: %s", schema_type)
        return ('', HTTPStatus.OK)

    logging.info("Starting BQ export task for table: %s", table_name)

    export_table_then_load_table(bq_client, table_name, cloud_sql_to_bq_config)
    return ('', HTTPStatus.OK)


@cloud_sql_to_bq_blueprint.route('/monitor_refresh_bq_tasks', methods=['POST'])
@requires_gae_auth
def monitor_refresh_bq_tasks() -> Tuple[str, int]:
    """Worker function to publish a message to a Pub/Sub topic once all tasks in
    the BIGQUERY_QUEUE queue have completed.
    """
    json_data = request.get_data(as_text=True)
    data = json.loads(json_data)
    schema = data['schema']
    topic = data['topic']
    message = data['message']

    task_manager = BQRefreshCloudTaskManager()

    # If any of the tasks in the queue have task_name containing schema, consider BQ tasks in queue
    bq_tasks_in_queue = False
    bq_task_list = task_manager.get_bq_queue_info().task_names
    for task_name in bq_task_list:
        task_id = task_name[task_name.find('/tasks/'):]
        if schema in task_id:
            bq_tasks_in_queue = True

    # If there are BQ tasks in the queue, then re-queue this task in a minute
    if bq_tasks_in_queue:
        logging.info("Tasks still in bigquery queue. Re-queuing bq monitor"
                     " task.")
        task_manager.create_bq_refresh_monitor_task(schema, topic, message)
        return '', HTTPStatus.OK

    # Publish a message to the Pub/Sub topic once state BQ export is complete
    if topic:
        pubsub_helper.publish_message_to_topic(message=message, topic=topic)

    # Unlock export lock when all BQ exports complete
    lock_manager = GCSPseudoLockManager()
    lock_manager.unlock(postgres_to_bq_lock_name_for_schema(schema))
    logging.info('Done running export for %s, unlocking Postgres to BigQuery export', schema)

    # Kick scheduler to restart ingest
    kick_all_schedulers()

    return ('', HTTPStatus.OK)


@cloud_sql_to_bq_blueprint.route('/create_refresh_bq_tasks/<schema_arg>')
@requires_gae_auth
def wait_for_ingest_to_create_tasks(schema_arg: str) -> Tuple[str, HTTPStatus]:
    """Worker function to wait until ingest is not running to create_all_bq_refresh_tasks_for_schema.
    When ingest is not running/locked, creates task to create_all_bq_refresh_tasks_for_schema.
    When ingest is running/locked, re-enqueues this task to run again in 60 seconds.
    """
    task_manager = BQRefreshCloudTaskManager()
    lock_manager = GCSPseudoLockManager()
    json_data_text = request.get_data(as_text=True)
    try:
        json_data = json.loads(json_data_text)
    except (TypeError, json.decoder.JSONDecodeError):
        json_data = {}
    if 'lock_id' not in json_data:
        lock_id = str(uuid.uuid4())
    else:
        lock_id = json_data['lock_id']

    if not lock_manager.is_locked(postgres_to_bq_lock_name_for_schema(schema_arg)):
        time = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        contents_as_json = {"time": time, "lock_id": lock_id}
        contents = json.dumps(contents_as_json)
        lock_manager.lock(postgres_to_bq_lock_name_for_schema(schema_arg), contents)
    else:
        contents = lock_manager.get_lock_contents(postgres_to_bq_lock_name_for_schema(schema_arg))
        try:
            contents_json = json.loads(contents)
        except (TypeError, json.decoder.JSONDecodeError):
            contents_json = {}
        if 'lock_id' not in contents_json or lock_id is not contents_json['lock_id']:
            raise GCSPseudoLockAlreadyExists(f"Pseudo export lock already exists with UUID {lock_id}")

    no_regions_running = lock_manager.no_active_locks_with_prefix(GCS_TO_POSTGRES_INGEST_RUNNING_LOCK_NAME)
    if not no_regions_running:
        logging.info('Regions running, renqueuing this task.')
        task_id = '{}-{}-{}'.format(
            'renqueue_wait_task',
            str(datetime.utcnow().date()),
            uuid.uuid4())
        body = {'schema_type': schema_arg, 'lock_id': lock_id}
        task_manager.job_monitor_cloud_task_queue_manager.create_task(
            task_id=task_id,
            body=body,
            relative_uri=f'/cloud_sql_to_bq/create_refresh_bq_tasks/{schema_arg}',
            schedule_delay_seconds=60
        )
        return '', HTTPStatus.OK
    logging.info('No regions running, calling create_refresh_bq_tasks')
    create_all_bq_refresh_tasks_for_schema(schema_arg)
    return '', HTTPStatus.OK


def create_all_bq_refresh_tasks_for_schema(schema_arg: str) -> None:
    """Creates an export task for each table to be exported.

    A task is created for each table defined in the schema.

    Re-creates all tasks if any task fails to be created.
    """
    try:
        schema_type = SchemaType(schema_arg.upper())
    except ValueError:
        return

    logging.info("Beginning BQ export for %s schema tables.", schema_type.value)

    task_manager = BQRefreshCloudTaskManager()

    cloud_sql_to_bq_config = CloudSqlToBQConfig.for_schema_type(schema_type)
    if cloud_sql_to_bq_config is None:
        logging.info("Cloud SQL to BQ is disabled for: %s", schema_type)
        return

    for table in cloud_sql_to_bq_config.get_tables_to_export():
        task_manager.create_refresh_bq_table_task(table.name, schema_type)

    if schema_type is SchemaType.STATE:
        pub_sub_topic = 'v1.calculator.trigger_daily_pipelines'
        pub_sub_message = 'State export to BQ complete'
    else:
        pub_sub_topic = ''
        pub_sub_message = ''

    task_manager.create_bq_refresh_monitor_task(schema_type.value, pub_sub_topic, pub_sub_message)


def postgres_to_bq_lock_name_for_schema(schema: str) -> str:
    return POSTGRES_TO_BQ_EXPORT_RUNNING_LOCK_NAME + schema.upper()
