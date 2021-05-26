# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Endpoints and control logic for the CloudSQL -> BigQuery refresh."""
import json
import logging
import uuid
from http import HTTPStatus
from typing import Tuple

import flask
from flask import request

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_storage.gcs_pseudo_lock_manager import GCSPseudoLockDoesNotExist
from recidiviz.ingest.direct.direct_ingest_control import kick_all_schedulers
from recidiviz.persistence.database.bq_refresh.bq_refresh_cloud_task_manager import (
    BQRefreshCloudTaskManager,
)
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_lock_manager import (
    CloudSqlToBQLockManager,
)
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_runner import (
    export_table_then_load_table,
)
from recidiviz.persistence.database.bq_refresh.federated_cloud_sql_to_bq_refresh import (
    federated_bq_schema_refresh,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.utils import pubsub_helper
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils import environment

cloud_sql_to_bq_blueprint = flask.Blueprint("export_manager", __name__)


@cloud_sql_to_bq_blueprint.route("/refresh_bq_table", methods=["POST"])
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
    table_name = data["table_name"]
    schema_type_str = data["schema_type"]

    try:
        schema_type = SchemaType(schema_type_str.upper())
    except ValueError:
        return (f"Unknown schema type [{schema_type_str}]", HTTPStatus.BAD_REQUEST)

    if not CloudSqlToBQConfig.is_valid_schema_type(schema_type):
        return (
            f"Unsuppported schema type: [{schema_type}]",
            HTTPStatus.BAD_REQUEST,
        )

    bq_client = BigQueryClientImpl()

    logging.info("Starting BQ export task for table: %s", table_name)

    export_table_then_load_table(bq_client, table_name, schema_type)
    return ("", HTTPStatus.OK)


@cloud_sql_to_bq_blueprint.route("/monitor_refresh_bq_tasks", methods=["POST"])
@requires_gae_auth
def monitor_refresh_bq_tasks() -> Tuple[str, int]:
    """Worker function to publish a message to a Pub/Sub topic once all tasks in
    the BIGQUERY_QUEUE queue have completed.
    """
    json_data = request.get_data(as_text=True)
    data = json.loads(json_data)
    schema = data["schema"]
    topic = data["topic"]
    message = data["message"]

    try:
        schema_type = SchemaType(schema.upper())
    except ValueError:
        return (f"Unknown schema type [{schema}]", HTTPStatus.BAD_REQUEST)

    task_manager = BQRefreshCloudTaskManager()

    # If any of the tasks in the queue have task_name containing schema, consider BQ tasks in queue
    bq_tasks_in_queue = False
    bq_task_list = task_manager.get_bq_queue_info().task_names
    for task_name in bq_task_list:
        task_id = task_name[task_name.find("/tasks/") :]
        if schema in task_id:
            bq_tasks_in_queue = True

    # If there are BQ tasks in the queue, then re-queue this task in a minute
    if bq_tasks_in_queue:
        logging.info("Tasks still in bigquery queue. Re-queuing bq monitor" " task.")
        task_manager.create_bq_refresh_monitor_task(schema, topic, message)
        # TODO(#6257): Add an expiration refresh to our lock in case the total export
        # takes more than an hour.
        return "", HTTPStatus.OK

    # Publish a message to the Pub/Sub topic once state BQ export is complete
    if topic:
        pubsub_helper.publish_message_to_topic(message=message, topic=topic)

    # Unlock export lock when all BQ exports complete
    lock_manager = CloudSqlToBQLockManager()
    lock_manager.release_lock(schema_type)
    logging.info(
        "Done running export for %s, unlocking Postgres to BigQuery export", schema
    )

    # Kick scheduler to restart ingest
    kick_all_schedulers()

    return ("", HTTPStatus.OK)


@cloud_sql_to_bq_blueprint.route(
    "/create_refresh_bq_tasks/<schema_arg>", methods=["GET", "POST"]
)
@requires_gae_auth
def wait_for_ingest_to_create_tasks(schema_arg: str) -> Tuple[str, HTTPStatus]:
    """Worker function to wait until ingest is not running to create_all_bq_refresh_tasks_for_schema.
    When ingest is not running/locked, creates task to create_all_bq_refresh_tasks_for_schema.
    When ingest is running/locked, re-enqueues this task to run again in 60 seconds.
    """
    try:
        schema_type = SchemaType(schema_arg.upper())
    except ValueError:
        return (
            f"Unexpected value for schema_arg: [{schema_arg}]",
            HTTPStatus.BAD_REQUEST,
        )
    if not CloudSqlToBQConfig.is_valid_schema_type(schema_type):
        return (
            f"Unsuppported schema type: [{schema_type}]",
            HTTPStatus.BAD_REQUEST,
        )

    lock_id = get_or_create_lock_id()
    logging.info("Request lock id: %s", lock_id)

    lock_manager = CloudSqlToBQLockManager()
    lock_manager.acquire_lock(schema_type=schema_type, lock_id=lock_id)

    task_manager = BQRefreshCloudTaskManager()
    if not lock_manager.can_proceed(schema_type):
        logging.info("Regions running, renqueuing this task.")
        task_manager.create_reattempt_create_refresh_tasks_task(
            lock_id=lock_id, schema=schema_arg
        )
        return "", HTTPStatus.OK

    logging.info("No regions running, triggering BQ refresh.")
    if environment.in_gcp_production():
        # TODO(#7397): Ship federated export to production and delete this code.
        create_all_bq_refresh_tasks_for_schema(schema_type)
    else:
        task_manager.create_refresh_bq_schema_task(schema_type=schema_type)
    return "", HTTPStatus.OK


# TODO(#7397): Ship federated export to production and delete this code.
def create_all_bq_refresh_tasks_for_schema(schema_type: SchemaType) -> None:
    """Creates an export task for each table to be exported.

    A task is created for each table defined in the schema.

    Re-creates all tasks if any task fails to be created.
    """
    logging.info("Beginning BQ export for %s schema tables.", schema_type.value)

    task_manager = BQRefreshCloudTaskManager()

    cloud_sql_to_bq_config = CloudSqlToBQConfig.for_schema_type(schema_type)

    for table in cloud_sql_to_bq_config.get_tables_to_export():
        task_manager.create_refresh_bq_table_task(table.name, schema_type)

    if schema_type is SchemaType.STATE:
        pub_sub_topic = "v1.calculator.trigger_daily_pipelines"
        pub_sub_message = "State export to BQ complete"
    else:
        pub_sub_topic = ""
        pub_sub_message = ""

    task_manager.create_bq_refresh_monitor_task(
        schema_type.value, pub_sub_topic, pub_sub_message
    )


@cloud_sql_to_bq_blueprint.route(
    "/refresh_bq_schema/<schema_arg>", methods=["GET", "POST"]
)
@requires_gae_auth
def refresh_bq_schema(schema_arg: str) -> Tuple[str, HTTPStatus]:
    """Performs a full refresh of BigQuery data for a given schema, pulling data from
    the appropriate CloudSQL Postgres instance.

    On completion, triggers Dataflow pipelines (when necessary), releases the refresh
    lock and restarts any paused ingest work.
    """
    try:
        schema_type = SchemaType(schema_arg.upper())
    except ValueError:
        return (
            f"Unexpected value for schema_arg: [{schema_arg}]",
            HTTPStatus.BAD_REQUEST,
        )
    if not CloudSqlToBQConfig.is_valid_schema_type(schema_type):
        return (
            f"Unsuppported schema type: [{schema_type}]",
            HTTPStatus.BAD_REQUEST,
        )

    lock_manager = CloudSqlToBQLockManager()

    try:
        can_proceed = lock_manager.can_proceed(schema_type)
    except GCSPseudoLockDoesNotExist as e:
        logging.exception(e)
        return (
            f"Expected lock for [{schema_arg}] BQ refresh to already exist.",
            HTTPStatus.EXPECTATION_FAILED,
        )

    if not can_proceed:
        return (
            f"Expected to be able to proceed with refresh before this endpoint was "
            f"called for [{schema_arg}].",
            HTTPStatus.EXPECTATION_FAILED,
        )

    federated_bq_schema_refresh(schema_type=schema_type)

    # Publish a message to the Pub/Sub topic once state BQ export is complete
    if schema_type is SchemaType.STATE:
        pubsub_helper.publish_message_to_topic(
            message="State export to BQ complete",
            topic="v1.calculator.trigger_daily_pipelines",
        )

    # Unlock export lock when all BQ exports complete
    lock_manager = CloudSqlToBQLockManager()
    lock_manager.release_lock(schema_type)
    logging.info(
        "Done running refresh for [%s], unlocking Postgres to BigQuery export",
        schema_type.value,
    )

    # Kick scheduler to restart ingest
    kick_all_schedulers()

    return "", HTTPStatus.OK


def get_or_create_lock_id() -> str:
    json_data_text = request.get_data(as_text=True)
    try:
        json_data = json.loads(json_data_text)
    except (TypeError, json.decoder.JSONDecodeError):
        json_data = {}
    if "lock_id" not in json_data:
        lock_id = str(uuid.uuid4())
    else:
        lock_id = json_data["lock_id"]

    return lock_id
