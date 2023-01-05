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
import datetime
import json
import logging
import time
import uuid
from http import HTTPStatus
from typing import Optional, Tuple

import flask
from flask import request

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.rematerialization_success_persister import (
    RefreshBQDatasetSuccessPersister,
)
from recidiviz.calculator.pipeline.pipeline_type import MetricPipelineRunType
from recidiviz.cloud_functions.cloudsql_to_bq_refresh_utils import (
    PIPELINE_RUN_TYPE_REQUEST_ARG,
)
from recidiviz.cloud_storage.gcs_pseudo_lock_manager import GCSPseudoLockDoesNotExist
from recidiviz.cloud_tasks.utils import get_current_cloud_task_id
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
from recidiviz.persistence.database.bq_refresh.federated_cloud_sql_to_bq_refresh import (
    federated_bq_schema_refresh,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.utils.auth.gae import requires_gae_auth

cloud_sql_to_bq_blueprint = flask.Blueprint("export_manager", __name__)

# TODO(#15930): This function will be deleted once the all dependencies on this endpoint are moved to the DAG
@cloud_sql_to_bq_blueprint.route(
    "/create_refresh_bq_schema_task/<schema_arg>", methods=["GET", "POST"]
)
@requires_gae_auth
def wait_for_ingest_to_create_tasks(
    schema_arg: str,
) -> Tuple[str, HTTPStatus]:
    """Worker function to wait until ingest is not running to queue a task to run
    /refresh_bq_schema. Before doing anything, grabs the refresh lock to indicate that
    a refresh wants to start and ingest should yield ASAP. Then:
    * When ingest is not running/locked, creates task to run /refresh_bq_schema.
    * When ingest is running/locked, re-enqueues this task to run again in 60 seconds.
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
    if schema_type == SchemaType.STATE:
        return (
            f"Schema type should only be refreshed through DAG: [{schema_type}]",
            HTTPStatus.BAD_REQUEST,
        )

    pipeline_run_type_arg = get_value_from_request(PIPELINE_RUN_TYPE_REQUEST_ARG)

    if pipeline_run_type_arg:
        try:
            _ = MetricPipelineRunType(pipeline_run_type_arg.upper())
        except ValueError:
            return (
                f"Unexpected value for pipeline_run_type: [{pipeline_run_type_arg}]",
                HTTPStatus.BAD_REQUEST,
            )

    lock_id = get_value_from_request("lock_id", str(uuid.uuid4()))
    if not lock_id:
        raise ValueError(
            "Response from get_value_from_request() should not return "
            "None when provided with a default value."
        )
    logging.info("Request lock id: %s", lock_id)

    lock_manager = CloudSqlToBQLockManager()
    lock_manager.acquire_lock(schema_type=schema_type, lock_id=lock_id)

    task_manager = BQRefreshCloudTaskManager()
    if not lock_manager.can_proceed(schema_type):
        logging.info("Regions running, re-enqueuing this task.")
        task_manager.create_reattempt_create_refresh_tasks_task(
            lock_id=lock_id,
            schema=schema_arg,
            pipeline_run_type=pipeline_run_type_arg,
        )
        return "", HTTPStatus.OK

    logging.info("No regions running, triggering BQ refresh.")
    task_manager.create_refresh_bq_schema_task(
        schema_type=schema_type,
        pipeline_run_type=pipeline_run_type_arg,
    )
    return "", HTTPStatus.OK


# TODO(#15930): This function will be deleted once the all dependencies on this endpoint are moved to the DAG
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
            f"Unsupported schema type: [{schema_type}]",
            HTTPStatus.BAD_REQUEST,
        )
    if schema_type == SchemaType.STATE:
        return (
            f"Schema type should only be refreshed through DAG: [{schema_type}]",
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

    # Unlock export lock when all BQ exports complete
    lock_manager = CloudSqlToBQLockManager()
    lock_manager.release_lock(schema_type)
    logging.info(
        "Done running refresh for [%s], unlocking Postgres to BigQuery export",
        schema_type.value,
    )

    # Kick scheduler to restart ingest
    if schema_type in {SchemaType.STATE, SchemaType.OPERATIONS}:
        kick_all_schedulers()

    return "", HTTPStatus.OK


@cloud_sql_to_bq_blueprint.route("/acquire_lock/<schema_arg>", methods=["GET", "POST"])
@requires_gae_auth
def acquire_lock(schema_arg: str) -> Tuple[str, HTTPStatus]:
    """
    Creates a refresh lock for a given schema type. Must provide lock_id in request.
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

    lock_id = get_value_from_request("lock_id")
    if not lock_id:
        raise ValueError("Need to provide lock_id in request.")
    logging.info("Request lock id: %s", lock_id)

    lock_manager = CloudSqlToBQLockManager()
    lock_manager.acquire_lock(schema_type=schema_type, lock_id=lock_id)

    return "", HTTPStatus.OK


@cloud_sql_to_bq_blueprint.route(
    "/check_can_refresh_proceed/<schema_arg>", methods=["GET", "POST"]
)
@requires_gae_auth
def check_can_refresh_proceed(schema_arg: str) -> Tuple[str, HTTPStatus]:
    """
    Checks if all other processes that talk to the Postgres DB have stopped so the refresh can proceed. The refresh lock must already have been grabbed via /acquire_lock.
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
            f"Unsupported schema type: [{schema_type}]",
            HTTPStatus.BAD_REQUEST,
        )

    lock_manager = CloudSqlToBQLockManager()

    try:
        can_proceed = lock_manager.can_proceed(schema_type)
    except GCSPseudoLockDoesNotExist as e:
        logging.exception(e)
        # Since this endpoint is being called in the context of an Airflow DAG,
        # the DAG should have already acquired a lock before invoking this endpoint.
        return (
            f"Expected lock for [{schema_arg}] BQ refresh to already exist.",
            HTTPStatus.EXPECTATION_FAILED,
        )

    return str(can_proceed), HTTPStatus.OK


@cloud_sql_to_bq_blueprint.route("/release_lock/<schema_arg>", methods=["GET", "POST"])
@requires_gae_auth
def release_lock(schema_arg: str) -> Tuple[str, HTTPStatus]:
    """
    Releases refresh lock for a given schema type.
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

    # Unlock export lock when all BQ exports complete
    lock_manager = CloudSqlToBQLockManager()
    lock_manager.release_lock(schema_type)

    return "", HTTPStatus.OK


@cloud_sql_to_bq_blueprint.route(
    "/refresh_bq_dataset/<schema_arg>", methods=["GET", "POST"]
)
@requires_gae_auth
def refresh_bq_dataset(schema_arg: str) -> Tuple[str, HTTPStatus]:
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
            f"Unsupported schema type: [{schema_type}]",
            HTTPStatus.BAD_REQUEST,
        )

    lock_manager = CloudSqlToBQLockManager()

    try:
        can_proceed = lock_manager.can_proceed(schema_type)
    except GCSPseudoLockDoesNotExist as e:
        logging.exception(e)
        # Since this endpoint is being called in the context of an Airflow DAG,
        # the DAG should have already acquired a lock before invoking this endpoint.
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

    dry_run = request.args.get("dry_run", default=False, type=bool)
    start = datetime.datetime.now()

    # TODO(#11437): `dry_run` will go away once development of the DAG integration is complete
    if dry_run:
        time.sleep(10)
    else:
        federated_bq_schema_refresh(schema_type=schema_type)

    logging.info(
        "Done running refresh for [%s], unlocking Postgres to BigQuery export",
        schema_type.value,
    )

    end = datetime.datetime.now()
    runtime_sec = int((end - start).total_seconds())

    success_persister = RefreshBQDatasetSuccessPersister(bq_client=BigQueryClientImpl())
    success_persister.record_success_in_bq(
        schema_type=schema_type,
        runtime_sec=runtime_sec,
        cloud_task_id=get_current_cloud_task_id(),
    )

    logging.info("Finished saving success record to database.")

    return "", HTTPStatus.OK


def get_value_from_request(
    key: str, default_value: Optional[str] = None
) -> Optional[str]:
    json_data_text = request.get_data(as_text=True)
    try:
        json_data = json.loads(json_data_text)
    except (TypeError, json.decoder.JSONDecodeError):
        json_data = {}

    return json_data.get(key, default_value)
