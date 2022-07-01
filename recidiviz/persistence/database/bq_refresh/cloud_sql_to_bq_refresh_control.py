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
from typing import Optional, Tuple

import flask
from flask import request

from recidiviz.calculator.pipeline.pipeline_type import MetricPipelineRunType
from recidiviz.cloud_functions.cloudsql_to_bq_refresh_utils import (
    PIPELINE_RUN_TYPE_NONE_VALUE,
    PIPELINE_RUN_TYPE_REQUEST_ARG,
    UPDATE_MANAGED_VIEWS_REQUEST_ARG,
)
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
from recidiviz.persistence.database.bq_refresh.federated_cloud_sql_to_bq_refresh import (
    federated_bq_schema_refresh,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.utils import metadata
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.composer import trigger_dag

cloud_sql_to_bq_blueprint = flask.Blueprint("export_manager", __name__)


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

    pipeline_run_type_arg = get_value_from_request(PIPELINE_RUN_TYPE_REQUEST_ARG)
    update_managed_views_arg = get_value_from_request(UPDATE_MANAGED_VIEWS_REQUEST_ARG)

    logging.info("Update managed views arg in request: [%s]", update_managed_views_arg)

    if not pipeline_run_type_arg and schema_type == SchemaType.STATE:
        # If no pipeline_run_type_arg is specified for the STATE schema, then we default
        # to the incremental run type
        pipeline_run_type_arg = MetricPipelineRunType.INCREMENTAL.value

    if pipeline_run_type_arg and pipeline_run_type_arg != PIPELINE_RUN_TYPE_NONE_VALUE:
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
            update_managed_views=update_managed_views_arg,
        )
        return "", HTTPStatus.OK

    logging.info("No regions running, triggering BQ refresh.")
    task_manager.create_refresh_bq_schema_task(
        schema_type=schema_type,
        pipeline_run_type=pipeline_run_type_arg,
        update_managed_views=update_managed_views_arg,
    )
    return "", HTTPStatus.OK


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

    if schema_type is SchemaType.STATE:
        update_managed_views_arg = get_value_from_request(
            UPDATE_MANAGED_VIEWS_REQUEST_ARG
        )

        json_data_text = request.get_data(as_text=True)
        logging.info("Request data: %s", json_data_text)
        logging.info("Update managed views arg: [%s]", update_managed_views_arg)

        if update_managed_views_arg:
            logging.info(
                "Creating task to hit /view_update/update_all_managed_views endpoint."
            )

            # TODO(#11437): Hitting this endpoint here is a **temporary** solution,
            #  and will be deleted once we put the BigQuery view update into the DAG.
            task_manager = BQRefreshCloudTaskManager()
            task_manager.create_update_managed_views_task()

        pipeline_run_type_arg = get_value_from_request(PIPELINE_RUN_TYPE_REQUEST_ARG)

        if (
            pipeline_run_type_arg
            and pipeline_run_type_arg != PIPELINE_RUN_TYPE_NONE_VALUE
        ):
            logging.info("Triggering %s pipeline DAG.", pipeline_run_type_arg)

            trigger_dag(
                f"{metadata.project_id()}_{pipeline_run_type_arg.lower()}_calculation_pipeline_dag"
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


def get_value_from_request(
    key: str, default_value: Optional[str] = None
) -> Optional[str]:
    json_data_text = request.get_data(as_text=True)
    try:
        json_data = json.loads(json_data_text)
    except (TypeError, json.decoder.JSONDecodeError):
        json_data = {}

    return json_data.get(key, default_value)
