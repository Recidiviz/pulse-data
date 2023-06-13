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
import logging
import time
import uuid
from http import HTTPStatus
from typing import Optional, Tuple

import flask

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.success_persister import RefreshBQDatasetSuccessPersister
from recidiviz.cloud_storage.gcs_pseudo_lock_manager import GCSPseudoLockDoesNotExist
from recidiviz.cloud_tasks.utils import get_current_cloud_task_id
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_lock_manager import (
    CloudSqlToBQLockManager,
)
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.bq_refresh.federated_cloud_sql_to_bq_refresh import (
    federated_bq_schema_refresh,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.endpoint_helpers import get_value_from_request

# TODO(#21446) Remove these endpoints once we move the callsite to Kubernetes in Airflow.
cloud_sql_to_bq_blueprint = flask.Blueprint("export_manager", __name__)


@cloud_sql_to_bq_blueprint.route("/acquire_lock", methods=["POST"])
@requires_gae_auth
def acquire_lock() -> Tuple[str, HTTPStatus]:
    """
    Creates a refresh lock for a given schema type. Must provide lock_id in request.
    """
    try:
        schema_type = SchemaType(get_value_from_request("schema_type"))
    except ValueError as exc:
        return str(exc), HTTPStatus.BAD_REQUEST
    if not CloudSqlToBQConfig.is_valid_schema_type(schema_type):
        return (
            f"Unsupported schema type: [{schema_type}]",
            HTTPStatus.BAD_REQUEST,
        )

    try:
        ingest_instance = DirectIngestInstance(
            get_value_from_request("ingest_instance")
        )
    except ValueError as exc:
        return str(exc), HTTPStatus.BAD_REQUEST

    lock_id: Optional[str] = get_value_from_request("lock_id")
    if not lock_id:
        raise ValueError("Need to provide lock_id in request.")
    logging.info("Request lock id: %s", lock_id)

    lock_manager = CloudSqlToBQLockManager()
    lock_manager.acquire_lock(
        lock_id=lock_id,
        schema_type=schema_type,
        ingest_instance=ingest_instance,
    )

    return "", HTTPStatus.OK


@cloud_sql_to_bq_blueprint.route("/check_can_refresh_proceed", methods=["POST"])
@requires_gae_auth
def check_can_refresh_proceed() -> Tuple[str, HTTPStatus]:
    """
    Checks if all other processes that talk to the Postgres DB have stopped so the refresh can proceed. The refresh lock must already have been grabbed via /acquire_lock.
    """
    try:
        schema_type = SchemaType(get_value_from_request("schema_type"))
    except ValueError as exc:
        return str(exc), HTTPStatus.BAD_REQUEST
    if not CloudSqlToBQConfig.is_valid_schema_type(schema_type):
        return (
            f"Unsupported schema type: [{schema_type}]",
            HTTPStatus.BAD_REQUEST,
        )

    try:
        ingest_instance = DirectIngestInstance(
            get_value_from_request("ingest_instance")
        )
    except ValueError as exc:
        return str(exc), HTTPStatus.BAD_REQUEST

    lock_manager = CloudSqlToBQLockManager()

    try:
        can_proceed = lock_manager.can_proceed(schema_type, ingest_instance)
    except GCSPseudoLockDoesNotExist as e:
        logging.exception(e)
        # Since this endpoint is being called in the context of an Airflow DAG,
        # the DAG should have already acquired a lock before invoking this endpoint.
        return (
            f"Expected lock for [{schema_type}, {ingest_instance}] BQ refresh to already exist.",
            HTTPStatus.EXPECTATION_FAILED,
        )

    return str(can_proceed), HTTPStatus.OK


@cloud_sql_to_bq_blueprint.route("/release_lock", methods=["POST"])
@requires_gae_auth
def release_lock() -> Tuple[str, HTTPStatus]:
    """
    Releases refresh lock for a given schema type.
    """
    try:
        schema_type = SchemaType(get_value_from_request("schema_type"))
    except ValueError as exc:
        return str(exc), HTTPStatus.BAD_REQUEST
    if not CloudSqlToBQConfig.is_valid_schema_type(schema_type):
        return (
            f"Unsupported schema type: [{schema_type}]",
            HTTPStatus.BAD_REQUEST,
        )

    try:
        ingest_instance = DirectIngestInstance(
            get_value_from_request("ingest_instance")
        )
    except ValueError as exc:
        return str(exc), HTTPStatus.BAD_REQUEST

    # Unlock export lock when all BQ exports complete
    lock_manager = CloudSqlToBQLockManager()
    lock_manager.release_lock(schema_type, ingest_instance)
    return "", HTTPStatus.OK


@cloud_sql_to_bq_blueprint.route("/refresh_bq_dataset", methods=["POST"])
@requires_gae_auth
def refresh_bq_dataset() -> Tuple[str, HTTPStatus]:
    """Performs a full refresh of BigQuery data for a given schema, pulling data from
    the appropriate CloudSQL Postgres instance.

    On completion, triggers Dataflow pipelines (when necessary), releases the refresh
    lock and restarts any paused ingest work.
    """
    try:
        schema_type = SchemaType(get_value_from_request("schema_type"))
    except ValueError as exc:
        return str(exc), HTTPStatus.BAD_REQUEST
    if not CloudSqlToBQConfig.is_valid_schema_type(schema_type):
        return (
            f"Unsupported schema type: [{schema_type}]",
            HTTPStatus.BAD_REQUEST,
        )

    try:
        ingest_instance = DirectIngestInstance(
            get_value_from_request("ingest_instance")
        )
    except ValueError as exc:
        return str(exc), HTTPStatus.BAD_REQUEST

    sandbox_prefix: Optional[str] = get_value_from_request("sandbox_prefix")

    lock_manager = CloudSqlToBQLockManager()

    try:
        can_proceed = lock_manager.can_proceed(schema_type, ingest_instance)
    except GCSPseudoLockDoesNotExist as e:
        logging.exception(e)
        # Since this endpoint is being called in the context of an Airflow DAG,
        # the DAG should have already acquired a lock before invoking this endpoint.
        return (
            f"Expected lock for [{schema_type.value}] BQ refresh to already exist.",
            HTTPStatus.EXPECTATION_FAILED,
        )

    if not can_proceed:
        return (
            f"Expected to be able to proceed with refresh before this endpoint was "
            f"called for [{schema_type.value}].",
            HTTPStatus.EXPECTATION_FAILED,
        )

    start = datetime.datetime.now()

    federated_bq_schema_refresh(
        schema_type=schema_type,
        direct_ingest_instance=ingest_instance
        if schema_type == SchemaType.STATE
        else None,
        dataset_override_prefix=sandbox_prefix,
    )

    logging.info(
        "Done running refresh for [%s], unlocking Postgres to BigQuery export",
        schema_type.value,
    )

    end = datetime.datetime.now()
    runtime_sec = int((end - start).total_seconds())

    success_persister = RefreshBQDatasetSuccessPersister(bq_client=BigQueryClientImpl())
    success_persister.record_success_in_bq(
        schema_type=schema_type,
        direct_ingest_instance=ingest_instance,
        dataset_override_prefix=sandbox_prefix,
        runtime_sec=runtime_sec,
        cloud_task_id=get_current_cloud_task_id(),
    )

    logging.info("Finished saving success record to database.")

    return "", HTTPStatus.OK


LOCK_WAIT_SLEEP_INTERVAL_SECONDS = 60  # 1 minute
LOCK_WAIT_SLEEP_MAXIMUM_TIMEOUT = 60 * 60 * 4  # 4 hours


def execute_cloud_sql_to_bq_refresh(
    schema_type: SchemaType,
    ingest_instance: DirectIngestInstance,
    sandbox_prefix: Optional[str] = None,
) -> None:
    """Executes the Cloud SQL to BQ refresh for a given schema_type, ingest instance and
    sandbox_prefix."""
    if not CloudSqlToBQConfig.is_valid_schema_type(schema_type):
        raise ValueError(f"Unsupported schema type: [{schema_type}]")

    lock_manager = CloudSqlToBQLockManager()
    lock_manager.acquire_lock(
        lock_id=str(uuid.uuid4()),
        schema_type=schema_type,
        ingest_instance=ingest_instance,
    )

    try:
        secs_waited = 0
        while (
            not (can_proceed := lock_manager.can_proceed(schema_type, ingest_instance))
            and secs_waited < LOCK_WAIT_SLEEP_MAXIMUM_TIMEOUT
        ):
            time.sleep(LOCK_WAIT_SLEEP_INTERVAL_SECONDS)
            secs_waited += LOCK_WAIT_SLEEP_INTERVAL_SECONDS

        if not can_proceed:
            raise ValueError(
                f"Could not acquire lock after waiting {LOCK_WAIT_SLEEP_MAXIMUM_TIMEOUT} seconds for {schema_type}."
            )
        start = datetime.datetime.now()
        federated_bq_schema_refresh(
            schema_type=schema_type,
            direct_ingest_instance=ingest_instance
            if schema_type == SchemaType.STATE
            else None,
            dataset_override_prefix=sandbox_prefix,
        )
        end = datetime.datetime.now()
        runtime_sec = int((end - start).total_seconds())
        success_persister = RefreshBQDatasetSuccessPersister(
            bq_client=BigQueryClientImpl()
        )
        # TODO(#21537) Remove the use of `cloud_task_id` when all tasks have been moved to Kubernetes.
        success_persister.record_success_in_bq(
            schema_type=schema_type,
            direct_ingest_instance=ingest_instance,
            dataset_override_prefix=sandbox_prefix,
            runtime_sec=runtime_sec,
            cloud_task_id="AIRFLOW_FEDERATED_REFRESH",
        )
    finally:
        lock_manager.release_lock(schema_type, ingest_instance)
