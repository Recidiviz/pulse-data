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
"""Defines admin panel routes for ingest operations."""
import logging
from http import HTTPStatus
from typing import Tuple

from flask import Blueprint, jsonify, request

from recidiviz.admin_panel.admin_stores import AdminStores
from recidiviz.cloud_sql.cloud_sql_client import CloudSQLClientImpl
from recidiviz.cloud_storage.gcs_pseudo_lock_manager import (
    GCSPseudoLockAlreadyExists,
    GCSPseudoLockDoesNotExist,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.direct_ingest_region_lock_manager import (
    DirectIngestRegionLockManager,
)
from recidiviz.persistence.database.sqlalchemy_database_key import (
    SQLAlchemyDatabaseKey,
    SQLAlchemyStateDatabaseVersion,
)
from recidiviz.utils import metadata
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.environment import GCP_PROJECT_STAGING, in_gcp


def _get_state_code_from_str(state_code_str: str) -> StateCode:
    if not StateCode.is_state_code(state_code_str):
        raise ValueError(
            f"Unknown region_code [{state_code_str}] received, must be a valid state code."
        )

    return StateCode[state_code_str.upper()]


def add_ingest_ops_routes(bp: Blueprint, admin_stores: AdminStores) -> None:
    """Adds routes for ingest operations."""

    project_id = GCP_PROJECT_STAGING if not in_gcp() else metadata.project_id()
    STATE_INGEST_EXPORT_URI = f"gs://{project_id}-cloud-sql-exports"

    @bp.route("/api/ingest_operations/fetch_ingest_state_codes", methods=["POST"])
    @requires_gae_auth
    def _fetch_ingest_state_codes() -> Tuple[str, HTTPStatus]:
        all_state_codes = (
            admin_stores.ingest_operations_store.state_codes_launched_in_env
        )
        state_code_info = []
        for state_code in all_state_codes:
            code_to_name = {
                "code": state_code.value,
                "name": state_code.get_state().name,
            }
            state_code_info.append(code_to_name)
        return jsonify(state_code_info), HTTPStatus.OK

    # Start an ingest run for a specific instance
    @bp.route(
        "/api/ingest_operations/<state_code_str>/start_ingest_run", methods=["POST"]
    )
    @requires_gae_auth
    def _start_ingest_run(state_code_str: str) -> Tuple[str, HTTPStatus]:
        state_code = _get_state_code_from_str(state_code_str)
        instance = request.json["instance"]
        admin_stores.ingest_operations_store.start_ingest_run(state_code, instance)
        return "", HTTPStatus.OK

    # Update ingest queues
    @bp.route(
        "/api/ingest_operations/<state_code_str>/update_ingest_queues_state",
        methods=["POST"],
    )
    @requires_gae_auth
    def _update_ingest_queues_state(state_code_str: str) -> Tuple[str, HTTPStatus]:
        state_code = _get_state_code_from_str(state_code_str)
        new_queue_state = request.json["new_queue_state"]
        admin_stores.ingest_operations_store.update_ingest_queues_state(
            state_code, new_queue_state
        )
        return "", HTTPStatus.OK

    # Get all ingest queues and their state for given state code
    @bp.route("/api/ingest_operations/<state_code_str>/get_ingest_queue_states")
    @requires_gae_auth
    def _get_ingest_queue_states(state_code_str: str) -> Tuple[str, HTTPStatus]:
        state_code = _get_state_code_from_str(state_code_str)
        ingest_queue_states = (
            admin_stores.ingest_operations_store.get_ingest_queue_states(state_code)
        )
        return jsonify(ingest_queue_states), HTTPStatus.OK

    # Get summaries of all ingest instances for state
    @bp.route("/api/ingest_operations/<state_code_str>/get_ingest_instance_summaries")
    @requires_gae_auth
    def _get_ingest_instance_summaries(state_code_str: str) -> Tuple[str, HTTPStatus]:
        state_code = _get_state_code_from_str(state_code_str)
        ingest_instance_summaries = (
            admin_stores.ingest_operations_store.get_ingest_instance_summaries(
                state_code
            )
        )
        return jsonify(ingest_instance_summaries), HTTPStatus.OK

    @bp.route("/api/ingest_operations/export_database_to_gcs", methods=["POST"])
    @requires_gae_auth
    def _export_database_to_gcs() -> Tuple[str, HTTPStatus]:
        try:
            state_code = StateCode(request.json["stateCode"])
            db_version = SQLAlchemyStateDatabaseVersion(
                request.json["ingestInstance"].lower()
            )
        except ValueError:
            return "invalid parameters provided", HTTPStatus.BAD_REQUEST

        lock_manager = DirectIngestRegionLockManager.for_state_ingest(state_code)
        if not lock_manager.can_proceed():
            return (
                "other locks blocking ingest have been acquired; aborting operation",
                HTTPStatus.CONFLICT,
            )

        db_key = SQLAlchemyDatabaseKey.for_state_code(state_code, db_version)
        cloud_sql_client = CloudSQLClientImpl(project_id=project_id)

        operation_id = cloud_sql_client.export_to_gcs_sql(
            db_key,
            GcsfsFilePath.from_absolute_path(
                f"{STATE_INGEST_EXPORT_URI}/{db_version.value}/{state_code.value}"
            ),
        )
        if operation_id is None:
            return (
                "Cloud SQL export operation was not started successfully.",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        operation_succeeded = cloud_sql_client.wait_until_operation_completed(
            operation_id, seconds_to_wait=60
        )
        if not operation_succeeded:
            return (
                "Cloud SQL import did not complete within 60 seconds",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        return operation_id, HTTPStatus.OK

    @bp.route("/api/ingest_operations/import_database_from_gcs", methods=["POST"])
    @requires_gae_auth
    def _import_database_from_gcs() -> Tuple[str, HTTPStatus]:
        try:
            state_code = StateCode(request.json["stateCode"])
            db_version = SQLAlchemyStateDatabaseVersion(
                request.json["importToDatabaseVersion"].lower()
            )
            exported_db_version = SQLAlchemyStateDatabaseVersion(
                request.json["exportedDatabaseVersion"].lower()
            )
        except ValueError:
            return "invalid parameters provided", HTTPStatus.BAD_REQUEST

        if db_version == SQLAlchemyStateDatabaseVersion.LEGACY:
            return "ingestInstance cannot be LEGACY", HTTPStatus.BAD_REQUEST

        lock_manager = DirectIngestRegionLockManager.for_state_ingest(state_code)
        if not lock_manager.can_proceed():
            return (
                "other locks blocking ingest have been acquired; aborting operation",
                HTTPStatus.CONFLICT,
            )

        db_key = SQLAlchemyDatabaseKey.for_state_code(state_code, db_version)
        cloud_sql_client = CloudSQLClientImpl(project_id=project_id)

        operation_id = cloud_sql_client.import_gcs_sql(
            db_key,
            GcsfsFilePath.from_absolute_path(
                f"{STATE_INGEST_EXPORT_URI}/{exported_db_version.value}/{state_code.value}"
            ),
        )
        if operation_id is None:
            return (
                "Cloud SQL import operation was not started successfully.",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        operation_succeeded = cloud_sql_client.wait_until_operation_completed(
            operation_id, seconds_to_wait=60
        )
        if not operation_succeeded:
            return (
                "Cloud SQL import did not complete within 60 seconds",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        return operation_id, HTTPStatus.OK

    @bp.route("/api/ingest_operations/acquire_ingest_lock", methods=["POST"])
    @requires_gae_auth
    def _acquire_ingest_lock() -> Tuple[str, HTTPStatus]:
        try:
            state_code = StateCode(request.json["stateCode"])
        except ValueError:
            return "invalid parameters provided", HTTPStatus.BAD_REQUEST

        lock_manager = DirectIngestRegionLockManager.for_state_ingest(state_code)
        try:
            lock_manager.acquire_lock()
        except GCSPseudoLockAlreadyExists:
            return "lock already exists", HTTPStatus.CONFLICT

        if not lock_manager.can_proceed():
            try:
                lock_manager.release_lock()
            except Exception as e:
                logging.exception(e)
            return (
                "other locks blocking ingest have been acquired; releasing lock",
                HTTPStatus.CONFLICT,
            )

        return "", HTTPStatus.OK

    @bp.route("/api/ingest_operations/release_ingest_lock", methods=["POST"])
    @requires_gae_auth
    def _release_ingest_lock() -> Tuple[str, HTTPStatus]:
        try:
            state_code = StateCode(request.json["stateCode"])
        except ValueError:
            return "invalid parameters provided", HTTPStatus.BAD_REQUEST

        lock_manager = DirectIngestRegionLockManager.for_state_ingest(state_code)
        try:
            lock_manager.release_lock()
        except GCSPseudoLockDoesNotExist:
            return "lock does not exist", HTTPStatus.NOT_FOUND

        return "", HTTPStatus.OK
