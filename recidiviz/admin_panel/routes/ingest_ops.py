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

from recidiviz.admin_panel.admin_stores import AdminStores, fetch_state_codes
from recidiviz.cloud_sql.cloud_sql_client import CloudSQLClientImpl
from recidiviz.cloud_storage.gcs_pseudo_lock_manager import (
    GCSPseudoLockAlreadyExists,
    GCSPseudoLockDoesNotExist,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.direct_ingest_instance import (
    DirectIngestInstance,
)
from recidiviz.ingest.direct.controllers.direct_ingest_instance_status_manager import (
    DirectIngestInstanceStatusManager,
)
from recidiviz.ingest.direct.controllers.direct_ingest_region_lock_manager import (
    DirectIngestRegionLockManager,
)
from recidiviz.utils import metadata
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.environment import GCP_PROJECT_STAGING, in_gcp

GCS_IMPORT_EXPORT_TIMEOUT_SEC = 60 * 30  # 30 min


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
        state_code_info = fetch_state_codes(all_state_codes)
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
            ingest_instance = DirectIngestInstance(
                request.json["ingestInstance"].upper()
            )
        except ValueError:
            return "invalid parameters provided", HTTPStatus.BAD_REQUEST

        lock_manager = DirectIngestRegionLockManager.for_state_ingest(
            state_code, ingest_instance
        )
        if not lock_manager.can_proceed():
            return (
                "other locks blocking ingest have been acquired; aborting operation",
                HTTPStatus.CONFLICT,
            )

        db_key = ingest_instance.database_key_for_state(state_code)
        cloud_sql_client = CloudSQLClientImpl(project_id=project_id)

        operation_id = cloud_sql_client.export_to_gcs_sql(
            db_key,
            GcsfsFilePath.from_absolute_path(
                f"{STATE_INGEST_EXPORT_URI}/{ingest_instance.value.lower()}/{state_code.value}"
            ),
        )
        if operation_id is None:
            return (
                "Cloud SQL export operation was not started successfully.",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        operation_succeeded = cloud_sql_client.wait_until_operation_completed(
            operation_id, seconds_to_wait=GCS_IMPORT_EXPORT_TIMEOUT_SEC
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
            import_to_ingest_instance = DirectIngestInstance(
                request.json["importToDatabaseInstance"].upper()
            )
            exported_ingest_instance = DirectIngestInstance(
                request.json["exportedDatabaseInstance"].upper()
            )
        except ValueError:
            return "invalid parameters provided", HTTPStatus.BAD_REQUEST

        lock_manager = DirectIngestRegionLockManager.for_state_ingest(
            state_code, ingest_instance=import_to_ingest_instance
        )
        if not lock_manager.can_proceed():
            return (
                "other locks blocking ingest have been acquired; aborting operation",
                HTTPStatus.CONFLICT,
            )

        db_key = import_to_ingest_instance.database_key_for_state(state_code)
        cloud_sql_client = CloudSQLClientImpl(project_id=project_id)

        operation_id = cloud_sql_client.import_gcs_sql(
            db_key,
            GcsfsFilePath.from_absolute_path(
                f"{STATE_INGEST_EXPORT_URI}/{exported_ingest_instance.value.lower()}/{state_code.value}"
            ),
        )
        if operation_id is None:
            return (
                "Cloud SQL import operation was not started successfully.",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        operation_succeeded = cloud_sql_client.wait_until_operation_completed(
            operation_id, seconds_to_wait=GCS_IMPORT_EXPORT_TIMEOUT_SEC
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
            ingest_instance = DirectIngestInstance(request.json["ingestInstance"])
        except ValueError:
            return "invalid parameters provided", HTTPStatus.BAD_REQUEST

        lock_manager = DirectIngestRegionLockManager.for_state_ingest(
            state_code, ingest_instance=ingest_instance
        )
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
            ingest_instance = DirectIngestInstance(request.json["ingestInstance"])
        except ValueError:
            return "invalid parameters provided", HTTPStatus.BAD_REQUEST

        lock_manager = DirectIngestRegionLockManager.for_state_ingest(
            state_code, ingest_instance=ingest_instance
        )
        try:
            lock_manager.release_lock()
        except GCSPseudoLockDoesNotExist:
            return "lock does not exist", HTTPStatus.NOT_FOUND

        return "", HTTPStatus.OK

    @bp.route("/api/ingest_operations/pause_direct_ingest_instance", methods=["POST"])
    @requires_gae_auth
    def _pause_direct_ingest_instance() -> Tuple[str, HTTPStatus]:
        try:
            state_code = StateCode(request.json["stateCode"])
            ingest_instance = DirectIngestInstance(request.json["ingestInstance"])
        except ValueError:
            return "invalid parameters provided", HTTPStatus.BAD_REQUEST

        ingest_status_manager = DirectIngestInstanceStatusManager(
            region_code=state_code.value, ingest_instance=ingest_instance
        )
        try:
            ingest_status_manager.pause_instance()
        except Exception:
            return (
                "something went wrong pausing the intance",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        return "", HTTPStatus.OK
