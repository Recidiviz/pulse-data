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
from typing import Dict, List, Optional, Tuple, Union

from flask import Blueprint, Response, jsonify, request
from google.cloud import storage

from recidiviz.admin_panel.admin_stores import (
    fetch_state_codes,
    get_ingest_operations_store,
)
from recidiviz.admin_panel.ingest_operations.ingest_utils import (
    check_is_valid_sandbox_bucket,
    get_unprocessed_raw_files_in_bucket,
    import_raw_files_to_bq_sandbox,
)
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_sql.cloud_sql_client import CloudSQLClientImpl
from recidiviz.cloud_storage.gcs_pseudo_lock_manager import (
    GCSPseudoLockAlreadyExists,
    GCSPseudoLockDoesNotExist,
)
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.direct_ingest_region_lock_manager import (
    DirectIngestRegionLockManager,
)
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.metadata.direct_ingest_instance_pause_status_manager import (
    DirectIngestInstancePauseStatusManager,
)
from recidiviz.ingest.direct.metadata.direct_ingest_view_materialization_metadata_manager import (
    DirectIngestViewMaterializationMetadataManager,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.flash_database_tools import (
    move_ingest_view_results_between_instances,
    move_ingest_view_results_to_backup,
)
from recidiviz.utils import metadata
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.environment import GCP_PROJECT_STAGING, in_gcp
from recidiviz.utils.types import assert_type

GCS_IMPORT_EXPORT_TIMEOUT_SEC = 60 * 30  # 30 min


def _get_state_code_from_str(state_code_str: str) -> StateCode:
    if not StateCode.is_state_code(state_code_str):
        raise ValueError(
            f"Unknown region_code [{state_code_str}] received, must be a valid state code."
        )

    return StateCode[state_code_str.upper()]


def _sql_export_path(
    project_id: str, state_code: StateCode, ingest_instance: DirectIngestInstance
) -> GcsfsFilePath:
    return GcsfsFilePath.from_absolute_path(
        f"gs://{project_id}-cloud-sql-exports/{ingest_instance.value.lower()}/{state_code.value}"
    )


def add_ingest_ops_routes(bp: Blueprint) -> None:
    """Adds routes for ingest operations."""

    project_id = GCP_PROJECT_STAGING if not in_gcp() else metadata.project_id()

    @bp.route("/api/ingest_operations/fetch_ingest_state_codes", methods=["POST"])
    @requires_gae_auth
    def _fetch_ingest_state_codes() -> Tuple[Response, HTTPStatus]:
        all_state_codes = get_ingest_operations_store().state_codes_launched_in_env
        state_code_info = fetch_state_codes(all_state_codes)
        return jsonify(state_code_info), HTTPStatus.OK

    # Trigger the task scheduler for a specific instance
    @bp.route(
        "/api/ingest_operations/<state_code_str>/trigger_task_scheduler",
        methods=["POST"],
    )
    @requires_gae_auth
    def _trigger_task_scheduler(state_code_str: str) -> Tuple[str, HTTPStatus]:
        request_json = assert_type(request.json, dict)
        state_code = _get_state_code_from_str(state_code_str)
        instance = request_json["instance"]
        get_ingest_operations_store().trigger_task_scheduler(state_code, instance)
        return "", HTTPStatus.OK

    # Start an ingest rerun in secondary
    @bp.route(
        "/api/ingest_operations/<state_code_str>/start_ingest_rerun",
        methods=["POST"],
    )
    @requires_gae_auth
    def _start_ingest_rerun(state_code_str: str) -> Tuple[str, HTTPStatus]:
        state_code = _get_state_code_from_str(state_code_str)
        try:
            request_json = assert_type(request.json, dict)
            ingest_instance = DirectIngestInstance(request_json["instance"].upper())
            raw_data_source_instance = DirectIngestInstance(
                request_json["rawDataSourceInstance"].upper()
            )
        except ValueError:
            return "invalid ingest instance provided", HTTPStatus.BAD_REQUEST

        get_ingest_operations_store().start_ingest_rerun(
            state_code, ingest_instance, raw_data_source_instance
        )
        return "", HTTPStatus.OK

    # Update ingest queues
    @bp.route(
        "/api/ingest_operations/<state_code_str>/update_ingest_queues_state",
        methods=["POST"],
    )
    @requires_gae_auth
    def _update_ingest_queues_state(state_code_str: str) -> Tuple[str, HTTPStatus]:
        request_json = assert_type(request.json, dict)
        state_code = _get_state_code_from_str(state_code_str)
        new_queue_state = request_json["new_queue_state"]
        get_ingest_operations_store().update_ingest_queues_state(
            state_code, new_queue_state
        )
        return "", HTTPStatus.OK

    # Get all ingest queues and their state for given state code
    @bp.route("/api/ingest_operations/<state_code_str>/get_ingest_queue_states")
    @requires_gae_auth
    def _get_ingest_queue_states(state_code_str: str) -> Tuple[Response, HTTPStatus]:
        state_code = _get_state_code_from_str(state_code_str)
        ingest_queue_states = get_ingest_operations_store().get_ingest_queue_states(
            state_code
        )
        return jsonify(ingest_queue_states), HTTPStatus.OK

    # Get summary of an ingest instance for a state
    @bp.route(
        "/api/ingest_operations/<state_code_str>/get_ingest_instance_summary/<ingest_instance_str>"
    )
    @requires_gae_auth
    def _get_ingest_instance_summary(
        state_code_str: str, ingest_instance_str: str
    ) -> Tuple[Union[str, Response], HTTPStatus]:
        state_code = _get_state_code_from_str(state_code_str)
        try:
            ingest_instance = DirectIngestInstance(ingest_instance_str.upper())
        except ValueError:
            return "invalid parameters provided", HTTPStatus.BAD_REQUEST
        ingest_instance_summary = (
            get_ingest_operations_store().get_ingest_instance_summary(
                state_code, ingest_instance
            )
        )
        return jsonify(ingest_instance_summary), HTTPStatus.OK

    # Get processing status of filetags for an ingest instance for a state
    @bp.route(
        "/api/ingest_operations/<state_code_str>/get_ingest_raw_file_processing_status/<ingest_instance_str>"
    )
    @requires_gae_auth
    def _get_ingest_raw_file_processing_status(
        state_code_str: str, ingest_instance_str: str
    ) -> Tuple[Union[str, Response], HTTPStatus]:
        state_code = _get_state_code_from_str(state_code_str)
        try:
            ingest_instance = DirectIngestInstance(ingest_instance_str.upper())
        except ValueError:
            return "invalid parameters provided", HTTPStatus.BAD_REQUEST
        ingest_file_processing_status = (
            get_ingest_operations_store().get_ingest_raw_file_processing_status(
                state_code, ingest_instance
            )
        )
        return jsonify(ingest_file_processing_status), HTTPStatus.OK

    @bp.route("/api/ingest_operations/export_database_to_gcs", methods=["POST"])
    @requires_gae_auth
    def _export_database_to_gcs() -> Tuple[str, HTTPStatus]:
        try:
            request_json = assert_type(request.json, dict)
            state_code = StateCode(request_json["stateCode"])
            ingest_instance = DirectIngestInstance(
                request_json["ingestInstance"].upper()
            )
        except ValueError:
            return "invalid parameters provided", HTTPStatus.BAD_REQUEST

        lock_manager = DirectIngestRegionLockManager.for_state_ingest(
            state_code, ingest_instance
        )
        # Assert that we still have the lock. We do not check `can_proceed` as we do not
        # want to yield to any other tasks (e.g. if the export takes a subsequent lock).
        # So long as we still have our lock it is safe to proceed.
        if not lock_manager.is_locked():
            return (
                "ingest lock no longer held; aborting operation",
                HTTPStatus.CONFLICT,
            )

        db_key = ingest_instance.database_key_for_state(state_code)
        cloud_sql_client = CloudSQLClientImpl(project_id=project_id)

        operation_id = cloud_sql_client.export_to_gcs_sql(
            db_key,
            _sql_export_path(
                project_id=project_id,
                ingest_instance=ingest_instance,
                state_code=state_code,
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
            request_json = assert_type(request.json, dict)
            state_code = StateCode(request_json["stateCode"])
            import_to_ingest_instance = DirectIngestInstance(
                request_json["importToDatabaseInstance"].upper()
            )
            exported_ingest_instance = DirectIngestInstance(
                request_json["exportedDatabaseInstance"].upper()
            )
        except ValueError:
            return "invalid parameters provided", HTTPStatus.BAD_REQUEST

        lock_manager = DirectIngestRegionLockManager.for_state_ingest(
            state_code, ingest_instance=import_to_ingest_instance
        )
        # Assert that we still have the lock. We do not check `can_proceed` as we do not
        # want to yield to any other tasks (e.g. if the export takes a subsequent lock).
        # So long as we still have our lock it is safe to proceed.
        if not lock_manager.is_locked():
            return (
                "ingest lock no longer held; aborting operation",
                HTTPStatus.CONFLICT,
            )

        db_key = import_to_ingest_instance.database_key_for_state(state_code)
        cloud_sql_client = CloudSQLClientImpl(project_id=project_id)

        operation_id = cloud_sql_client.import_gcs_sql(
            db_key,
            _sql_export_path(
                project_id=project_id,
                ingest_instance=exported_ingest_instance,
                state_code=state_code,
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

    @bp.route(
        "/api/ingest_operations/delete_database_import_gcs_files", methods=["POST"]
    )
    @requires_gae_auth
    def _delete_database_import_gcs_files() -> Tuple[str, HTTPStatus]:
        try:
            request_json = assert_type(request.json, dict)
            state_code = StateCode(request_json["stateCode"])
            exported_ingest_instance = DirectIngestInstance(
                request_json["exportedDatabaseInstance"].upper()
            )
        except ValueError:
            return "invalid parameters provided", HTTPStatus.BAD_REQUEST

        path_to_delete = _sql_export_path(
            project_id=project_id,
            ingest_instance=exported_ingest_instance,
            state_code=state_code,
        )

        fs = DirectIngestGCSFileSystem(GcsfsFactory.build())
        fs.delete(path=path_to_delete)

        return "", HTTPStatus.OK

    @bp.route("/api/ingest_operations/acquire_ingest_lock", methods=["POST"])
    @requires_gae_auth
    def _acquire_ingest_lock() -> Tuple[str, HTTPStatus]:
        try:
            request_json = assert_type(request.json, dict)
            state_code = StateCode(request_json["stateCode"])
            ingest_instance = DirectIngestInstance(request_json["ingestInstance"])
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
            request_json = assert_type(request.json, dict)
            state_code = StateCode(request_json["stateCode"])
            ingest_instance = DirectIngestInstance(request_json["ingestInstance"])
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
            request_json = assert_type(request.json, dict)
            state_code = StateCode(request_json["stateCode"])
            ingest_instance = DirectIngestInstance(request_json["ingestInstance"])
        except ValueError:
            return "invalid parameters provided", HTTPStatus.BAD_REQUEST

        ingest_status_manager = DirectIngestInstancePauseStatusManager(
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

    @bp.route("/api/ingest_operations/unpause_direct_ingest_instance", methods=["POST"])
    @requires_gae_auth
    def _unpause_direct_ingest_instance() -> Tuple[str, HTTPStatus]:
        try:
            request_json = assert_type(request.json, dict)
            state_code = StateCode(request_json["stateCode"])
            ingest_instance = DirectIngestInstance(request_json["ingestInstance"])
        except ValueError:
            return "invalid parameters provided", HTTPStatus.BAD_REQUEST

        ingest_status_manager = DirectIngestInstancePauseStatusManager(
            region_code=state_code.value, ingest_instance=ingest_instance
        )
        try:
            ingest_status_manager.unpause_instance()
        except Exception:
            return (
                "something went wrong unpausing the intance",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        return "", HTTPStatus.OK

    @bp.route("/api/ingest_operations/direct/sandbox_raw_data_import", methods=["POST"])
    @requires_gae_auth
    def _sandbox_raw_data_import() -> Tuple[Union[str, Response], HTTPStatus]:
        try:
            data = assert_type(request.json, dict)
            state_code = StateCode(data["stateCode"])
            sandbox_dataset_prefix = data["sandboxDatasetPrefix"]
            source_bucket = GcsfsBucketPath(data["sourceBucket"])
            file_tags: Optional[List[str]] = data.get("fileTagFilters", None)
        except ValueError:
            return "invalid parameters provided", HTTPStatus.BAD_REQUEST

        if not file_tags:
            file_tags = None

        try:
            import_status = import_raw_files_to_bq_sandbox(
                state_code=state_code,
                sandbox_dataset_prefix=sandbox_dataset_prefix,
                source_bucket=source_bucket,
                file_tag_filters=file_tags,
                allow_incomplete_configs=True,
            )
        except ValueError as error:
            return str(error), HTTPStatus.INTERNAL_SERVER_ERROR

        return (
            jsonify(
                {
                    "fileStatusList": import_status.to_serializable()["fileStatuses"],
                }
            ),
            HTTPStatus.OK,
        )

    @bp.route("/api/ingest_operations/direct/list_sandbox_buckets", methods=["POST"])
    @requires_gae_auth
    def _list_sandbox_buckets() -> Tuple[Union[str, Response], HTTPStatus]:
        try:
            storage_client = storage.Client()
            buckets = storage_client.list_buckets()

            bucket_names = [bucket.name for bucket in buckets]

            filtered_buckets = []
            for bucket in bucket_names:
                try:
                    check_is_valid_sandbox_bucket(GcsfsBucketPath(bucket))
                    filtered_buckets.append(bucket)
                except ValueError:
                    continue

        except Exception:
            return (
                "something went wrong getting the list of buckets",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        return (
            jsonify({"bucketNames": filtered_buckets}),
            HTTPStatus.OK,
        )

    @bp.route("/api/ingest_operations/direct/list_raw_files", methods=["POST"])
    @requires_gae_auth
    def _list_raw_files_in_sandbox_bucket() -> Tuple[Union[str, Response], HTTPStatus]:
        try:
            data = assert_type(request.json, dict)
            state_code = StateCode(data["stateCode"])
            source_bucket = GcsfsBucketPath(data["sourceBucket"])
        except ValueError:
            return "invalid source bucket", HTTPStatus.BAD_REQUEST

        try:
            region_code = state_code.value.lower()
            region = get_direct_ingest_region(region_code)
            raw_files_to_import = get_unprocessed_raw_files_in_bucket(
                fs=DirectIngestGCSFileSystem(GcsfsFactory.build()),
                bucket_path=source_bucket,
                region_raw_file_config=DirectIngestRegionRawFileConfig(
                    region_code=region.region_code,
                    region_module=region.region_module,
                ),
            )
        except ValueError:
            return (
                f"Something went wrong trying to get unprocessed raw files from {source_bucket} bucket",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        raw_files_list = []
        for file_path in raw_files_to_import:
            parts = filename_parts_from_path(file_path)
            raw_files_list.append(
                {
                    "fileTag": parts.file_tag,
                    "uploadDate": parts.utc_upload_datetime_str,
                }
            )

        return (
            jsonify({"rawFilesList": raw_files_list}),
            HTTPStatus.OK,
        )

    @bp.route(
        "/api/ingest_operations/flash_primary_db/move_ingest_view_results_to_backup",
        methods=["POST"],
    )
    @requires_gae_auth
    def _move_ingest_view_results_to_backup() -> Tuple[str, HTTPStatus]:
        try:
            request_json = assert_type(request.json, dict)
            state_code = StateCode(request_json["stateCode"])
            ingest_instance = DirectIngestInstance(
                request_json["ingestInstance"].upper()
            )
        except ValueError:
            return "Invalid input data", HTTPStatus.BAD_REQUEST

        try:
            move_ingest_view_results_to_backup(
                state_code, ingest_instance, BigQueryClientImpl()
            )

            return (
                "",
                HTTPStatus.OK,
            )

        except ValueError as error:
            logging.exception(error)
            return f"{error}", HTTPStatus.INTERNAL_SERVER_ERROR

    @bp.route(
        "/api/ingest_operations/flash_primary_db/move_ingest_view_results_between_instances",
        methods=["POST"],
    )
    @requires_gae_auth
    def _move_ingest_view_results_between_instances() -> Tuple[str, HTTPStatus]:
        try:
            request_json = assert_type(request.json, dict)
            state_code = StateCode(request_json["stateCode"])
            ingest_instance_source = DirectIngestInstance(
                request_json["srcIngestInstance"].upper()
            )
            ingest_instance_destination = DirectIngestInstance(
                request_json["destIngestInstance"].upper()
            )
        except ValueError:
            return "Invalid input data", HTTPStatus.BAD_REQUEST

        try:
            move_ingest_view_results_between_instances(
                state_code=state_code,
                ingest_instance_source=ingest_instance_source,
                ingest_instance_destination=ingest_instance_destination,
                big_query_client=BigQueryClientImpl(),
            )

            return (
                "",
                HTTPStatus.OK,
            )

        except ValueError as error:
            logging.exception(error)
            return f"{error}", HTTPStatus.INTERNAL_SERVER_ERROR

    @bp.route(
        "/api/ingest_operations/flash_primary_db/mark_instance_ingest_view_data_invalidated",
        methods=["POST"],
    )
    @requires_gae_auth
    def _mark_instance_ingest_view_data_invalidated() -> Tuple[str, HTTPStatus]:
        try:
            request_json = assert_type(request.json, dict)
            state_code = StateCode(request_json["stateCode"])
            ingest_instance = DirectIngestInstance(
                request_json["ingestInstance"].upper()
            )
        except ValueError:
            return "Invalid input data", HTTPStatus.BAD_REQUEST

        try:
            ingest_view_metadata_manager = (
                DirectIngestViewMaterializationMetadataManager(
                    region_code=state_code.value, ingest_instance=ingest_instance
                )
            )
            ingest_view_metadata_manager.mark_instance_data_invalidated()

            return (
                "",
                HTTPStatus.OK,
            )

        except ValueError as error:
            logging.exception(error)
            return f"{error}", HTTPStatus.INTERNAL_SERVER_ERROR

    @bp.route(
        "/api/ingest_operations/flash_primary_db/transfer_ingest_view_metadata_to_new_instance",
        methods=["POST"],
    )
    @requires_gae_auth
    def _transfer_ingest_view_metadata_to_new_instance() -> Tuple[str, HTTPStatus]:
        try:
            request_json = assert_type(request.json, dict)
            state_code = StateCode(request_json["stateCode"])
            src_ingest_instance = DirectIngestInstance(
                request_json["srcIngestInstance"].upper()
            )
            dest_ingest_instance = DirectIngestInstance(
                request_json["destIngestInstance"].upper()
            )
        except ValueError:
            return "Invalid input data", HTTPStatus.BAD_REQUEST

        try:
            ingest_view_metadata_manager = (
                DirectIngestViewMaterializationMetadataManager(
                    region_code=state_code.value, ingest_instance=src_ingest_instance
                )
            )

            new_instance_manager = DirectIngestViewMaterializationMetadataManager(
                region_code=state_code.value,
                ingest_instance=dest_ingest_instance,
            )
            ingest_view_metadata_manager.transfer_metadata_to_new_instance(
                new_instance_manager=new_instance_manager
            )

            return (
                "",
                HTTPStatus.OK,
            )

        except ValueError as error:
            logging.exception(error)
            return f"{error}", HTTPStatus.INTERNAL_SERVER_ERROR

    @bp.route("/api/ingest_operations/all_ingest_instance_statuses")
    @requires_gae_auth
    def _all_ingest_instance_statuses() -> Tuple[Response, HTTPStatus]:
        all_instance_statuses = (
            get_ingest_operations_store().get_all_current_ingest_instance_statuses()
        )

        all_instance_statuses_strings: Dict[str, Dict[str, Optional[str]]] = {}

        for instance_state_code, instances in all_instance_statuses.items():
            all_instance_statuses_strings[instance_state_code.value] = {
                instance.value: curr_status.value if curr_status is not None else None
                for instance, curr_status in instances.items()
            }

        return jsonify(all_instance_statuses_strings), HTTPStatus.OK
