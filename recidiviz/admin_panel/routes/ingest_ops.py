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
from typing import Tuple, Union

from flask import Blueprint, Response, jsonify, request

from recidiviz.admin_panel.admin_stores import (
    fetch_state_codes,
    get_ingest_operations_store,
)
from recidiviz.admin_panel.ingest_dataflow_operations import (
    get_latest_run_ingest_view_results,
    get_latest_run_raw_data_watermarks,
    get_latest_run_state_results,
    get_raw_data_tags_not_meeting_watermark,
)
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataLockActor,
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.metadata.direct_ingest_dataflow_job_manager import (
    DirectIngestDataflowJobManager,
)
from recidiviz.ingest.direct.metadata.direct_ingest_raw_data_flash_status_manager import (
    DirectIngestRawDataFlashStatusManager,
)
from recidiviz.ingest.direct.metadata.direct_ingest_raw_data_resource_lock_manager import (
    DirectIngestRawDataResourceLockManager,
)
from recidiviz.ingest.direct.metadata.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileImportManager,
)
from recidiviz.ingest.direct.metadata.direct_ingest_raw_file_metadata_manager import (
    DirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.flash_database_tools import (
    copy_raw_data_between_instances,
    copy_raw_data_to_backup,
    delete_contents_of_raw_data_tables,
    delete_tables_in_pruning_datasets,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.errors import (
    DirectIngestRawDataResourceLockAlreadyReleasedError,
    DirectIngestRawDataResourceLockHeldError,
)
from recidiviz.pipelines.ingest.dataset_config import (
    ingest_view_materialization_results_dataset,
    state_dataset_for_state_code,
)
from recidiviz.utils.trigger_dag_helpers import (
    trigger_calculation_dag,
    trigger_raw_data_import_dag,
)
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

    @bp.route("/api/ingest_operations/fetch_ingest_state_codes", methods=["POST"])
    def _fetch_ingest_state_codes() -> Tuple[Response, HTTPStatus]:
        all_state_codes = get_ingest_operations_store().state_codes_launched_in_env
        state_code_info = fetch_state_codes(all_state_codes)
        return jsonify(state_code_info), HTTPStatus.OK

    # Get summary of an ingest instance for a state
    @bp.route(
        "/api/ingest_operations/<state_code_str>/get_ingest_instance_resources/<ingest_instance_str>"
    )
    def _get_ingest_instance_resources(
        state_code_str: str, ingest_instance_str: str
    ) -> Tuple[Union[str, Response], HTTPStatus]:
        state_code = _get_state_code_from_str(state_code_str)
        try:
            ingest_instance = DirectIngestInstance(ingest_instance_str.upper())
        except ValueError:
            return "invalid parameters provided", HTTPStatus.BAD_REQUEST
        ingest_instance_resources = (
            get_ingest_operations_store().get_ingest_instance_resources(
                state_code, ingest_instance
            )
        )
        return jsonify(ingest_instance_resources), HTTPStatus.OK

    # Get processing status of filetags for an ingest instance for a state
    @bp.route(
        "/api/ingest_operations/<state_code_str>/get_ingest_raw_file_processing_status/<ingest_instance_str>"
    )
    def _get_ingest_raw_file_processing_status(
        state_code_str: str, ingest_instance_str: str
    ) -> Tuple[Union[str, Response], HTTPStatus]:
        state_code = _get_state_code_from_str(state_code_str)
        try:
            ingest_instance = DirectIngestInstance(ingest_instance_str.upper())
        except ValueError:
            return "invalid parameters provided", HTTPStatus.BAD_REQUEST
        ingest_file_processing_statuses = (
            get_ingest_operations_store().get_ingest_raw_file_processing_statuses(
                state_code, ingest_instance
            )
        )
        return (
            jsonify([status.for_api() for status in ingest_file_processing_statuses]),
            HTTPStatus.OK,
        )

    @bp.route("/api/ingest_operations/get_all_latest_ingest_dataflow_jobs")
    def _all_dataflow_jobs() -> Tuple[Response, HTTPStatus]:
        all_instance_statuses = (
            get_ingest_operations_store().get_most_recent_dataflow_job_statuses()
        )

        return (
            jsonify(
                {
                    instance_state_code.value: (
                        curr_status_info.for_api() if curr_status_info else None
                    )
                    for instance_state_code, curr_status_info in all_instance_statuses.items()
                }
            ),
            HTTPStatus.OK,
        )

    @bp.route("/api/ingest_operations/get_latest_ingest_dataflow_job/<state_code_str>")
    def _get_latest_ingest_dataflow_job(
        state_code_str: str,
    ) -> Tuple[Response, HTTPStatus]:
        try:
            state_code = StateCode(state_code_str)
        except ValueError:
            return (jsonify("Invalid input data"), HTTPStatus.BAD_REQUEST)

        all_primary_pipeline_statuses = (
            get_ingest_operations_store().get_most_recent_dataflow_job_statuses()
        )

        job_info = all_primary_pipeline_statuses[state_code]

        return (
            jsonify(job_info.for_api() if job_info else None),
            HTTPStatus.OK,
        )

    # TODO(#30695): Actually rip SECONDARY instance dataflow info out of the admin panel
    #  and remove the instance_str arg from this endpoint.
    @bp.route(
        "/api/ingest_operations/get_dataflow_job_additional_metadata_by_instance/<state_code_str>/<instance_str>"
    )
    def _get_dataflow_job_additional_metadata_by_instance(
        state_code_str: str,
        instance_str: str,  # pylint: disable=unused-argument
    ) -> Tuple[Response, HTTPStatus]:
        try:
            state_code = StateCode(state_code_str)
        except ValueError:
            return (jsonify("Invalid input data"), HTTPStatus.BAD_REQUEST)

        ingest_view_results_dataset = ingest_view_materialization_results_dataset(
            state_code
        )
        state_results_dataset = state_dataset_for_state_code(state_code)

        return (
            jsonify(
                {
                    "ingestViewResultsDatasetName": ingest_view_results_dataset,
                    "stateResultsDatasetName": state_results_dataset,
                }
            ),
            HTTPStatus.OK,
        )

    @bp.route(
        "/api/ingest_operations/flash_primary_db/copy_raw_data_to_backup",
        methods=["POST"],
    )
    def _copy_raw_data_to_backup() -> Tuple[str, HTTPStatus]:
        try:
            request_json = assert_type(request.json, dict)
            state_code = StateCode(request_json["stateCode"])
            ingest_instance = DirectIngestInstance(
                request_json["ingestInstance"].upper()
            )
        except ValueError:
            return "Invalid input data", HTTPStatus.BAD_REQUEST

        try:
            copy_raw_data_to_backup(state_code, ingest_instance, BigQueryClientImpl())

            return (
                "",
                HTTPStatus.OK,
            )

        except ValueError as error:
            logging.exception(error)
            return f"{error}", HTTPStatus.INTERNAL_SERVER_ERROR

    @bp.route(
        "/api/ingest_operations/flash_primary_db/copy_raw_data_between_instances",
        methods=["POST"],
    )
    def _copy_raw_data_between_instances() -> Tuple[str, HTTPStatus]:
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
            copy_raw_data_between_instances(
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
        "/api/ingest_operations/flash_primary_db/delete_contents_of_raw_data_tables",
        methods=["POST"],
    )
    def _delete_contents_of_raw_data_tables() -> Tuple[str, HTTPStatus]:
        try:
            request_json = assert_type(request.json, dict)
            state_code = StateCode(request_json["stateCode"])
            ingest_instance = DirectIngestInstance(
                request_json["ingestInstance"].upper()
            )
        except ValueError:
            return "Invalid input data", HTTPStatus.BAD_REQUEST

        try:
            delete_contents_of_raw_data_tables(
                state_code=state_code,
                ingest_instance=ingest_instance,
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
        "/api/ingest_operations/flash_primary_db/invalidate_ingest_pipeline_runs",
        methods=["POST"],
    )
    def _invalidate_ingest_pipeline_runs() -> Tuple[str, HTTPStatus]:
        try:
            request_json = assert_type(request.json, dict)
            state_code = StateCode(request_json["stateCode"].upper())
            ingest_instance = DirectIngestInstance(
                request_json["ingestInstance"].upper()
            )
        except ValueError:
            return "Invalid input data", HTTPStatus.BAD_REQUEST

        try:
            dataflow_job_manager = DirectIngestDataflowJobManager()
            dataflow_job_manager.invalidate_all_dataflow_jobs(
                state_code, ingest_instance
            )
            return ("", HTTPStatus.OK)
        except ValueError as error:
            logging.exception(error)
            return f"{error}", HTTPStatus.INTERNAL_SERVER_ERROR

    @bp.route(
        "/api/ingest_operations/get_latest_ingest_dataflow_raw_data_watermarks/<state_code_str>/<instance_str>"
    )
    def _get_latest_ingest_dataflow_raw_data_watermarks(
        state_code_str: str,
        instance_str: str,
    ) -> Tuple[Response, HTTPStatus]:
        try:
            state_code = StateCode(state_code_str)
            instance = DirectIngestInstance(instance_str)
        except ValueError:
            return (jsonify("Invalid input data"), HTTPStatus.BAD_REQUEST)

        raw_data_watermarks = get_latest_run_raw_data_watermarks(state_code, instance)

        return (
            jsonify(
                {
                    file_tag: watermark.isoformat()
                    for file_tag, watermark in raw_data_watermarks.items()
                }
            ),
            HTTPStatus.OK,
        )

    @bp.route(
        "/api/ingest_operations/get_latest_raw_data_tags_not_meeting_watermark/<state_code_str>/<instance_str>"
    )
    def _get_latest_raw_data_tags_not_meeting_watermark(
        state_code_str: str,
        instance_str: str,
    ) -> Tuple[Response, HTTPStatus]:
        try:
            state_code = StateCode(state_code_str)
            instance = DirectIngestInstance(instance_str)
        except ValueError:
            return (jsonify("Invalid input data"), HTTPStatus.BAD_REQUEST)

        raw_data_tags = get_raw_data_tags_not_meeting_watermark(state_code, instance)

        return (
            jsonify(raw_data_tags),
            HTTPStatus.OK,
        )

    # TODO(#30695): Actually rip SECONDARY instance dataflow info out of the admin panel
    #  and remove the instance_str arg from this endpoint.
    @bp.route(
        "/api/ingest_operations/get_latest_run_ingest_view_results/<state_code_str>/<instance_str>"
    )
    def _get_latest_run_ingest_view_results(
        state_code_str: str,
        instance_str: str,  # pylint: disable=unused-argument
    ) -> Tuple[Response, HTTPStatus]:
        try:
            state_code = StateCode(state_code_str)
        except ValueError:
            return (jsonify("Invalid input data"), HTTPStatus.BAD_REQUEST)

        ingest_view_results = get_latest_run_ingest_view_results(state_code)

        return (jsonify(ingest_view_results), HTTPStatus.OK)

    # TODO(#30695): Actually rip SECONDARY instance dataflow info out of the admin panel
    #  and remove the instance_str arg from this endpoint.
    @bp.route(
        "/api/ingest_operations/get_latest_run_state_results/<state_code_str>/<instance_str>"
    )
    def _get_latest_run_state_results(
        state_code_str: str,
        instance_str: str,  # pylint: disable=unused-argument
    ) -> Tuple[Response, HTTPStatus]:
        try:
            state_code = StateCode(state_code_str)
        except ValueError:
            return (jsonify("Invalid input data"), HTTPStatus.BAD_REQUEST)

        ingest_view_results = get_latest_run_state_results(state_code)

        return (jsonify(ingest_view_results), HTTPStatus.OK)

    @bp.route(
        "/api/ingest_operations/delete_tables_in_pruning_datasets",
        methods=["POST"],
    )
    def _delete_tables_in_pruning_datasets() -> Tuple[Union[str, Response], HTTPStatus]:
        try:
            request_json = assert_type(request.json, dict)
            state_code = StateCode(request_json["stateCode"])
            ingest_instance = DirectIngestInstance(request_json["instance"].upper())
        except ValueError:
            return "Invalid input data", HTTPStatus.BAD_REQUEST

        try:
            delete_tables_in_pruning_datasets(
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
        "/api/ingest_operations/trigger_calculation_dag",
        methods=["POST"],
    )
    def _trigger_calculation_dag() -> Tuple[Union[str, Response], HTTPStatus]:
        try:
            trigger_calculation_dag()
            return "", HTTPStatus.OK

        except ValueError as error:
            logging.exception(error)
            return f"{error}", HTTPStatus.INTERNAL_SERVER_ERROR

    @bp.route(
        "/api/ingest_operations/all_latest_raw_data_import_run_info",
    )
    def _all_latest_raw_data_import_run_info() -> Tuple[Response, HTTPStatus]:

        all_import_run_summaries = (
            get_ingest_operations_store().get_all_latest_raw_data_import_run_info()
        )

        return (
            jsonify(
                {
                    state_code.value: summary.for_api()
                    for state_code, summary in all_import_run_summaries.items()
                }
            ),
            HTTPStatus.OK,
        )

    @bp.route(
        "/api/ingest_operations/all_current_lock_summaries",
    )
    def _all_current_lock_summaries() -> Tuple[Response, HTTPStatus]:

        all_current_lock_summaries = (
            get_ingest_operations_store().get_all_current_lock_summaries()
        )

        return (
            jsonify(
                {
                    state_code.value: {
                        resource.value: maybe_actor.value if maybe_actor else None
                        for resource, maybe_actor in lock_summary.items()
                    }
                    for state_code, lock_summary in all_current_lock_summaries.items()
                }
            ),
            HTTPStatus.OK,
        )

    @bp.route(
        "/api/ingest_operations/get_latest_raw_data_imports/<state_code_str>/<instance_str>/<file_tag_str>"
    )
    def _get_latest_raw_data_import_runs(
        state_code_str: str,
        instance_str: str,
        file_tag_str: str,
    ) -> Tuple[Response, HTTPStatus]:
        state_code = StateCode(state_code_str)
        raw_data_instance = DirectIngestInstance(instance_str)

        latest_raw_data_import_runs = DirectIngestRawFileImportManager(
            state_code.value, raw_data_instance
        ).get_n_most_recent_imports_for_file_tag(file_tag_str, n=100)

        return (
            jsonify(
                [run.for_api() for run in latest_raw_data_import_runs],
            ),
            HTTPStatus.OK,
        )

    @bp.route("/api/ingest_operations/raw_file_config/<state_code_str>/<file_tag_str>")
    def _get_raw_file_config_for_file_tag(
        state_code_str: str,
        file_tag_str: str,
    ) -> Tuple[Response, HTTPStatus]:
        state_code = StateCode(state_code_str)

        raw_config = get_ingest_operations_store().get_raw_file_config(
            state_code, file_tag_str
        )

        return (
            jsonify(raw_config.for_admin_panel_api() if raw_config else None),
            HTTPStatus.OK,
        )

    @bp.route(
        "/api/ingest_operations/trigger_raw_data_import_dag",
        methods=["POST"],
    )
    def _trigger_raw_data_import_dag() -> Tuple[Union[str, Response], HTTPStatus]:
        try:
            request_json = assert_type(request.json, dict)
            state_code = StateCode(request_json["stateCode"].upper())
            raw_data_instance = DirectIngestInstance(
                request_json["rawDataInstance"].upper()
            )
        except ValueError:
            return "Invalid input data", HTTPStatus.BAD_REQUEST

        try:
            trigger_raw_data_import_dag(
                raw_data_instance=raw_data_instance, state_code_filter=state_code
            )
            return (
                "",
                HTTPStatus.OK,
            )

        except ValueError as error:
            logging.exception(error)
            return f"{error}", HTTPStatus.INTERNAL_SERVER_ERROR

    @bp.route("/api/ingest_operations/is_flashing_in_progress/<state_code_str>")
    def _get_flash_status(
        state_code_str: str,
    ) -> Tuple[Response, HTTPStatus]:
        state_code = StateCode(state_code_str)

        is_flashing = DirectIngestRawDataFlashStatusManager(
            state_code.value
        ).is_flashing_in_progress()

        return (
            jsonify(is_flashing),
            HTTPStatus.OK,
        )

    @bp.route("/api/ingest_operations/is_flashing_in_progress/update", methods=["POST"])
    def _set_flash_status() -> Tuple[str, HTTPStatus]:
        try:
            request_json = assert_type(request.json, dict)
            state_code = StateCode(request_json["stateCode"])
            is_flashing = assert_type(request_json["isFlashing"], bool)
        except ValueError:
            return "Invalid input data", HTTPStatus.BAD_REQUEST

        manager = DirectIngestRawDataFlashStatusManager(state_code.value)

        if is_flashing:
            manager.set_flashing_started()
        else:
            manager.set_flashing_finished()

        return (
            "",
            HTTPStatus.OK,
        )

    @bp.route("/api/ingest_operations/stale_secondary/<state_code_str>")
    def _get_stale_secondary(
        state_code_str: str,
    ) -> Tuple[Response, HTTPStatus]:
        state_code = StateCode(state_code_str)

        stale_secondary = DirectIngestRawFileMetadataManager(
            state_code.value, DirectIngestInstance.SECONDARY
        ).stale_secondary_raw_data()

        return (
            jsonify(stale_secondary),
            HTTPStatus.OK,
        )

    @bp.route(
        "/api/ingest_operations/resource_locks/list_all/<state_code_str>/<instance_str>"
    )
    def _list_all_resource_locks(
        state_code_str: str,
        instance_str: str,
    ) -> Tuple[Response, HTTPStatus]:
        state_code = StateCode(state_code_str)
        raw_data_instance = DirectIngestInstance(instance_str)

        locks = DirectIngestRawDataResourceLockManager(
            region_code=state_code.value,
            raw_data_source_instance=raw_data_instance,
            with_proxy=False,
        ).get_most_recent_locks_for_all_resources()

        return (
            jsonify(
                [
                    {
                        "lockId": lock.lock_id,
                        "rawDataInstance": lock.raw_data_source_instance.value,
                        "lockAcquisitionTime": lock.lock_acquisition_time.isoformat(),
                        "ttlSeconds": lock.lock_ttl_seconds,
                        "description": lock.lock_description,
                        "resource": lock.lock_resource.value,
                        "released": lock.released,
                        "actor": lock.lock_actor.value,
                    }
                    for lock in locks
                ]
            ),
            HTTPStatus.OK,
        )

    @bp.route("/api/ingest_operations/resource_locks/metadata")
    def _get_resource_lock_metadata() -> Tuple[Response, HTTPStatus]:
        metadata = {
            "resources": {
                val.value: description
                for val, description in DirectIngestRawDataResourceLockResource.get_value_descriptions().items()
            },
            "actors": {
                val.value: description
                for val, description in DirectIngestRawDataLockActor.get_value_descriptions().items()
            },
        }
        return jsonify(metadata), HTTPStatus.OK

    @bp.route(
        "/api/ingest_operations/resource_locks/acquire_all",
        methods=["POST"],
    )
    def _acquire_all_locks_for_state_and_instance() -> (
        Tuple[Union[str, Response], HTTPStatus]
    ):
        try:
            request_json = assert_type(request.json, dict)
            state_code = StateCode(request_json["stateCode"])
            raw_data_instance = DirectIngestInstance(request_json["rawDataInstance"])
            description = assert_type(request_json["description"], str)
            ttl_seconds = request_json["ttlSeconds"]
        except ValueError:
            return "Invalid input data", HTTPStatus.BAD_REQUEST

        try:
            _locks = DirectIngestRawDataResourceLockManager(
                region_code=state_code.value,
                raw_data_source_instance=raw_data_instance,
                with_proxy=False,
            ).acquire_all_locks(
                actor=DirectIngestRawDataLockActor.ADHOC,
                description=description,
                ttl_seconds=ttl_seconds,
            )
        except DirectIngestRawDataResourceLockHeldError as e:
            logging.exception(e)
            return f"{e}", HTTPStatus.CONFLICT
        except Exception as e:
            logging.exception(e)
            return f"{e}", HTTPStatus.INTERNAL_SERVER_ERROR

        return (
            "",
            HTTPStatus.OK,
        )

    @bp.route("/api/ingest_operations/resource_locks/release_all", methods=["POST"])
    def _release_locks_by_id() -> Tuple[str, HTTPStatus]:
        try:
            request_json = assert_type(request.json, dict)
            state_code = StateCode(request_json["stateCode"])
            raw_data_instance = DirectIngestInstance(request_json["rawDataInstance"])
            lock_ids = assert_type(request_json["lockIds"], list)
        except ValueError:
            return "Invalid input data", HTTPStatus.BAD_REQUEST

        try:
            manager = DirectIngestRawDataResourceLockManager(
                region_code=state_code.value,
                raw_data_source_instance=raw_data_instance,
                with_proxy=False,
            )
            for lock_id in lock_ids:
                manager.release_lock_by_id(lock_id)
        except DirectIngestRawDataResourceLockAlreadyReleasedError as e:
            logging.exception(e)
            return f"{e}", HTTPStatus.CONFLICT
        except Exception as e:
            logging.exception(e)
            return f"{e}", HTTPStatus.INTERNAL_SERVER_ERROR

        return (
            "",
            HTTPStatus.OK,
        )

    @bp.route(
        "/api/ingest_operations/flash_primary_db/mark_instance_raw_data_invalidated",
        methods=["POST"],
    )
    def _mark_instance_raw_data_invalidated() -> Tuple[str, HTTPStatus]:
        try:
            request_json = assert_type(request.json, dict)
            state_code = StateCode(request_json["stateCode"])
            raw_data_instance = DirectIngestInstance(
                request_json["rawDataInstance"].upper()
            )
        except ValueError:
            return "Invalid input data", HTTPStatus.BAD_REQUEST

        try:
            raw_file_metadata_manager = DirectIngestRawFileMetadataManager(
                region_code=state_code.value,
                raw_data_instance=raw_data_instance,
            )
            raw_file_metadata_manager.mark_instance_data_invalidated()

            return (
                "",
                HTTPStatus.OK,
            )

        except ValueError as error:
            logging.exception(error)
            return f"{error}", HTTPStatus.INTERNAL_SERVER_ERROR

    @bp.route(
        "/api/ingest_operations/flash_primary_db/transfer_raw_data_metadata_to_new_instance",
        methods=["POST"],
    )
    def _transfer_raw_data_metadata_to_new_instance() -> Tuple[str, HTTPStatus]:
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

            import_run_manager = DirectIngestRawFileImportManager(
                region_code=state_code.value,
                raw_data_instance=src_ingest_instance,
            )
            new_import_run_manager = DirectIngestRawFileImportManager(
                region_code=state_code.value,
                raw_data_instance=dest_ingest_instance,
            )

            raw_data_manager = DirectIngestRawFileMetadataManager(
                region_code=state_code.value,
                raw_data_instance=src_ingest_instance,
            )

            new_instance_manager = DirectIngestRawFileMetadataManager(
                region_code=state_code.value,
                raw_data_instance=dest_ingest_instance,
            )

            # wrap w/in a single db transaction so everything is rolled back
            with SessionFactory.using_database(
                import_run_manager.database_key
            ) as session:

                # first import runs, as it uses is_invalidated to make sure all import
                # runs with associated metadata have been properly invalidated
                import_run_manager.transfer_metadata_to_new_instance(
                    new_instance_manager=new_import_run_manager, session=session
                )

                # then the two metadata db
                raw_data_manager.transfer_metadata_to_new_instance(
                    new_instance_manager=new_instance_manager, session=session
                )

            return (
                "",
                HTTPStatus.OK,
            )
        except ValueError as error:
            logging.exception(error)
            return f"{error}", HTTPStatus.INTERNAL_SERVER_ERROR
