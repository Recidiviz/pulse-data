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

"""Requests handlers for direct ingest control requests.
"""
import json
import logging
from http import HTTPStatus
from typing import Optional, Tuple

from flask import Blueprint, request

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import DirectIngestGCSFileSystem
from recidiviz.ingest.direct.controllers.direct_ingest_raw_data_table_latest_view_updater import \
    DirectIngestRawDataTableLatestViewUpdater
from recidiviz.ingest.direct.controllers.direct_ingest_raw_update_cloud_task_manager import \
    DirectIngestRawUpdateCloudTaskManager
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsDirectIngestController
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import GcsfsRawDataBQImportArgs, \
    GcsfsIngestViewExportArgs, GcsfsDirectIngestFileType
from recidiviz.cloud_storage.gcsfs_path import \
    GcsfsFilePath, GcsfsPath
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import \
    DirectIngestCloudTaskManager
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import \
    BaseDirectIngestController
from recidiviz.ingest.direct.controllers.direct_ingest_types import IngestArgs, CloudTaskArgs
from recidiviz.ingest.direct.direct_ingest_controller_utils import check_is_region_launched_in_env
from recidiviz.ingest.direct.direct_ingest_region_utils import get_existing_region_dir_names
from recidiviz.ingest.direct.errors import DirectIngestError, \
    DirectIngestErrorType
from recidiviz.utils import regions, monitoring, metadata
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.monitoring import TagKey
from recidiviz.utils.params import get_str_param_value, get_bool_param_value
from recidiviz.utils.regions import get_supported_direct_ingest_region_codes, get_region

direct_ingest_control = Blueprint('direct_ingest_control', __name__)


@direct_ingest_control.route('/normalize_raw_file_path')
@requires_gae_auth
def normalize_raw_file_path() -> Tuple[str, HTTPStatus]:
    """Called from a Cloud Function when a new file is added to a bucket that is configured to rename files but not
    ingest them. For example, a bucket that is being used for automatic data transfer testing.
    """
    # The bucket name for the file to normalize
    bucket = get_str_param_value('bucket', request.args)
    # The relative path to the file, not including the bucket name
    relative_file_path = get_str_param_value('relative_file_path',
                                             request.args, preserve_case=True)

    if not bucket or not relative_file_path:
        return f'Bad parameters [{request.args}]', HTTPStatus.BAD_REQUEST

    path = GcsfsPath.from_bucket_and_blob_name(bucket_name=bucket,
                                               blob_name=relative_file_path)

    if not isinstance(path, GcsfsFilePath):
        raise ValueError(f'Incorrect type [{type(path)}] for path: {path.uri()}')

    fs = DirectIngestGCSFileSystem(GcsfsFactory.build())
    fs.mv_path_to_normalized_path(path, file_type=GcsfsDirectIngestFileType.RAW_DATA)

    return '', HTTPStatus.OK


@direct_ingest_control.route('/handle_direct_ingest_file')
@requires_gae_auth
def handle_direct_ingest_file() -> Tuple[str, HTTPStatus]:
    """Called from a Cloud Function when a new file is added to a direct ingest
    bucket. Will trigger a job that deals with normalizing and splitting the
    file as is appropriate, then start the scheduler if allowed.
    """
    region_code = get_str_param_value('region', request.args)
    # The bucket name for the file to ingest
    bucket = get_str_param_value('bucket', request.args)
    # The relative path to the file, not including the bucket name
    relative_file_path = get_str_param_value('relative_file_path',
                                             request.args, preserve_case=True)
    start_ingest = \
        get_bool_param_value('start_ingest', request.args, default=False)

    if not region_code or not bucket \
            or not relative_file_path or start_ingest is None:
        return f'Bad parameters [{request.args}]', HTTPStatus.BAD_REQUEST

    with monitoring.push_region_tag(region_code):
        controller = controller_for_region_code(region_code,
                                                allow_unlaunched=True)
        if not isinstance(controller, GcsfsDirectIngestController):
            raise DirectIngestError(
                msg=f"Unexpected controller type [{type(controller)}].",
                error_type=DirectIngestErrorType.INPUT_ERROR)

        path = GcsfsPath.from_bucket_and_blob_name(bucket_name=bucket,
                                                   blob_name=relative_file_path)

        if isinstance(path, GcsfsFilePath):
            controller.handle_file(path, start_ingest=start_ingest)

    return '', HTTPStatus.OK


@direct_ingest_control.route('/handle_new_files', methods=['GET', 'POST'])
@requires_gae_auth
def handle_new_files() -> Tuple[str, HTTPStatus]:
    """Normalizes and splits files in the ingest bucket for a given region as
    is appropriate. Will schedule the next process_job task if no renaming /
    splitting work has been done that will trigger subsequent calls to this
    endpoint.
    """
    logging.info('Received request for direct ingest handle_new_files: %s',
                 request.values)
    region_code = get_str_param_value('region', request.values)
    can_start_ingest = \
        get_bool_param_value('can_start_ingest', request.values, default=False)

    if not region_code or can_start_ingest is None:
        return f'Bad parameters [{request.values}]', HTTPStatus.BAD_REQUEST

    with monitoring.push_region_tag(region_code):
        try:
            controller = controller_for_region_code(region_code, allow_unlaunched=True)
        except DirectIngestError as e:
            if e.is_bad_request():
                return str(e), HTTPStatus.BAD_REQUEST
            raise e

        if not isinstance(controller, GcsfsDirectIngestController):
            raise DirectIngestError(
                msg=f"Unexpected controller type [{type(controller)}].",
                error_type=DirectIngestErrorType.INPUT_ERROR)

        controller.handle_new_files(can_start_ingest=can_start_ingest)
    return '', HTTPStatus.OK


@direct_ingest_control.route('/ensure_all_file_paths_normalized',
                             methods=['GET', 'POST'])
@requires_gae_auth
def ensure_all_file_paths_normalized() -> Tuple[str, HTTPStatus]:
    logging.info(
        'Received request for direct ingest ensure_all_file_paths_normalized: '
        '%s', request.values)

    supported_regions = get_supported_direct_ingest_region_codes()
    for region_code in supported_regions:
        logging.info("Ensuring paths normalized for region [%s]", region_code)
        with monitoring.push_region_tag(region_code):
            try:
                controller = controller_for_region_code(region_code,
                                                        allow_unlaunched=True)
            except DirectIngestError as e:
                raise e
            if not isinstance(controller, BaseDirectIngestController):
                raise DirectIngestError(
                    msg=f"Unexpected controller type [{type(controller)}].",
                    error_type=DirectIngestErrorType.INPUT_ERROR)

            if not isinstance(controller, GcsfsDirectIngestController):
                continue

            can_start_ingest = controller.region.is_ingest_launched_in_env()
            controller.cloud_task_manager.\
                create_direct_ingest_handle_new_files_task(
                    controller.region, can_start_ingest=can_start_ingest)
    return '', HTTPStatus.OK


@direct_ingest_control.route('/raw_data_import', methods=['POST'])
@requires_gae_auth
def raw_data_import() -> Tuple[str, HTTPStatus]:
    """Imports a single raw direct ingest CSV file from a location in GCS File System to its corresponding raw data
    table in BQ.
    """
    logging.info('Received request to do direct ingest raw data import: [%s]', request.values)
    region_code = get_str_param_value('region', request.values)

    if not region_code:
        return f'Bad parameters [{request.values}]', HTTPStatus.BAD_REQUEST

    with monitoring.push_region_tag(region_code):
        json_data = request.get_data(as_text=True)
        data_import_args = _parse_cloud_task_args(json_data)

        if not data_import_args:
            raise DirectIngestError(msg="raw_data_import was called with no IngestArgs.",
                                    error_type=DirectIngestErrorType.INPUT_ERROR)

        if not isinstance(data_import_args, GcsfsRawDataBQImportArgs):
            raise DirectIngestError(
                msg=f"raw_data_import was called with incorrect args type [{type(data_import_args)}].",
                error_type=DirectIngestErrorType.INPUT_ERROR)

        with monitoring.push_tags({TagKey.RAW_DATA_IMPORT_TAG: data_import_args.task_id_tag()}):
            try:
                controller = controller_for_region_code(region_code)
            except DirectIngestError as e:
                if e.is_bad_request():
                    return str(e), HTTPStatus.BAD_REQUEST
                raise e

            if not isinstance(controller, GcsfsDirectIngestController):
                raise DirectIngestError(
                    msg=f"Unexpected controller type [{type(controller)}].",
                    error_type=DirectIngestErrorType.INPUT_ERROR)

            controller.do_raw_data_import(data_import_args)
    return '', HTTPStatus.OK


@direct_ingest_control.route('/create_raw_data_latest_view_update_tasks', methods=['GET', 'POST'])
@requires_gae_auth
def create_raw_data_latest_view_update_tasks() -> Tuple[str, HTTPStatus]:
    """Creates tasks for every direct ingest region with SQL preprocessing
    enabled to update the raw data table latest views.
    """
    raw_update_ctm = DirectIngestRawUpdateCloudTaskManager()

    for region_code in get_existing_region_dir_names():
        with monitoring.push_region_tag(region_code):
            region = get_region(region_code, is_direct_ingest=True)
            if region.are_raw_data_bq_imports_enabled_in_env():
                logging.info('Creating raw data latest view update task for region [%s]', region_code)
                raw_update_ctm.create_raw_data_latest_view_update_task(region_code)
            else:
                logging.info('Skipping raw data latest view update for region [%s] - raw data imports not enabled.',
                             region_code)
    return '', HTTPStatus.OK


@direct_ingest_control.route('/update_raw_data_latest_views_for_state', methods=['POST'])
@requires_gae_auth
def update_raw_data_latest_views_for_state() -> Tuple[str, HTTPStatus]:
    """Updates raw data tables for a given state
    """
    logging.info('Received request to do direct ingest raw data update: [%s]', request.values)
    region_code = get_str_param_value('region', request.values)

    if not region_code:
        return f'Bad parameters [{request.values}]', HTTPStatus.BAD_REQUEST

    with monitoring.push_region_tag(region_code):
        bq_client = BigQueryClientImpl(project_id=metadata.project_id())
        controller = DirectIngestRawDataTableLatestViewUpdater(region_code, metadata.project_id(), bq_client)
        controller.update_views_for_state()
    return '', HTTPStatus.OK


@direct_ingest_control.route('/ingest_view_export', methods=['POST'])
@requires_gae_auth
def ingest_view_export() -> Tuple[str, HTTPStatus]:
    """Exports an ingest view from BQ to a file in the region's GCS File System ingest bucket that is ready to be
    processed and ingested into our Recidiviz DB.
    """
    logging.info('Received request to do direct ingest view export: [%s]', request.values)
    region_code = get_str_param_value('region', request.values)

    if not region_code:
        return f'Bad parameters [{request.values}]', HTTPStatus.BAD_REQUEST

    with monitoring.push_region_tag(region_code):
        json_data = request.get_data(as_text=True)
        ingest_view_export_args = _parse_cloud_task_args(json_data)

        if not ingest_view_export_args:
            raise DirectIngestError(msg="raw_data_import was called with no IngestArgs.",
                                    error_type=DirectIngestErrorType.INPUT_ERROR)

        if not isinstance(ingest_view_export_args, GcsfsIngestViewExportArgs):
            raise DirectIngestError(
                msg=f"raw_data_import was called with incorrect args type [{type(ingest_view_export_args)}].",
                error_type=DirectIngestErrorType.INPUT_ERROR)
        with monitoring.push_tags({TagKey.INGEST_VIEW_EXPORT_TAG: ingest_view_export_args.task_id_tag()}):
            try:
                controller = controller_for_region_code(region_code)
            except DirectIngestError as e:
                if e.is_bad_request():
                    return str(e), HTTPStatus.BAD_REQUEST
                raise e

            if not isinstance(controller, GcsfsDirectIngestController):
                raise DirectIngestError(
                    msg=f"Unexpected controller type [{type(controller)}].",
                    error_type=DirectIngestErrorType.INPUT_ERROR)

            controller.do_ingest_view_export(ingest_view_export_args)
    return '', HTTPStatus.OK


@direct_ingest_control.route('/process_job', methods=['POST'])
@requires_gae_auth
def process_job() -> Tuple[str, HTTPStatus]:
    """Processes a single direct ingest file, specified in the provided ingest
    arguments.
    """
    logging.info('Received request to process direct ingest job: [%s]',
                 request.values)
    region_code = get_str_param_value('region', request.values)

    if not region_code:
        return f'Bad parameters [{request.values}]', HTTPStatus.BAD_REQUEST

    with monitoring.push_region_tag(region_code):
        json_data = request.get_data(as_text=True)
        ingest_args = _parse_cloud_task_args(json_data)

        if not ingest_args:
            raise DirectIngestError(msg="process_job was called with no IngestArgs.",
                                    error_type=DirectIngestErrorType.INPUT_ERROR)

        if not isinstance(ingest_args, IngestArgs):
            raise DirectIngestError(msg=f"process_job was called with incorrect args type [{type(ingest_args)}].",
                                    error_type=DirectIngestErrorType.INPUT_ERROR)

        if not ingest_args:
            return 'Could not parse ingest args', HTTPStatus.BAD_REQUEST
        with monitoring.push_tags({TagKey.INGEST_TASK_TAG: ingest_args.task_id_tag()}):
            try:
                controller = controller_for_region_code(region_code)
            except DirectIngestError as e:
                if e.is_bad_request():
                    return str(e), HTTPStatus.BAD_REQUEST
                raise e

            controller.run_ingest_job_and_kick_scheduler_on_completion(ingest_args)
    return '', HTTPStatus.OK


@direct_ingest_control.route('/scheduler', methods=['GET', 'POST'])
@requires_gae_auth
def scheduler() -> Tuple[str, HTTPStatus]:
    logging.info('Received request for direct ingest scheduler: %s',
                 request.values)
    region_code = get_str_param_value('region', request.values)
    just_finished_job = \
        get_bool_param_value('just_finished_job', request.values, default=False)

    if not region_code or just_finished_job is None:
        return f'Bad parameters [{request.values}]', HTTPStatus.BAD_REQUEST

    with monitoring.push_region_tag(region_code):
        try:
            controller = controller_for_region_code(region_code)
        except DirectIngestError as e:
            if e.is_bad_request():
                return str(e), HTTPStatus.BAD_REQUEST
            raise e

        controller.schedule_next_ingest_job_or_wait_if_necessary(
            just_finished_job)
    return '', HTTPStatus.OK


def kick_all_schedulers() -> None:
    """Kicks all ingest schedulers to restart ingest"""
    supported_regions = get_supported_direct_ingest_region_codes()
    for region_code in supported_regions:
        with monitoring.push_region_tag(region_code):
            try:
                controller = controller_for_region_code(region_code, allow_unlaunched=False)
            except DirectIngestError as e:
                raise e
            if not isinstance(controller, BaseDirectIngestController):
                raise DirectIngestError(
                    msg=f"Unexpected controller type [{type(controller)}].",
                    error_type=DirectIngestErrorType.INPUT_ERROR)

            if not isinstance(controller, GcsfsDirectIngestController):
                continue

            controller.kick_scheduler(just_finished_job=False)


def controller_for_region_code(
        region_code: str,
        allow_unlaunched: bool = False) -> BaseDirectIngestController:
    """Returns an instance of the region's controller, if one exists."""
    if region_code not in get_supported_direct_ingest_region_codes():
        raise DirectIngestError(
            msg=f"Unsupported direct ingest region [{region_code}] in project [{metadata.project_id()}]",
            error_type=DirectIngestErrorType.INPUT_ERROR,
        )

    try:
        region = regions.get_region(region_code, is_direct_ingest=True)
    except FileNotFoundError as e:
        raise DirectIngestError(
            msg=f"Region [{region_code}] has no registered manifest",
            error_type=DirectIngestErrorType.INPUT_ERROR,
        ) from e

    if not allow_unlaunched and not region.is_ingest_launched_in_env():
        check_is_region_launched_in_env(region)

    controller = region.get_ingestor()

    if not isinstance(controller, BaseDirectIngestController):
        raise DirectIngestError(
            msg=f"Controller for direct ingest region [{region_code}] has unexpected type [{type(controller)}]",
            error_type=DirectIngestErrorType.INPUT_ERROR,
        )

    return controller


def _parse_cloud_task_args(
        json_data_str: str,
) -> Optional[CloudTaskArgs]:
    if not json_data_str:
        return None
    data = json.loads(json_data_str)
    return DirectIngestCloudTaskManager.json_to_cloud_task_args(data)
