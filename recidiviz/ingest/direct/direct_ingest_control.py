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

from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsDirectIngestController
from recidiviz.ingest.direct.controllers.gcsfs_path import \
    GcsfsFilePath, GcsfsPath
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import \
    DirectIngestCloudTaskManager
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import \
    BaseDirectIngestController
from recidiviz.ingest.direct.controllers.direct_ingest_types import IngestArgs
from recidiviz.ingest.direct.errors import DirectIngestError, \
    DirectIngestErrorType
from recidiviz.utils import environment, regions
from recidiviz.utils.auth import authenticate_request
from recidiviz.utils.params import get_str_param_value, get_bool_param_value
from recidiviz.utils.regions import get_supported_direct_ingest_region_codes

direct_ingest_control = Blueprint('direct_ingest_control', __name__)


@direct_ingest_control.route('/handle_direct_ingest_file')
@authenticate_request
def handle_direct_ingest_file() -> Tuple[str, HTTPStatus]:
    """Called from a Cloud Function when a new file is added to a direct ingest
    bucket. Will trigger a job that deals with normalizing and splitting the
    file as is appropriate, then start the scheduler if allowed.
    """
    region_name = get_str_param_value('region', request.args)
    # The bucket name for the file to ingest
    bucket = get_str_param_value('bucket', request.args)
    # The relative path to the file, not including the bucket name
    relative_file_path = get_str_param_value('relative_file_path',
                                             request.args, preserve_case=True)
    start_ingest = \
        get_bool_param_value('start_ingest', request.args, default=False)

    if not region_name or not bucket \
            or not relative_file_path or start_ingest is None:
        return f'Bad parameters [{request.args}]', HTTPStatus.BAD_REQUEST

    controller = controller_for_region_code(region_name,
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
@authenticate_request
def handle_new_files() -> Tuple[str, HTTPStatus]:
    """Normalizes and splits files in the ingest bucket for a given region as
    is appropriate. Will schedule the next process_job task if no renaming /
    splitting work has been done that will trigger subsequent calls to this
    endpoint.
    """
    logging.info('Received request for direct ingest handle_new_files: %s',
                 request.values)
    region_value = get_str_param_value('region', request.values)
    can_start_ingest = \
        get_bool_param_value('can_start_ingest', request.values, default=False)

    if not region_value or can_start_ingest is None:
        return f'Bad parameters [{request.values}]', HTTPStatus.BAD_REQUEST

    try:
        controller = controller_for_region_code(region_value)
    except DirectIngestError as e:
        if e.error_type == DirectIngestErrorType.INPUT_ERROR:
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
@authenticate_request
def ensure_all_file_paths_normalized() -> Tuple[str, HTTPStatus]:
    logging.info(
        'Received request for direct ingest ensure_all_file_paths_normalized: '
        '%s', request.values)

    supported_regions = get_supported_direct_ingest_region_codes()
    for region_code in supported_regions:
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


@direct_ingest_control.route('/process_job', methods=['POST'])
@authenticate_request
def process_job() -> Tuple[str, HTTPStatus]:
    logging.info('Received request to process direct ingest job: [%s]',
                 request.values)
    region_value = get_str_param_value('region', request.values)

    if not region_value:
        return f'Bad parameters [{request.values}]', HTTPStatus.BAD_REQUEST

    json_data = request.get_data(as_text=True)
    ingest_args = _get_ingest_args(json_data)

    if not ingest_args:
        return f'Could not parse ingest args', HTTPStatus.BAD_REQUEST

    try:
        if not ingest_args:
            raise DirectIngestError(
                msg=f"process_job was called with no IngestArgs.",
                error_type=DirectIngestErrorType.INPUT_ERROR)

        controller = controller_for_region_code(region_value)
    except DirectIngestError as e:
        if e.error_type == DirectIngestErrorType.INPUT_ERROR:
            return str(e), HTTPStatus.BAD_REQUEST
        raise e

    controller.run_ingest_job_and_kick_scheduler_on_completion(ingest_args)
    return '', HTTPStatus.OK


@direct_ingest_control.route('/scheduler', methods=['GET', 'POST'])
@authenticate_request
def scheduler() -> Tuple[str, HTTPStatus]:
    logging.info('Received request for direct ingest scheduler: %s',
                 request.values)
    region_value = get_str_param_value('region', request.values)
    just_finished_job = \
        get_bool_param_value('just_finished_job', request.values, default=False)

    if not region_value or just_finished_job is None:
        return f'Bad parameters [{request.values}]', HTTPStatus.BAD_REQUEST

    try:
        controller = controller_for_region_code(region_value)
    except DirectIngestError as e:
        if e.error_type == DirectIngestErrorType.INPUT_ERROR:
            return str(e), HTTPStatus.BAD_REQUEST
        raise e

    controller.schedule_next_ingest_job_or_wait_if_necessary(just_finished_job)
    return '', HTTPStatus.OK


def controller_for_region_code(
        region_code: str,
        allow_unlaunched: bool = False) -> BaseDirectIngestController:
    """Returns an instance of the region's controller, if one exists."""
    if region_code not in get_supported_direct_ingest_region_codes():
        raise DirectIngestError(
            msg=f"Unsupported direct ingest region [{region_code}]",
            error_type=DirectIngestErrorType.INPUT_ERROR,
        )

    try:
        region = regions.get_region(region_code, is_direct_ingest=True)
    except FileNotFoundError:
        raise DirectIngestError(
            msg=f"Unsupported direct ingest region [{region_code}]",
            error_type=DirectIngestErrorType.INPUT_ERROR,
        )

    if not allow_unlaunched and not region.is_ingest_launched_in_env():
        gae_env = environment.get_gae_environment()
        raise DirectIngestError(
            msg=f"Bad environment [{gae_env}] for direct region "
            f"[{region_code}].",
            error_type=DirectIngestErrorType.INPUT_ERROR,
        )

    controller = region.get_ingestor()

    if not isinstance(controller, BaseDirectIngestController):
        raise DirectIngestError(
            msg=f"Unsupported direct ingest region [{region_code}]",
            error_type=DirectIngestErrorType.INPUT_ERROR,
        )

    return controller


def _get_ingest_args(
        json_data_str: str,
) -> Optional[IngestArgs]:
    if not json_data_str:
        return None
    data = json.loads(json_data_str)
    return DirectIngestCloudTaskManager.json_to_ingest_args(data)
