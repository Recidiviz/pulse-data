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
from typing import Optional

from flask import Blueprint, request

from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import \
    DirectIngestCloudTaskManager
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import \
    BaseDirectIngestController
from recidiviz.ingest.direct.controllers.direct_ingest_types import IngestArgs
from recidiviz.ingest.direct.errors import DirectIngestError, \
    DirectIngestErrorType
from recidiviz.utils import environment, regions
from recidiviz.utils.auth import authenticate_request
from recidiviz.utils.environment import in_gae_production
from recidiviz.utils.params import get_value
from recidiviz.utils.regions import get_supported_direct_ingest_region_codes

direct_ingest_control = Blueprint('direct_ingest_control', __name__)


@direct_ingest_control.route('/process_job', methods=['POST'])
@authenticate_request
def process_job():
    logging.info('Received request to process direct ingest job: [%s]',
                 request.values)
    region_value = get_value('region', request.values)
    json_data = request.get_data(as_text=True)
    ingest_args = _get_ingest_args(json_data)
    if not ingest_args:
        raise DirectIngestError(
            msg=f"process_job was called with no IngestArgs.",
            error_type=DirectIngestErrorType.INPUT_ERROR)
    controller = controller_for_region_code(region_value)
    controller.run_ingest_job_and_kick_scheduler_on_completion(ingest_args)
    return '', HTTPStatus.OK


@direct_ingest_control.route('/scheduler', methods=['GET', 'POST'])
@authenticate_request
def scheduler():
    logging.info('Received request for direct ingest scheduler: %s',
                 request.values)
    region_value = get_value('region', request.values)
    just_finished_job_str = get_value('just_finished_job', request.values)
    just_finished_job = just_finished_job_str is not None and \
        just_finished_job_str.lower() == 'true'
    controller = controller_for_region_code(region_value)
    controller.schedule_next_ingest_job_or_wait_if_necessary(just_finished_job)
    return '', HTTPStatus.OK


def controller_for_region_code(region_code: str) -> BaseDirectIngestController:
    """Returns an instance of the region's controller, if one exists."""
    if region_code not in get_supported_direct_ingest_region_codes():
        raise DirectIngestError(
            msg=f"Unsupported direct ingest region {region_code}",
            error_type=DirectIngestErrorType.INPUT_ERROR,
        )

    try:
        region = regions.get_region(region_code, is_direct_ingest=True)
    except FileNotFoundError:
        raise DirectIngestError(
            msg=f"Unsupported direct ingest region {region_code}",
            error_type=DirectIngestErrorType.INPUT_ERROR,
        )

    gae_env = environment.get_gae_environment()
    # If we are in prod, the region config must be explicitly set to specify
    #  this region can be run in prod. All regions can be triggered to run in
    #  staging.
    if in_gae_production() and region.environment != gae_env:
        raise DirectIngestError(
            msg=f"Bad environment {gae_env} for direct region {region_code}.",
            error_type=DirectIngestErrorType.INPUT_ERROR,
        )

    controller = region.get_ingestor()

    if not isinstance(controller, BaseDirectIngestController):
        raise DirectIngestError(
            msg=f"Unsupported direct ingest region {region_code}",
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
