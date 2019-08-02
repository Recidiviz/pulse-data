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

from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import \
    BaseDirectIngestController
from recidiviz.ingest.direct.controllers.direct_ingest_types import IngestArgs
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsIngestArgs
from recidiviz.ingest.direct.errors import DirectIngestError, \
    DirectIngestErrorType
from recidiviz.utils import environment, regions
from recidiviz.utils.auth import authenticate_request
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
    controller.run_ingest_job(ingest_args)
    return '', HTTPStatus.OK


@direct_ingest_control.route('/scheduler', methods=['GET', 'POST'])
@authenticate_request
def scheduler():
    logging.info('Received request for direct ingest scheduler: %s',
                 request.values)
    region_value = get_value('region', request.values)
    json_data = request.get_data(as_text=True)
    ingest_args = _get_ingest_args(json_data)
    controller = controller_for_region_code(region_value)
    controller.run_next_ingest_job_if_necessary_or_schedule_wait(ingest_args)
    return '', HTTPStatus.OK


def controller_for_region_code(region_code: str) -> BaseDirectIngestController:
    """Returns an instance of the region's controller, if one exists."""
    if region_code not in get_supported_direct_ingest_region_codes():
        raise DirectIngestError(
            msg=f"Unsupported direct ingest region {region_code}",
            error_type=DirectIngestErrorType.INPUT_ERROR,
        )

    gae_env = environment.get_gae_environment()

    try:
        region = regions.get_region(region_code, is_direct_ingest=True)
    except FileNotFoundError:
        raise DirectIngestError(
            msg=f"Unsupported direct ingest region {region_code}",
            error_type=DirectIngestErrorType.INPUT_ERROR,
        )

    if region.environment != gae_env:
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


def _get_ingest_args(json_data: str) -> Optional[IngestArgs]:
    if not json_data:
        return None
    data = json.loads(json_data)
    if 'ingest_args' in data and 'args_type' in data:
        args_type = data['args_type']
        ingest_args = data['ingest_args']
        if args_type == IngestArgs.__name__:
            return IngestArgs.from_serializable(ingest_args)
        if args_type == GcsfsIngestArgs.__name__:
            return GcsfsIngestArgs.from_serializable(ingest_args)
        logging.error('Unexpected args_type in json_data: %s', args_type)
    return None
