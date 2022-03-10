# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Contains the API for generating supplemental datasets that do not go through
traditional calculation or ingest pipelines or rely on reference views."""
import datetime
import logging
import uuid
from http import HTTPStatus
from typing import Tuple
from urllib.parse import urlencode

import pytz
from flask import Blueprint, request

from recidiviz.calculator.supplemental.supplemental_dataset import (
    StateSpecificSupplementalDatasetGenerator,
)
from recidiviz.calculator.supplemental.supplemental_dataset_generator_factory import (
    SupplementalDatasetGeneratorFactory,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.google_cloud.cloud_task_queue_manager import (
    CloudTaskQueueInfo,
    CloudTaskQueueManager,
)
from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import (
    BIGQUERY_QUEUE_V2,
)
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.params import get_str_param_value

# TODO(#11312): Remove once dataflow pipeline is deployed.
supplemental_dataset_manager_blueprint = Blueprint("supplemental_dataset", __name__)


@supplemental_dataset_manager_blueprint.route(
    "/generate_supplemental_dataset", methods=["GET", "POST"]
)
@requires_gae_auth
def generate_supplemental_dataset() -> Tuple[str, HTTPStatus]:
    """Endpoint that generates a single BigQuery dataset that is apart from
    calculation or ingest pipelines or depends on reference views. Relies on both
    a region_code and a table_id to be supplied in order to appropriately execute the
    generation."""

    region_code = get_str_param_value("region", request.values)
    table_id = get_str_param_value("table_id", request.values)
    dataset_override = get_str_param_value("dataset_override", request.values)

    if not region_code or not table_id:
        return (
            f"Missing region_code or table_id: {request.values}",
            HTTPStatus.BAD_REQUEST,
        )

    state_code = StateCode.get(region_code)
    if not state_code:
        return (f"Invalid region_code: {region_code}", HTTPStatus.BAD_REQUEST)

    try:
        supplemental_dataset_generator: StateSpecificSupplementalDatasetGenerator = (
            SupplementalDatasetGeneratorFactory.build(region_code=state_code)
        )
    except ValueError:
        return (
            f"Unexpected region_code for supplemental dataset generator: {region_code}",
            HTTPStatus.BAD_REQUEST,
        )

    try:
        supplemental_dataset_generator.generate_dataset_table(
            table_id, dataset_override
        )
    except Exception as e:
        return (
            f"Unexpected error when generating {table_id}: {e}",
            HTTPStatus.INTERNAL_SERVER_ERROR,
        )

    return "", HTTPStatus.OK


@supplemental_dataset_manager_blueprint.route(
    "/handle_supplemental_dataset_generation", methods=["GET", "POST"]
)
@requires_gae_auth
def handle_supplemental_dataset_generation() -> Tuple[str, HTTPStatus]:
    """Handles a request to generate a supplemental dataset and creates a Cloud Task
    in the BigQuery queue to begin the generation process."""

    region_code = get_str_param_value("region", request.values)
    table_id = get_str_param_value("table_id", request.values)
    dataset_override = get_str_param_value("dataset_override", request.values)

    bq_cloud_task_manager = CloudTaskQueueManager(
        queue_info_cls=CloudTaskQueueInfo, queue_name=BIGQUERY_QUEUE_V2
    )

    if not region_code or not table_id:
        response = f"Bad parameters [{request.values}]"
        return response, HTTPStatus.BAD_REQUEST

    task_id = "-".join(
        [
            "generate_supplemental_dataset",
            str(datetime.datetime.now(tz=pytz.UTC).date()),
            str(uuid.uuid4()),
        ]
    )
    params = {"region": region_code, "table_id": table_id}
    if dataset_override:
        params["dataset_override"] = dataset_override
    relative_uri = (
        f"/supplemental_dataset/generate_supplemental_dataset?{urlencode(params)}"
    )

    logging.info("Received request to generate %s", table_id)
    bq_cloud_task_manager.create_task(
        task_id=task_id, relative_uri=relative_uri, body={}
    )

    return "", HTTPStatus.OK
