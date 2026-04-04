# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Shared route helper functions for writeback endpoints."""
import logging
from http import HTTPStatus
from typing import Any, Callable

from flask import Response, g, jsonify, make_response, request

from recidiviz.case_triage.api_schemas_utils import load_api_schema
from recidiviz.case_triage.workflows.constants import ExternalSystemRequestStatus
from recidiviz.case_triage.workflows.utils import jsonify_response
from recidiviz.case_triage.workflows.writeback.base import (
    WritebackExecutorInterface,
    WritebackStatusTracker,
)
from recidiviz.common.google_cloud.single_cloud_task_queue_manager import (
    CloudTaskQueueInfo,
    SingleCloudTaskQueueManager,
    get_cloud_task_json_body,
)

WORKFLOWS_EXTERNAL_SYSTEM_REQUESTS_QUEUE = "workflows-external-system-requests-queue"

# TODO(#68381): Make it so we always use a str (and probably pass around external id
# type!). Once this is done, consider making this an actual factory interface with a
# build function.
WritebackExecutorFactory = (
    Callable[[str], WritebackExecutorInterface]
    | Callable[[int], WritebackExecutorInterface]
)


def _execute_writeback(
    writeback_executor: WritebackExecutorInterface,
    status_tracker: WritebackStatusTracker,
    operation_action_description: str,
    request_data: dict[str, Any],
) -> Response:
    """Parses request data, executes writeback, tracks status, and returns a response."""
    try:
        data = writeback_executor.parse_request_data(request_data)
        writeback_executor.execute(data)
    except Exception as e:
        logging.error("'%s' failed: %s", operation_action_description, e)
        status_tracker.set_status(ExternalSystemRequestStatus.FAILURE)
        return jsonify_response(
            f"'{operation_action_description}' failed",
            HTTPStatus.INTERNAL_SERVER_ERROR,
        )
    status_tracker.set_status(ExternalSystemRequestStatus.SUCCESS)
    return jsonify_response(
        f"'{operation_action_description}' completed",
        HTTPStatus.OK,
    )


def handle_writeback_direct(
    writeback_executor_factory: WritebackExecutorFactory,
) -> Response:
    """Writes directly to the external system using the provided interface.

    Args:
        writeback_executor_factory: Factory for the executor to use to perform writeback.
    """
    writeback_executor = writeback_executor_factory(g.api_data["person_external_id"])
    writeback_config = writeback_executor.config()
    status_tracker = writeback_executor.create_status_tracker()

    status_tracker.set_status(ExternalSystemRequestStatus.IN_PROGRESS)
    raw_data = writeback_config.api_schema_cls().dump(g.api_data)
    return _execute_writeback(
        writeback_executor,
        status_tracker,
        writeback_config.operation_action_description,
        raw_data,
    )


def handle_writeback_enqueue(
    writeback_executor_factory: WritebackExecutorFactory,
    base_url: str,
    handler_path: str,
) -> Response:
    """Enqueues a Cloud Task to perform the writeback separately.

    Args:
        writeback_executor_factory: Factory for the executor to use to perform writeback.
        base_url: Base URL of the service (for Cloud Task Referer header and handler URL).
        handler_path: Relative path to the Cloud Task handler endpoint.
    """
    writeback_executor = writeback_executor_factory(g.api_data["person_external_id"])
    writeback_config = writeback_executor.config()
    status_tracker = writeback_executor.create_status_tracker()

    try:
        cloud_task_manager = SingleCloudTaskQueueManager(
            queue_info_cls=CloudTaskQueueInfo,
            queue_name=WORKFLOWS_EXTERNAL_SYSTEM_REQUESTS_QUEUE,
        )

        headers_copy = dict(request.headers)
        headers_copy["Referer"] = base_url

        cloud_task_manager.create_task(
            absolute_uri=f"{base_url}{handler_path}",
            body=writeback_config.api_schema_cls().dump(g.api_data),
            headers=headers_copy,
        )
    except Exception as e:
        logging.error(e)
        status_tracker.set_status(ExternalSystemRequestStatus.FAILURE)
        return jsonify_response(
            f"'{writeback_config.operation_action_description}' failed to queue",
            HTTPStatus.INTERNAL_SERVER_ERROR,
        )

    status_tracker.set_status(ExternalSystemRequestStatus.IN_PROGRESS)
    return make_response(
        jsonify(
            f"'{writeback_config.operation_action_description}' queued successfully"
        ),
        HTTPStatus.OK,
    )


def handle_writeback_cloud_task(
    writeback_executor_factory: WritebackExecutorFactory,
) -> Response:
    """Writes to the external system, pulling from the cloud task request body.

    Args:
        writeback_executor_factory: Factory for the executor to use to perform writeback.
    """
    cloud_task_body = get_cloud_task_json_body()
    person_external_id = cloud_task_body.get("person_external_id")

    if person_external_id is None:
        logging.error("No person_external_id provided")
        return jsonify_response(
            "person_external_id is required for writeback requests",
            HTTPStatus.BAD_REQUEST,
        )

    writeback_executor = writeback_executor_factory(person_external_id)
    writeback_config = writeback_executor.config()
    status_tracker = writeback_executor.create_status_tracker()

    try:
        # Validate schema
        raw_data = load_api_schema(writeback_config.api_schema_cls, cloud_task_body)
    except Exception as e:
        logging.error("Schema validation failed: %s", e)
        status_tracker.set_status(ExternalSystemRequestStatus.FAILURE)
        return jsonify_response(
            f"{writeback_config.operation_action_description}' failed due to invalid request body",
            HTTPStatus.BAD_REQUEST,
        )

    return _execute_writeback(
        writeback_executor,
        status_tracker,
        writeback_config.operation_action_description,
        request_data=writeback_config.api_schema_cls().dump(raw_data),
    )
