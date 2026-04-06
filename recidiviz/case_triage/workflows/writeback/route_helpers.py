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

from flask import Response, jsonify, make_response, request

from recidiviz.case_triage.workflows.constants import ExternalSystemRequestStatus
from recidiviz.case_triage.workflows.utils import jsonify_response
from recidiviz.case_triage.workflows.writeback.base import (
    RequestDataT,
    WritebackExecutorInterface,
)
from recidiviz.common.google_cloud.single_cloud_task_queue_manager import (
    CloudTaskQueueInfo,
    SingleCloudTaskQueueManager,
)

WORKFLOWS_EXTERNAL_SYSTEM_REQUESTS_QUEUE = "workflows-external-system-requests-queue"


def handle_writeback(
    writeback_executor: WritebackExecutorInterface[RequestDataT],
) -> Response:
    """Writes directly to the external system.

    Args:
        writeback_executor: The executor to use to perform writeback.
    """
    status_tracker = writeback_executor.create_status_tracker()

    status_tracker.set_status(ExternalSystemRequestStatus.IN_PROGRESS)

    try:
        writeback_executor.execute()
    except Exception as e:
        logging.error(
            "'%s' failed: %s", writeback_executor.operation_action_description, e
        )
        status_tracker.set_status(ExternalSystemRequestStatus.FAILURE)
        return jsonify_response(
            f"'{writeback_executor.operation_action_description}' failed",
            HTTPStatus.INTERNAL_SERVER_ERROR,
        )
    status_tracker.set_status(ExternalSystemRequestStatus.SUCCESS)
    return jsonify_response(
        f"'{writeback_executor.operation_action_description}' completed",
        HTTPStatus.OK,
    )


def handle_writeback_enqueue(
    writeback_executor: WritebackExecutorInterface[RequestDataT],
    base_url: str,
    handler_path: str,
) -> Response:
    """Enqueues a Cloud Task to perform the writeback separately.

    Args:
        writeback_executor: The executor to use to perform writeback.
        base_url: Base URL of the service (for Cloud Task Referer header and handler URL).
        handler_path: Relative path to the Cloud Task handler endpoint.
    """
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
            body=writeback_executor.to_cloud_task_payload(),
            headers=headers_copy,
        )
    except Exception as e:
        logging.error(
            "'%s' failed to queue: %s",
            writeback_executor.operation_action_description,
            e,
        )
        status_tracker.set_status(ExternalSystemRequestStatus.FAILURE)
        return jsonify_response(
            f"'{writeback_executor.operation_action_description}' failed to queue",
            HTTPStatus.INTERNAL_SERVER_ERROR,
        )

    status_tracker.set_status(ExternalSystemRequestStatus.IN_PROGRESS)
    return make_response(
        jsonify(
            f"'{writeback_executor.operation_action_description}' queued successfully"
        ),
        HTTPStatus.OK,
    )
