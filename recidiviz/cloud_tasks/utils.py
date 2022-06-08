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
"""Helpers related to Google Cloud Tasks."""

from flask import request


def get_current_cloud_task_id() -> str:
    """Returns the cloud task id of the cloud task that this code is currently
    being executed from. This function can only be called from within a task created
    with the `app_engine_http_request` arg, i.e.:

    my_task = tasks_v2.types.task_pb2.Task(
        name=task_path,
        app_engine_http_request={
            ...
        },
    ).

    For a task with a full path like this:
    '/projects/{project}/locations/{location}/queues/{queue}/tasks/{task_name}'

    ... this function returns the 'task_name' portion of that path.
    """
    current_task_id = request.headers.get("X-AppEngine-TaskName")
    if not current_task_id:
        raise ValueError(
            f"Expected 'X-AppEngine-TaskName' in headers: "
            f"{list(request.headers.keys())}. This function can only be called within"
            f"the context of an App Engine Cloud Task."
        )
    return current_task_id
