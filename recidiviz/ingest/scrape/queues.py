# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

"""Cloud Tasks queue helper functions."""

import json
import uuid

from google.cloud import tasks_v2beta3

from recidiviz.utils import environment, metadata

_client = None
def client():
    global _client
    if not _client:
        _client = tasks_v2beta3.CloudTasksClient()
    return _client

@environment.test_only
def clear_client():
    global _client
    _client = None

def format_queue_path(queue_name):
    """Formats a queue name into its full Cloud Tasks queue path.

    Args:
        queue_name: `str` queue name.
    Returns:
        A Cloud Tasks queue path string.
    """
    full_queue_path = client().queue_path(
        metadata.project_id(),
        metadata.region(),
        queue_name)

    return full_queue_path


def format_task_path(queue_name: str, region_code: str, task_id: str):
    """Creates a task path out of the necessary parts.

    Task path is of the form:
        '/projects/{project}/locations/{location}'
        '/queues/{queue}/tasks/{region_code}-{task_id}'
    """
    return client().task_path(
        metadata.project_id(),
        metadata.region(),
        queue_name,
        '{}-{}'.format(region_code, task_id))


def purge_queue(queue_name):
    """Purge a queue.

    Args:
        queue_name: `str` queue name.
    """
    queue_path = format_queue_path(queue_name)
    client().purge_queue(queue_path)


def create_task(*, region_code, queue_name, url, body):
    """Create a task in a queue.

    Args:
        url: `str` App Engine worker url.
        queue_name: `str` queue name.
        body: `dict` task body to be passed to worker.
    """
    body_encoded = json.dumps(body).encode()
    task = {
        'name': format_task_path(queue_name, region_code, uuid.uuid4()),
        'app_engine_http_request': {
            'relative_uri': url,
            'body': body_encoded
        }
    }

    queue_path = format_queue_path(queue_name)
    client().create_task(queue_path, task)
