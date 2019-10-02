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
"""Configuration for the task queues that need to be initialized in Google
App Engine.
"""
from typing import List

from google.cloud.tasks_v2.proto import queue_pb2

from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import \
    DIRECT_INGEST_SCHEDULER_QUEUE_V2, \
    DIRECT_INGEST_STATE_PROCESS_JOB_QUEUE_V2, \
    DIRECT_INGEST_JAILS_PROCESS_JOB_QUEUE_V2
from recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper import \
    GoogleCloudTasksClientWrapper
from recidiviz.common.google_cloud.protobuf_builder import ProtobufBuilder

DIRECT_INGEST_QUEUE_BASE_CONFIG = queue_pb2.Queue(
    rate_limits=queue_pb2.RateLimits(
        max_dispatches_per_second=100,
        max_concurrent_dispatches=1,
    ),
    retry_config=queue_pb2.RetryConfig(
        max_attempts=5,
    )
)


def _build_cloud_task_queue_configs(
        queue_manager: GoogleCloudTasksClientWrapper) -> List[queue_pb2.Queue]:
    queues = []

    # Direct ingest queues for handling /process_job requests
    for queue_name in [DIRECT_INGEST_STATE_PROCESS_JOB_QUEUE_V2,
                       DIRECT_INGEST_JAILS_PROCESS_JOB_QUEUE_V2,
                       DIRECT_INGEST_SCHEDULER_QUEUE_V2]:
        queue = \
            ProtobufBuilder(queue_pb2.Queue).compose(
                DIRECT_INGEST_QUEUE_BASE_CONFIG
            ).update_args(
                name=queue_manager.format_queue_path(queue_name),
            ).build()
        queues.append(queue)

    # TODO(2428): Migrate all queues created in build_queue_config.py to be
    #  created here instead.
    return queues


def initialize_queues():
    queue_manager = GoogleCloudTasksClientWrapper()
    queue_manager.initialize_cloud_task_queues(
        _build_cloud_task_queue_configs(queue_manager))
