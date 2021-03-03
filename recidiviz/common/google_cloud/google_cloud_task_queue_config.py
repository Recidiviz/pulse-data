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
import logging
from multiprocessing.pool import ThreadPool
from typing import List

from google.cloud import tasks_v2
from google.cloud.tasks_v2.proto import queue_pb2
from google.oauth2 import credentials
from google.protobuf import duration_pb2

from recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper import (
    GoogleCloudTasksClientWrapper,
)
from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import (
    BIGQUERY_QUEUE_V2,
    DIRECT_INGEST_BQ_IMPORT_EXPORT_QUEUE_V2,
    DIRECT_INGEST_JAILS_PROCESS_JOB_QUEUE_V2,
    DIRECT_INGEST_SCHEDULER_QUEUE_V2,
    DIRECT_INGEST_STATE_PROCESS_JOB_QUEUE_V2,
    JOB_MONITOR_QUEUE_V2,
    SCRAPER_PHASE_QUEUE_V2,
)
from recidiviz.common.google_cloud.protobuf_builder import ProtobufBuilder
from recidiviz.utils import metadata, regions, vendors

DIRECT_INGEST_QUEUE_BASE_CONFIG = queue_pb2.Queue(
    rate_limits=queue_pb2.RateLimits(
        max_dispatches_per_second=100,
        max_concurrent_dispatches=1,
    ),
    retry_config=queue_pb2.RetryConfig(
        max_attempts=5,
    ),
    stackdriver_logging_config=queue_pb2.StackdriverLoggingConfig(
        sampling_ratio=1.0,
    ),
)

BIGQUERY_QUEUE_CONFIG = queue_pb2.Queue(
    name=BIGQUERY_QUEUE_V2,
    rate_limits=queue_pb2.RateLimits(
        max_dispatches_per_second=1,
        max_concurrent_dispatches=1,
    ),
    retry_config=queue_pb2.RetryConfig(
        max_attempts=1,
    ),
    stackdriver_logging_config=queue_pb2.StackdriverLoggingConfig(
        sampling_ratio=1.0,
    ),
)

JOB_MONITOR_QUEUE_CONFIG = queue_pb2.Queue(
    name=JOB_MONITOR_QUEUE_V2,
    rate_limits=queue_pb2.RateLimits(
        max_dispatches_per_second=1,
        max_concurrent_dispatches=1,
    ),
    retry_config=queue_pb2.RetryConfig(
        min_backoff=duration_pb2.Duration(seconds=5),
        max_backoff=duration_pb2.Duration(seconds=120),
        max_attempts=5,
    ),
    stackdriver_logging_config=queue_pb2.StackdriverLoggingConfig(
        sampling_ratio=1.0,
    ),
)

BASE_SCRAPER_QUEUE_CONFIG = queue_pb2.Queue(
    rate_limits=queue_pb2.RateLimits(
        max_dispatches_per_second=0.08333333333,  # 5/min
        max_concurrent_dispatches=3,
    ),
    retry_config=queue_pb2.RetryConfig(
        min_backoff=duration_pb2.Duration(seconds=5),
        max_backoff=duration_pb2.Duration(seconds=300),
        max_attempts=5,
    ),
    stackdriver_logging_config=queue_pb2.StackdriverLoggingConfig(
        sampling_ratio=1.0,
    ),
)

SCRAPER_PHASE_QUEUE_CONFIG = queue_pb2.Queue(
    rate_limits=queue_pb2.RateLimits(
        max_dispatches_per_second=100,
        max_concurrent_dispatches=100,
    ),
    retry_config=queue_pb2.RetryConfig(
        min_backoff=duration_pb2.Duration(seconds=5),
        max_backoff=duration_pb2.Duration(seconds=300),
        max_attempts=5,
    ),
    stackdriver_logging_config=queue_pb2.StackdriverLoggingConfig(
        sampling_ratio=1.0,
    ),
)


def _queue_config_with_name(
    client_wrapper: GoogleCloudTasksClientWrapper,
    base_config: queue_pb2.Queue,
    queue_name: str,
) -> queue_pb2.Queue:
    return (
        ProtobufBuilder(queue_pb2.Queue)
        .compose(base_config)
        .update_args(
            name=client_wrapper.format_queue_path(queue_name),
        )
        .build()
    )


def _build_cloud_task_queue_configs(
    client_wrapper: GoogleCloudTasksClientWrapper,
) -> List[queue_pb2.Queue]:
    """Builds a list of configurations for all Google Cloud Tasks queues that
    should be deployed in this environment.
    """

    queues = []

    # Direct ingest queues for handling /process_job requests
    for queue_name in [
        DIRECT_INGEST_JAILS_PROCESS_JOB_QUEUE_V2,
        DIRECT_INGEST_STATE_PROCESS_JOB_QUEUE_V2,
        DIRECT_INGEST_BQ_IMPORT_EXPORT_QUEUE_V2,
        DIRECT_INGEST_SCHEDULER_QUEUE_V2,
    ]:
        queues.append(
            _queue_config_with_name(
                client_wrapper, DIRECT_INGEST_QUEUE_BASE_CONFIG, queue_name
            )
        )

    queues.append(
        ProtobufBuilder(queue_pb2.Queue)
        .compose(
            _queue_config_with_name(
                client_wrapper, SCRAPER_PHASE_QUEUE_CONFIG, SCRAPER_PHASE_QUEUE_V2
            )
        )
        .update_args(
            rate_limits=queue_pb2.RateLimits(
                max_dispatches_per_second=1,
            ),
        )
        .build()
    )

    queues.append(
        _queue_config_with_name(
            client_wrapper, BIGQUERY_QUEUE_CONFIG, BIGQUERY_QUEUE_V2
        )
    )
    queues.append(
        _queue_config_with_name(
            client_wrapper, JOB_MONITOR_QUEUE_CONFIG, JOB_MONITOR_QUEUE_V2
        )
    )

    for vendor in vendors.get_vendors():
        queue_params = vendors.get_vendor_queue_params(vendor)
        if queue_params is None:
            continue
        vendor_queue_name = "vendor-{}-scraper-v2".format(vendor.replace("_", "-"))
        queue = (
            ProtobufBuilder(queue_pb2.Queue)
            .compose(
                _queue_config_with_name(
                    client_wrapper, BASE_SCRAPER_QUEUE_CONFIG, vendor_queue_name
                )
            )
            .compose(queue_pb2.Queue(**queue_params))
            .build()
        )
        queues.append(queue)

    for region in regions.get_supported_regions():
        if region.shared_queue or not region.is_ingest_launched_in_env():
            continue

        queue = _queue_config_with_name(
            client_wrapper, BASE_SCRAPER_QUEUE_CONFIG, region.get_queue_name()
        )
        if region.queue:
            queue = (
                ProtobufBuilder(queue_pb2.Queue)
                .compose(queue)
                .compose(queue_pb2.Queue(**region.queue))
                .build()
            )
        queues.append(queue)

    return queues


def initialize_queues(google_auth_token: str) -> None:
    cloud_tasks_client = tasks_v2.CloudTasksClient(
        credentials=credentials.Credentials(google_auth_token)
    )
    client_wrapper = GoogleCloudTasksClientWrapper(
        cloud_tasks_client=cloud_tasks_client,
        project_id=metadata.project_id(),
    )

    logging.info("Building queue configurations...")
    configs = _build_cloud_task_queue_configs(client_wrapper)
    logging.info("Start creating/updating Cloud Task queues...")
    thread_pool = ThreadPool(processes=12)
    thread_pool.map(client_wrapper.initialize_cloud_task_queue, configs)
    logging.info("Finished creating/updating Cloud Task queues.")
