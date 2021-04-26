# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
""" Contains functions for enqueueing data discovery tasks """
import abc
import json
from typing import Any, Dict
from urllib.parse import urljoin

import requests

from recidiviz.admin_panel.data_discovery.discovery import DataDiscoveryArgs
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.google_cloud.cloud_task_queue_manager import (
    CloudTaskQueueManager,
    CloudTaskQueueInfo,
)
from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import (
    ADMIN_PANEL_DATA_DISCOVERY_QUEUE,
)
from recidiviz.utils import environment


@environment.local_only
def _local_task(task_args: Dict[str, Any]) -> None:
    requests.post(
        urljoin("http://localhost:5000", task_args["relative_uri"]),
        data=json.dumps(task_args["body"]).encode(),
    )


def build_discovery_task(data_discovery_args: DataDiscoveryArgs) -> Dict[str, Any]:
    return {
        "task_id": data_discovery_args.id,
        "relative_uri": "/admin/data_discovery/discovery_task",
        "body": {"discovery_id": data_discovery_args.id},
    }


def build_cache_ingest_file_as_parquet_task(
    gcs_file: GcsfsFilePath, separator: str, encoding: str, quoting: int
) -> Dict[str, Any]:
    return {
        "task_id": gcs_file.file_name,
        "relative_uri": "/admin/data_discovery/cache_ingest_file_as_parquet_task",
        "body": {
            "gcs_file_uri": gcs_file.uri(),
            "file_separator": separator,
            "file_encoding": encoding,
            "file_quoting": quoting,
        },
    }


class AbstractAdminPanelDataDiscoveryCloudTaskManager:
    """ Abstract base class for AdminPanelDataDiscoveryCloudTaskManager """

    @abc.abstractmethod
    def create_discovery_task(self, data_discovery_args: DataDiscoveryArgs) -> None:
        """ Create a data discovery task """

    @abc.abstractmethod
    def create_cache_ingest_file_as_parquet_task(
        self, gcs_file: GcsfsFilePath, separator: str, encoding: str, quoting: int
    ) -> None:
        """ Create a cache ingest file as parquet task """


class AdminPanelDataDiscoveryCloudTaskManager(
    AbstractAdminPanelDataDiscoveryCloudTaskManager
):
    """Class for interacting with cloud tasks related to data discovery."""

    def __init__(self) -> None:
        self.cloud_task_queue_manager = CloudTaskQueueManager(
            queue_info_cls=CloudTaskQueueInfo,
            queue_name=ADMIN_PANEL_DATA_DISCOVERY_QUEUE,
        )

    def create_discovery_task(self, data_discovery_args: DataDiscoveryArgs) -> None:
        self.cloud_task_queue_manager.create_task(
            **build_discovery_task(data_discovery_args)
        )

    def create_cache_ingest_file_as_parquet_task(
        self, gcs_file: GcsfsFilePath, separator: str, encoding: str, quoting: int
    ) -> None:
        self.cloud_task_queue_manager.create_task(
            **build_cache_ingest_file_as_parquet_task(
                gcs_file, separator, encoding, quoting
            )
        )


class DevelopmentAdminPanelDataDiscoveryCloudTaskManager(
    AbstractAdminPanelDataDiscoveryCloudTaskManager
):
    """ Class for running data discovery cloud tasks locally """

    @environment.local_only
    def create_discovery_task(self, data_discovery_args: DataDiscoveryArgs) -> None:
        _local_task(build_discovery_task(data_discovery_args))

    @environment.local_only
    def create_cache_ingest_file_as_parquet_task(
        self, gcs_file: GcsfsFilePath, separator: str, encoding: str, quoting: int
    ) -> None:
        _local_task(
            build_cache_ingest_file_as_parquet_task(
                gcs_file, separator, encoding, quoting
            )
        )
