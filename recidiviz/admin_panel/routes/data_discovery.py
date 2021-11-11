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
""" Endpoints for the data discovery tool. Combination of cloud task endpoints and user-facing endpoints
    Cloud task endpoints are suffixed with `_task`
"""
import threading
import uuid
from http import HTTPStatus
from typing import Tuple

import attr
import flask
from flask import Blueprint, Response, jsonify, request

from recidiviz.admin_panel.data_discovery.arguments import DataDiscoveryArgsFactory
from recidiviz.admin_panel.data_discovery.cache_ingest_file_as_parquet import (
    CacheIngestFileAsParquetDelegate,
    SingleIngestFileParquetCache,
)
from recidiviz.admin_panel.data_discovery.discovery import discover_data
from recidiviz.admin_panel.data_discovery.file_configs import (
    get_ingest_view_configs,
    get_raw_data_configs,
)
from recidiviz.admin_panel.data_discovery.task_manager import (
    AbstractAdminPanelDataDiscoveryCloudTaskManager,
    AdminPanelDataDiscoveryCloudTaskManager,
    DevelopmentAdminPanelDataDiscoveryCloudTaskManager,
)
from recidiviz.admin_panel.data_discovery.types import DataDiscoveryTTL
from recidiviz.admin_panel.data_discovery.utils import (
    get_data_discovery_cache,
    get_data_discovery_communicator,
)
from recidiviz.cloud_storage.gcsfs_csv_reader import (
    COMMON_RAW_FILE_ENCODINGS,
    GcsfsCsvReader,
)
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.google_cloud.cloud_task_queue_manager import (
    get_cloud_task_json_body,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsDirectIngestFileType,
)
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.environment import in_gcp

CACHE_HIT = "CACHE_HIT"
CACHE_MISS = "CACHE_MISS"


def add_data_discovery_routes(blueprint: Blueprint) -> None:
    """ Adds data discovery routes to the passed Flask Blueprint"""

    @blueprint.route(
        "/data_discovery/cache_ingest_file_as_parquet_task", methods=["POST"]
    )
    @requires_gae_auth
    def _cache_ingest_file_as_parquet_task() -> Tuple[str, int]:
        """Downloads a GCS file and stores it to our Redis cache in Parquet format

         Example:
             POST /admin/data_discovery/cache_ingest_file_as_parquet_task
         Request Body:
             gcs_file_uri: (string) The `gs://` URI of the file to cache
             file_encoding: (string) The encoding of said file
             file_separator: (string) The value delimiter of side file
             file_quoting: (int) A `csv.QUOTE_*` value for the parser i.e. 3 (csv.QUOTE_NONE)
        Args:
             N/A
         Returns:
             Cache hit/miss result
        """
        cache = get_data_discovery_cache()
        body = get_cloud_task_json_body()
        path = GcsfsFilePath.from_absolute_path(body["gcs_file_uri"])
        parquet_path = SingleIngestFileParquetCache.parquet_cache_key(path)

        if not cache.exists(parquet_path):
            fs = GcsfsFactory.build()
            parquet_cache = SingleIngestFileParquetCache(
                get_data_discovery_cache(), path, expiry=DataDiscoveryTTL.PARQUET_FILES
            )
            csv_reader = GcsfsCsvReader(fs)
            csv_reader.streaming_read(
                path,
                CacheIngestFileAsParquetDelegate(parquet_cache, path),
                encodings_to_try=list(
                    {
                        body["file_encoding"],
                        *COMMON_RAW_FILE_ENCODINGS,
                    }
                ),
                delimiter=body["file_separator"],
                quoting=body["file_quoting"],
                lineterminator=body.get("file_custom_line_terminator"),
                chunk_size=75000,
                index_col=False,
                keep_default_na=False,
            )

            return CACHE_MISS, HTTPStatus.CREATED

        return CACHE_HIT, HTTPStatus.OK

    @blueprint.route("/data_discovery/discovery_task", methods=["POST"])
    @requires_gae_auth
    def _discovery_task() -> Tuple[str, int]:
        """Cloud task responsible for orchestrating ingest data parquet-ification tasks,
            loading parqueted files, and applying the DataDiscoveryArgs filters against the data

        Example:
            POST /admin/data_discovery/discovery_task
        Request Body:
            discovery_id: (string) The ID of this discovery task, as returned by /create_discovery
        Returns:
            N/A
        """
        body = get_cloud_task_json_body()
        if in_gcp():
            discover_data(body["discovery_id"])
        else:
            # Run discovery in a thread locally
            threading.Thread(target=discover_data, args=[body["discovery_id"]]).start()

        return "", HTTPStatus.OK

    @blueprint.route("/data_discovery/create_discovery", methods=["POST"])
    @requires_gae_auth
    def _create_discovery() -> flask.Response:
        """Endpoint responsible for creating and enqueueing a new discovery task

        Example:
            POST /admin/data_discovery/create_discovery
        Request Body:
            JSON representation of the `DataDiscoveryArgs` data class
        Returns:
            JSON representation of the hydrated `DataDiscoveryArgs` data class
        """
        data_discovery_args = DataDiscoveryArgsFactory.create(**request.get_json())

        if in_gcp():
            task_manager: AbstractAdminPanelDataDiscoveryCloudTaskManager = (
                AdminPanelDataDiscoveryCloudTaskManager()
            )
        else:
            task_manager = DevelopmentAdminPanelDataDiscoveryCloudTaskManager()

        task_manager.create_discovery_task(data_discovery_args)

        return jsonify(attr.asdict(data_discovery_args))

    @blueprint.route("/data_discovery/discovery_status", methods=["POST"])
    @requires_gae_auth
    def _discovery_status() -> Tuple[flask.Response, int]:
        """Long-polling endpoint responsible for checking on the status of a discovery task
        Connections are held idle until a new message appears in the communication channel

        Request Body:
            discovery_id: (string) The ID of this discovery task, as returned by /create_discovery
            message_cursor: (int)  the cursor of the last read message in the communication channel

        Returns:
            communicator.Message: latest message added to the communication channel

        """
        communicator = get_data_discovery_communicator(
            uuid.UUID(request.json["discovery_id"])
        )

        for message in communicator.listen(request.json["message_cursor"]):
            return jsonify(message.to_json()), HTTPStatus.OK

        return jsonify({"error": "No message yielded"}), HTTPStatus.REQUEST_TIMEOUT

    @blueprint.route("/data_discovery/files", methods=["POST"])
    @requires_gae_auth
    def _files() -> Response:
        """Endpoint responsible for sending raw file / ingest view configs to the front-end

        Example:
            POST /admin/data_discovery/files
        Request Body:
            region_code: (string) The region code you wish to receive file configs for
        Returns:
            JSON representation of `DataDiscoveryStandardizedFileConfig`s for the region
        """
        return jsonify(
            {
                GcsfsDirectIngestFileType.RAW_DATA.value: {
                    config.file_tag: attr.asdict(config)
                    for config in get_raw_data_configs(request.json["region_code"])
                },
                GcsfsDirectIngestFileType.INGEST_VIEW.value: {
                    config.file_tag: attr.asdict(config)
                    for config in get_ingest_view_configs(request.json["region_code"])
                },
            }
        )
