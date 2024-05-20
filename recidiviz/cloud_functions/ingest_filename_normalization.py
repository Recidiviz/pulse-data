# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""This file contains all the cloud function logic to normalize ingest file names in GCS."""
import logging
import os
from datetime import date
from distutils.util import strtobool
from http import HTTPStatus
from typing import Optional, Tuple

import functions_framework
import google.auth.transport.requests
import google.oauth2.id_token
import requests
from cloudevents.http import CloudEvent
from flask import Request

from recidiviz.cloud_functions.cloud_function_utils import cloud_functions_log
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath, GcsfsPath
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_deprecated_storage_directory_path_for_state,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

# GCFs will reuse global variables across invocations if they execute on the same function instance
# This is useful for caching expensive resources like establishing a connection to GCS
fs: Optional[DirectIngestGCSFileSystem] = None


@functions_framework.cloud_event
def normalize_filename(cloud_event: CloudEvent) -> None:
    """Function to normalize ingest filenames in GCS triggered by a finalized object event in GCS.
    If the file is a zip file, it will invoke another cloud function to handle the unzipping
    in order to allocate more memory to the process."""
    data = cloud_event.data

    bucket = data.get("bucket")
    relative_file_path = data.get("name")

    if not bucket or not relative_file_path:
        cloud_functions_log(
            severity="ERROR", message="Missing bucket or file name in event: {data}"
        )
        return

    path = GcsfsPath.from_bucket_and_blob_name(
        bucket_name=bucket, blob_name=relative_file_path
    )
    if not isinstance(path, GcsfsFilePath):
        cloud_functions_log(
            severity="ERROR",
            message=f"Incorrect type [{type(path)}] for path: [{path.uri()}]",
        )
        return

    logging.info("Handling file [%s]", path.abs_path())

    has_normalized_filename = DirectIngestGCSFileSystem.is_normalized_file_path(path)
    is_zipfile = path.has_zip_extension

    if has_normalized_filename and not is_zipfile:
        cloud_functions_log(
            severity="INFO",
            message=f"File [{path.abs_path()}] is already normalized. Returning.",
        )
        return

    dry_run = strtobool(os.getenv("DRY_RUN", "False"))

    global fs
    if fs is None:
        fs = DirectIngestGCSFileSystem(GcsfsFactory.build())

    if not has_normalized_filename:
        if dry_run:
            cloud_functions_log(
                severity="INFO", message="Dry run enabled. Skipping normalization."
            )
            return
        try:
            new_path = fs.mv_raw_file_to_normalized_path(path)
            cloud_functions_log(
                severity="INFO",
                message=f"File [{path.abs_path()}] normalized to [{new_path.abs_path()}]",
            )
        except ValueError as e:
            cloud_functions_log(
                severity="ERROR",
                message=f"Error normalizing file [{path.abs_path()}]: {e}",
            )
        # The file rename will retrigger the cloud function
        # so if the file is a zip, it will be unzipped in the next invocation
        return

    if is_zipfile:
        if dry_run:
            cloud_functions_log(
                severity="INFO", message="Dry run enabled. Skipping unzip."
            )
            return
        response = _invoke_zipfile_handler(bucket, relative_file_path)
        cloud_functions_log(
            severity="ERROR" if response.status_code != 200 else "INFO",
            message=f"Response from zipfile handler function: {response.status_code} - {response.text}",
        )


def _invoke_zipfile_handler(bucket: str, relative_file_path: str) -> requests.Response:
    function_url = os.environ["ZIPFILE_HANDLER_FUNCTION_URL"]
    payload = {"bucket": bucket, "name": relative_file_path}
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_get_access_token(function_url)}",
    }

    cloud_functions_log(
        severity="INFO", message=f"Invoking {function_url} with payload: {payload}"
    )
    return requests.post(function_url, headers=headers, json=payload, timeout=540)


def _get_access_token(audience: str) -> str:
    request = google.auth.transport.requests.Request()
    return google.oauth2.id_token.fetch_id_token(request, audience)


@functions_framework.http
def handle_zipfile(request: Request) -> Tuple[str, HTTPStatus]:
    """Function to handle zip files in GCS triggered by HTTP POST request.
    If the file is a zip file, it will be unzipped and moved to deprecated storage."""
    if (data := request.get_json(silent=True)) is None:
        error_msg = f"Missing data in request: {request}"
        cloud_functions_log(severity="ERROR", message=error_msg)
        return error_msg, HTTPStatus.BAD_REQUEST

    bucket = data.get("bucket")
    relative_file_path = data.get("name")

    if not bucket or not relative_file_path:
        error_msg = f"Missing bucket or file name in request data: {data}"
        cloud_functions_log(severity="ERROR", message=error_msg)
        return error_msg, HTTPStatus.BAD_REQUEST

    path = GcsfsPath.from_bucket_and_blob_name(
        bucket_name=bucket, blob_name=relative_file_path
    )
    if not isinstance(path, GcsfsFilePath):
        error_msg = f"Incorrect type [{type(path)}] for path: [{path.uri()}]"
        cloud_functions_log(severity="ERROR", message=error_msg)
        return error_msg, HTTPStatus.BAD_REQUEST

    logging.info("Handling file [%s]", path.abs_path())

    if not path.has_zip_extension:
        cloud_functions_log(
            severity="INFO",
            message=f"File [{path.abs_path()}] is not a zipfile. Returning.",
        )
        return "OK", HTTPStatus.OK

    global fs
    if fs is None:
        fs = DirectIngestGCSFileSystem(GcsfsFactory.build())

    cloud_functions_log(
        severity="INFO",
        message=f"File [{path.abs_path()}] is a zip file, unzipping...",
    )
    try:
        fs.unzip(path, GcsfsBucketPath(bucket))
        fs.mv(
            src_path=path,
            dst_path=gcsfs_direct_ingest_deprecated_storage_directory_path_for_state(
                region_code=os.environ["STATE_CODE"],
                ingest_instance=DirectIngestInstance(os.environ["INGEST_INSTANCE"]),
                deprecated_on_date=date.today(),
                project_id=os.environ["PROJECT_ID"],
            ),
        )
    except ValueError as e:
        error_msg = f"Error unzipping file [{path.abs_path()}]: {e}"
        cloud_functions_log(severity="ERROR", message=error_msg)
        return error_msg, HTTPStatus.INTERNAL_SERVER_ERROR

    return "OK", HTTPStatus.OK
