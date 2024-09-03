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

"""Requests handlers for direct ingest control requests.
"""
import json
import logging
from http import HTTPStatus
from typing import Optional, Tuple

from flask import Blueprint, request

from recidiviz.cloud_storage.gcs_pseudo_lock_manager import GCSPseudoLockAlreadyExists
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath, GcsfsPath
from recidiviz.cloud_tasks.utils import get_current_cloud_task_id
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.controllers.legacy_ingest_raw_file_import_controller_factory import (
    LegacyIngestRawFileImportControllerFactory,
)
from recidiviz.ingest.direct.direct_ingest_bucket_name_utils import (
    get_region_code_from_direct_ingest_bucket,
)
from recidiviz.ingest.direct.direct_ingest_cloud_task_queue_manager import (
    DirectIngestCloudTaskQueueManager,
)
from recidiviz.ingest.direct.direct_ingest_regions import DirectIngestRegion
from recidiviz.ingest.direct.gating import is_raw_data_import_dag_enabled
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.direct.types.cloud_task_args import (
    CloudTaskArgs,
    GcsfsRawDataBQImportArgs,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.direct_ingest_instance_factory import (
    DirectIngestInstanceFactory,
)
from recidiviz.ingest.direct.types.errors import (
    DirectIngestError,
    DirectIngestErrorType,
    DirectIngestGatingError,
)
from recidiviz.monitoring import context
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.params import get_bool_param_value, get_str_param_value
from recidiviz.utils.pubsub_helper import (
    BUCKET_ID,
    OBJECT_ID,
    extract_pubsub_message_from_json,
)

direct_ingest_control = Blueprint("direct_ingest_control", __name__)

# TODO(#28239) remove all endpoints once raw data import dag is fully rolled out
@direct_ingest_control.route("/normalize_raw_file_path", methods=["POST"])
@requires_gae_auth
def normalize_raw_file_path() -> Tuple[str, HTTPStatus]:
    """Called from a Cloud Storage Notification when a new file is added to a bucket that is
    configured to rename files but not ingest them. For example, a bucket that is being used for
    automatic data transfer testing.
    """
    try:
        message = extract_pubsub_message_from_json(request.get_json())
    except Exception as e:
        return str(e), HTTPStatus.BAD_REQUEST

    if not message.attributes:
        return "Invalid Pub/Sub message", HTTPStatus.BAD_REQUEST

    attributes = message.attributes

    # The bucket name for the file to normalize
    bucket = attributes[BUCKET_ID]
    # The relative path to the file, not including the bucket name
    relative_file_path = attributes[OBJECT_ID]

    if not bucket or not relative_file_path:
        return f"Bad parameters [{request.args}]", HTTPStatus.BAD_REQUEST

    path = GcsfsPath.from_bucket_and_blob_name(
        bucket_name=bucket, blob_name=relative_file_path
    )

    logging.info("Handling file %s", path.abs_path())
    if not isinstance(path, GcsfsFilePath):
        raise ValueError(f"Incorrect type [{type(path)}] for path: {path.uri()}")

    fs = DirectIngestGCSFileSystem(GcsfsFactory.build())
    if fs.is_normalized_file_path(path):
        logging.info("Path already normalized: [%s]. Returning.", path.abs_path())
        return "", HTTPStatus.OK

    fs.mv_raw_file_to_normalized_path(path)

    return "", HTTPStatus.OK


@direct_ingest_control.route("/handle_direct_ingest_file", methods=["POST"])
@requires_gae_auth
def handle_direct_ingest_file() -> Tuple[str, HTTPStatus]:
    """Called from a Cloud Storage Notification when a new file is added to a direct ingest
    bucket. Will trigger a job that deals with normalizing and splitting the
    file as is appropriate, then start the scheduler if allowed.

    `start_ingest` can be set to `false` when a region has turned on nightly/weekly
    automatic data transfer before we are ready to schedule and process ingest
    jobs for that region (e.g. before ingest is "launched"). This will just
    rename the incoming files to have a normalized path with a timestamp
    so subsequent nightly uploads do not have naming conflicts.
    """
    try:
        message = extract_pubsub_message_from_json(request.get_json())
    except Exception as e:
        return str(e), HTTPStatus.BAD_REQUEST

    if not message.attributes:
        return "Invalid Pub/Sub message", HTTPStatus.BAD_REQUEST

    attributes = message.attributes
    # The bucket name for the file to ingest
    bucket = attributes[BUCKET_ID]
    region_code = get_region_code_from_direct_ingest_bucket(bucket)
    if not region_code:
        response = f"Cannot parse region code from bucket {bucket}, returning."
        logging.error(response)
        return response, HTTPStatus.BAD_REQUEST

    # The relative path to the file, not including the bucket name
    relative_file_path = attributes[OBJECT_ID]

    start_ingest = get_bool_param_value("start_ingest", request.args, default=False)

    if not region_code or not bucket or not relative_file_path or start_ingest is None:
        response = f"Bad parameters [{request.args}]"
        logging.error(response)
        return response, HTTPStatus.BAD_REQUEST

    bucket_path = GcsfsBucketPath(bucket_name=bucket)
    ingest_instance = DirectIngestInstanceFactory.for_ingest_bucket(bucket_path)

    path = GcsfsPath.from_bucket_and_blob_name(
        bucket_name=bucket, blob_name=relative_file_path
    )

    with context.push_region_context(
        region_code,
        ingest_instance=ingest_instance.value,
    ):
        if is_raw_data_import_dag_enabled(
            StateCode(region_code.upper()), ingest_instance
        ):
            raise DirectIngestGatingError(
                f"App Engine endpoint /handle_direct_ingest_file for region "
                f"[{region_code}] and instance [{ingest_instance.value}] should not be "
                f"called when raw data import DAG is enabled"
            )

        logging.info("Handling file %s", path.abs_path())
        try:
            controller = LegacyIngestRawFileImportControllerFactory.build(
                region_code=region_code,
                ingest_instance=ingest_instance,
                allow_unlaunched=True,
            )
        except DirectIngestError as e:
            if e.is_bad_request():
                logging.error(str(e))
                return str(e), HTTPStatus.BAD_REQUEST
            raise e

        if isinstance(path, GcsfsFilePath):
            controller.handle_file(path, start_ingest=start_ingest)

    return "", HTTPStatus.OK


@direct_ingest_control.route("/handle_new_files", methods=["GET", "POST"])
@requires_gae_auth
def handle_new_files() -> Tuple[str, HTTPStatus]:
    """Normalizes and splits files in the ingest bucket for a given region as
    is appropriate. Will schedule the next extract_and_merge task if no renaming /
    splitting work has been done that will trigger subsequent calls to this
    endpoint.
    """
    logging.info(
        "Received request for direct ingest handle_new_files: %s", request.values
    )
    region_code = get_str_param_value("region", request.values)
    can_start_ingest = get_bool_param_value(
        "can_start_ingest", request.values, default=False
    )
    ingest_instance_str = get_str_param_value("ingest_instance", request.values)
    current_task_id = get_current_cloud_task_id()

    if not region_code or can_start_ingest is None or not ingest_instance_str:
        response = f"Bad parameters [{request.values}]"
        logging.error(response)
        return response, HTTPStatus.BAD_REQUEST

    try:
        ingest_instance = DirectIngestInstance(ingest_instance_str.upper())
    except ValueError:
        response = f"Bad ingest instance value [{ingest_instance_str}]"
        logging.error(response)
        return response, HTTPStatus.BAD_REQUEST

    with context.push_region_context(
        region_code,
        ingest_instance=ingest_instance.value,
    ):

        if is_raw_data_import_dag_enabled(
            StateCode(region_code.upper()), ingest_instance
        ):
            raise DirectIngestGatingError(
                f"App Engine endpoint /handle_new_files for region [{region_code}] and "
                f"instance [{ingest_instance.value}] should not be called when raw "
                f"data import DAG is enabled"
            )

        try:
            controller = LegacyIngestRawFileImportControllerFactory.build(
                region_code=region_code,
                ingest_instance=ingest_instance,
                allow_unlaunched=True,
            )
        except DirectIngestError as e:
            if e.is_bad_request():
                logging.error(str(e))
                return str(e), HTTPStatus.BAD_REQUEST
            raise e

        controller.handle_new_files(
            current_task_id=current_task_id,
            can_start_ingest=can_start_ingest,
        )
    return "", HTTPStatus.OK


@direct_ingest_control.route(
    "/ensure_all_raw_file_paths_normalized", methods=["GET", "POST"]
)
@requires_gae_auth
def ensure_all_raw_file_paths_normalized() -> Tuple[str, HTTPStatus]:
    """Ensures that all files in the ingest buckets for all direct ingest states have
    properly normalized  file names, to ensure that repeat uploads of files into those
    buckets don't fail or overwrite data. This provides a layer of protection against
    cloud function failures.
    """
    logging.info(
        "Received request for direct ingest ensure_all_raw_file_paths_normalized: "
        "%s",
        request.values,
    )

    supported_states = get_direct_ingest_states_existing_in_env()
    for state_code in supported_states:
        for ingest_instance in DirectIngestInstance:
            if is_raw_data_import_dag_enabled(state_code, ingest_instance):
                logging.info(
                    "Skipping normalization for region [%s] and instance [%s] as raw "
                    "data import DAG is enabled",
                    state_code.value,
                    ingest_instance.value,
                )
                continue

            logging.info(
                "Ensuring paths normalized for region [%s] and instance [%s]",
                state_code.value,
                ingest_instance.value,
            )
            with context.push_region_context(
                state_code.value, ingest_instance=ingest_instance.value
            ):
                try:
                    controller = LegacyIngestRawFileImportControllerFactory.build(
                        region_code=state_code.value.lower(),
                        ingest_instance=ingest_instance,
                        allow_unlaunched=True,
                    )
                except DirectIngestError as e:
                    if e.is_bad_request():
                        logging.error(str(e))
                        return str(e), HTTPStatus.BAD_REQUEST
                    raise e

                can_start_ingest = controller.region.is_ingest_launched_in_env()
                controller.cloud_task_manager.create_direct_ingest_handle_new_files_task(
                    controller.region,
                    ingest_instance=controller.ingest_instance,
                    can_start_ingest=can_start_ingest,
                )
    return "", HTTPStatus.OK


@direct_ingest_control.route("/raw_data_import", methods=["POST"])
@requires_gae_auth
def raw_data_import() -> Tuple[str, HTTPStatus]:
    """Imports a single raw direct ingest CSV file from a location in GCS File System to its corresponding raw data
    table in BQ.
    """
    logging.info(
        "Received request to do direct ingest raw data import: [%s]", request.values
    )
    region_code = get_str_param_value("region", request.values)
    file_path = get_str_param_value("file_path", request.values, preserve_case=True)

    if not region_code or not file_path:
        response = f"Bad parameters [{request.values}]"
        logging.error(response)
        return response, HTTPStatus.BAD_REQUEST

    gcsfs_path = GcsfsFilePath.from_absolute_path(file_path)

    ingest_instance = DirectIngestInstanceFactory.for_ingest_bucket(
        gcsfs_path.bucket_path
    )
    with context.push_region_context(
        region_code,
        ingest_instance=ingest_instance.value,
    ):
        if is_raw_data_import_dag_enabled(
            StateCode(region_code.upper()), ingest_instance
        ):
            raise DirectIngestGatingError(
                f"App Engine endpoint /raw_data_import for region [{region_code}] and "
                f"instance [{ingest_instance.value}] should not be called when raw data"
                f" import DAG is enabled"
            )

        json_data = request.get_data(as_text=True)
        data_import_args = _parse_cloud_task_args(json_data)

        if not data_import_args:
            raise DirectIngestError(
                msg="raw_data_import was called with no GcsfsRawDataBQImportArgs.",
                error_type=DirectIngestErrorType.INPUT_ERROR,
            )

        if not isinstance(data_import_args, GcsfsRawDataBQImportArgs):
            raise DirectIngestError(
                msg=f"raw_data_import was called with incorrect args type [{type(data_import_args)}].",
                error_type=DirectIngestErrorType.INPUT_ERROR,
            )

        if gcsfs_path != data_import_args.raw_data_file_path:
            raise DirectIngestError(
                msg=f"Different paths were passed in the url and request body\n"
                f"url: {gcsfs_path.uri()}\n"
                f"body: {data_import_args.raw_data_file_path.uri()}",
                error_type=DirectIngestErrorType.INPUT_ERROR,
            )

        try:
            controller = LegacyIngestRawFileImportControllerFactory.build(
                region_code=region_code,
                ingest_instance=ingest_instance,
                allow_unlaunched=False,
            )
        except DirectIngestError as e:
            if e.is_bad_request():
                logging.error(str(e))
                return str(e), HTTPStatus.BAD_REQUEST
            raise e

        try:
            controller.do_raw_data_import(data_import_args)
        except GCSPseudoLockAlreadyExists as e:
            logging.warning(str(e))
            return str(e), HTTPStatus.CONFLICT

    return "", HTTPStatus.OK


@direct_ingest_control.route("/scheduler", methods=["GET", "POST"])
@requires_gae_auth
def scheduler() -> Tuple[str, HTTPStatus]:
    """Checks the state of the ingest instance and schedules any tasks to be run."""
    logging.info("Received request for direct ingest scheduler: %s", request.values)
    region_code = get_str_param_value("region", request.values)
    current_task_id = get_current_cloud_task_id()

    ingest_instance_str = get_str_param_value("ingest_instance", request.args)

    if not region_code or not ingest_instance_str:
        response = f"Bad parameters [{request.values}]"
        logging.error(response)
        return response, HTTPStatus.BAD_REQUEST

    try:
        ingest_instance = DirectIngestInstance(ingest_instance_str.upper())
    except ValueError:
        response = f"Bad ingest instance value [{ingest_instance_str}]"
        logging.error(response)
        return response, HTTPStatus.BAD_REQUEST

    with context.push_region_context(
        region_code,
        ingest_instance=ingest_instance.value,
    ):

        if is_raw_data_import_dag_enabled(
            StateCode(region_code.upper()), ingest_instance
        ):
            raise DirectIngestGatingError(
                f"App Engine endpoint /scheduler for region [{region_code}] and "
                f"instance [{ingest_instance.value}] should not be called when raw "
                f"data import DAG is enabled"
            )

        try:
            controller = LegacyIngestRawFileImportControllerFactory.build(
                region_code=region_code,
                ingest_instance=ingest_instance,
                allow_unlaunched=False,
            )
        except DirectIngestError as e:
            if e.is_bad_request():
                logging.error(str(e))
                return str(e), HTTPStatus.BAD_REQUEST
            raise e

        controller.schedule_next_ingest_task(current_task_id=current_task_id)
    return "", HTTPStatus.OK


@direct_ingest_control.route("/heartbeat", methods=["GET", "POST"])
@requires_gae_auth
def heartbeat() -> Tuple[str, HTTPStatus]:
    """Endpoint that can regularly be called to restart ingest if it has been stopped
    (e.g. earlier tasks failed due to locking conflicts). This is safe to call, even if
    another process is running that should block ingest.
    """
    kick_all_schedulers()
    return "", HTTPStatus.OK


def kick_all_schedulers() -> None:
    """Kicks all ingest schedulers to restart ingest"""
    supported_states = get_direct_ingest_states_existing_in_env()
    for state_code in supported_states:
        region = _region_for_region_code(region_code=state_code.value)
        if not region.is_ingest_launched_in_env():
            continue
        for ingest_instance in DirectIngestInstance:
            if is_raw_data_import_dag_enabled(state_code, ingest_instance):
                continue

            with context.push_region_context(
                state_code.value, ingest_instance=ingest_instance.value
            ):
                controller = LegacyIngestRawFileImportControllerFactory.build(
                    region_code=state_code.value,
                    ingest_instance=ingest_instance,
                    allow_unlaunched=False,
                )

                controller.kick_scheduler()


def _region_for_region_code(region_code: str) -> DirectIngestRegion:
    try:
        return direct_ingest_regions.get_direct_ingest_region(region_code.lower())
    except FileNotFoundError as e:
        raise DirectIngestError(
            msg=f"Region [{region_code}] has no registered manifest",
            error_type=DirectIngestErrorType.INPUT_ERROR,
        ) from e


def _parse_cloud_task_args(
    json_data_str: str,
) -> Optional[CloudTaskArgs]:
    if not json_data_str:
        return None
    data = json.loads(json_data_str)
    return DirectIngestCloudTaskQueueManager.json_to_cloud_task_args(data)
