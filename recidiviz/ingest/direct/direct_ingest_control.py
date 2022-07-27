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
import datetime
import json
import logging
from http import HTTPStatus
from typing import Optional, Tuple

import pytz
from flask import Blueprint, request
from opencensus.stats import aggregation, measure, view

from recidiviz.cloud_storage.gcs_pseudo_lock_manager import GCSPseudoLockAlreadyExists
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath, GcsfsPath
from recidiviz.cloud_tasks.utils import get_current_cloud_task_id
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.direct_ingest_controller_factory import (
    DirectIngestControllerFactory,
)
from recidiviz.ingest.direct.direct_ingest_bucket_name_utils import (
    get_region_code_from_direct_ingest_bucket,
)
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import (
    DirectIngestCloudTaskManager,
    DirectIngestCloudTaskManagerImpl,
)
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.sftp.base_upload_state_files_to_ingest_bucket_controller import (
    UploadStateFilesToIngestBucketController,
)
from recidiviz.ingest.direct.sftp.download_files_from_sftp import (
    DownloadFilesFromSftpController,
)
from recidiviz.ingest.direct.types.cloud_task_args import (
    CloudTaskArgs,
    ExtractAndMergeArgs,
    GcsfsRawDataBQImportArgs,
    IngestViewMaterializationArgs,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.direct_ingest_instance_factory import (
    DirectIngestInstanceFactory,
)
from recidiviz.ingest.direct.types.errors import (
    DirectIngestError,
    DirectIngestErrorType,
    DirectIngestInstanceError,
)
from recidiviz.utils import metadata, monitoring, regions
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.monitoring import TagKey
from recidiviz.utils.params import get_bool_param_value, get_str_param_value
from recidiviz.utils.pubsub_helper import (
    BUCKET_ID,
    OBJECT_ID,
    extract_pubsub_message_from_json,
)
from recidiviz.utils.regions import Region, get_supported_direct_ingest_region_codes

m_sftp_attempts = measure.MeasureInt(
    "ingest/sftp/attempts",
    "Counts of files that were attempted to be uploaded to GCS from SFTP for direct ingest",
    "1",
)
m_sftp_errors = measure.MeasureInt(
    "ingest/sftp/errors",
    "Counts of files that errored when attempting to be uploaded to GCS from SFTP for direct ingest",
    "1",
)
sftp_attempts_view = view.View(
    "recidiviz/ingest/sftp/attempts",
    "The sum of attempts that were made for SFTP",
    [monitoring.TagKey.REGION, monitoring.TagKey.SFTP_TASK_TYPE],
    m_sftp_attempts,
    aggregation.SumAggregation(),
)
sftp_errors_view = view.View(
    "recidiviz/ingest/sftp/errors",
    "The sum of errors that were made for SFTP",
    [monitoring.TagKey.REGION, monitoring.TagKey.SFTP_TASK_TYPE],
    m_sftp_errors,
    aggregation.SumAggregation(),
)
monitoring.register_views([sftp_attempts_view, sftp_errors_view])

direct_ingest_control = Blueprint("direct_ingest_control", __name__)


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
    path = GcsfsPath.from_bucket_and_blob_name(
        bucket_name=bucket, blob_name=relative_file_path
    )
    logging.info("Handling file %s", path.abs_path())

    ingest_instance = DirectIngestInstanceFactory.for_ingest_bucket(bucket_path)

    with monitoring.push_region_tag(
        region_code,
        ingest_instance=ingest_instance.value,
    ):
        try:
            controller = DirectIngestControllerFactory.build(
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

    with monitoring.push_region_tag(
        region_code,
        ingest_instance=ingest_instance.value,
    ):
        try:
            controller = DirectIngestControllerFactory.build(
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

    supported_regions = get_supported_direct_ingest_region_codes()
    for region_code in supported_regions:
        logging.info("Ensuring paths normalized for region [%s]", region_code)
        # The only type of file that wouldn't be normalized is a raw file, which
        # should only ever be in the PRIMARY bucket.
        ingest_instance = DirectIngestInstance.PRIMARY
        with monitoring.push_region_tag(
            region_code, ingest_instance=ingest_instance.value
        ):
            try:
                controller = DirectIngestControllerFactory.build(
                    region_code=region_code,
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
    with monitoring.push_region_tag(
        region_code,
        ingest_instance=ingest_instance.value,
    ):
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

        with monitoring.push_tags(
            {TagKey.RAW_DATA_IMPORT_TAG: data_import_args.task_id_tag()}
        ):
            try:
                controller = DirectIngestControllerFactory.build(
                    region_code=region_code,
                    ingest_instance=ingest_instance,
                    allow_unlaunched=False,
                )
            except DirectIngestError as e:
                if e.is_bad_request():
                    logging.error(str(e))
                    return str(e), HTTPStatus.BAD_REQUEST
                raise e

            controller.do_raw_data_import(data_import_args)
    return "", HTTPStatus.OK


@direct_ingest_control.route("/materialize_ingest_view", methods=["POST"])
@requires_gae_auth
def materialize_ingest_view() -> Tuple[str, HTTPStatus]:
    """Generates results for an ingest view that changed between two dates and writes
    those results to the appropriate `ux_xx_ingest_view_results` table so that they
    can be processed via the /direct/extract_and_merge endpoint.
    """
    logging.info(
        "Received request to do ingest view materialization: [%s]", request.values
    )
    region_code = get_str_param_value("region", request.values)
    ingest_instance_str = get_str_param_value("ingest_instance", request.values)
    ingest_view_name = get_str_param_value(
        "ingest_view_name", request.values, preserve_case=True
    )

    if not region_code or not ingest_instance_str or not ingest_view_name:
        response = f"Bad parameters [{request.values}]"
        logging.error(response)
        return response, HTTPStatus.BAD_REQUEST

    try:
        ingest_instance = DirectIngestInstance(ingest_instance_str.upper())
    except ValueError:
        response = f"Bad ingest instance value [{ingest_instance_str}]"
        logging.error(response)
        return response, HTTPStatus.BAD_REQUEST

    with monitoring.push_region_tag(region_code, ingest_instance=ingest_instance.value):
        json_data = request.get_data(as_text=True)
        args = _parse_cloud_task_args(json_data)

        if not args:
            raise DirectIngestError(
                msg="materialize_ingest_view was called with no IngestViewMaterializationArgs.",
                error_type=DirectIngestErrorType.INPUT_ERROR,
            )

        if not isinstance(args, IngestViewMaterializationArgs):
            raise DirectIngestError(
                msg=f"materialize_ingest_view was called with incorrect args type [{type(args)}].",
                error_type=DirectIngestErrorType.INPUT_ERROR,
            )

        if ingest_instance != args.ingest_instance:
            raise DirectIngestError(
                msg=f"Different ingest instances were passed in the url and request body\n"
                f"url: {ingest_instance}\n"
                f"body: {args.ingest_instance}",
                error_type=DirectIngestErrorType.INPUT_ERROR,
            )

        if ingest_view_name != args.ingest_view_name:
            raise DirectIngestError(
                msg=f"Different ingest view names were passed in the url and request body\n"
                f"url: {ingest_view_name}\n"
                f"body: {args.ingest_view_name}",
                error_type=DirectIngestErrorType.INPUT_ERROR,
            )

        with monitoring.push_tags(
            {TagKey.INGEST_VIEW_MATERIALIZATION_TAG: args.task_id_tag()}
        ):
            try:
                controller = DirectIngestControllerFactory.build(
                    region_code=region_code,
                    ingest_instance=ingest_instance,
                    allow_unlaunched=False,
                )
            except DirectIngestError as e:
                if e.is_bad_request():
                    logging.error(str(e))
                    return str(e), HTTPStatus.BAD_REQUEST
                raise e

            controller.do_ingest_view_materialization(args)
    return "", HTTPStatus.OK


@direct_ingest_control.route("/extract_and_merge", methods=["POST"])
@requires_gae_auth
def extract_and_merge() -> Tuple[str, HTTPStatus]:
    """Reads a chunk of ingest view results, parses them into schema objects, then
    commits those results to our Postgres database.
    """
    logging.info(
        "Received request to run ingest extract and merge job: [%s]", request.values
    )
    region_code = get_str_param_value("region", request.values)
    ingest_instance_str = get_str_param_value("ingest_instance", request.values)
    ingest_view_name = get_str_param_value(
        "ingest_view_name", request.values, preserve_case=True
    )
    if not region_code or not ingest_instance_str or not ingest_view_name:
        response = f"Bad parameters [{request.values}]"
        logging.error(response)
        return response, HTTPStatus.BAD_REQUEST

    try:
        ingest_instance = DirectIngestInstance(ingest_instance_str.upper())
    except ValueError:
        response = f"Bad ingest instance value [{ingest_instance_str}]"
        logging.error(response)
        return response, HTTPStatus.BAD_REQUEST

    with monitoring.push_region_tag(
        region_code,
        ingest_instance=ingest_instance.value,
    ):
        json_data = request.get_data(as_text=True)
        ingest_args = _parse_cloud_task_args(json_data)

        if not ingest_args:
            response = "/extract_and_merge was called with no ExtractAndMergeArgs."
            logging.error(response)
            return response, HTTPStatus.BAD_REQUEST

        if not isinstance(ingest_args, ExtractAndMergeArgs):
            response = f"/extract_and_merge was called with incorrect args type [{type(ingest_args)}]."
            logging.error(response)
            return response, HTTPStatus.BAD_REQUEST

        if ingest_instance != ingest_args.ingest_instance:
            response = (
                f"Different ingest instances were passed in the url and request body\n"
                f"url: {ingest_instance}\n"
                f"body: {ingest_args.ingest_instance}"
            )
            logging.error(response)
            return response, HTTPStatus.BAD_REQUEST

        if ingest_view_name != ingest_args.ingest_view_name:
            response = (
                f"Different ingest view names were passed in the url and request body\n"
                f"url: {ingest_view_name}\n"
                f"body: {ingest_args.ingest_view_name}"
            )
            logging.error(response)
            return response, HTTPStatus.BAD_REQUEST

        with monitoring.push_tags({TagKey.INGEST_TASK_TAG: ingest_args.task_id_tag()}):
            try:
                controller = DirectIngestControllerFactory.build(
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
                controller.run_extract_and_merge_job_and_kick_scheduler_on_completion(
                    ingest_args
                )
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
    just_finished_job = get_bool_param_value(
        "just_finished_job", request.values, default=False
    )
    current_task_id = get_current_cloud_task_id()

    ingest_instance_str = get_str_param_value("ingest_instance", request.args)

    if not region_code or just_finished_job is None or not ingest_instance_str:
        response = f"Bad parameters [{request.values}]"
        logging.error(response)
        return response, HTTPStatus.BAD_REQUEST

    try:
        ingest_instance = DirectIngestInstance(ingest_instance_str.upper())
    except ValueError:
        response = f"Bad ingest instance value [{ingest_instance_str}]"
        logging.error(response)
        return response, HTTPStatus.BAD_REQUEST

    with monitoring.push_region_tag(
        region_code,
        ingest_instance=ingest_instance.value,
    ):
        try:
            controller = DirectIngestControllerFactory.build(
                region_code=region_code,
                ingest_instance=ingest_instance,
                allow_unlaunched=False,
            )
        except DirectIngestError as e:
            if e.is_bad_request():
                logging.error(str(e))
                return str(e), HTTPStatus.BAD_REQUEST
            raise e

        controller.schedule_next_ingest_task(
            current_task_id=current_task_id, just_finished_job=just_finished_job
        )
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
    supported_regions = get_supported_direct_ingest_region_codes()
    for region_code in supported_regions:
        region = _region_for_region_code(region_code=region_code)
        if not region.is_ingest_launched_in_env():
            continue
        system_level = SystemLevel.for_region(region)
        for ingest_instance in DirectIngestInstance:
            with monitoring.push_region_tag(
                region_code, ingest_instance=ingest_instance.value
            ):
                try:
                    ingest_instance.check_is_valid_system_level(system_level)
                except DirectIngestInstanceError:
                    continue
                controller = DirectIngestControllerFactory.build(
                    region_code=region_code,
                    ingest_instance=ingest_instance,
                    allow_unlaunched=False,
                )

                controller.kick_scheduler(just_finished_job=False)


@direct_ingest_control.route("/upload_from_sftp", methods=["GET", "POST"])
@requires_gae_auth
def upload_from_sftp() -> Tuple[str, HTTPStatus]:
    """Connects to remote SFTP servers and uploads the files in both raw and normalized form
    to GCS buckets to start the ingest process. Should only be called from a task queue scheduler.

    Args:
        region_code (Optional[str]): required as part of the request to identify the region
        date_str (Optional[str]): ISO format date string,
            used to determine the lower bound date in which to start
            pulling items from the SFTP server. If None, uses yesterday as the default lower
            bound time, otherwises creates a datetime from the string.
        bucket_str (Optional[str]): GCS bucket name, used to override the
            destination in which the SFTP assets are downloaded to and moved for proper
            ingest (therefore used in both controllers). If None, uses the bucket determined
            by |region_code| otherwise, uses this destination.
    """
    logging.info("Received request for uploading files from SFTP: %s", request.values)
    region_code = get_str_param_value("region", request.values)
    date_str = get_str_param_value("date", request.values)
    bucket_str = get_str_param_value("bucket", request.values)
    gcs_destination_bucket_override = (
        GcsfsBucketPath(bucket_str) if bucket_str else None
    )

    if not region_code:
        response = f"Bad parameters [{request.values}]"
        logging.error(response)
        return response, HTTPStatus.BAD_REQUEST

    with monitoring.push_region_tag(region_code, ingest_instance=None):
        lower_bound_update_datetime = (
            datetime.datetime.fromisoformat(date_str)
            if date_str is not None
            else datetime.datetime.now(tz=pytz.UTC) - datetime.timedelta(1)
        )
        sftp_controller = DownloadFilesFromSftpController(
            project_id=metadata.project_id(),
            region_code=region_code,
            lower_bound_update_datetime=lower_bound_update_datetime,
            gcs_destination_path=gcs_destination_bucket_override,
        )
        download_result = sftp_controller.do_fetch()

        with monitoring.measurements(
            {TagKey.SFTP_TASK_TYPE: "download"}
        ) as download_measurements:
            download_measurements.measure_int_put(
                m_sftp_attempts,
                len(download_result.successes) + len(download_result.failures),
            )
            download_measurements.measure_int_put(
                m_sftp_errors, len(download_result.failures)
            )

        unable_to_download_text = (
            f"Unable to download the following files: {download_result.failures}"
            if download_result.failures
            else ""
        )
        skipped_download_text = (
            f"Skipped downloading the following files: {download_result.skipped}"
            if download_result.skipped
            else ""
        )

        if not download_result.successes and download_result.failures:
            return (
                f"All files failed to download. {unable_to_download_text}",
                HTTPStatus.BAD_REQUEST,
            )

        if not download_result.successes and not download_result.skipped:
            return f"No items to download for {region_code}", HTTPStatus.OK

        if not download_result.successes and download_result.skipped:
            return f"All files skipped. {skipped_download_text}", HTTPStatus.OK

        if not download_result.successes:
            raise ValueError("Expected non-empty successes here.")

        upload_controller = UploadStateFilesToIngestBucketController(
            paths_with_timestamps=download_result.successes,
            project_id=metadata.project_id(),
            region_code=region_code,
            gcs_destination_path=gcs_destination_bucket_override,
        )
        upload_result = upload_controller.do_upload()

        with monitoring.measurements(
            {TagKey.SFTP_TASK_TYPE: "upload"}
        ) as upload_measurements:
            upload_measurements.measure_int_put(
                m_sftp_attempts,
                len(upload_result.successes) + len(upload_result.failures),
            )
            upload_measurements.measure_int_put(
                m_sftp_errors, len(upload_result.failures)
            )

        unable_to_upload_text = (
            f"Unable to upload the following files: {upload_result.failures}"
            if upload_result.failures
            else ""
        )
        skipped_text = (
            f"Skipped uploading the following files: {upload_controller.skipped_files}"
            if upload_result.skipped
            else ""
        )
        if not upload_result.successes and not upload_result.skipped:
            return (
                f"{unable_to_download_text}"
                f" All files failed to upload. {unable_to_upload_text}"
                f"{skipped_text}",
                HTTPStatus.BAD_REQUEST,
            )

        if download_result.failures or upload_result.failures:
            return (
                f"{unable_to_download_text}"
                f" {unable_to_upload_text}"
                f"{skipped_text}",
                HTTPStatus.MULTI_STATUS,
            )

        if not upload_result.successes and upload_result.skipped:
            return f"All files skipped. {skipped_text}", HTTPStatus.OK

        if upload_result.skipped:
            return (
                f"{unable_to_download_text}"
                f" {unable_to_upload_text}"
                f"{skipped_text}",
                HTTPStatus.MULTI_STATUS,
            )

        # Trigger ingest to handle copied files (in case queue has emptied already while
        # ingest was paused).
        direct_ingest_cloud_task_manager = DirectIngestCloudTaskManagerImpl()

        if not gcs_destination_bucket_override:
            try:
                ingest_instance = DirectIngestInstanceFactory.for_ingest_bucket(
                    upload_controller.destination_ingest_bucket
                )
            except ValueError:
                # If the destination path is not a real ingest bucket, do not
                # trigger the scheduler.
                ingest_instance = None

            if ingest_instance:
                direct_ingest_cloud_task_manager.create_direct_ingest_handle_new_files_task(
                    region=_region_for_region_code(region_code),
                    ingest_instance=ingest_instance,
                    can_start_ingest=True,
                )

        return "", HTTPStatus.OK


@direct_ingest_control.route("/handle_sftp_files", methods=["GET", "POST"])
@requires_gae_auth
def handle_sftp_files() -> Tuple[str, HTTPStatus]:
    """Schedules the SFTP downloads into the appropriate cloud task queue."""
    logging.info("Received request for handling SFTP files: %s", request.values)
    region_code = get_str_param_value("region", request.values)

    if not region_code:
        response = f"Bad parameters [{request.values}]"
        logging.error(response)
        return response, HTTPStatus.BAD_REQUEST

    with monitoring.push_region_tag(region_code, ingest_instance=None):
        try:
            region = _region_for_region_code(region_code)
            direct_ingest_cloud_task_manager = DirectIngestCloudTaskManagerImpl()
            direct_ingest_cloud_task_manager.create_direct_ingest_sftp_download_task(
                region
            )
        except FileNotFoundError as e:
            raise DirectIngestError(
                msg=f"Region [{region_code}] has no registered manifest",
                error_type=DirectIngestErrorType.INPUT_ERROR,
            ) from e

    return "", HTTPStatus.OK


def _region_for_region_code(region_code: str) -> Region:
    try:
        return regions.get_region(region_code.lower(), is_direct_ingest=True)
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
    return DirectIngestCloudTaskManager.json_to_cloud_task_args(data)
