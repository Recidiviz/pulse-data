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

from flask import Blueprint, request
from opencensus.stats import measure, view, aggregation
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_storage.gcs_pseudo_lock_manager import GCSPseudoLockAlreadyExists
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.direct_ingest_controller_factory import (
    DirectIngestControllerFactory,
)
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.controllers.direct_ingest_instance import (
    DirectIngestInstance,
)
from recidiviz.ingest.direct.controllers.direct_ingest_raw_data_table_latest_view_updater import (
    DirectIngestRawDataTableLatestViewUpdater,
)
from recidiviz.ingest.direct.controllers.direct_ingest_raw_update_cloud_task_manager import (
    DirectIngestRawUpdateCloudTaskManager,
)
from recidiviz.ingest.direct.controllers.download_files_from_sftp import (
    DownloadFilesFromSftpController,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsRawDataBQImportArgs,
    GcsfsIngestViewExportArgs,
    GcsfsDirectIngestFileType,
    gcsfs_direct_ingest_bucket_for_region,
    GcsfsIngestArgs,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath, GcsfsPath
from recidiviz.ingest.direct.controllers.upload_state_files_to_ingest_bucket_with_date import (
    UploadStateFilesToIngestBucketController,
)
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import (
    DirectIngestCloudTaskManager,
    DirectIngestCloudTaskManagerImpl,
)
from recidiviz.ingest.direct.controllers.direct_ingest_types import (
    CloudTaskArgs,
)
from recidiviz.ingest.direct.direct_ingest_region_utils import (
    get_existing_region_dir_names,
)
from recidiviz.ingest.direct.errors import DirectIngestError, DirectIngestErrorType
from recidiviz.utils import regions, monitoring, metadata
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.monitoring import TagKey
from recidiviz.utils.params import get_str_param_value, get_bool_param_value
from recidiviz.utils.regions import (
    Region,
    get_supported_direct_ingest_region_codes,
)

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


@direct_ingest_control.route("/normalize_raw_file_path")
@requires_gae_auth
def normalize_raw_file_path() -> Tuple[str, HTTPStatus]:
    """Called from a Cloud Function when a new file is added to a bucket that is configured to rename files but not
    ingest them. For example, a bucket that is being used for automatic data transfer testing.
    """
    # The bucket name for the file to normalize
    bucket = get_str_param_value("bucket", request.args)
    # The relative path to the file, not including the bucket name
    relative_file_path = get_str_param_value(
        "relative_file_path", request.args, preserve_case=True
    )

    if not bucket or not relative_file_path:
        return f"Bad parameters [{request.args}]", HTTPStatus.BAD_REQUEST

    path = GcsfsPath.from_bucket_and_blob_name(
        bucket_name=bucket, blob_name=relative_file_path
    )

    if not isinstance(path, GcsfsFilePath):
        raise ValueError(f"Incorrect type [{type(path)}] for path: {path.uri()}")

    fs = DirectIngestGCSFileSystem(GcsfsFactory.build())
    fs.mv_path_to_normalized_path(path, file_type=GcsfsDirectIngestFileType.RAW_DATA)

    return "", HTTPStatus.OK


@direct_ingest_control.route("/handle_direct_ingest_file")
@requires_gae_auth
def handle_direct_ingest_file() -> Tuple[str, HTTPStatus]:
    """Called from a Cloud Function when a new file is added to a direct ingest
    bucket. Will trigger a job that deals with normalizing and splitting the
    file as is appropriate, then start the scheduler if allowed.
    """
    region_code = get_str_param_value("region", request.args)
    # The bucket name for the file to ingest
    bucket = get_str_param_value("bucket", request.args)
    # The relative path to the file, not including the bucket name
    relative_file_path = get_str_param_value(
        "relative_file_path", request.args, preserve_case=True
    )
    start_ingest = get_bool_param_value("start_ingest", request.args, default=False)

    if not region_code or not bucket or not relative_file_path or start_ingest is None:
        return f"Bad parameters [{request.args}]", HTTPStatus.BAD_REQUEST

    with monitoring.push_region_tag(region_code):
        try:
            controller = DirectIngestControllerFactory.build(
                ingest_bucket_path=GcsfsBucketPath(bucket_name=bucket),
                allow_unlaunched=True,
            )
        except DirectIngestError as e:
            if e.is_bad_request():
                return str(e), HTTPStatus.BAD_REQUEST
            raise e

        path = GcsfsPath.from_bucket_and_blob_name(
            bucket_name=bucket, blob_name=relative_file_path
        )

        if isinstance(path, GcsfsFilePath):
            controller.handle_file(path, start_ingest=start_ingest)

    return "", HTTPStatus.OK


@direct_ingest_control.route("/handle_new_files", methods=["GET", "POST"])
@requires_gae_auth
def handle_new_files() -> Tuple[str, HTTPStatus]:
    """Normalizes and splits files in the ingest bucket for a given region as
    is appropriate. Will schedule the next process_job task if no renaming /
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
    bucket = get_str_param_value("bucket", request.values)

    if not region_code or can_start_ingest is None or not bucket:
        return f"Bad parameters [{request.values}]", HTTPStatus.BAD_REQUEST

    with monitoring.push_region_tag(region_code):
        try:
            controller = DirectIngestControllerFactory.build(
                ingest_bucket_path=GcsfsBucketPath(bucket_name=bucket),
                allow_unlaunched=True,
            )
        except DirectIngestError as e:
            if e.is_bad_request():
                return str(e), HTTPStatus.BAD_REQUEST
            raise e

        controller.handle_new_files(can_start_ingest=can_start_ingest)
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
        with monitoring.push_region_tag(region_code):
            ingest_bucket = gcsfs_direct_ingest_bucket_for_region(
                region_code=region_code,
                system_level=SystemLevel.for_region(
                    _region_for_region_code(region_code)
                ),
                # The only type of file that wouldn't be normalized is a raw file, which
                # should only ever be in the PRIMARY bucket.
                ingest_instance=DirectIngestInstance.PRIMARY,
            )
            try:
                controller = DirectIngestControllerFactory.build(
                    ingest_bucket_path=ingest_bucket,
                    allow_unlaunched=True,
                )
            except DirectIngestError as e:
                if e.is_bad_request():
                    return str(e), HTTPStatus.BAD_REQUEST
                raise e

            can_start_ingest = controller.region.is_ingest_launched_in_env()
            controller.cloud_task_manager.create_direct_ingest_handle_new_files_task(
                controller.region,
                ingest_bucket=controller.ingest_bucket_path,
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

    if not region_code:
        return f"Bad parameters [{request.values}]", HTTPStatus.BAD_REQUEST

    with monitoring.push_region_tag(region_code):
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

        with monitoring.push_tags(
            {TagKey.RAW_DATA_IMPORT_TAG: data_import_args.task_id_tag()}
        ):
            try:
                controller = DirectIngestControllerFactory.build(
                    ingest_bucket_path=data_import_args.raw_data_file_path.bucket_path,
                    allow_unlaunched=False,
                )
            except DirectIngestError as e:
                if e.is_bad_request():
                    return str(e), HTTPStatus.BAD_REQUEST
                raise e

            controller.do_raw_data_import(data_import_args)
    return "", HTTPStatus.OK


@direct_ingest_control.route(
    "/create_raw_data_latest_view_update_tasks", methods=["GET", "POST"]
)
@requires_gae_auth
def create_raw_data_latest_view_update_tasks() -> Tuple[str, HTTPStatus]:
    """Creates tasks for every direct ingest region with SQL preprocessing
    enabled to update the raw data table latest views.
    """
    raw_update_ctm = DirectIngestRawUpdateCloudTaskManager()

    for region_code in get_existing_region_dir_names():
        with monitoring.push_region_tag(region_code):
            region = _region_for_region_code(region_code)
            if region.is_ingest_launched_in_env():
                logging.info(
                    "Creating raw data latest view update task for region [%s]",
                    region_code,
                )
                raw_update_ctm.create_raw_data_latest_view_update_task(region_code)
            else:
                logging.info(
                    "Skipping raw data latest view update for region [%s] - ingest not enabled.",
                    region_code,
                )
    return "", HTTPStatus.OK


@direct_ingest_control.route(
    "/update_raw_data_latest_views_for_state", methods=["POST"]
)
@requires_gae_auth
def update_raw_data_latest_views_for_state() -> Tuple[str, HTTPStatus]:
    """Updates raw data tables for a given state"""
    logging.info(
        "Received request to do direct ingest raw data update: [%s]", request.values
    )
    region_code = get_str_param_value("region", request.values)

    if not region_code:
        return f"Bad parameters [{request.values}]", HTTPStatus.BAD_REQUEST

    with monitoring.push_region_tag(region_code):
        bq_client = BigQueryClientImpl(project_id=metadata.project_id())
        controller = DirectIngestRawDataTableLatestViewUpdater(
            region_code, metadata.project_id(), bq_client
        )
        controller.update_views_for_state()
    return "", HTTPStatus.OK


@direct_ingest_control.route("/ingest_view_export", methods=["POST"])
@requires_gae_auth
def ingest_view_export() -> Tuple[str, HTTPStatus]:
    """Exports an ingest view from BQ to a file in the region's GCS File System ingest bucket that is ready to be
    processed and ingested into our Recidiviz DB.
    """
    logging.info(
        "Received request to do direct ingest view export: [%s]", request.values
    )
    region_code = get_str_param_value("region", request.values)

    if not region_code:
        return f"Bad parameters [{request.values}]", HTTPStatus.BAD_REQUEST

    with monitoring.push_region_tag(region_code):
        json_data = request.get_data(as_text=True)
        ingest_view_export_args = _parse_cloud_task_args(json_data)

        if not ingest_view_export_args:
            raise DirectIngestError(
                msg="raw_data_import was called with no GcsfsIngestViewExportArgs.",
                error_type=DirectIngestErrorType.INPUT_ERROR,
            )

        if not isinstance(ingest_view_export_args, GcsfsIngestViewExportArgs):
            raise DirectIngestError(
                msg=f"raw_data_import was called with incorrect args type [{type(ingest_view_export_args)}].",
                error_type=DirectIngestErrorType.INPUT_ERROR,
            )
        with monitoring.push_tags(
            {TagKey.INGEST_VIEW_EXPORT_TAG: ingest_view_export_args.task_id_tag()}
        ):
            try:
                controller = DirectIngestControllerFactory.build(
                    ingest_bucket_path=GcsfsBucketPath(
                        ingest_view_export_args.output_bucket_name
                    ),
                    allow_unlaunched=False,
                )
            except DirectIngestError as e:
                if e.is_bad_request():
                    return str(e), HTTPStatus.BAD_REQUEST
                raise e

            controller.do_ingest_view_export(ingest_view_export_args)
    return "", HTTPStatus.OK


@direct_ingest_control.route("/process_job", methods=["POST"])
@requires_gae_auth
def process_job() -> Tuple[str, HTTPStatus]:
    """Processes a single direct ingest file, specified in the provided ingest
    arguments.
    """
    logging.info("Received request to process direct ingest job: [%s]", request.values)
    region_code = get_str_param_value("region", request.values)

    if not region_code:
        return f"Bad parameters [{request.values}]", HTTPStatus.BAD_REQUEST

    with monitoring.push_region_tag(region_code):
        json_data = request.get_data(as_text=True)
        ingest_args = _parse_cloud_task_args(json_data)

        if not ingest_args:
            raise DirectIngestError(
                msg="process_job was called with no GcsfsIngestArgs.",
                error_type=DirectIngestErrorType.INPUT_ERROR,
            )

        if not isinstance(ingest_args, GcsfsIngestArgs):
            raise DirectIngestError(
                msg=f"process_job was called with incorrect args type [{type(ingest_args)}].",
                error_type=DirectIngestErrorType.INPUT_ERROR,
            )

        if not ingest_args:
            return "Could not parse ingest args", HTTPStatus.BAD_REQUEST
        with monitoring.push_tags({TagKey.INGEST_TASK_TAG: ingest_args.task_id_tag()}):
            try:
                controller = DirectIngestControllerFactory.build(
                    ingest_bucket_path=ingest_args.file_path.bucket_path,
                    allow_unlaunched=False,
                )
            except DirectIngestError as e:
                if e.is_bad_request():
                    return str(e), HTTPStatus.BAD_REQUEST
                raise e

            try:
                controller.run_ingest_job_and_kick_scheduler_on_completion(ingest_args)
            except GCSPseudoLockAlreadyExists as e:
                return str(e), HTTPStatus.CONFLICT
    return "", HTTPStatus.OK


@direct_ingest_control.route("/scheduler", methods=["GET", "POST"])
@requires_gae_auth
def scheduler() -> Tuple[str, HTTPStatus]:
    logging.info("Received request for direct ingest scheduler: %s", request.values)
    region_code = get_str_param_value("region", request.values)
    just_finished_job = get_bool_param_value(
        "just_finished_job", request.values, default=False
    )

    # The bucket name for ingest instance to schedule work out of
    bucket = get_str_param_value("bucket", request.args)

    if not region_code or just_finished_job is None or not bucket:
        return f"Bad parameters [{request.values}]", HTTPStatus.BAD_REQUEST

    with monitoring.push_region_tag(region_code):
        try:
            controller = DirectIngestControllerFactory.build(
                ingest_bucket_path=GcsfsBucketPath(bucket), allow_unlaunched=False
            )
        except DirectIngestError as e:
            if e.is_bad_request():
                return str(e), HTTPStatus.BAD_REQUEST
            raise e

        controller.schedule_next_ingest_job(just_finished_job)
    return "", HTTPStatus.OK


def kick_all_schedulers() -> None:
    """Kicks all ingest schedulers to restart ingest"""
    supported_regions = get_supported_direct_ingest_region_codes()
    for region_code in supported_regions:
        with monitoring.push_region_tag(region_code):
            region = _region_for_region_code(region_code=region_code)
            if not region.is_ingest_launched_in_env():
                continue
            for ingest_instance in DirectIngestInstance:
                ingest_bucket = gcsfs_direct_ingest_bucket_for_region(
                    region_code=region_code,
                    system_level=SystemLevel.for_region(region),
                    ingest_instance=ingest_instance,
                )
                controller = DirectIngestControllerFactory.build(
                    ingest_bucket_path=ingest_bucket,
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
    gcs_destination_path = GcsfsBucketPath(bucket_str) if bucket_str else None

    if not region_code:
        return f"Bad parameters [{request.values}]", HTTPStatus.BAD_REQUEST

    with monitoring.push_region_tag(region_code):
        lower_bound_update_datetime = (
            datetime.datetime.fromisoformat(date_str)
            if date_str is not None
            else datetime.datetime.utcnow() - datetime.timedelta(1)
        )
        sftp_controller = DownloadFilesFromSftpController(
            project_id=metadata.project_id(),
            region=region_code,
            lower_bound_update_datetime=lower_bound_update_datetime,
            gcs_destination_path=gcs_destination_path,
        )
        downloaded_items, unable_to_download_items = sftp_controller.do_fetch()

        with monitoring.measurements(
            {TagKey.SFTP_TASK_TYPE: "download"}
        ) as download_measurements:
            download_measurements.measure_int_put(
                m_sftp_attempts, len(downloaded_items) + len(unable_to_download_items)
            )
            download_measurements.measure_int_put(
                m_sftp_errors, len(unable_to_download_items)
            )

        if downloaded_items:
            (
                uploaded_files,
                unable_to_upload_files,
            ) = UploadStateFilesToIngestBucketController(
                paths_with_timestamps=downloaded_items,
                project_id=metadata.project_id(),
                region=region_code,
                gcs_destination_path=gcs_destination_path,
            ).do_upload()

            with monitoring.measurements(
                {TagKey.SFTP_TASK_TYPE: "upload"}
            ) as upload_measurements:
                upload_measurements.measure_int_put(
                    m_sftp_attempts, len(uploaded_files) + len(unable_to_upload_files)
                )
                upload_measurements.measure_int_put(
                    m_sftp_errors, len(unable_to_upload_files)
                )

            sftp_controller.clean_up()

            if unable_to_download_items or unable_to_upload_files:
                return (
                    f"Unable to download the following files: {unable_to_download_items}, "
                    f"and upload the following files: {unable_to_upload_files}",
                    HTTPStatus.MULTI_STATUS,
                )
        elif unable_to_download_items:
            return (
                f"Unable to download the following files {unable_to_download_items}",
                HTTPStatus.MULTI_STATUS,
            )
        elif not downloaded_items and not unable_to_download_items:
            return f"No items to download for {region_code}", HTTPStatus.MULTI_STATUS
    return "", HTTPStatus.OK


@direct_ingest_control.route("/handle_sftp_files", methods=["GET", "POST"])
@requires_gae_auth
def handle_sftp_files() -> Tuple[str, HTTPStatus]:
    """Schedules the SFTP downloads into the appropriate cloud task queue."""
    logging.info("Received request for handling SFTP files: %s", request.values)
    region_code = get_str_param_value("region", request.values)

    if not region_code:
        return f"Bad parameters [{request.values}]", HTTPStatus.BAD_REQUEST

    with monitoring.push_region_tag(region_code):
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
