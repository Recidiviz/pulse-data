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
"""Ingest Operations utilities """

import logging
from collections import defaultdict
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import attr
import cattr

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.view_update_manager import (
    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
)
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsDirectoryPath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileImportManager,
)
from recidiviz.persistence.entity.operations.entities import DirectIngestRawFileMetadata
from recidiviz.utils.regions import get_region


def check_is_valid_sandbox_bucket(bucket: GcsfsBucketPath) -> None:
    if (
        "test" not in bucket.bucket_name
        and "scratch" not in bucket.bucket_name
        and "sandbox" not in bucket.bucket_name
    ):
        raise ValueError(
            f"Invalid bucket [{bucket.bucket_name}] - must have 'test', "
            f"'sandbox', or 'scratch' in the name."
        )


class Status(Enum):
    SUCCEEDED = "succeeded"
    SKIPPED = "skipped"
    FAILED = "failed"


@attr.s
class FileStatus:
    fileTag: str = attr.ib()
    status: Status = attr.ib()
    errorMessage: Optional[str] = attr.ib()


@attr.s
class SandboxRawFileImportResult:
    fileStatuses: List[FileStatus] = attr.ib()

    def to_serializable(self) -> Dict[str, Any]:
        return cattr.Converter().unstructure(self)


def import_raw_files_to_bq_sandbox(
    state_code: StateCode,
    sandbox_dataset_prefix: str,
    source_bucket: GcsfsBucketPath,
    file_tag_filters: Optional[List[str]],
) -> SandboxRawFileImportResult:
    """Imports a set of raw data files in the given source bucket into a sandbox
    dataset. If |file_tag_filters| is set, then will only import files with that set
    of tags.
    """

    file_status_list = []

    try:
        region_code = state_code.value.lower()
        import_manager = DirectIngestRawFileImportManager(
            region=get_region(region_code, is_direct_ingest=True),
            fs=DirectIngestGCSFileSystem(GcsfsFactory.build()),
            ingest_bucket_path=source_bucket,
            temp_output_directory_path=GcsfsDirectoryPath.from_dir_and_subdir(
                source_bucket, "temp_raw_data"
            ),
            big_query_client=BigQueryClientImpl(),
            sandbox_dataset_prefix=sandbox_dataset_prefix,
            allow_incomplete_configs=True,
        )

        bq_client = BigQueryClientImpl()

        # Create the dataset up front with table expiration
        bq_client.create_dataset_if_necessary(
            bq_client.dataset_ref_for_id(dataset_id=import_manager.raw_tables_dataset),
            default_table_expiration_ms=TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
        )

        raw_files_to_import = import_manager.get_unprocessed_raw_files_to_import()

    except ValueError as error:
        raise ValueError(
            "Something went wrong trying to get unprocessed raw files to import"
        ) from error

    failures_by_exception = defaultdict(list)

    for i, file_path in enumerate(raw_files_to_import):
        parts = filename_parts_from_path(file_path)
        if file_tag_filters is not None and parts.file_tag not in file_tag_filters:
            file_status_list.append(
                FileStatus(
                    fileTag=parts.file_tag,
                    status=Status.SKIPPED,
                    errorMessage=None,
                )
            )
            logging.info("** Skipping file with tag [%s] **", parts.file_tag)
            continue

        logging.info("Running file with tag [%s]", parts.file_tag)

        try:
            import_manager.import_raw_file_to_big_query(
                file_path,
                DirectIngestRawFileMetadata(
                    file_id=i,
                    region_code=state_code.value,
                    file_tag=parts.file_tag,
                    processed_time=None,
                    discovery_time=datetime.now(),
                    normalized_file_name=file_path.file_name,
                    datetimes_contained_upper_bound_inclusive=parts.utc_upload_datetime,
                ),
            )

            file_status_list.append(
                FileStatus(
                    fileTag=parts.file_tag,
                    status=Status.SUCCEEDED,
                    errorMessage=None,
                )
            )

        except Exception as e:
            logging.exception(e)
            file_status_list.append(
                FileStatus(
                    fileTag=parts.file_tag,
                    status=Status.FAILED,
                    errorMessage=str(e),
                )
            )
            failures_by_exception[str(e)].append(file_path.abs_path())

    if failures_by_exception:
        logging.error("************************* FAILURES ************************")
        total_files = 0
        all_failed_paths = []
        for item_error, file_list in failures_by_exception.items():
            total_files += len(file_list)
            all_failed_paths += file_list

            logging.error(
                "Failed [%s] files with error [%s]: %s",
                len(file_list),
                item_error,
                file_list,
            )

        logging.error("***********************************************************")
        logging.error("Failed to import [%s] files: %s", total_files, all_failed_paths)
        return SandboxRawFileImportResult(fileStatuses=file_status_list)

    return SandboxRawFileImportResult(fileStatuses=file_status_list)
