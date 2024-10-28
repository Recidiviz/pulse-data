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
"""Utilities for a raw file sandbox import."""
import datetime
import logging
from collections import defaultdict
from enum import Enum
from typing import Dict, List, Optional

import attr
import google_crc32c

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.constants import TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
from recidiviz.cloud_storage.gcsfs_csv_reader import GcsfsCsvReader
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_temporary_output_directory_path,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.raw_data.legacy_direct_ingest_raw_file_import_manager import (
    LegacyDirectIngestRawFileImportManager,
)
from recidiviz.ingest.direct.raw_data_table_schema_utils import (
    update_raw_data_table_schema,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.entity.operations.entities import DirectIngestRawFileMetadata


def _id_for_file(state_code: StateCode, file_name: str) -> int:
    """Create an id for the file based on the file name.

    In production, the file id is an automatically incremented id created by Postgres.
    Here we generate one by hashing the state code and file name, which are the
    components of the primary key in the Postgres table.
    """
    checksum = google_crc32c.Checksum()
    checksum.update(state_code.value.encode())
    checksum.update(file_name.encode())
    return int.from_bytes(checksum.digest(), byteorder="big")


class SandboxImportStatus(Enum):
    SKIPPED = "skipped"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@attr.s
class SandboxRawFileImportResult:
    path: GcsfsFilePath = attr.ib()
    status: SandboxImportStatus = attr.ib()
    error_message: Optional[str] = attr.ib()

    def format_for_print(self) -> str:
        suffix = f": {self.error_message}" if self.error_message else ""
        return f"[{self.status.name}] {self.path.blob_name} {suffix}"


@attr.s
class SandboxImportRun:
    status_to_imports: Dict[
        SandboxImportStatus, List[SandboxRawFileImportResult]
    ] = attr.ib()


def legacy_import_raw_files_to_bq_sandbox(
    *,
    state_code: StateCode,
    sandbox_dataset_prefix: str,
    files_to_import: List[GcsfsFilePath],
    allow_incomplete_configs: bool,
    big_query_client: BigQueryClient,
    fs: DirectIngestGCSFileSystem,
) -> SandboxImportRun:
    """Imports a set of raw data files in the given source bucket into a sandbox
    dataset. If |file_tag_filters| is set, then will only import files with that set
    of tags.
    """

    csv_reader = GcsfsCsvReader(fs)

    try:
        region_code = state_code.value.lower()
        region = get_direct_ingest_region(region_code)

        import_manager = LegacyDirectIngestRawFileImportManager(
            region=region,
            fs=fs,
            temp_output_directory_path=gcsfs_direct_ingest_temporary_output_directory_path(
                subdir=sandbox_dataset_prefix
            ),
            csv_reader=csv_reader,
            big_query_client=big_query_client,
            sandbox_dataset_prefix=sandbox_dataset_prefix,
            allow_incomplete_configs=allow_incomplete_configs,
            # Sandbox instance can be primary
            instance=DirectIngestInstance.PRIMARY,
        )

        # Create the dataset up front with table expiration
        big_query_client.create_dataset_if_necessary(
            dataset_id=import_manager.raw_tables_dataset,
            default_table_expiration_ms=TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
        )

    except ValueError as error:
        raise ValueError(
            "Something went wrong trying to get unprocessed raw files to import"
        ) from error

    seen_tags = set()
    status_to_imports = defaultdict(list)

    for file_path in files_to_import:
        parts = filename_parts_from_path(file_path)

        logging.info(
            "Importing import for tag [%s] with update datetime [%s]",
            parts.file_tag,
            parts.utc_upload_datetime_str,
        )

        # Update the schema if this is the first file with this tag.
        # if we are allowing incomplete configs, we cannot update the raw data table
        # schema as we do not know before reading the raw data file what the rows
        # will be
        if parts.file_tag not in seen_tags and not allow_incomplete_configs:
            update_raw_data_table_schema(
                state_code=state_code,
                instance=DirectIngestInstance.PRIMARY,
                raw_file_tag=parts.file_tag,
                big_query_client=big_query_client,
                sandbox_dataset_prefix=sandbox_dataset_prefix,
            )
            seen_tags.add(parts.file_tag)

        try:
            import_manager.import_raw_file_to_big_query(
                file_path,
                DirectIngestRawFileMetadata(
                    file_id=_id_for_file(state_code, file_path.file_name),
                    region_code=state_code.value,
                    file_tag=parts.file_tag,
                    file_processed_time=None,
                    file_discovery_time=datetime.datetime.now(),
                    normalized_file_name=file_path.file_name,
                    update_datetime=parts.utc_upload_datetime,
                    # Sandbox instance can be primary
                    raw_data_instance=DirectIngestInstance.PRIMARY,
                ),
            )

            status_to_imports[SandboxImportStatus.SUCCEEDED].append(
                SandboxRawFileImportResult(
                    path=file_path,
                    status=SandboxImportStatus.SUCCEEDED,
                    error_message=None,
                )
            )

        except Exception as e:
            logging.exception(e)
            status_to_imports[SandboxImportStatus.FAILED].append(
                SandboxRawFileImportResult(
                    path=file_path,
                    status=SandboxImportStatus.FAILED,
                    error_message=str(e),
                )
            )

    return SandboxImportRun(status_to_imports=status_to_imports)
