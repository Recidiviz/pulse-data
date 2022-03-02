# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Helpers related to building bucket/directory paths for use in ingest."""
import os
from typing import Optional

from recidiviz.cloud_functions.direct_ingest_bucket_name_utils import (
    INGEST_PRIMARY_BUCKET_SUFFIX,
    INGEST_SECONDARY_BUCKET_SUFFIX,
    INGEST_SFTP_BUCKET_SUFFIX,
    build_ingest_bucket_name,
    build_ingest_storage_bucket_name,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsDirectoryPath
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.gcs.file_type import GcsfsDirectIngestFileType
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils import metadata


def gcsfs_direct_ingest_temporary_output_directory_path(
    project_id: Optional[str] = None,
) -> GcsfsDirectoryPath:
    if project_id is None:
        project_id = metadata.project_id()
        if not project_id:
            raise ValueError("Project id not set")

    return GcsfsDirectoryPath.from_absolute_path(
        f"{project_id}-direct-ingest-temporary-files"
    )


def _bucket_suffix_for_ingest_instance(ingest_instance: DirectIngestInstance) -> str:
    if ingest_instance == DirectIngestInstance.PRIMARY:
        return INGEST_PRIMARY_BUCKET_SUFFIX
    if ingest_instance == DirectIngestInstance.SECONDARY:
        return INGEST_SECONDARY_BUCKET_SUFFIX
    raise ValueError(f"Unexpected ingest instance [{ingest_instance}]")


def gcsfs_direct_ingest_storage_directory_path_for_region(
    *,
    region_code: str,
    system_level: SystemLevel,
    ingest_instance: DirectIngestInstance,
    file_type: Optional[GcsfsDirectIngestFileType] = None,
    project_id: Optional[str] = None,
) -> GcsfsDirectoryPath:
    if project_id is None:
        project_id = metadata.project_id()
        if not project_id:
            raise ValueError("Project id not set")

    suffix = _bucket_suffix_for_ingest_instance(ingest_instance)
    bucket_name = build_ingest_storage_bucket_name(
        project_id=project_id,
        system_level_str=system_level.value.lower(),
        suffix=suffix,
    )
    storage_bucket = GcsfsBucketPath(bucket_name)

    if file_type is not None:
        subdir = os.path.join(region_code.lower(), file_type.value)
    else:
        subdir = region_code.lower()
    return GcsfsDirectoryPath.from_dir_and_subdir(storage_bucket, subdir)


def gcsfs_direct_ingest_bucket_for_region(
    *,
    region_code: str,
    system_level: SystemLevel,
    ingest_instance: DirectIngestInstance,
    project_id: Optional[str] = None,
) -> GcsfsBucketPath:
    if project_id is None:
        project_id = metadata.project_id()
        if not project_id:
            raise ValueError("Project id not set")

    suffix = _bucket_suffix_for_ingest_instance(ingest_instance)
    bucket_name = build_ingest_bucket_name(
        project_id=project_id,
        region_code=region_code,
        system_level_str=system_level.value.lower(),
        suffix=suffix,
    )
    return GcsfsBucketPath(bucket_name=bucket_name)


def gcsfs_sftp_download_bucket_path_for_region(
    region_code: str, system_level: SystemLevel, project_id: Optional[str] = None
) -> GcsfsBucketPath:
    """Returns the GCS Directory Path for the bucket that will hold the SFTP downloaded files."""
    if project_id is None:
        project_id = metadata.project_id()
        if not project_id:
            raise ValueError("Project id not set")

    bucket_name = build_ingest_bucket_name(
        project_id=project_id,
        region_code=region_code,
        system_level_str=system_level.value.lower(),
        suffix=INGEST_SFTP_BUCKET_SUFFIX,
    )
    return GcsfsBucketPath(bucket_name)
