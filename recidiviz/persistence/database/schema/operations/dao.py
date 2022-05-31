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
"""Data Access Object (DAO) with logic for accessing operations DB information from a SQL Database."""
import datetime
from typing import List, Optional

from more_itertools import one

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.session import Session


def get_raw_file_metadata_row_for_path(
    session: Session,
    region_code: str,
    path: GcsfsFilePath,
    raw_data_instance: DirectIngestInstance,
) -> schema.DirectIngestRawFileMetadata:
    """Returns metadata information for the provided path, throws if it doesn't exist."""
    results = (
        session.query(schema.DirectIngestRawFileMetadata)
        .filter_by(
            region_code=region_code.upper(),
            normalized_file_name=path.file_name,
            is_invalidated=False,
            raw_data_instance=raw_data_instance.value,
        )
        .all()
    )

    if len(results) != 1:
        raise ValueError(
            f"Unexpected number of metadata results for path {path.abs_path()}: [{len(results)}]"
        )

    return one(results)


def get_sftp_file_metadata_row_for_path(
    session: Session, region_code: str, remote_file_path: str
) -> schema.DirectIngestSftpFileMetadata:
    """Returns metadata information for the provided path, throws if it doesn't exist."""
    results = (
        session.query(schema.DirectIngestSftpFileMetadata)
        .filter_by(region_code=region_code.upper(), remote_file_path=remote_file_path)
        .all()
    )

    if len(results) != 1:
        raise ValueError(
            f"Unexpected number of metadata results for path {remote_file_path}: [{len(results)}]"
        )

    return one(results)


def get_metadata_for_raw_files_discovered_after_datetime(
    session: Session,
    region_code: str,
    raw_file_tag: str,
    discovery_time_lower_bound_exclusive: Optional[datetime.datetime],
    raw_data_instance: DirectIngestInstance,
) -> List[schema.DirectIngestRawFileMetadata]:
    """Returns metadata for all raw files with a given tag that have been updated after the provided date."""

    query = session.query(schema.DirectIngestRawFileMetadata).filter_by(
        region_code=region_code.upper(),
        file_tag=raw_file_tag,
        is_invalidated=False,
        raw_data_instance=raw_data_instance.value,
    )
    if discovery_time_lower_bound_exclusive:
        query = query.filter(
            schema.DirectIngestRawFileMetadata.discovery_time
            > discovery_time_lower_bound_exclusive
        )

    return query.all()


def get_raw_file_rows_count_for_region(
    session: Session, region_code: str, is_processed: bool
) -> int:
    """Counts all operations DB raw file metadata rows for the given region.
    If is_processed is True, returns the count of processed files. If it is False,
    returns the count of unprocessed files.
    """
    query = session.query(schema.DirectIngestRawFileMetadata.file_id).filter_by(
        region_code=region_code.upper()
    )
    if is_processed:
        query = query.filter(
            schema.DirectIngestRawFileMetadata.processed_time.isnot(None),
        )
    else:
        query = query.filter(
            schema.DirectIngestRawFileMetadata.processed_time.is_(None),
        )

    return query.count()
