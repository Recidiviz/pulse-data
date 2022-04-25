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
from recidiviz.ingest.direct.gcs.file_type import GcsfsDirectIngestFileType
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.session import Session


def get_ingest_file_metadata_row(
    session: Session,
    file_id: int,
    ingest_database_name: str,
) -> schema.DirectIngestIngestFileMetadata:
    """Queries for the ingest file metadata row by the metadata row primary key."""

    results = (
        session.query(schema.DirectIngestIngestFileMetadata)
        .filter_by(file_id=file_id, ingest_database_name=ingest_database_name)
        .all()
    )
    return one(results)


def get_ingest_file_metadata_row_for_path(
    session: Session, region_code: str, path: GcsfsFilePath, ingest_database_name: str
) -> schema.DirectIngestIngestFileMetadata:
    """Returns metadata information for the provided path, throws if it doesn't exist."""

    parts = filename_parts_from_path(path)

    if parts.file_type != GcsfsDirectIngestFileType.INGEST_VIEW:
        raise ValueError(f"Unexpected file type [{parts.file_type}]")

    results = (
        session.query(schema.DirectIngestIngestFileMetadata)
        .filter_by(
            region_code=region_code.upper(),
            is_invalidated=False,
            normalized_file_name=path.file_name,
            ingest_database_name=ingest_database_name,
        )
        .all()
    )

    if len(results) != 1:
        raise ValueError(
            f"Unexpected number of metadata results for path {path.abs_path()}: [{len(results)}]"
        )

    return one(results)


def get_raw_file_metadata_row_for_path(
    session: Session,
    region_code: str,
    path: GcsfsFilePath,
) -> schema.DirectIngestRawFileMetadata:
    """Returns metadata information for the provided path, throws if it doesn't exist."""

    parts = filename_parts_from_path(path)

    if parts.file_type != GcsfsDirectIngestFileType.RAW_DATA:
        raise ValueError(f"Unexpected file type [{parts.file_type}]")
    results = (
        session.query(schema.DirectIngestRawFileMetadata)
        .filter_by(region_code=region_code.upper(), normalized_file_name=path.file_name)
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


def get_ingest_view_metadata_for_export_job(
    session: Session,
    region_code: str,
    file_tag: str,
    datetimes_contained_lower_bound_exclusive: Optional[datetime.datetime],
    datetimes_contained_upper_bound_inclusive: datetime.datetime,
    ingest_database_name: str,
) -> Optional[schema.DirectIngestIngestFileMetadata]:
    """Returns the ingest file metadata row corresponding to the export job with the provided args. Throws if such a
    row does not exist.
    """
    results = (
        session.query(schema.DirectIngestIngestFileMetadata)
        .filter_by(
            region_code=region_code.upper(),
            file_tag=file_tag,
            is_invalidated=False,
            is_file_split=False,
            datetimes_contained_lower_bound_exclusive=datetimes_contained_lower_bound_exclusive,
            datetimes_contained_upper_bound_inclusive=datetimes_contained_upper_bound_inclusive,
            ingest_database_name=ingest_database_name,
        )
        .all()
    )

    if not results:
        return None

    return one(results)


def get_ingest_view_metadata_for_most_recent_valid_job(
    session: Session,
    region_code: str,
    file_tag: str,
    ingest_database_name: str,
) -> Optional[schema.DirectIngestIngestFileMetadata]:
    """Returns most recently created export metadata row where is_invalidated is False, or None if there are no
    metadata rows for this file tag for this manager's region."""

    results = (
        session.query(schema.DirectIngestIngestFileMetadata)
        .filter_by(
            region_code=region_code.upper(),
            is_invalidated=False,
            is_file_split=False,
            file_tag=file_tag,
            ingest_database_name=ingest_database_name,
        )
        .order_by(schema.DirectIngestIngestFileMetadata.job_creation_time.desc())
        .limit(1)
        .all()
    )

    if not results:
        return None

    return one(results)


def get_ingest_view_metadata_pending_export(
    session: Session, region_code: str, ingest_database_name: str
) -> List[schema.DirectIngestIngestFileMetadata]:
    """Returns metadata for all ingest files have not yet been exported."""

    return (
        session.query(schema.DirectIngestIngestFileMetadata)
        .filter_by(
            region_code=region_code.upper(),
            is_invalidated=False,
            is_file_split=False,
            export_time=None,
            ingest_database_name=ingest_database_name,
        )
        .all()
    )


def get_metadata_for_raw_files_discovered_after_datetime(
    session: Session,
    region_code: str,
    raw_file_tag: str,
    discovery_time_lower_bound_exclusive: Optional[datetime.datetime],
) -> List[schema.DirectIngestRawFileMetadata]:
    """Returns metadata for all raw files with a given tag that have been updated after the provided date."""

    query = session.query(schema.DirectIngestRawFileMetadata).filter_by(
        region_code=region_code.upper(), file_tag=raw_file_tag
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


def get_date_sorted_unprocessed_ingest_view_files_for_region(
    session: Session,
    region_code: str,
    ingest_database_name: str,
) -> List[schema.DirectIngestIngestFileMetadata]:
    """Returns metadata for all ingest files that do not have a processed_time from earliest to latest"""
    return (
        session.query(schema.DirectIngestIngestFileMetadata)
        .filter_by(
            region_code=region_code.upper(),
            processed_time=None,
            ingest_database_name=ingest_database_name,
            is_invalidated=False,
        )
        .order_by(
            schema.DirectIngestIngestFileMetadata.datetimes_contained_upper_bound_inclusive.asc()
        )
        .all()
    )


def get_ingest_view_file_rows_count_for_region(
    session: Session, region_code: str, ingest_database_name: str, is_processed: bool
) -> int:
    """Counts all operations DB ingest view file metadata rows for the given ingest
    instance. If is_processed is True, returns the count of processed files. If it is
    False, returns the count of unprocessed files.
    """
    query = session.query(schema.DirectIngestIngestFileMetadata.file_id).filter_by(
        region_code=region_code.upper(),
        ingest_database_name=ingest_database_name,
        is_invalidated=False,
    )

    if is_processed:
        query = query.filter(
            schema.DirectIngestIngestFileMetadata.processed_time.isnot(None),
        )
    else:
        query = query.filter(
            schema.DirectIngestIngestFileMetadata.processed_time.is_(None),
        )

    return query.count()
