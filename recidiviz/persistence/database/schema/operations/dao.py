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
from typing import Union, Optional, List

from more_itertools import one

from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import GcsfsDirectIngestFileType, \
    filename_parts_from_path
from recidiviz.ingest.direct.controllers.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.session import Session


def get_file_metadata_row(
        session: Session,
        file_type: GcsfsDirectIngestFileType,
        file_id: int
) -> Union[schema.DirectIngestRawFileMetadata, schema.DirectIngestIngestFileMetadata]:
    """Queries for the file metadata row by the metadata row primary key."""

    if file_type == GcsfsDirectIngestFileType.INGEST_VIEW:
        results = session.query(schema.DirectIngestIngestFileMetadata).filter_by(file_id=file_id).all()
    elif file_type == GcsfsDirectIngestFileType.RAW_DATA:
        results = session.query(schema.DirectIngestRawFileMetadata).filter_by(file_id=file_id).all()
    else:
        raise ValueError(f'Unexpected path type: {file_type}')

    return one(results)


def get_file_metadata_row_for_path(
        session: Session,
        region_code: str,
        path: GcsfsFilePath
) -> Union[schema.DirectIngestRawFileMetadata, schema.DirectIngestIngestFileMetadata]:
    """Returns metadata information for the provided path. If the file has not yet been registered in the
    appropriate metadata table, this function will generate a file_id to return with the metadata.
    """

    parts = filename_parts_from_path(path)

    if parts.file_type == GcsfsDirectIngestFileType.INGEST_VIEW:
        results = session.query(schema.DirectIngestIngestFileMetadata).filter_by(
            region_code=region_code,
            is_invalidated=False,
            normalized_file_name=path.file_name
        ).all()
    elif parts.file_type == GcsfsDirectIngestFileType.RAW_DATA:
        results = session.query(schema.DirectIngestRawFileMetadata).filter_by(
            region_code=region_code,
            normalized_file_name=path.file_name
        ).all()
    else:
        raise ValueError(f'Unexpected path type: {parts.file_type}')

    if len(results) != 1:
        raise ValueError(f'Unexpected number of metadata results for path {path.abs_path()}: [{len(results)}]')

    return one(results)


def get_ingest_view_metadata_for_export_job(
        session: Session,
        region_code: str,
        file_tag: str,
        datetimes_contained_lower_bound_exclusive: Optional[datetime.datetime],
        datetimes_contained_upper_bound_inclusive: datetime.datetime
) -> Optional[schema.DirectIngestIngestFileMetadata]:
    """Returns the ingest file metadata row corresponding to the export job with the provided args. Throws if such a
    row does not exist.
    """
    results = session.query(schema.DirectIngestIngestFileMetadata).filter_by(
        region_code=region_code,
        file_tag=file_tag,
        is_invalidated=False,
        is_file_split=False,
        datetimes_contained_lower_bound_exclusive=datetimes_contained_lower_bound_exclusive,
        datetimes_contained_upper_bound_inclusive=datetimes_contained_upper_bound_inclusive
    ).all()

    if not results:
        return None

    return one(results)


def get_ingest_view_metadata_for_most_recent_valid_job(
        session: Session,
        region_code: str,
        file_tag: str
) -> Optional[schema.DirectIngestIngestFileMetadata]:
    """Returns most recently created export metadata row where is_invalidated is False, or None if there are no
    metadata rows for this file tag for this manager's region."""

    results = session.query(schema.DirectIngestIngestFileMetadata).filter_by(
        region_code=region_code,
        is_invalidated=False,
        is_file_split=False,
        file_tag=file_tag
    ).order_by(schema.DirectIngestIngestFileMetadata.job_creation_time.desc()).limit(1).all()

    if not results:
        return None

    return one(results)


def get_ingest_view_metadata_pending_export(
        session: Session,
        region_code: str
) -> List[schema.DirectIngestIngestFileMetadata]:
    """Returns metadata for all ingest files have not yet been exported."""

    return session.query(schema.DirectIngestIngestFileMetadata).filter_by(
        region_code=region_code,
        is_invalidated=False,
        is_file_split=False,
        export_time=None
    ).all()


def get_metadata_for_raw_files_discovered_after_datetime(
        session: Session,
        region_code: str,
        raw_file_tag: str,
        discovery_time_lower_bound_exclusive: Optional[datetime.datetime]
) -> List[schema.DirectIngestRawFileMetadata]:
    """Returns metadata for all raw files with a given tag that have been updated after the provided date."""

    query = session.query(schema.DirectIngestRawFileMetadata).filter_by(region_code=region_code,
                                                                        file_tag=raw_file_tag)
    if discovery_time_lower_bound_exclusive:
        query = query.filter(schema.DirectIngestRawFileMetadata.discovery_time > discovery_time_lower_bound_exclusive)

    return query.all()
