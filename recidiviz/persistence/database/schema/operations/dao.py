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
import logging
from typing import List, Optional

import sqlalchemy
from more_itertools import one
from sqlalchemy import case, func

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.metadata.direct_ingest_file_metadata_manager import (
    DirectIngestRawFileMetadataSummary,
)
from recidiviz.ingest.direct.metadata.postgres_direct_ingest_instance_status_manager import (
    PostgresDirectIngestInstanceStatusManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session import Session

# TODO(#14198): move operations/dao.py functionality into DirectIngestRawFileMetadataManager and migrate tests
# to postgres_direct_ingest_file_metadata_manager_test.
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


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


def get_raw_file_metadata_for_file_id(
    session: Session,
    region_code: str,
    file_id: int,
    raw_data_instance: DirectIngestInstance,
) -> schema.DirectIngestRawFileMetadata:
    """Returns metadata information for the provided file id. Throws if it doesn't exist."""
    results = (
        session.query(schema.DirectIngestRawFileMetadata)
        .filter_by(
            region_code=region_code.upper(),
            file_id=file_id,
            raw_data_instance=raw_data_instance.value,
        )
        .all()
    )

    if len(results) != 1:
        raise ValueError(
            f"Unexpected number of metadata results for file_id={file_id}, region_code={region_code.upper()}: "
            f"[{len(results)}]"
        )

    return one(results)


def mark_raw_file_as_invalidated(
    session: Session,
    region_code: str,
    file_id: int,
    raw_data_instance: DirectIngestInstance,
) -> None:
    """Marks the specified row associated with the file_id as invalidated."""
    metadata = get_raw_file_metadata_for_file_id(
        session, region_code, file_id, raw_data_instance
    )
    metadata.is_invalidated = True


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
            schema.DirectIngestRawFileMetadata.file_discovery_time
            > discovery_time_lower_bound_exclusive
        )

    return query.all()


def get_all_raw_file_metadata_rows_for_region(
    session: Session,
    region_code: str,
    raw_data_instance: DirectIngestInstance,
) -> List[DirectIngestRawFileMetadataSummary]:
    """Returns all operations DB raw file metadata rows for the given region."""
    results = (
        session.query(
            schema.DirectIngestRawFileMetadata.file_tag.label("file_tag"),
            func.count(1)
            .filter(schema.DirectIngestRawFileMetadata.file_processed_time.isnot(None))
            .label("num_processed_files"),
            func.count(1)
            .filter(schema.DirectIngestRawFileMetadata.file_processed_time.is_(None))
            .label("num_unprocessed_files"),
            func.max(schema.DirectIngestRawFileMetadata.file_processed_time).label(
                "latest_processed_time"
            ),
            func.max(schema.DirectIngestRawFileMetadata.file_discovery_time).label(
                "latest_discovery_time"
            ),
            func.max(
                case(
                    [
                        (
                            schema.DirectIngestRawFileMetadata.file_processed_time.is_(
                                None
                            ),
                            None,
                        )
                    ],
                    else_=schema.DirectIngestRawFileMetadata.update_datetime,
                )
            ).label("latest_update_datetime"),
        )
        .filter_by(
            region_code=region_code.upper(),
            is_invalidated=False,
            raw_data_instance=raw_data_instance.value,
        )
        .group_by(schema.DirectIngestRawFileMetadata.file_tag)
        .all()
    )
    return [
        DirectIngestRawFileMetadataSummary(
            file_tag=result.file_tag,
            num_processed_files=result.num_processed_files,
            num_unprocessed_files=result.num_unprocessed_files,
            latest_processed_time=result.latest_processed_time,
            latest_discovery_time=result.latest_discovery_time,
            latest_update_datetime=result.latest_update_datetime
            if not isinstance(result.latest_update_datetime, str)
            else datetime.datetime.fromisoformat(result.latest_update_datetime),
        )
        for result in results
    ]


def stale_secondary_raw_data(
    region_code: str, is_ingest_in_dataflow_enabled: bool
) -> bool:
    """Returns whether there is stale raw data in secondary, as defined by there being non-invalidated instances of
    a given normalized_file_name that exists in PRIMARY after the timestamp of the start of a secondary rerun that
    does not exist in SECONDARY."""
    region_code_upper = region_code.upper()
    secondary_status_manager = PostgresDirectIngestInstanceStatusManager(
        region_code=region_code,
        ingest_instance=DirectIngestInstance.SECONDARY,
        is_ingest_in_dataflow_enabled=is_ingest_in_dataflow_enabled,
    )
    secondary_rerun_start_timestamp = (
        secondary_status_manager.get_current_ingest_rerun_start_timestamp()
    )

    # TODO(#20930): Delete this logic once ingest in dataflow has shipped - the concept
    #  of a raw data source instance only applies in the pre-ingest in dataflow world.
    if not is_ingest_in_dataflow_enabled:
        # It is not possible to have stale SECONDARY raw data if there isn't a SECONDARY raw data import rerun in progress
        # or if a start of a rerun was not found in SECONDARY
        secondary_raw_data_source = (
            secondary_status_manager.get_raw_data_source_instance()
        )
        if secondary_raw_data_source != DirectIngestInstance.SECONDARY:
            logging.info(
                "[stale_secondary_raw_data] Secondary raw data source is not SECONDARY: %s. Raw data in secondary cannot "
                "be stale. Returning.",
                secondary_raw_data_source,
            )
            return False

    if not secondary_rerun_start_timestamp:
        logging.info(
            "[stale_secondary_raw_data] Could not locate the start of a secondary rerun. "
            "Raw data in secondary cannot be stale. Returning."
        )
        return False

    database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
    with SessionFactory.using_database(database_key) as session:
        logging.info(
            "[stale_secondary_raw_data] Executing query of direct_ingest_raw_file_metadata to determine if"
            "there are stale raw files in SECONDARY. Secondary rerun start timestamp is: %s",
            secondary_rerun_start_timestamp,
        )
        query = f"""
        WITH primary_raw_data AS (
            SELECT * 
            FROM direct_ingest_raw_file_metadata 
            WHERE raw_data_instance = 'PRIMARY' 
            AND region_code = '{region_code_upper}'
            AND is_invalidated IS False
        ), 
        secondary_raw_data AS (
            SELECT * 
            FROM direct_ingest_raw_file_metadata 
            WHERE raw_data_instance = 'SECONDARY' 
            AND region_code = '{region_code_upper}'
            AND is_invalidated IS False
        ) 
        SELECT COUNT(*)
        FROM primary_raw_data
        LEFT OUTER JOIN secondary_raw_data 
        ON primary_raw_data.normalized_file_name=secondary_raw_data.normalized_file_name  
        WHERE secondary_raw_data.normalized_file_name IS NULL 
        AND primary_raw_data.file_discovery_time > '{secondary_rerun_start_timestamp}';
        """
        results = session.execute(sqlalchemy.text(query))
        count = one(results)[0]
        is_stale = count > 0
        if is_stale:
            logging.info(
                "[%s][stale_secondary_raw_data] Found non-invalidated raw files in PRIMARY that were discovered "
                "after=[%s], indicating stale raw data in SECONDARY.",
                region_code,
                secondary_rerun_start_timestamp,
            )
        return is_stale
