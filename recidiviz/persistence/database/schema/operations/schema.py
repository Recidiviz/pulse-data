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
"""Define the ORM schema objects that map directly to the database for Recidiviz internal operations data.

The below schema uses only generic SQLAlchemy types, and therefore should be
portable between database implementations.
"""

from sqlalchemy import (
    Column,
    String,
    Integer,
    DateTime,
    Boolean,
    CheckConstraint,
    UniqueConstraint,
)

from recidiviz.persistence.database.base_schema import OperationsBase


class _DirectIngestFileMetadataRowSharedColumns:
    """A mixin which defines all columns common to each of the direct ingest file metadata columns."""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _DirectIngestFileMetadataRowSharedColumns:
            raise Exception(
                "_DirectIngestFileMetadataRowSharedColumns cannot be instantiated"
            )
        return super().__new__(cls)

    file_id = Column(Integer, primary_key=True)

    region_code = Column(String(255), nullable=False, index=True)
    file_tag = Column(String(255), nullable=False, index=True)

    # Unprocessed normalized file name for this file, either set at time of file discovery (raw files) or before export
    # (ingest view files).
    normalized_file_name = Column(String(255), index=True)

    # Time when the file is actually discovered by our controller's handle_new_files endpoint
    discovery_time = Column(DateTime)

    # Time when we have finished fully processing this file (either uploading to BQ or importing to Postgre)
    processed_time = Column(DateTime)


class DirectIngestRawFileMetadata(
    OperationsBase, _DirectIngestFileMetadataRowSharedColumns
):
    __tablename__ = "direct_ingest_raw_file_metadata"

    __table_args__ = (
        UniqueConstraint(
            "region_code", "normalized_file_name", name="one_normalized_name_per_region"
        ),
        CheckConstraint(
            "discovery_time IS NOT NULL", name="nonnull_raw_file_discovery_time"
        ),
        CheckConstraint(
            "normalized_file_name IS NOT NULL", name="nonnull_raw_normalized_file_name"
        ),
    )

    datetimes_contained_upper_bound_inclusive = Column(DateTime, nullable=False)


class DirectIngestIngestFileMetadata(
    OperationsBase, _DirectIngestFileMetadataRowSharedColumns
):
    """Represents the metadata known about a file that we processed through direct ingest."""

    __tablename__ = "direct_ingest_ingest_file_metadata"

    __table_args__ = (
        CheckConstraint(
            "export_time IS NULL OR normalized_file_name IS NOT NULL",
            name="export_after_normalized_file_name_set",
        ),
        CheckConstraint(
            "discovery_time IS NULL OR export_time IS NOT NULL",
            name="discovery_after_export",
        ),
        CheckConstraint(
            "processed_time IS NULL OR discovery_time IS NOT NULL",
            name="processed_after_discovery",
        ),
        CheckConstraint(
            "datetimes_contained_lower_bound_exclusive IS NULL OR "
            "datetimes_contained_lower_bound_exclusive < datetimes_contained_upper_bound_inclusive",
            name="datetimes_contained_ordering",
        ),
        CheckConstraint(
            "NOT is_file_split OR normalized_file_name IS NOT NULL",
            name="split_files_created_with_file_name",
        ),
    )

    # These fields are first set at export job creation time
    is_invalidated = Column(Boolean, nullable=False)

    # If true, indicates that this file is a split of an original ingest view export. If false, this file was exported
    # directly from BigQuery.
    is_file_split = Column(Boolean, nullable=False)

    # Time the export job is first scheduled for these time bounds
    job_creation_time = Column(DateTime, nullable=False)
    datetimes_contained_lower_bound_exclusive = Column(DateTime)
    datetimes_contained_upper_bound_inclusive = Column(DateTime, nullable=False)

    # Time of the actual view export (when the file is done writing to GCS), set at same time as normalized_file_name
    export_time = Column(DateTime)
