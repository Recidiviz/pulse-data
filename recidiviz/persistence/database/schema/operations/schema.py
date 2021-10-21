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
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.sql.sqltypes import Enum

from recidiviz.common.constants.operations import enum_canonical_strings
from recidiviz.persistence.database.base_schema import OperationsBase

direct_ingest_instance = Enum(
    enum_canonical_strings.direct_ingest_instance_primary,
    enum_canonical_strings.direct_ingest_instance_secondary,
    name="direct_ingest_instance",
)


class DirectIngestSftpFileMetadata(OperationsBase):
    """Represents the metadata known about a file that we processed from SFTP."""

    __tablename__ = "direct_ingest_sftp_file_metadata"

    __table_args__ = (
        UniqueConstraint(
            "region_code",
            "remote_file_path",
            name="one_remote_sftp_name_per_region",
        ),
        CheckConstraint(
            "discovery_time IS NOT NULL", name="nonnull_sftp_file_discovery_time"
        ),
        CheckConstraint(
            "remote_file_path IS NOT NULL", name="nonnull_sftp_remote_file_name"
        ),
        CheckConstraint(
            "(processed_time IS NULL) OR (discovery_time <= processed_time)",
            name="discovery_post_processed_time",
        ),
    )

    file_id = Column(Integer, primary_key=True)

    region_code = Column(String(255), nullable=False, index=True)

    # The remote file path on the SFTP server
    remote_file_path = Column(String(255), index=True)

    # Time when the file is actually discovered by the SFTP download controller
    discovery_time = Column(DateTime)

    # Time when we have finished fully processing this file by downloading to the SFTP bucket
    processed_time = Column(DateTime)


class DirectIngestRawFileMetadata(OperationsBase):
    """Represents the metadata known about a raw data file that we processed through direct ingest."""

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

    file_id = Column(Integer, primary_key=True)

    region_code = Column(String(255), nullable=False, index=True)

    # Shortened name for the raw file that corresponds to its YAML schema definition
    file_tag = Column(String(255), nullable=False, index=True)

    # Unprocessed normalized file name for this file, set at time of file discovery.
    normalized_file_name = Column(String(255), index=True)

    # Time when the file is actually discovered by our controller's handle_new_files endpoint.
    discovery_time = Column(DateTime)

    # Time when we have finished fully processing this file by uploading to BQ.
    processed_time = Column(DateTime)

    datetimes_contained_upper_bound_inclusive = Column(DateTime, nullable=False)


class DirectIngestIngestFileMetadata(OperationsBase):
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

    file_id = Column(Integer, primary_key=True)

    region_code = Column(String(255), nullable=False, index=True)

    # Shortened name for the raw file that corresponds to its YAML schema definition
    file_tag = Column(String(255), nullable=False, index=True)

    # Unprocessed normalized file name for this file before export
    normalized_file_name = Column(String(255), index=True)

    # Time when the file is actually discovered by our controller's handle_new_files endpoint.
    discovery_time = Column(DateTime)

    # Time when we have finished fully processing this file importing to Postgres.
    processed_time = Column(DateTime)

    # These fields are first set at export job creation time
    is_invalidated = Column(Boolean, nullable=False)

    # If true, indicates that this file is a split of an original ingest view export. If
    # false, this file was exported directly from BigQuery.
    is_file_split = Column(Boolean, nullable=False)

    # Time the export job is first scheduled for these time bounds
    job_creation_time = Column(DateTime, nullable=False)
    datetimes_contained_lower_bound_exclusive = Column(DateTime)
    datetimes_contained_upper_bound_inclusive = Column(DateTime, nullable=False)

    # Time of the actual view export (when the file is done writing to GCS), set at same
    # time as normalized_file_name
    export_time = Column(DateTime)

    # The name of the database that the data in this file has been or will be written
    # to.
    ingest_database_name = Column(String, nullable=False)


class DirectIngestInstanceStatus(OperationsBase):
    """This type is used to indicate the current operating status of a given state/instance
    pair. When `is_paused` is true, our ingest processes will all skip operations for the
    given state/instance pair.

    This will allow us to dynamically pause and resume ingest on the fly."""

    __tablename__ = "direct_ingest_instance_status"

    __table_args__ = (
        UniqueConstraint(
            "region_code", "instance", name="single_row_per_ingest_instance"
        ),
    )

    region_code = Column(String(255), nullable=False, index=True, primary_key=True)
    instance = Column(
        direct_ingest_instance, nullable=False, index=True, primary_key=True
    )

    is_paused = Column(Boolean, nullable=False, default=True)
