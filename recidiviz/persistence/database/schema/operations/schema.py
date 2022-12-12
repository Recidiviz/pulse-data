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
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeMeta, declarative_base
from sqlalchemy.sql.sqltypes import Enum

from recidiviz.common.constants.operations import enum_canonical_strings
from recidiviz.persistence.database.database_entity import DatabaseEntity

direct_ingest_instance = Enum(
    enum_canonical_strings.direct_ingest_instance_primary,
    enum_canonical_strings.direct_ingest_instance_secondary,
    name="direct_ingest_instance",
)

direct_ingest_status = Enum(
    enum_canonical_strings.direct_ingest_status_rerun_with_raw_data_import_started,
    enum_canonical_strings.direct_ingest_status_standard_rerun_started,
    enum_canonical_strings.direct_ingest_status_raw_data_import_in_progress,
    enum_canonical_strings.direct_ingest_status_ingest_view_materialization_in_progress,
    enum_canonical_strings.direct_ingest_status_extract_and_merge_in_progress,
    enum_canonical_strings.direct_ingest_status_ready_to_flash,
    enum_canonical_strings.direct_ingest_status_stale_raw_data,
    enum_canonical_strings.direct_ingest_status_up_to_date,
    enum_canonical_strings.direct_ingest_status_flash_in_progress,
    enum_canonical_strings.direct_ingest_status_flash_completed,
    enum_canonical_strings.direct_ingest_status_no_rerun_in_progress,
    enum_canonical_strings.direct_ingest_status_flash_canceled,
    enum_canonical_strings.direct_ingest_status_flash_cancellation_in_progress,
    name="direct_ingest_status",
)

# Defines the base class for all table classes in the shared operations schema.
OperationsBase: DeclarativeMeta = declarative_base(
    cls=DatabaseEntity, name="OperationsBase"
)


class DirectIngestInstanceStatus(OperationsBase):
    """Represents the status and various metadata about an ingest instance over time. Allows us to track the duration
    of reruns and the time spent on each part of ingest."""

    __tablename__ = "direct_ingest_instance_status"

    # The region code of a particular instance doing ingest.
    region_code = Column(String(255), nullable=False, index=True)

    # The timestamp of when the status of a particular instance changes.
    status_timestamp = Column(DateTime(timezone=True), nullable=False)

    # The particular instance doing ingest.
    instance = Column(direct_ingest_instance, nullable=False, index=True)

    # The status of a particular instance doing ingest.
    status = Column(direct_ingest_status, nullable=False)

    _table_args__ = PrimaryKeyConstraint(region_code, status_timestamp, instance)


class DirectIngestSftpRemoteFileMetadata(OperationsBase):
    """Represents the metadata known about a remote SFTP file that we download directly."""

    __tablename__ = "direct_ingest_sftp_remote_file_metadata"

    __table_args__ = (
        CheckConstraint(
            "discovery_time IS NOT NULL", name="nonnull_sftp_remote_file_discovery_time"
        ),
        CheckConstraint(
            "remote_file_path IS NOT NULL", name="nonnull_sftp_remote_file_path"
        ),
    )

    file_id = Column(Integer, primary_key=True)

    region_code = Column(String(255), nullable=False, index=True)

    # The remote file path on the SFTP server
    remote_file_path = Column(String(255), index=True)

    # Time when the file is actually discovered by the SFTP Airflow DAG
    discovery_time = Column(DateTime)

    # Time when the file is finished fully downloaded to the SFTP bucket
    download_time = Column(DateTime)


class DirectIngestSftpIngestReadyFileMetadata(OperationsBase):
    """Represents the metadata known about the ingest-ready file downloaded from SFTP.
    This file may be post-processed from the remote files we downloaded directly."""

    __tablename__ = "direct_ingest_sftp_ingest_ready_file_metadata"

    __table_args__ = (
        CheckConstraint(
            "discovery_time IS NOT NULL",
            name="nonnull_sftp_ingest_ready_discovery_time",
        ),
        CheckConstraint(
            "post_processed_normalized_file_path IS NOT NULL AND remote_file_path IS NOT NULL",
            name="nonnull post_processed_and_remote_paths",
        ),
    )

    file_id = Column(Integer, primary_key=True)

    region_code = Column(String(255), nullable=False, index=True)

    # The file path that is post-processed from the remote file path in the SFTP GCS Bucket.
    post_processed_normalized_file_path = Column(String(255), index=True)

    # The original remote_file_path that should match the remote_file_metadata table.
    remote_file_path = Column(String(255))

    # Time when the file is actually discovered by the SFTP Airflow DAG in the SFTP bucket.
    discovery_time = Column(DateTime)

    # Time when the file is finished fully uploaded to the ingest bucket
    upload_time = Column(DateTime)


class DirectIngestRawFileMetadata(OperationsBase):
    """Represents the metadata known about a raw data file that we processed through direct ingest."""

    __tablename__ = "direct_ingest_raw_file_metadata"

    __table_args__ = (
        UniqueConstraint(
            "region_code",
            "raw_data_instance",
            "normalized_file_name",
            name="one_normalized_name_per_region_and_instance",
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

    # The date we received the raw data. This is the field you should use when looking
    # for data current through date X. This is the date in the normalized file name
    # for this raw data file.
    datetimes_contained_upper_bound_inclusive = Column(DateTime, nullable=False)

    # The instance that this raw data was imported to.
    raw_data_instance = Column(direct_ingest_instance, nullable=False, index=True)

    # Whether or not this row is still valid.
    is_invalidated = Column(Boolean, nullable=False)


class DirectIngestViewMaterializationMetadata(OperationsBase):
    """Represents the metadata known about a job to materialize the results of an ingest
    view and save them for use later in ingest (as rows in a BQ table).
    """

    __tablename__ = "direct_ingest_view_materialization_metadata"

    __table_args__ = (
        CheckConstraint(
            "lower_bound_datetime_exclusive IS NULL OR "
            "lower_bound_datetime_exclusive < upper_bound_datetime_inclusive",
            name="datetime_bounds_ordering",
        ),
        CheckConstraint(
            "materialization_time IS NULL OR "
            "materialization_time >= job_creation_time",
            name="job_times_ordering",
        ),
        # Note: The tuple (region_code, instance, ingest_view_name,
        # upper_bound_datetime_inclusive, lower_bound_datetime_exclusive) acts as a
        # primary key for all rows where `is_invalidated` is False. This is enforced in
        # `recidiviz/persistence/database/schema/operations/session_listener.py`.
    )

    # Primary key for this row
    job_id = Column(Integer, primary_key=True)

    region_code = Column(String(255), nullable=False, index=True)

    # The ingest instance associated with this materialization job.
    instance = Column(direct_ingest_instance, nullable=False, index=True)

    # Shortened name for the ingest view file that corresponds to its ingest view / YAML
    # mappings definition.
    ingest_view_name = Column(String(255), nullable=False, index=True)

    # The upper bound date used to query data for these particular ingest view results.
    # The results will not contain any data we received after this date.
    upper_bound_datetime_inclusive = Column(DateTime, nullable=False)

    # The lower bound date used to query data for these particular ingest view results.
    # The results will not contain any rows that have remained unmodified with new raw
    # data updates we’ve gotten since this date.
    lower_bound_datetime_exclusive = Column(DateTime)

    # Time the materialization job is first scheduled for this view.
    job_creation_time = Column(DateTime, nullable=False)

    # Time the results of this view were materialized (i.e. written to BQ).
    materialization_time = Column(DateTime)

    # Whether or not this row is still valid (i.e. it applies to the current ingest
    # rerun).
    is_invalidated = Column(Boolean, nullable=False)
