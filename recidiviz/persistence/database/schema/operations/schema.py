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

When making updates to this schema, please ensure to generate a corresponding database migration using:

python -m recidiviz.tools.migrations.autogenerate_migration --database OPERATIONS --message add_field_foo
"""
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeMeta, declarative_base, relationship
from sqlalchemy.sql.sqltypes import Enum

from recidiviz.common.constants.operations import enum_canonical_strings
from recidiviz.persistence.database.database_entity import DatabaseEntity

direct_ingest_instance = Enum(
    enum_canonical_strings.direct_ingest_instance_primary,
    enum_canonical_strings.direct_ingest_instance_secondary,
    name="direct_ingest_instance",
)

direct_ingest_lock_actor = Enum(
    enum_canonical_strings.direct_ingest_lock_actor_adhoc,
    enum_canonical_strings.direct_ingest_lock_actor_process,
    name="direct_ingest_lock_actor",
)

direct_ingest_lock_resource = Enum(
    enum_canonical_strings.direct_ingest_lock_resource_bucket,
    enum_canonical_strings.direct_ingest_lock_resource_operations_database,
    enum_canonical_strings.direct_ingest_lock_resource_big_query_raw_data_dataset,
    name="direct_ingest_lock_resource",
)

direct_ingest_file_import_status = Enum(
    enum_canonical_strings.direct_ingest_raw_file_import_status_started,
    enum_canonical_strings.direct_ingest_raw_file_import_status_succeeded,
    enum_canonical_strings.direct_ingest_raw_file_import_status_deferred,
    enum_canonical_strings.direct_ingest_raw_file_import_status_failed_unknown,
    enum_canonical_strings.direct_ingest_raw_file_import_status_failed_load_step,
    enum_canonical_strings.direct_ingest_raw_file_import_status_failed_pre_import_normalization_step,
    enum_canonical_strings.direct_ingest_raw_file_import_status_failed_validation_step,
    enum_canonical_strings.direct_ingest_raw_file_import_status_failed_import_blocked,
    name="direct_ingest_file_import_status",
)


# Defines the base class for all table classes in the shared operations schema.
OperationsBase: DeclarativeMeta = declarative_base(
    cls=DatabaseEntity, name="OperationsBase"
)


class DirectIngestSftpRemoteFileMetadata(OperationsBase):
    """Represents the metadata known about a remote SFTP file that we download directly."""

    __tablename__ = "direct_ingest_sftp_remote_file_metadata"

    __table_args__ = (
        CheckConstraint(
            "file_discovery_time IS NOT NULL",
            name="nonnull_sftp_remote_file_discovery_time",
        ),
        CheckConstraint(
            "remote_file_path IS NOT NULL", name="nonnull_sftp_remote_file_path"
        ),
        CheckConstraint("sftp_timestamp IS NOT NULL", name="nonnull_sftp_timestamp"),
        UniqueConstraint(
            "region_code",
            "remote_file_path",
            "sftp_timestamp",
            name="remote_file_path_sftp_timestamps_unique_within_state",
        ),
    )

    file_id = Column(Integer, primary_key=True)

    region_code = Column(String(255), nullable=False, index=True)

    # The remote file path on the SFTP server
    remote_file_path = Column(String(255), index=True)

    # The original SFTP mtime (UNIX seconds since epoch) of the remote_file_path on the SFTP server
    sftp_timestamp = Column(Float)

    # Time when the file is actually discovered by the SFTP Airflow DAG
    file_discovery_time = Column(DateTime(timezone=True))

    # Time when the file is finished fully downloaded to the SFTP bucket
    file_download_time = Column(DateTime(timezone=True))


class DirectIngestSftpIngestReadyFileMetadata(OperationsBase):
    """Represents the metadata known about the ingest-ready file downloaded from SFTP.
    This file may be post-processed from the remote files we downloaded directly."""

    __tablename__ = "direct_ingest_sftp_ingest_ready_file_metadata"

    __table_args__ = (
        CheckConstraint(
            "file_discovery_time IS NOT NULL",
            name="nonnull_sftp_ingest_ready_file_discovery_time",
        ),
        CheckConstraint(
            "post_processed_normalized_file_path IS NOT NULL AND remote_file_path IS NOT NULL",
            name="nonnull post_processed_and_remote_paths",
        ),
        # This is sufficient because the SFTP timestamp is encoded as the first part of the
        # post_processed_normalized_file_path.
        UniqueConstraint(
            "region_code",
            "post_processed_normalized_file_path",
            "remote_file_path",
            name="post_processed_remote_file_paths_unique_within_state",
        ),
    )

    file_id = Column(Integer, primary_key=True)

    region_code = Column(String(255), nullable=False, index=True)

    # The file path that is post-processed from the remote file path in the SFTP GCS Bucket.
    post_processed_normalized_file_path = Column(String(255), index=True)

    # The original remote_file_path that should match the remote_file_metadata table.
    remote_file_path = Column(String(255))

    # Time when the file is actually discovered by the SFTP Airflow DAG in the SFTP bucket.
    file_discovery_time = Column(DateTime(timezone=True))

    # Time when the file is finished fully uploaded to the ingest bucket
    file_upload_time = Column(DateTime(timezone=True))


class DirectIngestDataflowJob(OperationsBase):
    """A record of the ingest jobs that have completed."""

    __tablename__ = "direct_ingest_dataflow_job"

    # The dataflow generated ID of the ingest job.
    # Of the form 'YYYY-MM-DD_HH_MM_SS-NNNNNNNNNNNNNNNNNNN', e.g. '2023-10-02_07_03_19-9234234904060676794'
    job_id = Column(String(255), primary_key=True)

    # The state code that we are ingesting for
    region_code = Column(String(255), nullable=False, index=True)

    # The ingest instance that we are running for
    ingest_instance = Column(direct_ingest_instance, nullable=False, index=True)

    # The location that the job ran in
    location = Column(String(255), nullable=True, index=True)

    # When ingest in Dataflow Airflow DAG saw the job completed and wrote this row
    completion_time = Column(DateTime, nullable=False)

    # This is used when swapping or removing raw data to allow the next watermarks to
    # be earlier than the current watermarks.
    is_invalidated = Column(Boolean, nullable=False)

    watermarks = relationship(
        "DirectIngestDataflowRawTableUpperBounds", backref="job", lazy="selectin"
    )


class DirectIngestDataflowRawTableUpperBounds(OperationsBase):
    """Stores the highest watermark (highest date for which there is complete raw data)
    for each raw data table, to ensure that ingest results are not accidentally
    incorporating partial raw data results."""

    __tablename__ = "direct_ingest_dataflow_raw_table_upper_bounds"

    __table_args__ = (
        UniqueConstraint(
            "job_id",
            "raw_data_file_tag",
            name="file_tags_unique_within_pipeline",
        ),
    )

    # Automatically generated by Postgres as the primary key
    watermark_id = Column(Integer, primary_key=True)

    # The state code that we are ingesting for
    region_code = Column(String(255), nullable=False, index=True)

    # The dataflow generated ID of the ingest pipeline that is reading this raw data.
    # Of the form 'YYYY-MM-DD_HH_MM_SS-NNNNNNNNNNNNNNNNNNN', e.g. '2023-10-02_07_03_19-9234234904060676794'
    job_id = Column(
        String(255),
        ForeignKey(
            "direct_ingest_dataflow_job.job_id",
            deferrable=True,
            initially="DEFERRED",
            name="direct_ingest_dataflow_raw_table_upper_bounds_job_id_fkey",
        ),
        nullable=False,
        index=True,
    )

    # The raw data table that we are looking at for this watermark
    raw_data_file_tag = Column(String(255), nullable=False, index=True)

    # The latest update_datetime of the raw data table
    watermark_datetime = Column(DateTime(timezone=True), nullable=False)


class DirectIngestRawDataResourceLock(OperationsBase):
    """A record of direct ingest raw data resource locks over time.

    n.b. while this table is defined here, it is also queried in the airflow context with
    raw sql, so any updates the table schema must also be propagated to the raw data
    import dag's sql query
    """

    __tablename__ = "direct_ingest_raw_data_resource_lock"

    lock_id = Column(Integer, primary_key=True)

    # The actor who is acquiring the lock
    lock_actor = Column(direct_ingest_lock_actor, nullable=False)

    # the resource that this lock is "locking"
    lock_resource = Column(direct_ingest_lock_resource, nullable=False, index=True)

    # The upper case region code associated with the raw data import
    region_code = Column(String(255), nullable=False)

    raw_data_source_instance = Column(direct_ingest_instance, nullable=False)

    # Whether or not this lock has been released (defaults to False)
    released = Column(Boolean, nullable=False)

    # The time this lock was acquired
    lock_acquisition_time = Column(DateTime(timezone=True))

    # The TTL for this lock in seconds. consider switching this to pg Interval which
    # sqlalchemy converts to datetime.timedelta object if bq federated queries support
    # the pg interval type in the future
    lock_ttl_seconds = Column(Integer)

    # Description for why the lock was acquired
    lock_description = Column(String(255), nullable=False)

    __table_args__ = (
        CheckConstraint(
            "lock_actor = 'ADHOC' OR (lock_actor = 'PROCESS' and lock_ttl_seconds IS NOT NULL)",
            name="all_process_actors_must_specify_ttl_seconds",
        ),
        Index(
            "at_most_one_active_lock_per_resource_region_and_instance",
            "lock_resource",
            "region_code",
            "raw_data_source_instance",
            unique=True,
            postgresql_where=(~released),
        ),
    )


class DirectIngestRawBigQueryFileMetadata(OperationsBase):
    """Metadata known about a "conceptual" file_id that exists in BigQuery.

    n.b. while this table is defined here, it is also queried in the airflow context with
    raw sql, so any updates the table schema must also be propagated to the raw data
    import dag's sql query
    """

    __tablename__ = "direct_ingest_raw_big_query_file_metadata"

    # "Conceptual" file id that corresponds to a single, conceptual file sent to us by
    # the state. For raw files states send us in chunks (such as ContactNoteComment),
    # each literal CSV that makes up the whole file will have a different gcs_file_id,
    # but all of those entries will have the same file_id.
    file_id = Column(Integer, primary_key=True)

    # The upper case region code associated with the raw data import
    region_code = Column(String(255), nullable=False, index=True)

    # The instance that this raw data was/will be imported to.
    raw_data_instance = Column(direct_ingest_instance, nullable=False, index=True)

    # Shortened name for the raw file that corresponds to its YAML schema definition
    file_tag = Column(String(255), nullable=False, index=True)

    # The date we received the raw data. This is the field you should use when looking
    # for data current through date X. This is the latest date of the normalized file
    # names associated with this file_id
    update_datetime = Column(DateTime(timezone=True), nullable=False)

    # Whether or not this row is still valid.
    # n.b.: when joining against direct_ingest_raw_gcs_file_metadata, be mindful
    # of `IS FALSE` vs `IS NOT TRUE` boolean logic as they can evaluate differently for
    # ungrouped direct_ingest_raw_gcs_file_metadata rows that have a null file_id
    is_invalidated = Column(Boolean, nullable=False)

    # Time when all parts of this conceptual file finished uploading to BigQuery
    file_processed_time = Column(DateTime(timezone=True), nullable=True)

    # References to the literal CSV files associated with this "conceptual" file
    gcs_files = relationship("DirectIngestRawGCSFileMetadata", back_populates="bq_file")

    # File imports associated with this bq_file
    file_imports = relationship("DirectIngestRawFileImport", back_populates="bq_file")


class DirectIngestRawGCSFileMetadata(OperationsBase):
    """Metadata known about a raw data csv file that exists in Google Cloud Storage.

    n.b. while this table is defined here, it is also queried in the airflow context with
    raw sql, so any updates the table schema must also be propagated to the raw data
    import dag's sql query
    """

    __tablename__ = "direct_ingest_raw_gcs_file_metadata"

    # An id that corresponds to the literal file in Google Cloud Storage. a single file
    # will always have a single gcs_file_id.
    gcs_file_id = Column(Integer, primary_key=True)

    # "Conceptual" file id that corresponds to a single, conceptual file sent to us by
    # the state. For raw files states send us in chunks (such as ContactNoteComment),
    # each literal CSV that makes up the whole file will have a different gcs_file_id,
    # but all of those entries will have the same file_id.
    # If file_id is null, that likely means that while this CSV file has been discovered,
    # we might still be waiting for the other chunks of the "conceptual" file to arrive
    # to create a single, conceptual DirectIngestRawBigQueryFileMetadata.
    file_id = Column(
        Integer,
        ForeignKey(
            "direct_ingest_raw_big_query_file_metadata.file_id",
            deferrable=True,
            initially="DEFERRED",
            name="direct_ingest_raw_big_query_file_metadata_file_id_fkey",
        ),
        nullable=True,
        index=True,
    )

    bq_file = relationship(
        "DirectIngestRawBigQueryFileMetadata", back_populates="gcs_files"
    )

    # The upper case region code associated with the raw data import
    region_code = Column(String(255), nullable=False, index=True)

    # The instance of the bucket that this raw data file was discovered in.
    raw_data_instance = Column(direct_ingest_instance, nullable=False, index=True)

    # Whether or not this row is still valid. if this row has a non-null file_id,
    # this value should always match the value in direct_ingest_raw_big_query_file_metadata.
    # n.b.: when joining against direct_ingest_raw_big_query_file_metadata, be mindful
    # of `IS FALSE` vs `IS NOT TRUE` boolean logic as they can evaluate differently for
    # ungrouped files that have a null file_id
    is_invalidated = Column(Boolean, nullable=False)

    # Shortened name for the raw file that corresponds to its YAML schema definition
    file_tag = Column(String(255), nullable=False, index=True)

    # Unprocessed normalized file name for this file, set at time of file discovery.
    normalized_file_name = Column(String(255), index=True, nullable=False)

    # Time that this file was uploaded into our ingest bucket. This is the date in the
    # normalized file name.
    update_datetime = Column(DateTime(timezone=True), nullable=False)

    # Time when the file is actually discovered by the raw data DAG
    file_discovery_time = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index(
            "one_non_invalidated_normalized_file_name_per_region_instance",
            "region_code",
            "raw_data_instance",
            "normalized_file_name",
            unique=True,
            postgresql_where=(~is_invalidated),
        ),
    )


class DirectIngestRawFileImportRun(OperationsBase):
    """Each row corresponds to a state-and-ingest-specific run of the raw data import
    DAG

    n.b. while this table is defined here, it is also queried in the airflow context with
    raw sql, so any updates the table schema must also be propagated to the raw data
    import dag's sql query
    """

    __tablename__ = "direct_ingest_raw_file_import_run"

    import_run_id = Column(Integer, primary_key=True)

    # The id of the raw data import run which should match DagRun::run_id in the airflow
    # metadata database
    dag_run_id = Column(String(255), nullable=True, index=True)

    # Time when the import run started
    import_run_start = Column(DateTime(timezone=True), nullable=False)

    # Time when the import run ended
    import_run_end = Column(DateTime(timezone=True))

    # The upper case region code associated with the raw data import run
    region_code = Column(String(255), nullable=False, index=True)

    # The raw data instance associated with the raw data import run
    raw_data_instance = Column(direct_ingest_instance, nullable=False, index=True)

    # The imports associated with this import run
    file_imports = relationship(
        "DirectIngestRawFileImport", back_populates="import_run"
    )


class DirectIngestRawFileImport(OperationsBase):
    """Each rows corresponds to an attempt to import an entry in the direct_ingest_raw_big_query_file_metadata
    table into BigQuery. This table has DAG-run-level information about a raw file
    import, which will change over the lifecycle of an import.

    n.b. while this table is defined here, it sis also queried in the airflow context with
    raw sql, so any updates the table schema must also be propagated to the raw data
    import dag's sql query
    """

    __tablename__ = "direct_ingest_raw_file_import"

    file_import_id = Column(Integer, primary_key=True)

    # The file_id from the direct_ingest_raw_big_query_file_metadata table
    file_id = Column(
        Integer,
        ForeignKey(
            "direct_ingest_raw_big_query_file_metadata.file_id",
            deferrable=True,
            initially="DEFERRED",
            name="direct_ingest_raw_big_query_file_metadata_file_id_fkey",
        ),
        nullable=False,
        index=True,
    )

    bq_file = relationship(
        "DirectIngestRawBigQueryFileMetadata", back_populates="file_imports"
    )

    # The import_run_id from the direct_ingest_raw_file_import_run associated with this
    # import
    import_run_id = Column(
        Integer,
        ForeignKey(
            "direct_ingest_raw_file_import_run.import_run_id",
            deferrable=True,
            initially="DEFERRED",
            name="direct_ingest_raw_file_import_run_import_run_id_fkey",
        ),
        nullable=False,
        index=True,
    )

    import_run = relationship(
        "DirectIngestRawFileImportRun", back_populates="file_imports"
    )

    # Status of the raw file import
    import_status = Column(direct_ingest_file_import_status, nullable=False)

    # The upper case region code associated with the raw data import
    region_code = Column(String(255), nullable=False, index=True)

    # The raw data instance associated with the raw data import
    raw_data_instance = Column(direct_ingest_instance, nullable=False, index=True)

    # Whether or not historical diffs were performed during the import of this file (or
    # would have been had the import successfully made it to that stage)
    historical_diffs_active = Column(Boolean, nullable=True)

    # The number of rows included in the raw data file. If historical_diffs_active,
    # this number will not be equal to the number of rows added to the raw data table.
    raw_rows = Column(Integer, nullable=True)

    # Number of net new or updated rows added during the diffing process)
    net_new_or_updated_rows = Column(Integer, nullable=True)

    # Number of rows added with is_deleted as True during the diffing process
    deleted_rows = Column(Integer, nullable=True)

    # Error message, typically a stack trace, associated with an import. Will likely
    # be non-null if import_status is fail-y
    error_message = Column(Text, nullable=True)

    __table_args__ = (
        CheckConstraint(
            "import_status != 'SUCCEEDED' OR (import_status = 'SUCCEEDED' AND raw_rows IS NOT NULL)",
            name="all_succeeded_imports_must_have_non_null_rows",
        ),
        CheckConstraint(
            "(historical_diffs_active IS FALSE OR import_status != 'SUCCEEDED')"
            "OR (historical_diffs_active IS TRUE AND import_status = 'SUCCEEDED' AND net_new_or_updated_rows IS NOT NULL AND deleted_rows IS NOT NULL)",
            name="historical_diffs_must_have_non_null_updated_and_deleted",
        ),
    )


class DirectIngestRawDataFlashStatus(OperationsBase):

    __tablename__ = "direct_ingest_raw_data_flash_status"

    # The upper case region code associated with the raw data import
    region_code = Column(String(255), nullable=False, index=True)

    # The timestamp of when this status row was created
    status_timestamp = Column(DateTime(timezone=True), nullable=False)

    # Whether or not this status row indicates that flashing is in progress
    flashing_in_progress = Column(Boolean, nullable=False)

    __table_args__ = (PrimaryKeyConstraint(region_code, status_timestamp),)


class DirectIngestRawDataPruningMetadata(OperationsBase):
    """Metadata about the information used to run automatic raw data pruning for a given file_tag in a given state and instance."""

    __tablename__ = "direct_ingest_raw_data_pruning_metadata"

    __table_args__ = (
        PrimaryKeyConstraint(
            "region_code",
            "raw_data_instance",
            "file_tag",
        ),
    )

    # The upper case region code associated with the raw data import
    region_code = Column(String(255), nullable=False, index=True)

    # The raw data instance associated with the raw data import
    raw_data_instance = Column(direct_ingest_instance, nullable=False, index=True)

    # Shortened name for the raw file that corresponds to its YAML schema definition
    file_tag = Column(String(255), nullable=False, index=True)

    # Timestamp of when this pruning metadata row was last updated
    updated_at = Column(DateTime(timezone=True), nullable=False)

    # Whether or not automatic pruning is enabled for this raw data table
    automatic_pruning_enabled = Column(Boolean, nullable=False)

    # Comma-separated list of primary key columns for this raw data table
    # in alphabetical order
    raw_file_primary_keys = Column(String(255), nullable=False, index=True)

    # True iff the export lookback window for a raw file is FULL_HISTORICAL_LOOKBACK
    # If False, we assume this file has an incremental lookback window
    raw_files_contain_full_historical_lookback = Column(Boolean, nullable=False)
