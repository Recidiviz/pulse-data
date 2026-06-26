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
    ForeignKeyConstraint,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
    text,
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

ingest_pipeline_type = Enum(
    enum_canonical_strings.ingest_pipeline_type_activity,
    enum_canonical_strings.ingest_pipeline_type_identity,
    name="ingest_pipeline_type",
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
    enum_canonical_strings.direct_ingest_raw_file_import_status_failed_dag_level,
    enum_canonical_strings.direct_ingest_raw_file_import_status_failed_load_step,
    enum_canonical_strings.direct_ingest_raw_file_import_status_failed_pre_import_normalization_step,
    enum_canonical_strings.direct_ingest_raw_file_import_status_failed_validation_step,
    enum_canonical_strings.direct_ingest_raw_file_import_status_failed_import_blocked,
    name="direct_ingest_file_import_status",
)

llm_extraction_job_result_type = Enum(
    enum_canonical_strings.llm_extraction_job_result_type_success,
    enum_canonical_strings.llm_extraction_job_result_type_partial_failure,
    enum_canonical_strings.llm_extraction_job_result_type_failure,
    name="llm_extraction_job_result_type",
)

llm_extraction_job_document_result_type = Enum(
    enum_canonical_strings.llm_extraction_job_document_result_type_success,
    enum_canonical_strings.llm_extraction_job_document_result_type_job_level_failure,
    enum_canonical_strings.llm_extraction_job_document_result_type_document_level_failure_transient,
    enum_canonical_strings.llm_extraction_job_document_result_type_document_level_failure_permanent,
    enum_canonical_strings.llm_extraction_job_document_result_type_document_level_failure_retries_exhausted,
    name="llm_extraction_job_document_result_type",
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

    # Which ingest pipeline kind (activity or identity) produced this job
    pipeline_type = Column(
        ingest_pipeline_type, nullable=False, index=True, server_default="ACTIVITY"
    )

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

    # Which ingest pipeline kind (activity or identity) produced this watermark.
    # Denormalized from direct_ingest_dataflow_job (via job_id) so diagnostic
    # queries can filter watermarks by pipeline type without a join.
    pipeline_type = Column(
        ingest_pipeline_type, nullable=False, index=True, server_default="ACTIVITY"
    )


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

    # Rendered message describing non-blocking validation failures associated with
    # an import. Can be populated regardless of import_status.
    non_blocking_failure_message = Column(Text, nullable=True)

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


class LLMExtractorCollection(OperationsBase):
    """Append-only metadata about a versioned LLM extractor collection.

    A new row is written when anything about the collection that would affect
    extraction output (output schema, description, default minimum confidence
    level) changes.
    """

    __tablename__ = "llm_extractor_collection"

    __table_args__ = (PrimaryKeyConstraint("collection_name", "collection_version_id"),)

    # Unique name of the extractor collection (e.g. CASE_NOTE_EMPLOYMENT_INFO).
    collection_name = Column(String(255), nullable=False)

    # Version ID that changes when anything about this collection changes that
    # would impact extraction output.
    collection_version_id = Column(String(255), nullable=False)

    # Hash of the output schema for versioning.
    output_schema_version = Column(String(255), nullable=False)

    # JSON Schema defining the output structure for all extractors in this collection.
    output_schema_json = Column(Text, nullable=False)

    # Description of what extractors in this collection do.
    description = Column(String(255), nullable=False)

    # Minimum ordinal confidence level for validated output. One of:
    # speculative, ambiguous, inferred, explicit, verbatim. Individual fields
    # can override this default.
    minimum_confidence_level = Column(String(255), nullable=False)

    # When this collection version was recorded.
    row_creation_datetime_utc = Column(DateTime(timezone=True), nullable=False)


class LLMExtractor(OperationsBase):
    """Append-only registry of logical state-specific LLM extractors.

    A new row is written when a new logical extractor is first defined. Both
    `extractor_collection_name` and `input_document_collection_name` are
    invariant for a given `extractor_id` — changing either requires a new
    `extractor_id`.
    """

    __tablename__ = "llm_extractor"

    __table_args__ = (PrimaryKeyConstraint("state_code", "extractor_id"),)

    state_code = Column(String(255), nullable=False, index=True)

    # Human-readable ID for the extractor (e.g. US_XX_CASE_NOTE_EMPLOYMENT_INFO).
    # Stable across all versions.
    extractor_id = Column(String(255), nullable=False)

    # The extractor collection this extractor belongs to. Invariant — changing
    # requires a new extractor_id.
    extractor_collection_name = Column(String(255), nullable=False)

    # The document collection this extractor runs against. Invariant — changing
    # requires a new extractor_id.
    input_document_collection_name = Column(String(255), nullable=False)

    # When this extractor was first defined.
    row_creation_datetime_utc = Column(DateTime(timezone=True), nullable=False)


class LLMExtractorVersion(OperationsBase):
    """Append-only versions of a logical LLM extractor's configuration.

    A new row is written when version-scoped configuration (prompt, model
    config) changes for a given `extractor_id`.
    """

    __tablename__ = "llm_extractor_version"

    __table_args__ = (
        PrimaryKeyConstraint("state_code", "extractor_version_id"),
        ForeignKeyConstraint(
            ["extractor_collection_name", "extractor_collection_version_id"],
            [
                "llm_extractor_collection.collection_name",
                "llm_extractor_collection.collection_version_id",
            ],
            name="llm_extractor_version_collection_fkey",
        ),
        ForeignKeyConstraint(
            ["state_code", "extractor_id"],
            ["llm_extractor.state_code", "llm_extractor.extractor_id"],
            name="llm_extractor_version_extractor_fkey",
        ),
        CheckConstraint(
            "(invalidated_datetime_utc IS NULL) = (invalidation_reason IS NULL)",
            name="llm_extractor_version_invalidation_fields_consistent",
        ),
    )

    state_code = Column(String(255), nullable=False, index=True)

    # Version ID that changes when version-scoped configuration changes.
    extractor_version_id = Column(String(255), nullable=False)

    # The logical extractor this version belongs to.
    extractor_id = Column(String(255), nullable=False)

    # Denormalized from llm_extractor to support the FK to
    # llm_extractor_collection.
    extractor_collection_name = Column(String(255), nullable=False)

    # The collection version this extractor version targets.
    extractor_collection_version_id = Column(String(255), nullable=False)

    # The fully-rendered instruction prompt.
    instructions_prompt = Column(Text, nullable=False)

    # SHA256 hash of the instructions prompt.
    instructions_prompt_hash = Column(String(255), nullable=False)

    # The model_config_name from the model registry.
    model_config_name = Column(String(255), nullable=False)

    # Hash of the resolved model config parameters.
    model_config_version_id = Column(String(255), nullable=False)

    # If set, this version is skipped for new job creation.
    invalidated_datetime_utc = Column(DateTime(timezone=True), nullable=True)

    # Human-readable reason; nonnull iff invalidated_datetime_utc is nonnull.
    invalidation_reason = Column(Text, nullable=True)

    # When this version was added.
    row_creation_datetime_utc = Column(DateTime(timezone=True), nullable=False)


class LLMExtractorDocumentFilter(OperationsBase):
    """Append-only document filter definitions per extractor.

    Filter changes do not trigger reprocessing — `document_filter_id` is
    intentionally not part of `extractor_version_id`.
    """

    __tablename__ = "llm_extractor_document_filter"

    __table_args__ = (
        PrimaryKeyConstraint("state_code", "extractor_id", "document_filter_id"),
        ForeignKeyConstraint(
            ["state_code", "extractor_id"],
            ["llm_extractor.state_code", "llm_extractor.extractor_id"],
            name="llm_extractor_document_filter_extractor_fkey",
        ),
    )

    state_code = Column(String(255), nullable=False, index=True)

    # The extractor associated with this filter.
    extractor_id = Column(String(255), nullable=False)

    # Filter version identifier.
    document_filter_id = Column(String(255), nullable=False)

    # BQ query template (format arg: project_id). Output: single
    # document_contents_id column.
    document_metadata_filter_query_template = Column(Text, nullable=False)

    # When this filter was added.
    row_creation_datetime_utc = Column(DateTime(timezone=True), nullable=False)


class LLMExtractionEligibleDocumentMetadata(OperationsBase):
    """Append-only, write-once per-document sizing metadata.

    Holds one row per (state_code, document_contents_id) ever seen as eligible
    under any extractor version or filter. Because `document_contents_id` is a
    SHA256 of the document text, `char_count` and `document_update_datetime`
    are fixed for all time and the row never needs updating.
    """

    __tablename__ = "llm_extraction_eligible_document_metadata"

    __table_args__ = (PrimaryKeyConstraint("state_code", "document_contents_id"),)

    state_code = Column(String(255), nullable=False, index=True)

    # SHA256 hash identifying the document.
    document_contents_id = Column(String(255), nullable=False)

    # Character count of the document text; used for the per-document size
    # guardrail and for cost observability.
    char_count = Column(Integer, nullable=False)

    # The document's date (from document collection metadata). Used to order
    # oldest-first when processing order matter (e.g. when we can't fit all
    # pending documents in a single batch job).
    document_update_datetime = Column(DateTime(timezone=True), nullable=False)

    # When this row was added.
    row_creation_datetime_utc = Column(DateTime(timezone=True), nullable=False)


class LLMExtractionEligibleDocument(OperationsBase):
    """Append-only list of documents eligible for extraction under a given
    (extractor version, filter) pair."""

    __tablename__ = "llm_extraction_eligible_document"

    __table_args__ = (
        PrimaryKeyConstraint(
            "state_code",
            "extractor_version_id",
            "document_filter_id",
            "document_contents_id",
        ),
        ForeignKeyConstraint(
            ["state_code", "extractor_version_id"],
            [
                "llm_extractor_version.state_code",
                "llm_extractor_version.extractor_version_id",
            ],
            name="llm_extraction_eligible_document_version_fkey",
        ),
        ForeignKeyConstraint(
            ["state_code", "document_contents_id"],
            [
                "llm_extraction_eligible_document_metadata.state_code",
                "llm_extraction_eligible_document_metadata.document_contents_id",
            ],
            name="llm_extraction_eligible_document_metadata_fkey",
        ),
    )

    state_code = Column(String(255), nullable=False, index=True)

    # The extractor version under which this document is eligible.
    extractor_version_id = Column(String(255), nullable=False)

    # The filter version under which this document was deemed eligible.
    document_filter_id = Column(String(255), nullable=False)

    # SHA256 hash identifying the document.
    document_contents_id = Column(String(255), nullable=False)

    # When this row was added.
    row_creation_datetime_utc = Column(DateTime(timezone=True), nullable=False)


class LLMExtractionJob(OperationsBase):
    """Tracks a single LLM extraction job.

    A row is added when new documents are identified for processing. A partial
    unique index ensures only one open (uncompleted) job exists per
    (state_code, extractor_version_id) at a time.
    """

    __tablename__ = "llm_extraction_job"

    __table_args__ = (
        PrimaryKeyConstraint("state_code", "job_id"),
        ForeignKeyConstraint(
            ["state_code", "extractor_version_id"],
            [
                "llm_extractor_version.state_code",
                "llm_extractor_version.extractor_version_id",
            ],
            name="llm_extraction_job_extractor_version_fkey",
        ),
        Index(
            "one_open_llm_extraction_job_per_extractor_version",
            "state_code",
            "extractor_version_id",
            unique=True,
            postgresql_where=text("completion_datetime_utc IS NULL"),
        ),
        CheckConstraint(
            "(completion_datetime_utc IS NULL) = (result_type IS NULL)",
            name="llm_extraction_job_completion_and_result_type_consistent",
        ),
        CheckConstraint(
            "start_datetime_utc IS NOT NULL OR completion_datetime_utc IS NULL",
            name="llm_extraction_job_completion_requires_start",
        ),
        CheckConstraint(
            "completion_datetime_utc IS NULL "
            "OR start_datetime_utc IS NULL "
            "OR completion_datetime_utc >= start_datetime_utc",
            name="llm_extraction_job_completion_after_start",
        ),
        CheckConstraint(
            "error_message IS NULL OR result_type = 'FAILURE'",
            name="llm_extraction_job_error_message_only_for_failure",
        ),
    )

    state_code = Column(String(255), nullable=False, index=True)

    # UUID of the extraction job.
    job_id = Column(String(255), nullable=False)

    # The extractor version this job processes documents for.
    extractor_version_id = Column(String(255), nullable=False)

    # When the job started (null if not yet started).
    start_datetime_utc = Column(DateTime(timezone=True), nullable=True)

    # When the job was identified as completed.
    completion_datetime_utc = Column(DateTime(timezone=True), nullable=True)

    # SUCCESS, FAILURE, or PARTIAL_FAILURE. Nonnull iff completion_datetime_utc is set.
    result_type = Column(llm_extraction_job_result_type, nullable=True)

    # Error details. Nonnull only for FAILURE result types.
    error_message = Column(String, nullable=True)


class LLMExtractionBatchJobMetadata(OperationsBase):
    """Provider-specific metadata for batch submissions.

    Only populated for batch jobs. A single `llm_extraction_job` may have
    multiple rows if documents were split across multiple JSONL files.
    """

    __tablename__ = "llm_extraction_batch_job_metadata"

    __table_args__ = (
        PrimaryKeyConstraint("state_code", "job_id", "batch_index"),
        ForeignKeyConstraint(
            ["state_code", "job_id"],
            ["llm_extraction_job.state_code", "llm_extraction_job.job_id"],
            name="llm_extraction_batch_job_metadata_job_fkey",
        ),
    )

    state_code = Column(String(255), nullable=False, index=True)

    # The extraction job.
    job_id = Column(String(255), nullable=False)

    # 0-indexed position of this submission within the job.
    batch_index = Column(Integer, nullable=False)

    # Provider-assigned batch job ID.
    external_job_id = Column(String(255), nullable=False)

    # The batch provider (e.g. VERTEX_AI_BATCH).
    external_job_provider = Column(String(255), nullable=False)

    # GCS path to the JSONL input file.
    gcs_input_uri = Column(String, nullable=False)

    # GCS path to the batch output (populated on completion).
    gcs_output_uri = Column(String, nullable=True)

    # When this row was created.
    row_creation_datetime_utc = Column(DateTime(timezone=True), nullable=False)


class LLMExtractionJobDocument(OperationsBase):
    """Documents associated with an LLM extraction job.

    Written before job submission. Result columns are populated as documents
    are processed.
    """

    __tablename__ = "llm_extraction_job_document"

    __table_args__ = (
        PrimaryKeyConstraint("state_code", "job_id", "document_contents_id"),
        UniqueConstraint(
            "state_code",
            "job_id",
            "batch_index",
            "job_index",
            name="llm_extraction_job_document_job_index_unique",
        ),
        ForeignKeyConstraint(
            ["state_code", "job_id"],
            ["llm_extraction_job.state_code", "llm_extraction_job.job_id"],
            name="llm_extraction_job_document_job_fkey",
        ),
        ForeignKeyConstraint(
            ["state_code", "job_id", "batch_index"],
            [
                "llm_extraction_batch_job_metadata.state_code",
                "llm_extraction_batch_job_metadata.job_id",
                "llm_extraction_batch_job_metadata.batch_index",
            ],
            name="llm_extraction_job_document_batch_fkey",
        ),
        CheckConstraint(
            "(result_datetime_utc IS NULL) = (result_type IS NULL)",
            name="llm_extraction_job_document_result_datetime_and_type_consistent",
        ),
        CheckConstraint(
            "error_message IS NULL "
            "OR result_type IN ("
            "'JOB_LEVEL_FAILURE',"
            "'DOCUMENT_LEVEL_FAILURE_TRANSIENT',"
            "'DOCUMENT_LEVEL_FAILURE_PERMANENT',"
            "'DOCUMENT_LEVEL_FAILURE_RETRIES_EXHAUSTED'"
            ")",
            name="llm_extraction_job_document_error_message_only_for_failure",
        ),
        CheckConstraint(
            "(is_relevant IS NOT NULL) = (result_type IS NOT DISTINCT FROM 'SUCCESS')",
            name="llm_extraction_job_document_is_relevant_iff_success",
        ),
        CheckConstraint(
            "job_index >= 0",
            name="llm_extraction_job_document_nonnegative_job_index",
        ),
        CheckConstraint(
            "(result_datetime_utc IS NULL) = (input_token_count IS NULL)"
            " AND (result_datetime_utc IS NULL) = (output_token_count IS NULL)"
            " AND (result_datetime_utc IS NULL) = (cached_input_token_count IS NULL)"
            " AND (result_datetime_utc IS NULL) = (thinking_token_count IS NULL)",
            name="llm_extraction_job_document_token_counts_iff_result_datetime",
        ),
    )

    state_code = Column(String(255), nullable=False, index=True)

    # UUID of the extraction job.
    job_id = Column(String(255), nullable=False)

    # SHA256 hash identifying the document.
    document_contents_id = Column(String(255), nullable=False)

    # Which batch submission this document was sent in (0-indexed). NULL for
    # sync jobs.
    batch_index = Column(Integer, nullable=True)

    # Submission-time ordering within the job (0-indexed). Used for internal
    # bookkeeping and uniqueness, not for result correlation — results are
    # correlated via document_contents_id embedded in the batch request.
    job_index = Column(Integer, nullable=False)

    # When the result was processed.
    result_datetime_utc = Column(DateTime(timezone=True), nullable=True)

    # SUCCESS, JOB_LEVEL_FAILURE, DOCUMENT_LEVEL_FAILURE_TRANSIENT,
    # DOCUMENT_LEVEL_FAILURE_PERMANENT, or DOCUMENT_LEVEL_FAILURE_RETRIES_EXHAUSTED.
    result_type = Column(llm_extraction_job_document_result_type, nullable=True)

    # Model's is_relevant determination. Non-null iff result_type = SUCCESS.
    # Denormalized for the REPROCESS_ALL_RELEVANT reprocessing directive.
    is_relevant = Column(Boolean, nullable=True)

    # Error details. Nonnull only for FAILURE result types.
    error_message = Column(String, nullable=True)

    # Total input tokens for this API call. Nonnull iff result_datetime_utc is set.
    input_token_count = Column(Integer, nullable=True)

    # Output tokens excluding thinking. Nonnull iff result_datetime_utc is set.
    output_token_count = Column(Integer, nullable=True)

    # Input tokens served from cache. Nonnull iff result_datetime_utc is set.
    cached_input_token_count = Column(Integer, nullable=True)

    # Thinking tokens (0 if thinking disabled). Nonnull iff result_datetime_utc is set.
    thinking_token_count = Column(Integer, nullable=True)


class LLMExtractionCapOverride(OperationsBase):
    """Temporary, time-windowed exceptions to an extractor's
    total_pending_document_count_hard_cap.

    An override raises the cap until `expires_datetime_utc` (or until the
    pending backlog drains below the cap), so a single override covers a
    multi-job reprocess rather than being consumed by one job. Auto-expire.
    """

    __tablename__ = "llm_extraction_cap_override"

    __table_args__ = (
        PrimaryKeyConstraint(
            "state_code", "extractor_version_id", "document_filter_id"
        ),
        ForeignKeyConstraint(
            ["state_code", "extractor_version_id"],
            [
                "llm_extractor_version.state_code",
                "llm_extractor_version.extractor_version_id",
            ],
            name="llm_extraction_cap_override_extractor_version_fkey",
        ),
    )

    state_code = Column(String(255), nullable=False, index=True)

    # The specific extractor version.
    extractor_version_id = Column(String(255), nullable=False)

    # The specific filter.
    document_filter_id = Column(String(255), nullable=False)

    # Total-pending char-count cap to enforce while this override is active
    # (raises total_pending_document_count_hard_cap).
    override_document_count_cap = Column(Integer, nullable=False)

    # When this override expires.
    expires_datetime_utc = Column(DateTime(timezone=True), nullable=False)

    # When this override was created.
    row_creation_datetime_utc = Column(DateTime(timezone=True), nullable=False)
