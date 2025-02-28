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
"""Domain logic entities for operations database data.

Note: These classes mirror the SQL Alchemy ORM objects but are kept separate. This allows these persistence layer
objects additional flexibility that the SQL Alchemy ORM objects can't provide.
"""

import datetime
from typing import Dict, List, Optional

import attr

from recidiviz.common import attr_validators
from recidiviz.common.attr_mixins import BuildableAttr, DefaultableAttr
from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataLockActor,
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.entity.base_entity import Entity


@attr.s(eq=False)
class DirectIngestSftpRemoteFileMetadata(Entity, BuildableAttr, DefaultableAttr):
    """Metadata about a file downloaded from SFTP for a particular region."""

    file_id: int = attr.ib()
    region_code: str = attr.ib()
    # The remote file path on the SFTP server
    remote_file_path: str = attr.ib()
    # The original SFTP mtime (UNIX seconds since epoch) of the remote_file_path on the SFTP server
    sftp_timestamp: float = attr.ib()
    # Time when the file is actually discovered by the SFTP Airflow DAG
    file_discovery_time: datetime.datetime = attr.ib()
    # Time when the file is finished fully downloaded to the SFTP bucket
    file_download_time: Optional[datetime.datetime] = attr.ib()


@attr.s(eq=False)
class DirectIngestSftpIngestReadyFileMetadata(Entity, BuildableAttr, DefaultableAttr):
    """Metadata about a file post-processed from remote SFTP files downloaded for a
    particular region."""

    file_id: int = attr.ib()
    region_code: str = attr.ib()
    # The file path that is post-processed from the remote file path in the SFTP GCS Bucket.
    post_processed_normalized_file_path: str = attr.ib()
    # The original remote_file_path that should match the remote_file_metadata table.
    remote_file_path: str = attr.ib()
    # Time when the file is actually discovered by the SFTP Airflow DAG
    file_discovery_time: datetime.datetime = attr.ib()
    # Time when the file is finished fully uploaded to the ingest bucket
    file_upload_time: Optional[datetime.datetime] = attr.ib()


@attr.s(eq=False)
class DirectIngestRawFileMetadata(Entity, BuildableAttr, DefaultableAttr):
    """Metadata about a raw file imported directly from a particular region."""

    file_id: int = attr.ib()
    region_code: str = attr.ib()

    # The instance that this raw data was imported to.
    raw_data_instance: DirectIngestInstance = attr.ib()
    # Shortened name for the raw file that corresponds to its YAML schema definition
    file_tag: str = attr.ib()
    # Unprocessed normalized file name for this file, set at time of file discovery.
    normalized_file_name: str = attr.ib()
    # Time when the file is actually discovered by our controller's handle_new_files endpoint.
    file_discovery_time: datetime.datetime = attr.ib()
    # Time when we have finished fully processing this file by uploading to BQ.
    file_processed_time: Optional[datetime.datetime] = attr.ib(
        validator=attr_validators.is_opt_utc_timezone_aware_datetime
    )
    is_invalidated: bool = attr.ib(validator=attr_validators.is_bool)

    update_datetime: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )


@attr.s(eq=False)
class DirectIngestInstanceStatus(Entity, BuildableAttr, DefaultableAttr):
    """Status of a direct instance ingest process."""

    # The region code of a particular instance doing ingest.
    region_code: str = attr.ib()

    # The timestamp of when the status of a particular instance changes.
    status_timestamp: datetime.datetime = attr.ib()

    # The particular instance doing ingest.
    instance: DirectIngestInstance = attr.ib()

    # The status of a particular instance doing ingest.
    status: DirectIngestStatus = attr.ib()

    def for_api(self) -> Dict[str, str]:
        """Serializes the instance status as a dictionary that can be passed to the
        frontend.
        """
        return {
            "regionCode": self.region_code,
            "instance": self.instance.value,
            "status": self.status.value,
            "statusTimestamp": self.status_timestamp.isoformat(),
        }


@attr.s(eq=False)
class DirectIngestDataflowJob(Entity, BuildableAttr, DefaultableAttr):
    """A record of the ingest jobs that have completed."""

    # The dataflow generated ID of the ingest pipeline that is reading this raw data.
    # Of the form 'YYYY-MM-DD_HH_MM_SS-NNNNNNNNNNNNNNNNNNN', e.g. '2023-10-02_07_03_19-9234234904060676794'
    job_id: str = attr.ib()
    # The state code that we are ingesting for
    region_code: str = attr.ib()
    # The ingest instance that we are running for
    ingest_instance: DirectIngestInstance = attr.ib()
    # When ingest in Dataflow Airflow DAG saw the job completed and wrote this row
    completion_time: datetime.datetime = attr.ib()
    # This is only to be used for secondary reruns in order to invalidate prior secondary reruns and start over from scratch.
    is_invalidated: bool = attr.ib()

    # Cross-entity relationships
    watermarks: List["DirectIngestDataflowRawTableUpperBounds"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )


@attr.s(eq=False)
class DirectIngestDataflowRawTableUpperBounds(Entity, BuildableAttr, DefaultableAttr):
    """Stores the highest watermark (highest date for which there is complete raw data)
    for each raw data table, to ensure that ingest results are not accidentally
    incorporating partial raw data results.
    """

    # Automatically generated by Postgres as the primary key
    watermark_id: int = attr.ib()
    # The raw data table that we are looking at for this watermark
    raw_data_file_tag: str = attr.ib()
    # The latest update_datetime of the raw data table
    watermark_datetime: datetime.datetime = attr.ib()

    # Cross-entity relationships
    job: DirectIngestDataflowJob = attr.ib()


@attr.s(eq=False)
class DirectIngestRawDataResourceLock(Entity, BuildableAttr, DefaultableAttr):
    """A record of direct ingest raw data resource locks over time.

    n.b. while this entity object is defined here, it is also queried in the airflow
    context with raw sql. any updates this entity must also be propagated to the raw
    data import dag's sql queries
    """

    lock_id: int = attr.ib()
    # The actor who is acquiring the lock
    lock_actor: DirectIngestRawDataLockActor = attr.ib()
    # the resource that this lock is "locking"
    lock_resource: DirectIngestRawDataResourceLockResource = attr.ib()
    region_code: str = attr.ib()
    raw_data_source_instance: DirectIngestInstance = attr.ib()
    # Whether or not this lock has been released (defaults to False)
    released: bool = attr.ib()
    # The time this lock was acquired
    lock_acquisition_time: datetime.datetime = attr.ib()
    # The TTL for this lock in seconds
    lock_ttl_seconds: Optional[int] = attr.ib()
    # Description for why the lock was acquired
    lock_description: str = attr.ib()


@attr.s(eq=False)
class DirectIngestRawBigQueryFileMetadata(Entity, BuildableAttr, DefaultableAttr):
    """Metadata known about a "conceptual" file_id that exists in BigQuery.

    n.b. while this entity object is defined here, it is also queried in the airflow
    context with raw sql. any updates this entity must also be propagated to the raw
    data import dag's sql queries
    """

    # "Conceptual" file id that corresponds to a single, conceptual file sent to us by
    # the state. For raw files states send us in chunks (such as ContactNoteComment),
    # each literal CSV that makes up the whole file will have a different gcs_file_id,
    # but all of those entries will have the same file_id.
    file_id: int = attr.ib(validator=attr_validators.is_int)
    region_code: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # The instance that this raw data was/will be imported to.
    raw_data_instance: DirectIngestInstance = attr.ib()
    # Shortened name for the raw file that corresponds to its YAML schema definition
    file_tag: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # The date we received the raw data. This is the field you should use when looking
    # for data current through date X. This is the maximum date of the normalized file
    # names associated with this file_id
    update_datetime: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    # Whether or not this row is still valid.
    is_invalidated: bool = attr.ib(validator=attr_validators.is_bool)
    # Time when all parts of this conceptual file finished uploading to BigQuery
    file_processed_time: Optional[datetime.datetime] = attr.ib(
        validator=attr_validators.is_opt_utc_timezone_aware_datetime
    )
    # The literal CSV files associated with this "conceptual" file
    gcs_files: List["DirectIngestRawGCSFileMetadata"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    # File imports associated with this bq_file
    file_imports: List["DirectIngestRawFileImport"] = attr.ib(factory=list)


@attr.s(eq=False)
class DirectIngestRawGCSFileMetadata(Entity, BuildableAttr, DefaultableAttr):
    """Metadata known about a raw data csv file that exists in Google Cloud Storage.

    n.b. while this entity object is defined here, it is also queried in the airflow
    context with raw sql. any updates this entity must also be propagated to the raw
    data import dag's sql queries
    """

    # An id that corresponds to the literal file in Google Cloud Storage. a single file
    # will always have a single gcs_file_id.
    gcs_file_id: int = attr.ib(validator=attr_validators.is_int)
    # "Conceptual" file id that corresponds to a single, conceptual file sent to us by
    # the state. For raw files states send us in chunks (such as ContactNoteComment),
    # each literal CSV that makes up the whole file will have a different gcs_file_id,
    # but all of those entries will have the same file_id.
    # If file_id is null, that likely means that while this CSV file has been discovered,
    # we might still be waiting for the other chunks of the "conceptual" file to arrive
    # to create a single, conceptual DirectIngestRawBigQueryFileMetadata.
    file_id: Optional[int] = attr.ib(validator=attr_validators.is_opt_int)
    region_code: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # The instance of the bucket that this raw data file was discovered in.
    raw_data_instance: DirectIngestInstance = attr.ib()
    # Shortened name for the raw file that corresponds to its YAML schema definition
    file_tag: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # Unprocessed normalized file name for this file, set at time of file discovery.
    normalized_file_name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # Time that this file was uploaded into our ingest bucket. This is the date in the
    # normalized file name.
    update_datetime: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    # Whether or not this row is still valid. *should* always match the is_invalidated
    # value of the associated file_id, if one exists
    is_invalidated: bool = attr.ib(validator=attr_validators.is_bool)
    # Time when the file is actually discovered by the raw data DAG
    file_discovery_time: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    # Conceptual Big Query file associated with this GCS file
    bq_file: Optional["DirectIngestRawBigQueryFileMetadata"] = attr.ib(default=None)


@attr.s(eq=False)
class DirectIngestRawFileImportRun(Entity, BuildableAttr, DefaultableAttr):
    """Each row corresponds to a state-and-ingest-specific run of the raw data import
    DAG

    n.b. this object's schema reflects the database schema defined in
    recidiviz/persistence/database/schema/operations/schema.py, but this object is
    populated by raw sql queries in the airflow context.
    """

    import_run_id: int = attr.ib()
    # The id of the raw data import run which should match DagRun::run_id in the airflow
    # metadata database
    dag_run_id: str = attr.ib(validator=attr_validators.is_str)
    # Time when the import run started
    import_run_start: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    # Time when the import run ended
    import_run_end: Optional[datetime.datetime] = attr.ib(
        validator=attr_validators.is_opt_utc_timezone_aware_datetime
    )
    # The region code associated with the raw data import run
    region_code: str = attr.ib(validator=attr_validators.is_str)
    # The raw data instance associated with the raw data import run
    raw_data_instance: DirectIngestInstance = attr.ib(
        validator=attr.validators.in_(DirectIngestInstance)
    )
    # The imports associated with this import run
    file_imports: List["DirectIngestRawFileImport"] = attr.ib(factory=list)


@attr.s(eq=False)
class DirectIngestRawFileImport(Entity, BuildableAttr, DefaultableAttr):
    """Each rows corresponds to an attempt to import an entry in the direct_ingest_raw_big_query_file_metadata
    table into BigQuery. This table has DAG-run-level information about a raw file
    import, which will change over the lifecycle of an import.

    n.b. while this entity object is defined here, it is also queried in the airflow
    context with raw sql. any updates this entity must also be propagated to the raw
    data import dag's sql queries
    """

    file_import_id: int = attr.ib()
    # The file_id from the direct_ingest_raw_big_query_file_metadata table
    file_id: int = attr.ib(validator=attr_validators.is_int)
    # The import_run_id from the direct_ingest_raw_file_import_run associated with this
    # import
    import_run_id: int = attr.ib(validator=attr_validators.is_int)
    # Status of the raw file import
    import_status: DirectIngestRawFileImportStatus = attr.ib(
        validator=attr.validators.in_(DirectIngestRawFileImportStatus)
    )
    # The region code associated with the raw data import
    region_code: str = attr.ib(validator=attr_validators.is_str)
    # The raw data instance associated with the raw data import
    raw_data_instance: DirectIngestInstance = attr.ib(
        validator=attr.validators.in_(DirectIngestInstance)
    )
    # Whether or not historical diffs were performed during the import of this file (or
    # would have been had the import successfully made it to that stage)
    historical_diffs_active: bool = attr.ib(validator=attr_validators.is_bool)
    # The number of rows included in the raw data file. If historical_diffs_active,
    # this number will not be equal to the number of rows added to the raw data table.
    raw_rows: Optional[int] = attr.ib(validator=attr_validators.is_opt_int)
    # Number of net new or updated rows added during the diffing process
    net_new_or_updated_rows: Optional[int] = attr.ib(
        validator=attr_validators.is_opt_int
    )
    # Number of rows added with is_deleted as True during the diffing process
    deleted_rows: Optional[int] = attr.ib(validator=attr_validators.is_opt_int)

    # Error message, typically a stack trace, associated with an import. Will likely
    # be non-null if import_status is fail-y
    error_message: Optional[str] = attr.ib(validator=attr_validators.is_opt_str)

    # RawBigQueryFile object from direct_ingest_raw_big_query_file_metadata table
    bq_file: Optional["DirectIngestRawBigQueryFileMetadata"] = attr.ib(default=None)
    # ImportRun object from direct_ingest_raw_file_import_run table
    import_run: Optional["DirectIngestRawFileImportRun"] = attr.ib(default=None)


@attr.s(eq=False)
class DirectIngestRawDataFlashStatus(Entity, BuildableAttr, DefaultableAttr):

    region_code: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # The timestamp of when the status of a particular instance changes.
    status_timestamp: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    # Whether or not this status row indicates that flashing is active
    flashing_in_progress: bool = attr.ib(validator=attr_validators.is_bool)
