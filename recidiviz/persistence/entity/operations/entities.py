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
        validator=attr_validators.is_utc_timezone_aware_datetime
    )

    update_datetime: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )

    @property
    def is_code_table(self) -> bool:
        """Whether or not this file is a code table.

        This means the file does not contain person level data associated with a particular date, but instead
        provides region-specific mappings (e.g. facility names, offense categorizations).
        """
        # TODO(#5935): Fuller support that isn't just based on table prefix.
        return self.file_tag.startswith("RECIDIVIZ_REFERENCE")


@attr.s(eq=False)
class DirectIngestViewMaterializationMetadata(Entity, BuildableAttr, DefaultableAttr):
    """Represents the metadata known about a job to materialize the results of an ingest
    view and save them for use later in ingest (as rows in a BQ table).
    """

    # The region associated with this materialization job (e.g. 'US_XX')
    region_code: str = attr.ib()

    # The ingest instance associated with this materialization job.
    instance: DirectIngestInstance = attr.ib()

    # Shortened name for the ingest view file that corresponds to its ingest view / YAML
    # mappings definition.
    ingest_view_name: str = attr.ib()

    # The upper bound date used to query data for these particular ingest view results.
    # The results will not contain any data we received after this date.
    upper_bound_datetime_inclusive: datetime.datetime = attr.ib()

    # The lower bound date used to query data for these particular ingest view results.
    # The results will not contain any rows that have remained unmodified with new raw
    # data updates we’ve gotten since this date.
    lower_bound_datetime_exclusive: Optional[datetime.datetime] = attr.ib()

    # Time the materialization job is first scheduled for this view.
    job_creation_time: datetime.datetime = attr.ib()

    # Time the results of this view were materialized (i.e. written to BQ).
    materialization_time: Optional[datetime.datetime] = attr.ib()

    # Whether or not this row is still valid (i.e. it applies to the current ingest
    # rerun).
    is_invalidated: bool = attr.ib()


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
