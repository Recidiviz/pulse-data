# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""An abstract interface for a class that handles writing metadata about pending and
completed ingest view materialization jobs to disk.
"""

import datetime
from typing import Dict, List, Optional, Union

import attr
import pytz
from sqlalchemy import and_, func

from recidiviz.ingest.direct.types.cloud_task_args import (
    BQIngestViewMaterializationArgs,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_schema_object_to_entity,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestViewMaterializationMetadata,
)
from recidiviz.utils import environment

SUMMARY_INGEST_VIEW_NAME_COL = "ingest_view_name"
SUMMARY_NUM_PENDING_JOBS_COL = "num_pending_jobs"
SUMMARY_NUM_COMPLETED_JOBS_COL = "num_completed_jobs"
SUMMARY_COMPLETED_JOBS_MAX_DATETIME_COL = "completed_jobs_max_datetime"
SUMMARY_PENDING_JOBS_MIN_DATETIME_COL = "pending_jobs_min_datetime"


@attr.define(frozen=True, kw_only=True)
class IngestViewMaterializationSummary:
    """Returns a map with a summary of the status of pending / completed
    materialization jobs for a given ingest view.
    """

    # The name of the ingest view this summary is about
    ingest_view_name: str

    # Number of raw data upload dates without materialized results
    num_pending_jobs: int

    # Number of raw data upload dates with materialized results
    num_completed_jobs: int

    # Max raw data datetime among completed materialization jobs
    completed_jobs_max_datetime: Optional[datetime.datetime]

    # Min raw data datetime among pending materialization jobs
    pending_jobs_min_datetime: Optional[datetime.datetime]

    def as_api_dict(self) -> Dict[str, Union[Optional[str], int]]:
        """Serializes this class into a dictionary that can be transmitted via an API
        to the frontend.
        """
        return {
            "ingestViewName": self.ingest_view_name,
            "numPendingJobs": self.num_pending_jobs,
            "numCompletedJobs": self.num_completed_jobs,
            "completedJobsMaxDatetime": (
                self.completed_jobs_max_datetime.isoformat()
                if self.completed_jobs_max_datetime
                else None
            ),
            "pendingJobsMinDatetime": (
                self.pending_jobs_min_datetime.isoformat()
                if self.pending_jobs_min_datetime
                else None
            ),
        }


class DirectIngestViewMaterializationMetadataManager:
    """A interface for a class that handles writing metadata about pending and completed
    ingest view materialization jobs to disk.
    """

    def __init__(self, region_code: str, ingest_instance: DirectIngestInstance):
        self.region_code = region_code.upper()
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        self.ingest_instance = ingest_instance

    @staticmethod
    def _schema_object_to_entity(
        schema_metadata: schema.DirectIngestViewMaterializationMetadata,
    ) -> DirectIngestViewMaterializationMetadata:
        entity_metadata = convert_schema_object_to_entity(schema_metadata)

        if not isinstance(entity_metadata, DirectIngestViewMaterializationMetadata):
            raise ValueError(f"Unexpected metadata type: {type(entity_metadata)}")

        return entity_metadata

    def register_ingest_materialization_job(
        self, job_args: BQIngestViewMaterializationArgs
    ) -> DirectIngestViewMaterializationMetadata:
        """Writes a new row to the ingest view metadata table with the expected path
        once the export job completes.
        """
        with SessionFactory.using_database(self.database_key) as session:
            metadata = schema.DirectIngestViewMaterializationMetadata(
                region_code=self.region_code,
                instance=self.ingest_instance.value,
                ingest_view_name=job_args.ingest_view_name,
                upper_bound_datetime_inclusive=job_args.upper_bound_datetime_inclusive,
                lower_bound_datetime_exclusive=job_args.lower_bound_datetime_exclusive,
                job_creation_time=datetime.datetime.now(tz=pytz.UTC),
                materialization_time=None,
                is_invalidated=False,
            )
            session.add(metadata)
            session.commit()
            return self._schema_object_to_entity(metadata)

    def get_job_completion_time_for_args(
        self, job_args: BQIngestViewMaterializationArgs
    ) -> Optional[datetime.datetime]:
        """Returns the completion time of the materialization job represented by the
        provided args.
        """
        return self._get_metadata_for_job_args(job_args).materialization_time

    def mark_ingest_view_materialized(
        self, job_args: BQIngestViewMaterializationArgs
    ) -> None:
        """Commits the current time as the materialization_time for the given ingest
        view materialization job.
        """
        with SessionFactory.using_database(self.database_key) as session:
            metadata = (
                session.query(schema.DirectIngestViewMaterializationMetadata)
                .filter_by(
                    region_code=self.region_code.upper(),
                    instance=self.ingest_instance.value,
                    ingest_view_name=job_args.ingest_view_name,
                    upper_bound_datetime_inclusive=job_args.upper_bound_datetime_inclusive,
                    lower_bound_datetime_exclusive=job_args.lower_bound_datetime_exclusive,
                    is_invalidated=False,
                )
                .one_or_none()
            )
            metadata.materialization_time = datetime.datetime.now(tz=pytz.UTC)
            return metadata

    def get_most_recent_registered_job(
        self, ingest_view_name: str
    ) -> Optional[DirectIngestViewMaterializationMetadata]:
        """Returns most recently created materialization metadata row where
        is_invalidated is False, or None if there are no metadata rows for this ingest
        view for this manager's region / ingest instance.
        """
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = (
                session.query(schema.DirectIngestViewMaterializationMetadata)
                .filter_by(
                    region_code=self.region_code.upper(),
                    instance=self.ingest_instance.value,
                    ingest_view_name=ingest_view_name,
                    is_invalidated=False,
                )
                .order_by(
                    schema.DirectIngestViewMaterializationMetadata.job_creation_time.desc()
                )
                .limit(1)
                .one_or_none()
            )
            if not metadata:
                return None

            return self._schema_object_to_entity(metadata)

    def get_jobs_pending_completion(
        self,
    ) -> List[DirectIngestViewMaterializationMetadata]:
        """Returns metadata for all ingest view materialization jobs that have not yet
        been completed.
        """
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            results = (
                session.query(schema.DirectIngestViewMaterializationMetadata)
                .filter_by(
                    region_code=self.region_code.upper(),
                    instance=self.ingest_instance.value,
                    materialization_time=None,
                    is_invalidated=False,
                )
                .all()
            )
            return [self._schema_object_to_entity(metadata) for metadata in results]

    def _get_metadata_for_job_args(
        self, job_args: BQIngestViewMaterializationArgs
    ) -> DirectIngestViewMaterializationMetadata:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = (
                session.query(schema.DirectIngestViewMaterializationMetadata)
                .filter_by(
                    region_code=self.region_code.upper(),
                    instance=self.ingest_instance.value,
                    ingest_view_name=job_args.ingest_view_name,
                    upper_bound_datetime_inclusive=job_args.upper_bound_datetime_inclusive,
                    lower_bound_datetime_exclusive=job_args.lower_bound_datetime_exclusive,
                    is_invalidated=False,
                )
                .one()
            )
            return self._schema_object_to_entity(metadata)

    @environment.test_only
    def get_metadata_for_job_args(
        self, job_args: BQIngestViewMaterializationArgs
    ) -> DirectIngestViewMaterializationMetadata:
        return self._get_metadata_for_job_args(job_args)

    @environment.test_only
    def clear_instance_metadata(
        self,
    ) -> None:
        """Deletes all metadata rows for this metadata manager's region and ingest
        instance.
        """
        with SessionFactory.using_database(
            self.database_key,
        ) as session:
            table_cls = schema.DirectIngestViewMaterializationMetadata
            delete_query = table_cls.__table__.delete().where(
                and_(
                    table_cls.region_code == self.region_code,
                    table_cls.instance == self.ingest_instance.value,
                )
            )
            session.execute(delete_query)

    def mark_instance_data_invalidated(self) -> None:
        """Sets the is_invalidated on all rows for the state/instance"""
        with SessionFactory.using_database(
            self.database_key,
        ) as session:
            table_cls = schema.DirectIngestViewMaterializationMetadata
            update_query = (
                table_cls.__table__.update()
                .where(
                    and_(
                        table_cls.region_code == self.region_code.upper(),
                        table_cls.instance == self.ingest_instance.value,
                    )
                )
                .values(is_invalidated=True)
            )
            session.execute(update_query)

    def transfer_metadata_to_new_instance(
        self,
        new_instance_manager: "DirectIngestViewMaterializationMetadataManager",
    ) -> None:
        """Take all rows where `is_invalidated=False` and transfer to the instance assocaited with
        the new_instance_manager
        """
        if (
            new_instance_manager.ingest_instance == self.ingest_instance
            or new_instance_manager.region_code != self.region_code
        ):
            raise ValueError(
                "Either state codes are not the same or new instance is same as origin."
            )

        with SessionFactory.using_database(
            self.database_key,
        ) as session:
            table_cls = schema.DirectIngestViewMaterializationMetadata
            # check destination instance does not have any valid metadata rows
            check_query = (
                session.query(schema.DirectIngestViewMaterializationMetadata)
                .filter_by(
                    region_code=self.region_code.upper(),
                    instance=new_instance_manager.ingest_instance.value,
                    is_invalidated=False,
                )
                .all()
            )
            if check_query:
                raise ValueError(
                    "Destination instance should not have any valid metadata rows."
                )

            update_query = (
                table_cls.__table__.update()
                .where(
                    and_(
                        table_cls.region_code == self.region_code.upper(),
                        table_cls.instance == self.ingest_instance.value,
                        # pylint: disable=singleton-comparison
                        table_cls.is_invalidated == False,
                    )
                )
                .values(
                    instance=new_instance_manager.ingest_instance.value,
                )
            )
            session.execute(update_query)

    def get_instance_summaries(self) -> Dict[str, IngestViewMaterializationSummary]:
        """Returns a map with a summary of the status of pending / completed
        materialization jobs for each ingest view.
        """
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            materialization_time_col = (
                schema.DirectIngestViewMaterializationMetadata.materialization_time
            )
            upper_bound_datetime_col = (
                schema.DirectIngestViewMaterializationMetadata.upper_bound_datetime_inclusive
            )
            result = (
                session.query(
                    schema.DirectIngestViewMaterializationMetadata.ingest_view_name,
                    (
                        func.count(1)
                        .filter(materialization_time_col.is_(None))
                        .label(SUMMARY_NUM_PENDING_JOBS_COL)
                    ),
                    (
                        func.count(1)
                        .filter(materialization_time_col.isnot(None))
                        .label(SUMMARY_NUM_COMPLETED_JOBS_COL)
                    ),
                    (
                        func.max(upper_bound_datetime_col)
                        .filter(materialization_time_col.isnot(None))
                        .label(SUMMARY_COMPLETED_JOBS_MAX_DATETIME_COL)
                    ),
                    (
                        func.min(upper_bound_datetime_col)
                        .filter(materialization_time_col.is_(None))
                        .label(SUMMARY_PENDING_JOBS_MIN_DATETIME_COL)
                    ),
                )
                .filter_by(
                    region_code=self.region_code.upper(),
                    instance=self.ingest_instance.value,
                    is_invalidated=False,
                )
                .group_by(
                    schema.DirectIngestViewMaterializationMetadata.ingest_view_name
                )
                .all()
            )

            summary = {}
            for row in result:
                ingest_view_name = row[SUMMARY_INGEST_VIEW_NAME_COL]
                summary[ingest_view_name] = IngestViewMaterializationSummary(
                    ingest_view_name=ingest_view_name,
                    num_pending_jobs=row[SUMMARY_NUM_PENDING_JOBS_COL],
                    num_completed_jobs=row[SUMMARY_NUM_COMPLETED_JOBS_COL],
                    completed_jobs_max_datetime=row[
                        SUMMARY_COMPLETED_JOBS_MAX_DATETIME_COL
                    ],
                    pending_jobs_min_datetime=row[
                        SUMMARY_PENDING_JOBS_MIN_DATETIME_COL
                    ],
                )

            return summary
