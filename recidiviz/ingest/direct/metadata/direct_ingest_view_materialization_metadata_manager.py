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
from typing import List, Optional

import pytz

from recidiviz.ingest.direct.types.cloud_task_args import IngestViewMaterializationArgs
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
        self, job_args: IngestViewMaterializationArgs
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
        self, job_args: IngestViewMaterializationArgs
    ) -> Optional[datetime.datetime]:
        """Returns the completion time of the materialization job represented by the
        provided args.
        """
        return self._get_metadata_for_job_args(job_args).materialization_time

    def mark_ingest_view_materialized(
        self, job_args: IngestViewMaterializationArgs
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
        self, job_args: IngestViewMaterializationArgs
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
        self, job_args: IngestViewMaterializationArgs
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
                table_cls.region_code == self.region_code
                and table_cls.instance == self.ingest_instance.value
            )
            session.execute(delete_query)
