# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Handles reading metadata about ingest pipeline jobs."""
import datetime
from collections import defaultdict
from typing import Optional, TypeAlias

from sqlalchemy import func

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.utils import environment

DataflowJobLocationID: TypeAlias = tuple[str | None, str]


class DirectIngestDataflowJobManager:
    """Reads metadata about ingest pipeline jobs."""

    def __init__(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)

    def get_most_recent_jobs_location_and_id_by_state_and_instance(
        self,
    ) -> dict[StateCode, dict[DirectIngestInstance, DataflowJobLocationID]]:
        """Returns a map with the (location, job_id) tuple for the most recent successful
        ingest Dataflow pipeline for each state_code and ingest instance that has ever
        had a successful pipeline complete.
        """
        with SessionFactory.using_database(self.database_key) as session:
            # Get the maximum completion_time for each region_code and ingest_instance
            max_completion_subquery = (
                session.query(
                    schema.DirectIngestDataflowJob.region_code,
                    schema.DirectIngestDataflowJob.ingest_instance,
                    func.max(schema.DirectIngestDataflowJob.completion_time).label(
                        "max_completion_time"
                    ),
                )
                .group_by(
                    schema.DirectIngestDataflowJob.region_code,
                    schema.DirectIngestDataflowJob.ingest_instance,
                )
                .subquery()
            )

            # Join back to get the full job records for the most recent completion times
            results = (
                session.query(schema.DirectIngestDataflowJob)
                .join(
                    max_completion_subquery,
                    (
                        schema.DirectIngestDataflowJob.region_code
                        == max_completion_subquery.c.region_code
                    )
                    & (
                        schema.DirectIngestDataflowJob.ingest_instance
                        == max_completion_subquery.c.ingest_instance
                    )
                    & (
                        schema.DirectIngestDataflowJob.completion_time
                        == max_completion_subquery.c.max_completion_time
                    ),
                )
                .all()
            )

            jobs: dict[
                StateCode, dict[DirectIngestInstance, DataflowJobLocationID]
            ] = defaultdict(dict)
            for job in results:
                state_code = StateCode(job.region_code)
                instance = DirectIngestInstance[job.ingest_instance]
                jobs[state_code][instance] = (job.location, job.job_id)

            return jobs

    def get_most_recent_job(
        self, state_code: StateCode, ingest_instance: DirectIngestInstance
    ) -> Optional[DataflowJobLocationID]:
        """For the provided state_code and ingest_instance, returns the ingest pipline
        Dataflow job id for the most recent successful run.
        """
        all_most_recent = (
            self.get_most_recent_jobs_location_and_id_by_state_and_instance()
        )
        if state_code not in all_most_recent:
            return None

        return all_most_recent[state_code].get(ingest_instance)

    def invalidate_all_dataflow_jobs(
        self, state_code: StateCode, ingest_instance: DirectIngestInstance
    ) -> None:
        """For the provided state_code and ingest_instance, invalidates all Dataflow jobs for
        that state-code and ingest instance. This should only be done after a raw data re-import
        in SECONDARY so that these jobs cannot be considered for the watermark for the Ingest DAG.
        """
        with SessionFactory.using_database(self.database_key) as session:
            session.query(schema.DirectIngestDataflowJob).filter(
                schema.DirectIngestDataflowJob.region_code == state_code.value,
                schema.DirectIngestDataflowJob.ingest_instance == ingest_instance.value,
            ).update(
                {schema.DirectIngestDataflowJob.is_invalidated: True},
                synchronize_session=False,
            )

    @environment.test_only
    def add_job(
        self,
        job_id: str,
        location: str,
        state_code: StateCode,
        ingest_instance: DirectIngestInstance,
        completion_time: datetime.datetime = datetime.datetime.now(),
        is_invalidated: bool = False,
    ) -> DataflowJobLocationID:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(
                schema.DirectIngestDataflowJob(
                    job_id=job_id,
                    region_code=state_code.value,
                    location=location,
                    ingest_instance=ingest_instance.value,
                    completion_time=completion_time,
                    is_invalidated=is_invalidated,
                )
            )
        return location, job_id
