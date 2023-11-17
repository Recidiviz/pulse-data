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
from typing import Dict, Optional

from sqlalchemy import func

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.utils import environment


class DirectIngestDataflowJobManager:
    """Reads metadata about ingest pipeline jobs."""

    def __init__(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)

    def get_most_recent_job_ids_by_state_and_instance(
        self,
    ) -> Dict[StateCode, Dict[DirectIngestInstance, str]]:
        """Returns a map with the job_id for the most recent successful ingest Dataflow
        pipeline for each state_code and ingest instance that has ever had a successful
        pipeline complete.
        """
        with SessionFactory.using_database(self.database_key) as session:
            subquery = session.query(
                schema.DirectIngestDataflowJob.region_code,
                schema.DirectIngestDataflowJob.ingest_instance,
                schema.DirectIngestDataflowJob.job_id,
                func.row_number()
                .over(
                    partition_by=[
                        schema.DirectIngestDataflowJob.region_code,
                        schema.DirectIngestDataflowJob.ingest_instance,
                    ],
                    order_by=schema.DirectIngestDataflowJob.completion_time.desc(),
                )
                .label("row_num"),
            ).subquery()
            results = session.query(subquery).filter(subquery.c.row_num == 1).all()

            jobs: Dict[StateCode, Dict[DirectIngestInstance, str]] = {}
            for region_code_str, instance_str, job_id, _rn in results:
                state_code = StateCode(region_code_str)
                instance = DirectIngestInstance[instance_str]
                if state_code not in jobs:
                    jobs[state_code] = {}
                jobs[state_code][instance] = job_id
            return jobs

    def get_job_id_for_most_recent_job(
        self, state_code: StateCode, ingest_instance: DirectIngestInstance
    ) -> Optional[str]:
        """For the provided state_code and ingest_instance, returns the ingest pipline
        Dataflow job id for the most recent successful run.
        """
        all_most_recent = self.get_most_recent_job_ids_by_state_and_instance()
        if state_code not in all_most_recent:
            return None

        return all_most_recent[state_code].get(ingest_instance)

    @environment.test_only
    def add_job(
        self,
        job_id: str,
        state_code: StateCode,
        ingest_instance: DirectIngestInstance,
        completion_time: datetime.datetime = datetime.datetime.now(),
        is_invalidated: bool = False,
    ) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(
                schema.DirectIngestDataflowJob(
                    job_id=job_id,
                    region_code=state_code.value,
                    ingest_instance=ingest_instance.value,
                    completion_time=completion_time,
                    is_invalidated=is_invalidated,
                )
            )
