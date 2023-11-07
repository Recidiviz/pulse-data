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
"""Handles reading metadata about the raw tables used in an ingest pipeline."""

import datetime
from typing import Dict

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.utils import environment


class DirectIngestDataflowWatermarkManager:
    """Reads metadata about the raw data watermarks for each ingest pipeline run."""

    def __init__(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)

    def get_raw_data_watermarks_for_latest_run(
        self, state_code: StateCode, ingest_instance: DirectIngestInstance
    ) -> Dict[str, datetime.datetime]:
        with SessionFactory.using_database(self.database_key) as session:
            # Note: The data in `state` will reflect the output of the most recent job,
            # even if that job has been invalidated, so we include invalidated jobs here
            latest_job = (
                session.query(schema.DirectIngestDataflowJob.job_id)
                .filter(
                    schema.DirectIngestDataflowJob.region_code == state_code.value,
                    schema.DirectIngestDataflowJob.ingest_instance
                    == ingest_instance.value,
                )
                .order_by(schema.DirectIngestDataflowJob.completion_time.desc())
                .limit(1)
                .scalar_subquery()
            )

            results = (
                session.query(
                    schema.DirectIngestDataflowRawTableUpperBounds.raw_data_file_tag,
                    schema.DirectIngestDataflowRawTableUpperBounds.watermark_datetime,
                )
                .filter(
                    schema.DirectIngestDataflowRawTableUpperBounds.job_id == latest_job,
                )
                .all()
            )
            return {
                result.raw_data_file_tag: result.watermark_datetime
                for result in results
            }

    @environment.test_only
    def add_raw_data_watermark(
        self,
        job_id: str,
        state_code: StateCode,
        raw_data_file_tag: str,
        watermark_datetime: datetime.datetime,
    ) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(
                schema.DirectIngestDataflowRawTableUpperBounds(
                    job_id=job_id,
                    region_code=state_code.value,
                    raw_data_file_tag=raw_data_file_tag,
                    watermark_datetime=watermark_datetime,
                )
            )

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
