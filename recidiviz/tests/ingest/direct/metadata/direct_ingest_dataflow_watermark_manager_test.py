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
"""Tests for the dataflow ingest watermark manager"""

import datetime
import unittest
from typing import Optional

import pytest
import pytz

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.metadata.direct_ingest_dataflow_watermark_manager import (
    DirectIngestDataflowWatermarkManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers


@pytest.mark.uses_db
class DirectIngestDataflowWatermarkManagerTest(unittest.TestCase):
    """Tests for DirectIngestDataflowWatermarkManager"""

    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.watermark_manager = DirectIngestDataflowWatermarkManager()
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.watermark_manager.database_key
        )

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.watermark_manager.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def add_job(
        self,
        job_id: str,
        state_code: StateCode,
        ingest_instance: DirectIngestInstance,
        completion_time: datetime.datetime = datetime.datetime.now(),
        is_invalidated: bool = False,
    ) -> None:
        with SessionFactory.using_database(
            self.watermark_manager.database_key
        ) as session:
            session.add(
                schema.DirectIngestDataflowJob(
                    job_id=job_id,
                    region_code=state_code.value,
                    ingest_instance=ingest_instance.value,
                    completion_time=completion_time,
                    is_invalidated=is_invalidated,
                )
            )

    def test_get_raw_data_watermarks_for_latest_run(self) -> None:
        # most recent job
        most_recent_job_id = "2020-10-01_00_00_00"
        self.add_job(
            job_id=most_recent_job_id,
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
            completion_time=datetime.datetime(2020, 10, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )
        self.watermark_manager.add_raw_data_watermark(
            job_id=most_recent_job_id,
            state_code=StateCode.US_XX,
            raw_data_file_tag="normal",
            watermark_datetime=datetime.datetime(
                2020, 7, 1, 12, 0, 30, tzinfo=pytz.UTC
            ),
        )
        self.watermark_manager.add_raw_data_watermark(
            job_id=most_recent_job_id,
            state_code=StateCode.US_XX,
            raw_data_file_tag="multiple",
            watermark_datetime=datetime.datetime(2020, 8, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )
        # earlier
        earlier_job_id = "2020-09-01_00_00_00"
        self.add_job(
            job_id=earlier_job_id,
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
            completion_time=datetime.datetime(2020, 9, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )
        self.watermark_manager.add_raw_data_watermark(
            job_id=earlier_job_id,
            state_code=StateCode.US_XX,
            raw_data_file_tag="multiple",
            watermark_datetime=datetime.datetime(2020, 7, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )
        # earlier and invalidated
        invalidated_job_id = "2020-09-01_00_00_00-invalidated"
        self.add_job(
            job_id=invalidated_job_id,
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
            completion_time=datetime.datetime(2020, 9, 1, 0, 0, 0, tzinfo=pytz.UTC),
            is_invalidated=True,
        )
        self.watermark_manager.add_raw_data_watermark(
            job_id=invalidated_job_id,
            state_code=StateCode.US_XX,
            raw_data_file_tag="multiple",
            watermark_datetime=datetime.datetime(2020, 9, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )
        # other instance
        other_instance_job_id = "2020-12-01_01_01-other_instance"
        self.add_job(
            job_id=other_instance_job_id,
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.SECONDARY,
            completion_time=datetime.datetime(2020, 12, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )
        self.watermark_manager.add_raw_data_watermark(
            job_id=other_instance_job_id,
            state_code=StateCode.US_XX,
            raw_data_file_tag="other_instance",
            watermark_datetime=datetime.datetime(
                2020, 7, 1, 12, 0, 30, tzinfo=pytz.UTC
            ),
        )
        # other region
        other_region_job_id = "2020-12-01_01_01-other_region"
        self.add_job(
            job_id=other_region_job_id,
            state_code=StateCode.US_YY,
            ingest_instance=DirectIngestInstance.PRIMARY,
            completion_time=datetime.datetime(2020, 12, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )
        self.watermark_manager.add_raw_data_watermark(
            job_id=other_region_job_id,
            state_code=StateCode.US_YY,
            raw_data_file_tag="other_region",
            watermark_datetime=datetime.datetime(
                2020, 7, 1, 12, 0, 30, tzinfo=pytz.UTC
            ),
        )

        # Act
        actual = self.watermark_manager.get_raw_data_watermarks_for_latest_run(
            state_code=StateCode.US_XX, ingest_instance=DirectIngestInstance.PRIMARY
        )

        # Assert
        self.assertDictEqual(
            actual,
            {
                "normal": datetime.datetime(2020, 7, 1, 12, 0, 30, tzinfo=pytz.UTC),
                "multiple": datetime.datetime(2020, 8, 1, 0, 0, 0, tzinfo=pytz.UTC),
            },
        )

    def test_get_raw_data_watermarks_for_latest_run_includes_invalidated(self) -> None:
        # initial job
        initial_job_id = "2020-10-01_00_00_00"
        self.add_job(
            job_id=initial_job_id,
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
            completion_time=datetime.datetime(2020, 10, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )
        self.watermark_manager.add_raw_data_watermark(
            job_id=initial_job_id,
            state_code=StateCode.US_XX,
            raw_data_file_tag="normal",
            watermark_datetime=datetime.datetime(
                2020, 7, 1, 12, 0, 30, tzinfo=pytz.UTC
            ),
        )
        self.watermark_manager.add_raw_data_watermark(
            job_id=initial_job_id,
            state_code=StateCode.US_XX,
            raw_data_file_tag="multiple",
            watermark_datetime=datetime.datetime(2020, 8, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )
        # invalidated
        invalidated_job_id = "2020-12-01_00_00_00-invalidated"
        self.add_job(
            job_id=invalidated_job_id,
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
            completion_time=datetime.datetime(2020, 12, 1, 0, 0, 0, tzinfo=pytz.UTC),
            is_invalidated=True,
        )
        self.watermark_manager.add_raw_data_watermark(
            job_id=invalidated_job_id,
            state_code=StateCode.US_XX,
            raw_data_file_tag="multiple",
            watermark_datetime=datetime.datetime(2020, 12, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )
        self.watermark_manager.add_raw_data_watermark(
            job_id=invalidated_job_id,
            state_code=StateCode.US_XX,
            raw_data_file_tag="invalidated",
            watermark_datetime=datetime.datetime(2020, 12, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )

        # Act
        actual = self.watermark_manager.get_raw_data_watermarks_for_latest_run(
            state_code=StateCode.US_XX, ingest_instance=DirectIngestInstance.PRIMARY
        )

        # Assert
        self.assertDictEqual(
            actual,
            {
                "invalidated": datetime.datetime(2020, 12, 1, 0, 0, 0, tzinfo=pytz.UTC),
                "multiple": datetime.datetime(2020, 12, 1, 0, 0, 0, tzinfo=pytz.UTC),
            },
        )
