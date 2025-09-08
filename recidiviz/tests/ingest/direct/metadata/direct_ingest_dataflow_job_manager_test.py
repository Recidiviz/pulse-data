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

import pytest
import pytz

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.metadata.direct_ingest_dataflow_job_manager import (
    DirectIngestDataflowJobManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult


@pytest.mark.uses_db
class DirectIngestDataflowJobManagerTest(unittest.TestCase):
    """Tests for DirectIngestDataflowWatermarkManager"""

    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    def setUp(self) -> None:
        self.job_manager = DirectIngestDataflowJobManager()
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.job_manager.database_key
        )

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.job_manager.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def test_get_latest_job(self) -> None:
        # most recent job
        most_recent_job_id = "2020-10-01_00_00_00"
        self.job_manager.add_job(
            job_id=most_recent_job_id,
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
            completion_time=datetime.datetime(2020, 10, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )
        # earlier
        earlier_job_id = "2020-09-01_00_00_00"
        self.job_manager.add_job(
            job_id=earlier_job_id,
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
            completion_time=datetime.datetime(2020, 9, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )
        # earlier and invalidated
        invalidated_job_id = "2020-09-01_00_00_00-invalidated"
        self.job_manager.add_job(
            job_id=invalidated_job_id,
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
            completion_time=datetime.datetime(2020, 9, 1, 0, 0, 0, tzinfo=pytz.UTC),
            is_invalidated=True,
        )

        # other instance
        other_instance_job_id = "2020-12-01_01_01-other_instance"
        self.job_manager.add_job(
            job_id=other_instance_job_id,
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.SECONDARY,
            completion_time=datetime.datetime(2020, 12, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )

        # other region
        other_region_job_id = "2020-12-01_01_01-other_region"
        self.job_manager.add_job(
            job_id=other_region_job_id,
            state_code=StateCode.US_YY,
            ingest_instance=DirectIngestInstance.PRIMARY,
            completion_time=datetime.datetime(2020, 12, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )

        # Act
        actual = self.job_manager.get_job_id_for_most_recent_job(
            state_code=StateCode.US_XX, ingest_instance=DirectIngestInstance.PRIMARY
        )

        # Assert
        self.assertEqual(actual, most_recent_job_id)

        # Act 2
        all_jobs = self.job_manager.get_most_recent_job_ids_by_state_and_instance()

        # Assert 2
        expected = {
            StateCode.US_YY: {DirectIngestInstance.PRIMARY: other_region_job_id},
            StateCode.US_XX: {
                DirectIngestInstance.PRIMARY: most_recent_job_id,
                DirectIngestInstance.SECONDARY: other_instance_job_id,
            },
        }

        self.assertEqual(expected, all_jobs)

    def test_get_latest_job_doesnt_exist(self) -> None:
        # other instance
        other_instance_job_id = "2020-12-01_01_01-other_instance"
        self.job_manager.add_job(
            job_id=other_instance_job_id,
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.SECONDARY,
            completion_time=datetime.datetime(2020, 12, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )

        # Act
        actual = self.job_manager.get_job_id_for_most_recent_job(
            state_code=StateCode.US_XX, ingest_instance=DirectIngestInstance.PRIMARY
        )

        # Assert
        self.assertEqual(actual, None)

        # Act 2
        all_jobs = self.job_manager.get_most_recent_job_ids_by_state_and_instance()

        # Assert 2
        expected = {
            StateCode.US_XX: {
                DirectIngestInstance.SECONDARY: other_instance_job_id,
            },
        }

        self.assertEqual(expected, all_jobs)
