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
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.session_factory import SessionFactory
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
        most_recent_job = self.job_manager.add_job(
            job_id="2020-10-01_00_00_00",
            state_code=StateCode.US_XX,
            location="us-east4",
            ingest_instance=DirectIngestInstance.PRIMARY,
            pipeline_type=IngestPipelineType.ACTIVITY,
            completion_time=datetime.datetime(2020, 10, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )
        # earlier
        _earlier_job = self.job_manager.add_job(
            job_id="2020-09-01_00_00_00",
            state_code=StateCode.US_XX,
            location="us-east2",
            ingest_instance=DirectIngestInstance.PRIMARY,
            pipeline_type=IngestPipelineType.ACTIVITY,
            completion_time=datetime.datetime(2020, 9, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )
        # earlier and invalidated
        _invalidated_job = self.job_manager.add_job(
            job_id="2020-09-01_00_00_00-invalidated",
            state_code=StateCode.US_XX,
            location="us-east1",
            ingest_instance=DirectIngestInstance.PRIMARY,
            pipeline_type=IngestPipelineType.ACTIVITY,
            completion_time=datetime.datetime(2020, 9, 1, 0, 0, 0, tzinfo=pytz.UTC),
            is_invalidated=True,
        )

        # other instance
        other_instance_job = self.job_manager.add_job(
            job_id="2020-12-01_01_01-other_instance",
            state_code=StateCode.US_XX,
            location="us-east4",
            ingest_instance=DirectIngestInstance.SECONDARY,
            pipeline_type=IngestPipelineType.ACTIVITY,
            completion_time=datetime.datetime(2020, 12, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )

        # other region
        other_region_job = self.job_manager.add_job(
            job_id="2020-12-01_01_01-other_region",
            state_code=StateCode.US_YY,
            location="us-east4",
            ingest_instance=DirectIngestInstance.PRIMARY,
            pipeline_type=IngestPipelineType.ACTIVITY,
            completion_time=datetime.datetime(2020, 12, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )

        # Act
        actual = self.job_manager.get_most_recent_job(
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
            pipeline_type=IngestPipelineType.ACTIVITY,
        )

        # Assert
        self.assertEqual(actual, most_recent_job)

        # Act 2
        all_jobs = (
            self.job_manager.get_most_recent_jobs_location_and_id_by_state_and_instance(
                pipeline_type=IngestPipelineType.ACTIVITY
            )
        )

        # Assert 2
        expected = {
            StateCode.US_YY: {DirectIngestInstance.PRIMARY: other_region_job},
            StateCode.US_XX: {
                DirectIngestInstance.PRIMARY: most_recent_job,
                DirectIngestInstance.SECONDARY: other_instance_job,
            },
        }

        self.assertEqual(expected, all_jobs)

    def test_get_latest_job_doesnt_exist(self) -> None:
        # other instance
        other_instance_job = self.job_manager.add_job(
            job_id="2020-12-01_01_01-other_instance",
            state_code=StateCode.US_XX,
            location="us-east4",
            ingest_instance=DirectIngestInstance.SECONDARY,
            pipeline_type=IngestPipelineType.ACTIVITY,
            completion_time=datetime.datetime(2020, 12, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )

        # Act
        actual = self.job_manager.get_most_recent_job(
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
            pipeline_type=IngestPipelineType.ACTIVITY,
        )

        # Assert
        self.assertEqual(actual, None)

        # Act 2
        all_jobs = (
            self.job_manager.get_most_recent_jobs_location_and_id_by_state_and_instance(
                pipeline_type=IngestPipelineType.ACTIVITY
            )
        )

        # Assert 2
        expected = {
            StateCode.US_XX: {
                DirectIngestInstance.SECONDARY: other_instance_job,
            },
        }

        self.assertEqual(expected, all_jobs)

    def test_filters_by_pipeline_type(self) -> None:
        activity_job = self.job_manager.add_job(
            job_id="2024-10-01_00_00_00-activity",
            state_code=StateCode.US_XX,
            location="us-east1",
            ingest_instance=DirectIngestInstance.PRIMARY,
            pipeline_type=IngestPipelineType.ACTIVITY,
            completion_time=datetime.datetime(2024, 10, 1, 0, 0, 0, tzinfo=pytz.UTC),
        )
        identity_job = self.job_manager.add_job(
            job_id="2024-10-02_00_00_00-identity",
            state_code=StateCode.US_XX,
            location="us-east1",
            ingest_instance=DirectIngestInstance.PRIMARY,
            pipeline_type=IngestPipelineType.IDENTITY,
            completion_time=datetime.datetime(2024, 10, 2, 0, 0, 0, tzinfo=pytz.UTC),
        )

        # get_most_recent_job returns only the job for the requested pipeline_type
        self.assertEqual(
            activity_job,
            self.job_manager.get_most_recent_job(
                state_code=StateCode.US_XX,
                ingest_instance=DirectIngestInstance.PRIMARY,
                pipeline_type=IngestPipelineType.ACTIVITY,
            ),
        )
        self.assertEqual(
            identity_job,
            self.job_manager.get_most_recent_job(
                state_code=StateCode.US_XX,
                ingest_instance=DirectIngestInstance.PRIMARY,
                pipeline_type=IngestPipelineType.IDENTITY,
            ),
        )

        # get_most_recent_jobs_location_and_id_by_state_and_instance filters too
        self.assertEqual(
            {StateCode.US_XX: {DirectIngestInstance.PRIMARY: activity_job}},
            self.job_manager.get_most_recent_jobs_location_and_id_by_state_and_instance(
                pipeline_type=IngestPipelineType.ACTIVITY
            ),
        )
        self.assertEqual(
            {StateCode.US_XX: {DirectIngestInstance.PRIMARY: identity_job}},
            self.job_manager.get_most_recent_jobs_location_and_id_by_state_and_instance(
                pipeline_type=IngestPipelineType.IDENTITY
            ),
        )

        # invalidate_all_dataflow_jobs only affects rows for the requested pipeline_type
        self.job_manager.invalidate_all_dataflow_jobs(
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
            pipeline_type=IngestPipelineType.ACTIVITY,
        )
        _, activity_job_id = activity_job
        _, identity_job_id = identity_job
        with SessionFactory.using_database(self.job_manager.database_key) as session:
            invalidated_by_job_id = {
                row.job_id: row.is_invalidated
                for row in session.query(schema.DirectIngestDataflowJob).all()
            }
        self.assertTrue(invalidated_by_job_id[activity_job_id])
        self.assertFalse(invalidated_by_job_id[identity_job_id])
