# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for ingest_pipeline_should_run_in_dag."""

import unittest
from unittest.mock import MagicMock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.entrypoints.ingest.ingest_pipeline_should_run_in_dag import (
    ingest_pipeline_should_run_in_dag,
)
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING


class TestIngestDagOrchestrationUtils(unittest.TestCase):
    """

    Tests for ingest dag orchestration utils.

    StateCode.US_DD: Has views, environment=production, has env variables in mappings
    StateCode.US_XX: Has views, environment=staging
    StateCode.US_YY: Views gated to secondary only, environment=staging, has env variables in mappings
    StateCode.US_WW: No views, environment=staging
    """

    def setUp(self) -> None:
        self.direct_ingest_regions_patcher = patch(
            "recidiviz.entrypoints.ingest.ingest_pipeline_should_run_in_dag.direct_ingest_regions",
            autospec=True,
        )
        self.mock_direct_ingest_regions = self.direct_ingest_regions_patcher.start()
        self.mock_direct_ingest_regions.get_direct_ingest_region.side_effect = (
            lambda region_code: get_direct_ingest_region(
                region_code, region_module_override=fake_regions_module
            )
        )

    def tearDown(self) -> None:
        self.direct_ingest_regions_patcher.stop()

    @patch(
        "recidiviz.utils.metadata.project_id",
        return_value=GCP_PROJECT_STAGING,
    )
    def test_ingest_pipeline_should_run_in_dag(
        self, _mock_project_id: MagicMock
    ) -> None:
        self.assertTrue(ingest_pipeline_should_run_in_dag(StateCode.US_XX))
        self.assertTrue(ingest_pipeline_should_run_in_dag(StateCode.US_YY))
        self.assertTrue(ingest_pipeline_should_run_in_dag(StateCode.US_DD))
        self.assertFalse(ingest_pipeline_should_run_in_dag(StateCode.US_WW))

    @patch(
        "recidiviz.utils.metadata.project_id",
        return_value=GCP_PROJECT_PRODUCTION,
    )
    def test_ingest_pipeline_should_run_in_dag_only_us_dd_launched_in_env(
        self, _mock_projec_id: MagicMock
    ) -> None:
        self.assertFalse(ingest_pipeline_should_run_in_dag(StateCode.US_XX))
        self.assertFalse(ingest_pipeline_should_run_in_dag(StateCode.US_XX))
        self.assertFalse(ingest_pipeline_should_run_in_dag(StateCode.US_YY))
        self.assertFalse(ingest_pipeline_should_run_in_dag(StateCode.US_YY))
        self.assertTrue(ingest_pipeline_should_run_in_dag(StateCode.US_DD))
        self.assertTrue(ingest_pipeline_should_run_in_dag(StateCode.US_DD))
        self.assertFalse(ingest_pipeline_should_run_in_dag(StateCode.US_WW))
        self.assertFalse(ingest_pipeline_should_run_in_dag(StateCode.US_WW))
