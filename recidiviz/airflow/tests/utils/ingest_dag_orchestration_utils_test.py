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
"""Unit tests for ingest dag orchestration utils"""
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.airflow.dags.utils.ingest_dag_orchestration_utils import (
    get_all_enabled_state_and_instance_pairs,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module


class TestIngestDagOrchestrationUtils(unittest.TestCase):
    """Tests for ingest dag orchestration utils."""

    def setUp(self) -> None:
        self.get_ingest_pipeline_enabled_states_patcher = patch(
            "recidiviz.airflow.dags.utils.ingest_dag_orchestration_utils.get_ingest_pipeline_enabled_states",
            return_value={
                # Has views, environment=production
                StateCode.US_DD,
                # Has views, environment=staging
                StateCode.US_XX,
                # Views gated to secondary only, environment=staging
                StateCode.US_YY,
                # No views, environment=staging
                StateCode.US_WW,
            },
        )
        self.get_ingest_pipeline_enabled_states_patcher.start()

        self.direct_ingest_regions_patcher = patch(
            "recidiviz.airflow.dags.utils.ingest_dag_orchestration_utils.direct_ingest_regions",
            autospec=True,
        )
        self.mock_direct_ingest_regions = self.direct_ingest_regions_patcher.start()
        self.mock_direct_ingest_regions.get_direct_ingest_region.side_effect = (
            lambda region_code: get_direct_ingest_region(
                region_code, region_module_override=fake_regions_module
            )
        )

        self.is_ingest_in_dataflow_enabled_patcher = patch(
            "recidiviz.airflow.dags.utils.ingest_dag_orchestration_utils.is_ingest_in_dataflow_enabled",
            return_value=True,
        )
        self.mock_is_ingest_in_dataflow_enabled = (
            self.is_ingest_in_dataflow_enabled_patcher.start()
        )

    def tearDown(self) -> None:
        self.get_ingest_pipeline_enabled_states_patcher.stop()
        self.direct_ingest_regions_patcher.stop()
        self.is_ingest_in_dataflow_enabled_patcher.stop()

    def test_get_all_enabled_state_and_instance_pairs(self) -> None:
        result = get_all_enabled_state_and_instance_pairs()

        self.assertSetEqual(
            set(result),
            {
                (StateCode.US_DD, DirectIngestInstance.PRIMARY),
                # TODO(#23242): This row should go away once should_run_secondary_ingest_pipeline is properly implemented
                (StateCode.US_DD, DirectIngestInstance.SECONDARY),
                (StateCode.US_XX, DirectIngestInstance.PRIMARY),
                # TODO(#23242): This row should go away once should_run_secondary_ingest_pipeline is properly implemented
                (StateCode.US_XX, DirectIngestInstance.SECONDARY),
                (StateCode.US_YY, DirectIngestInstance.SECONDARY),
            },
        )

    @patch(
        "recidiviz.ingest.direct.direct_ingest_regions.environment.get_gcp_environment",
        return_value="production",
    )
    @patch(
        "recidiviz.ingest.direct.direct_ingest_regions.environment.in_gcp_production",
        return_value=True,
    )
    def test_get_all_enabled_state_and_instance_pairs_only_us_xx_launched_in_env(
        self, _mock_in_gcp_production: MagicMock, _mock_get_gcp_environment: MagicMock
    ) -> None:
        result = get_all_enabled_state_and_instance_pairs()

        self.assertSetEqual(
            set(result),
            {
                (StateCode.US_DD, DirectIngestInstance.PRIMARY),
                # TODO(#23242): This row should go away once should_run_secondary_ingest_pipeline is properly implemented
                (StateCode.US_DD, DirectIngestInstance.SECONDARY),
            },
        )

    def test_get_all_enabled_state_and_instance_pairs_ingest_in_dataflow_not_enabled(
        self,
    ) -> None:
        self.mock_is_ingest_in_dataflow_enabled.side_effect = (
            lambda state_code, ingest_instance: state_code != StateCode.US_DD
        )

        result = get_all_enabled_state_and_instance_pairs()

        self.assertSetEqual(
            set(result),
            {
                (StateCode.US_XX, DirectIngestInstance.PRIMARY),
                # TODO(#23242): This row should go away once should_run_secondary_ingest_pipeline is properly implemented
                (StateCode.US_XX, DirectIngestInstance.SECONDARY),
                (StateCode.US_YY, DirectIngestInstance.SECONDARY),
            },
        )
