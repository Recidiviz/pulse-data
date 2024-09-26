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
"""Unit tests for dag orchestration utils"""
import unittest
from unittest.mock import patch

from recidiviz.airflow.dags.utils.dag_orchestration_utils import (
    get_raw_data_dag_enabled_state_and_instance_pairs,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


class TestIngestDagOrchestrationUtils(unittest.TestCase):
    """Tests for ingest dag orchestration utils."""

    def setUp(self) -> None:
        self.get_existing_states_patcher = patch(
            "recidiviz.airflow.dags.utils.dag_orchestration_utils.get_direct_ingest_states_existing_in_env",
            return_value={
                # Has views, environment=production, has env variables in mappings
                StateCode.US_DD,
                # Has views, environment=staging
                StateCode.US_XX,
                # Views gated to secondary only, environment=staging, has env variables in mappings
                StateCode.US_YY,
                # No views, environment=staging
                StateCode.US_WW,
            },
        )
        self.get_existing_states_patcher.start()
        self.raw_data_dag_enabled_patcher = patch(
            "recidiviz.airflow.dags.utils.dag_orchestration_utils.is_raw_data_import_dag_enabled",
        )
        self.raw_data_dag_enabled_mock = self.raw_data_dag_enabled_patcher.start()

    def tearDown(self) -> None:
        self.get_existing_states_patcher.stop()
        self.raw_data_dag_enabled_patcher.stop()

    def test_get_raw_data_dag_enabled_state_and_instance_pairs(self) -> None:
        self.raw_data_dag_enabled_mock.return_value = True
        result = get_raw_data_dag_enabled_state_and_instance_pairs()

        self.assertSetEqual(
            set(result),
            {
                (StateCode.US_DD, DirectIngestInstance.PRIMARY),
                (StateCode.US_DD, DirectIngestInstance.SECONDARY),
                (StateCode.US_XX, DirectIngestInstance.PRIMARY),
                (StateCode.US_XX, DirectIngestInstance.SECONDARY),
                (StateCode.US_YY, DirectIngestInstance.PRIMARY),
                (StateCode.US_YY, DirectIngestInstance.SECONDARY),
                (StateCode.US_WW, DirectIngestInstance.PRIMARY),
                (StateCode.US_WW, DirectIngestInstance.SECONDARY),
            },
        )
        enabled_states = {
            (StateCode.US_DD, DirectIngestInstance.PRIMARY),
            (StateCode.US_DD, DirectIngestInstance.SECONDARY),
            (StateCode.US_XX, DirectIngestInstance.SECONDARY),
        }
        self.raw_data_dag_enabled_mock.side_effect = (
            lambda state, instance: (state, instance) in enabled_states
        )
        result = get_raw_data_dag_enabled_state_and_instance_pairs()

        self.assertSetEqual(set(result), enabled_states)
