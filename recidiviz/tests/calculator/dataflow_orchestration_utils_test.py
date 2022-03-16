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
"""Tests for dataflow_orchestration_utils.py."""
import os
import unittest

import mock

from recidiviz.calculator.dataflow_orchestration_utils import (
    get_metric_pipeline_enabled_states,
)
from recidiviz.common.constants.states import StateCode

FAKE_PIPELINE_CONFIG_YAML_PATH = os.path.join(
    os.path.dirname(__file__),
    "fake_calculation_pipeline_templates.yaml",
)


class DataflowOrchestrationConfigTest(unittest.TestCase):
    """Tests for dataflow_orchestration_utils.py."""

    @mock.patch(
        "recidiviz.calculator.dataflow_orchestration_utils.PIPELINE_CONFIG_YAML_PATH",
        FAKE_PIPELINE_CONFIG_YAML_PATH,
    )
    def test_get_metric_pipeline_enabled_states(self) -> None:
        states = get_metric_pipeline_enabled_states()

        expected_states = {StateCode.US_XX, StateCode.US_YY}

        self.assertCountEqual(expected_states, states)
