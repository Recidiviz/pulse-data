#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2025 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests for workflows_opportunity_configs.py"""
from unittest import TestCase

from recidiviz.calculator.query.state.views.outliers.workflows_enabled_states import (
    get_workflows_enabled_states,
)
from recidiviz.calculator.query.state.views.reference.workflows_opportunity_configs import (
    WORKFLOWS_OPPORTUNITY_CONFIGS,
)


class TestWorkflowsOpportunityConfigs(TestCase):
    """Tests for workflows_opportunity_configs"""

    def test_enabled_states(self) -> None:
        enabled_states = get_workflows_enabled_states()
        for opp in WORKFLOWS_OPPORTUNITY_CONFIGS:
            state_code_str = opp.state_code.name
            if state_code_str == "US_IX":
                state_code_str = "US_ID"
            self.assertIn(
                state_code_str,
                enabled_states,
                f"Opportunity {opp.opportunity_type} configured for {state_code_str}, which is not in workflows_enabled_states.yaml",
            )
