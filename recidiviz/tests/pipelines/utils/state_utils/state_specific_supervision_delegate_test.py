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
"""Unit tests for state_specific_supervision_delegate default functions"""
import unittest

from parameterized import parameterized

from recidiviz.calculator.query.state.views.reference.supervision_location_ids_to_names import (
    SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_NAME,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.utils.state_utils import state_calculation_config_manager
from recidiviz.pipelines.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_supervision_delegate import (
    UsXxSupervisionDelegate,
)


class TestStateSpecificSupervisionDelegate(unittest.TestCase):
    """Unit tests for state_specific_supervision_delegate default function implementations."""

    def setUp(self) -> None:
        self.default_supervision_delegate = UsXxSupervisionDelegate()

    def test_no_new_supervision_location_from_supervision_site_override(self) -> None:
        allowed_override_states = [StateCode.US_PA]
        for state_code in StateCode:
            if state_code in allowed_override_states:
                continue
            try:
                # pylint: disable=protected-access
                delegate = state_calculation_config_manager.get_state_specific_supervision_delegate(
                    state_code.value
                )
                if (
                    delegate.supervision_location_from_supervision_site
                    is not StateSpecificSupervisionDelegate.supervision_location_from_supervision_site
                ):
                    raise ValueError(
                        f"Found override of supervision_location_from_supervision_site() "
                        f"on the StateSpecificSupervisionDelegate for state "
                        f"[{state_code.value}]. States should make every possible "
                        f"effort to avoid state-specific logic here. For most states, "
                        f"the level_2_supervision_location_external_id is derived in "
                        f"the most_recent_dataflow_metrics_* views by joining with the "
                        f"{SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_NAME} view."
                    )
            except ValueError:
                # No delegate for this state
                continue

    def test_supervision_location_from_supervision_site(self) -> None:
        (
            level_1,
            level_2,
        ) = self.default_supervision_delegate.supervision_location_from_supervision_site(
            "1"
        )
        self.assertEqual(level_1, "1")
        self.assertEqual(level_2, None)

    @parameterized.expand(
        [
            ("low", 19, "0-23"),
            ("med", 27, "24-29"),
            ("high", 30, "30-38"),
            ("max", 39, "39+"),
        ]
    )
    def test_lsir_score_bucket(self, _name: str, score: int, bucket: str) -> None:
        self.assertEqual(
            self.default_supervision_delegate.set_lsir_assessment_score_bucket(
                assessment_score=score,
                assessment_level=None,
            ),
            bucket,
        )
