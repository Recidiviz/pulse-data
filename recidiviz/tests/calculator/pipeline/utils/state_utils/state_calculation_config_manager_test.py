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
"""Tests the functions in the state_calculation_config_manager file."""
import unittest
from datetime import date
from typing import Optional

from recidiviz.calculator.pipeline.supervision.events import SupervisionPopulationEvent
from recidiviz.calculator.pipeline.utils.state_utils import (
    state_calculation_config_manager,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)


class TestSupervisionPeriodIsOutOfState(unittest.TestCase):
    """Tests the state-specific supervision_period_is_out_of_state function."""

    def test_supervision_period_is_out_of_state_us_id_with_identifier(self) -> None:
        self.assertTrue(
            state_calculation_config_manager.supervision_period_is_out_of_state(
                self.create_population_event(
                    "US_ID", "INTERSTATE PROBATION - remainder of identifier"
                )
            )
        )

    def test_supervision_period_is_out_of_state_us_id_with_incorrect_identifier(
        self,
    ) -> None:
        self.assertFalse(
            state_calculation_config_manager.supervision_period_is_out_of_state(
                self.create_population_event(
                    "US_ID", "Incorrect - remainder of identifier"
                )
            )
        )

    def test_supervision_period_is_out_of_state_us_id_with_empty_identifier(
        self,
    ) -> None:
        self.assertFalse(
            state_calculation_config_manager.supervision_period_is_out_of_state(
                self.create_population_event("US_ID", None)
            )
        )

    def test_supervision_period_is_out_of_state_us_mo_with_identifier(self) -> None:
        self.assertFalse(
            state_calculation_config_manager.supervision_period_is_out_of_state(
                self.create_population_event(
                    "US_MO", "INTERSTATE PROBATION - remainder of identifier"
                )
            )
        )

    def test_supervision_period_is_out_of_state_us_mo_with_incorrect_identifier(
        self,
    ) -> None:
        self.assertFalse(
            state_calculation_config_manager.supervision_period_is_out_of_state(
                self.create_population_event(
                    "US_MO", "Incorrect - remainder of identifier"
                )
            )
        )

    @staticmethod
    def create_population_event(
        state_code: str, supervising_district_external_id: Optional[str]
    ) -> SupervisionPopulationEvent:
        return SupervisionPopulationEvent(
            state_code=state_code,
            year=2010,
            month=1,
            event_date=date(2010, 1, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervising_district_external_id=supervising_district_external_id,
            projected_end_date=None,
        )
