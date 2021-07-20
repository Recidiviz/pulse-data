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
from typing import Any, Dict, Optional, Tuple

import attr

from recidiviz.calculator.pipeline.supervision.events import SupervisionPopulationEvent
from recidiviz.calculator.pipeline.utils.state_utils import (
    state_calculation_config_manager,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import (
    get_state_specific_supervising_officer_and_location_info_function,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod


class TestGetSupervisingOfficerAndLocationInfoFromSupervisionPeriod(unittest.TestCase):
    """Tests the state-specific get_supervising_officer_and_location_info_from_supervision_period function."""

    DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE: StateSupervisionPeriod = StateSupervisionPeriod.new_with_defaults(
        supervision_period_id=111,
        external_id="sp1",
        status=StateSupervisionPeriodStatus.TERMINATED,
        state_code="US_XX",
        start_date=date(2017, 3, 5),
        termination_date=date(2017, 5, 9),
        supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
    )

    DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS = {
        111: {
            "agent_id": 123,
            "agent_external_id": "agent_external_id_1",
            "supervision_period_id": 111,
        }
    }

    def _get_state_specific_supervising_officer_and_location_info(
        self,
        supervision_period: StateSupervisionPeriod,
        supervision_period_to_agent_associations: Optional[
            Dict[int, Dict[str, Any]]
        ] = None,
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        return get_state_specific_supervising_officer_and_location_info_function(
            supervision_period.state_code
        )(
            supervision_period,
            (
                supervision_period_to_agent_associations
                or self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            ),
        )

    def test_get_supervising_officer_and_location_info_from_supervision_period(
        self,
    ) -> None:
        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE, supervision_site="1"
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = self._get_state_specific_supervising_officer_and_location_info(
            supervision_period
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual("1", level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_from_supervision_period_no_site(
        self,
    ) -> None:
        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = self._get_state_specific_supervising_officer_and_location_info(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_no_agent(self) -> None:
        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            supervision_period_id=666,  # No mapping for this ID
            supervision_site="1",
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = self._get_state_specific_supervising_officer_and_location_info(
            supervision_period
        )

        self.assertEqual(None, supervising_officer_external_id)
        self.assertEqual("1", level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_from_supervision_period_US_ID(
        self,
    ) -> None:
        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_ID",
            supervision_site="DISTRICT OFFICE 6, POCATELLO|UNKNOWN",
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = self._get_state_specific_supervising_officer_and_location_info(
            supervision_period
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual("UNKNOWN", level_1_supervision_location)
        self.assertEqual("DISTRICT OFFICE 6, POCATELLO", level_2_supervision_location)

        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_ID",
            supervision_site="PAROLE COMMISSION OFFICE|DEPORTED",
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = self._get_state_specific_supervising_officer_and_location_info(
            supervision_period
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual("DEPORTED", level_1_supervision_location)
        self.assertEqual("PAROLE COMMISSION OFFICE", level_2_supervision_location)

        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_ID",
            supervision_site="DISTRICT OFFICE 4, BOISE|",
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = self._get_state_specific_supervising_officer_and_location_info(
            supervision_period
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual("DISTRICT OFFICE 4, BOISE", level_2_supervision_location)

        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_ID",
            supervision_site=None,
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = self._get_state_specific_supervising_officer_and_location_info(
            supervision_period
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_from_supervision_period_US_PA(
        self,
    ) -> None:
        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_PA",
            supervision_site="CO|CO - CENTRAL OFFICE|9110",
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = self._get_state_specific_supervising_officer_and_location_info(
            supervision_period, self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual("CO - CENTRAL OFFICE", level_1_supervision_location)
        self.assertEqual("CO", level_2_supervision_location)

        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_PA",
            supervision_site=None,
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = self._get_state_specific_supervising_officer_and_location_info(
            supervision_period
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_from_supervision_period_US_ND(
        self,
    ) -> None:
        nd_supervision_period_agent_associations = {
            111: {
                "agent_id": 123,
                "agent_external_id": "agent_external_id_1",
                "supervision_period_id": 111,
            }
        }

        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_ND",
            supervision_site="SITE_1",
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = self._get_state_specific_supervising_officer_and_location_info(
            supervision_period, nd_supervision_period_agent_associations
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual("SITE_1", level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_from_supervision_period_US_MO(
        self,
    ) -> None:
        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_MO",
            supervision_site="04C",
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = self._get_state_specific_supervising_officer_and_location_info(
            supervision_period
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual("04C", level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)


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
