# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
# pylint: disable=unused-import,wrong-import-order

"""Tests for supervision_period_utils.py."""
import unittest
from datetime import date
from typing import Optional

import attr
from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.supervision.events import SupervisionPopulationEvent
from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_supervision_delegate import (
    UsXxSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_delegate import (
    UsIdSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_delegate import (
    UsMoSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_delegate import (
    UsNdSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_delegate import (
    UsPaSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.supervision_period_utils import (
    get_post_incarceration_supervision_type,
    identify_most_severe_case_type,
    supervising_officer_and_location_info,
    supervision_period_is_out_of_state,
)
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionCaseTypeEntry,
    StateSupervisionPeriod,
)


class TestIdentifyMostSevereCaseType(unittest.TestCase):
    """Tests the _identify_most_severe_case_type function."""

    def test_identify_most_severe_case_type(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                ),
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX", case_type=StateSupervisionCaseType.SEX_OFFENSE
                ),
            ],
        )

        most_severe_case_type = identify_most_severe_case_type(supervision_period)

        self.assertEqual(most_severe_case_type, StateSupervisionCaseType.SEX_OFFENSE)

    def test_identify_most_severe_case_type_test_all_types(self):
        for case_type in StateSupervisionCaseType:
            supervision_period = StateSupervisionPeriod.new_with_defaults(
                state_code="US_XX",
                case_type_entries=[
                    StateSupervisionCaseTypeEntry.new_with_defaults(
                        state_code="US_XX", case_type=case_type
                    ),
                ],
            )

            most_severe_case_type = identify_most_severe_case_type(supervision_period)

            self.assertEqual(most_severe_case_type, case_type)

    def test_identify_most_severe_case_type_no_type_entries(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            case_type_entries=[],
        )

        most_severe_case_type = identify_most_severe_case_type(supervision_period)

        self.assertEqual(most_severe_case_type, StateSupervisionCaseType.GENERAL)


DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE: StateSupervisionPeriod = (
    StateSupervisionPeriod.new_with_defaults(
        supervision_period_id=111,
        external_id="sp1",
        state_code="US_XX",
        start_date=date(2017, 3, 5),
        termination_date=date(2017, 5, 9),
        supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
    )
)

DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS = {
    111: {
        "agent_id": 123,
        "agent_external_id": "agent_external_id_1",
        "supervision_period_id": 111,
        "agent_start_date": date(2017, 3, 5),
        "agent_end_date": date(2017, 5, 9),
    }
}


class TestSupervisingOfficerAndLocationInfo(unittest.TestCase):
    """Tests the supervising_officer_and_location_info function."""

    def test_get_supervising_officer_and_location_info_from_supervision_period(
        self,
    ) -> None:
        supervision_period = attr.evolve(
            DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE, supervision_site="1"
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = supervising_officer_and_location_info(
            supervision_period,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            UsXxSupervisionDelegate(),
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
        ) = supervising_officer_and_location_info(
            DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            UsXxSupervisionDelegate(),
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_no_agent(self) -> None:
        supervision_period = attr.evolve(
            DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            supervision_period_id=666,  # No mapping for this ID
            supervision_site="1",
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = supervising_officer_and_location_info(
            supervision_period,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            UsXxSupervisionDelegate(),
        )

        self.assertEqual(None, supervising_officer_external_id)
        self.assertEqual("1", level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_from_supervision_period_us_id(
        self,
    ) -> None:
        supervision_period = attr.evolve(
            DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_ID",
            supervision_site="DISTRICT OFFICE 6, POCATELLO|UNKNOWN",
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = supervising_officer_and_location_info(
            supervision_period,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            UsIdSupervisionDelegate(),
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual("UNKNOWN", level_1_supervision_location)
        self.assertEqual("DISTRICT OFFICE 6, POCATELLO", level_2_supervision_location)

        supervision_period = attr.evolve(
            DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_ID",
            supervision_site="PAROLE COMMISSION OFFICE|DEPORTED",
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = supervising_officer_and_location_info(
            supervision_period,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            UsIdSupervisionDelegate(),
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual("DEPORTED", level_1_supervision_location)
        self.assertEqual("PAROLE COMMISSION OFFICE", level_2_supervision_location)

        supervision_period = attr.evolve(
            DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_ID",
            supervision_site="DISTRICT OFFICE 4, BOISE|",
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = supervising_officer_and_location_info(
            supervision_period,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            UsIdSupervisionDelegate(),
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual("DISTRICT OFFICE 4, BOISE", level_2_supervision_location)

        supervision_period = attr.evolve(
            DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_ID",
            supervision_site=None,
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = supervising_officer_and_location_info(
            supervision_period,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            UsIdSupervisionDelegate(),
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_from_supervision_period_us_mo(
        self,
    ) -> None:
        supervision_period = attr.evolve(
            DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_MO",
            supervision_site="04C",
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = supervising_officer_and_location_info(
            supervision_period,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            UsMoSupervisionDelegate(),
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual("04C", level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_from_supervision_period_us_nd(
        self,
    ) -> None:
        supervision_period_agent_associations = {
            111: {
                "agent_id": 123,
                "agent_external_id": "agent_external_id_1",
                "supervision_period_id": 111,
            }
        }

        supervision_period = attr.evolve(
            DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_ND",
            supervision_site="SITE_1",
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = supervising_officer_and_location_info(
            supervision_period,
            supervision_period_agent_associations,
            UsNdSupervisionDelegate(),
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual("SITE_1", level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_from_supervision_period_us_pa(
        self,
    ) -> None:
        supervision_period = attr.evolve(
            DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_PA",
            supervision_site="CO|CO - CENTRAL OFFICE|9110",
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = supervising_officer_and_location_info(
            supervision_period,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            UsPaSupervisionDelegate(),
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual("CO - CENTRAL OFFICE", level_1_supervision_location)
        self.assertEqual("CO", level_2_supervision_location)

        supervision_period = attr.evolve(
            DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_PA",
            supervision_site=None,
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = supervising_officer_and_location_info(
            supervision_period,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            UsPaSupervisionDelegate(),
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)


class TestSupervisionPeriodIsOutOfState(unittest.TestCase):
    """Tests the supervision_period_is_out_of_state function."""

    def test_supervision_period_is_out_of_state_with_federal_authority(self) -> None:
        self.assertTrue(
            supervision_period_is_out_of_state(
                self.create_population_event(StateCustodialAuthority.FEDERAL),
                UsXxSupervisionDelegate(),
            )
        )

    def test_supervision_period_is_out_of_state_with_other_country_authority(
        self,
    ) -> None:
        self.assertTrue(
            supervision_period_is_out_of_state(
                self.create_population_event(StateCustodialAuthority.OTHER_COUNTRY),
                UsXxSupervisionDelegate(),
            )
        )

    def test_supervision_period_is_out_of_state_with_other_state_authority(
        self,
    ) -> None:
        self.assertTrue(
            supervision_period_is_out_of_state(
                self.create_population_event(StateCustodialAuthority.OTHER_STATE),
                UsXxSupervisionDelegate(),
            )
        )

    def test_supervision_period_is_out_of_state_with_supervision_authority(
        self,
    ) -> None:
        self.assertFalse(
            supervision_period_is_out_of_state(
                self.create_population_event(
                    StateCustodialAuthority.SUPERVISION_AUTHORITY
                ),
                UsXxSupervisionDelegate(),
            )
        )

    @staticmethod
    def create_population_event(
        custodial_authority: Optional[StateCustodialAuthority],
    ) -> SupervisionPopulationEvent:
        return SupervisionPopulationEvent(
            state_code="US_XX",
            year=2010,
            month=1,
            event_date=date(2010, 1, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervising_district_external_id="TEST",
            custodial_authority=custodial_authority,
            projected_end_date=None,
        )


class TestGetPostIncarcerationSupervisionType(unittest.TestCase):
    """Tests the get_post_incarceration_supervision_type function."""

    def setUp(self) -> None:
        self.admission_date = date(2020, 1, 3)
        self.release_date = date(2020, 1, 31)
        self.incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            state_code="US_XX",
            admission_date=self.admission_date,
            release_date=self.release_date,
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )
        self.supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            state_code="US_XX",
            start_date=self.release_date,
            termination_date=date(2020, 4, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )
        self.supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=[self.supervision_period]
        )
        self.supervision_delegate = UsXxSupervisionDelegate()

    def test_get_post_incarceration_supervision_type(self) -> None:
        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PAROLE,
            get_post_incarceration_supervision_type(
                self.incarceration_period,
                self.supervision_period_index,
                self.supervision_delegate,
            ),
        )

    def test_get_post_incarceration_supervision_type_no_release_date(self) -> None:
        no_release_date_period = attr.evolve(
            self.incarceration_period, release_date=None, release_reason=None
        )
        with self.assertRaises(ValueError):
            _ = get_post_incarceration_supervision_type(
                no_release_date_period,
                self.supervision_period_index,
                self.supervision_delegate,
            )

    def test_get_post_incarceration_supervision_type_supervision_period_too_far_out(
        self,
    ) -> None:
        supervision_period_far_out = attr.evolve(
            self.supervision_period,
            start_date=self.release_date + relativedelta(days=40),
        )
        self.assertIsNone(
            get_post_incarceration_supervision_type(
                self.incarceration_period,
                PreProcessedSupervisionPeriodIndex(
                    supervision_periods=[supervision_period_far_out]
                ),
                self.supervision_delegate,
            )
        )

    def test_get_post_incarceration_supervision_type_sorting_criteria_proximity_to_release_date(
        self,
    ) -> None:
        second_supervision_period = attr.evolve(
            self.supervision_period,
            start_date=self.release_date + relativedelta(days=40),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )
        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PAROLE,
            get_post_incarceration_supervision_type(
                self.incarceration_period,
                PreProcessedSupervisionPeriodIndex(
                    supervision_periods=[
                        self.supervision_period,
                        second_supervision_period,
                    ]
                ),
                self.supervision_delegate,
            ),
        )

    class TestSupervisionDelegate(UsXxSupervisionDelegate):
        def get_incarceration_period_supervision_type_at_release(
            self, incarceration_period: StateIncarcerationPeriod
        ) -> Optional[StateSupervisionPeriodSupervisionType]:
            return StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT

    def test_get_post_incarceration_supervision_type_sorting_criteria_delegate(
        self,
    ) -> None:
        second_supervision_period = attr.evolve(
            self.supervision_period,
            supervision_type=StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT,
        )
        self.assertEqual(
            StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT,
            get_post_incarceration_supervision_type(
                self.incarceration_period,
                PreProcessedSupervisionPeriodIndex(
                    supervision_periods=[
                        self.supervision_period,
                        second_supervision_period,
                    ]
                ),
                self.TestSupervisionDelegate(),
            ),
        )

    def test_get_post_incarceration_supervision_type_sorting_criteria_duration(
        self,
    ) -> None:
        second_supervision_period = attr.evolve(
            self.supervision_period,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            termination_date=date(2020, 6, 1),
        )
        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PROBATION,
            get_post_incarceration_supervision_type(
                self.incarceration_period,
                PreProcessedSupervisionPeriodIndex(
                    supervision_periods=[
                        self.supervision_period,
                        second_supervision_period,
                    ]
                ),
                self.supervision_delegate,
            ),
        )
