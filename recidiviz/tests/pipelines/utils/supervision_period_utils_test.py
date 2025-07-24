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
"""Tests for supervision_period_utils.py."""
import unittest
from datetime import date
from typing import Optional

import attr
from dateutil.relativedelta import relativedelta

from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionCaseTypeEntry,
    StateSupervisionPeriod,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
    NormalizedStateSupervisionPeriod,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_supervision_delegate import (
    UsXxSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ix.us_ix_supervision_delegate import (
    UsIxSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_supervision_delegate import (
    UsMoSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_pa.us_pa_supervision_delegate import (
    UsPaSupervisionDelegate,
)
from recidiviz.pipelines.utils.supervision_period_utils import (
    CASE_TYPE_SEVERITY_ORDER,
    filter_out_supervision_period_types_excluded_from_pre_admission_search,
    get_post_incarceration_supervision_type,
    identify_most_severe_case_type,
    supervising_location_info,
    supervision_periods_overlapping_with_date,
)
from recidiviz.tests.pipelines.utils.entity_normalization.normalization_testing_utils import (
    default_normalized_sp_index_for_tests,
)


class TestIdentifyMostSevereCaseType(unittest.TestCase):
    """Tests the _identify_most_severe_case_type function."""

    def test_case_type_severity_order_comprehensive(self) -> None:
        for case_type in StateSupervisionCaseType:
            self.assertIn(case_type, CASE_TYPE_SEVERITY_ORDER)

    def test_identify_most_severe_case_type(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            external_id="sp1",
            start_date=date(2017, 3, 5),
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                    case_type_raw_text="DV",
                ),
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.SEX_OFFENSE,
                    case_type_raw_text="SO",
                ),
            ],
        )

        (
            most_severe_case_type,
            most_severe_case_type_raw_text,
        ) = identify_most_severe_case_type(supervision_period)

        self.assertEqual(most_severe_case_type, StateSupervisionCaseType.SEX_OFFENSE)
        self.assertEqual(most_severe_case_type_raw_text, "SO")

    def test_identify_most_severe_case_type_test_all_types(self) -> None:
        for case_type in StateSupervisionCaseType:
            supervision_period = StateSupervisionPeriod.new_with_defaults(
                state_code="US_XX",
                external_id="sp1",
                start_date=date(2017, 3, 5),
                case_type_entries=[
                    StateSupervisionCaseTypeEntry.new_with_defaults(
                        state_code="US_XX", case_type=case_type
                    ),
                ],
            )

            (
                most_severe_case_type,
                most_severe_case_type_raw_text,
            ) = identify_most_severe_case_type(supervision_period)

            self.assertEqual(most_severe_case_type, case_type)
            self.assertEqual(most_severe_case_type_raw_text, None)

    def test_identify_most_severe_case_type_no_type_entries(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            external_id="sp1",
            start_date=date(2017, 3, 5),
            case_type_entries=[],
        )

        (
            most_severe_case_type,
            most_severe_case_type_raw_text,
        ) = identify_most_severe_case_type(supervision_period)

        self.assertEqual(most_severe_case_type, StateSupervisionCaseType.GENERAL)
        self.assertEqual(most_severe_case_type_raw_text, None)


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


class TestSupervisingOfficerAndLocationInfo(unittest.TestCase):
    """Tests the supervising_location_info function."""

    def test_get_supervising_officer_and_location_info_from_supervision_period(
        self,
    ) -> None:
        supervision_period = attr.evolve(
            DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE, supervision_site="1"
        )

        (
            level_1_supervision_location,
            level_2_supervision_location,
        ) = supervising_location_info(
            supervision_period,
            UsXxSupervisionDelegate(),
        )

        self.assertEqual("1", level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_from_supervision_period_no_site(
        self,
    ) -> None:
        (
            level_1_supervision_location,
            level_2_supervision_location,
        ) = supervising_location_info(
            DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            UsXxSupervisionDelegate(),
        )

        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_no_agent(self) -> None:
        supervision_period = attr.evolve(
            DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            supervision_period_id=666,  # No mapping for this ID
            supervision_site="1",
        )

        (
            level_1_supervision_location,
            level_2_supervision_location,
        ) = supervising_location_info(
            supervision_period,
            UsXxSupervisionDelegate(),
        )

        self.assertEqual("1", level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_from_supervision_period_us_ix(
        self,
    ) -> None:
        supervision_period = attr.evolve(
            DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_IX",
            supervision_site="DISTRICT OFFICE 6, POCATELLO",
        )

        (
            level_1_supervision_location,
            level_2_supervision_location,
        ) = supervising_location_info(
            supervision_period,
            UsIxSupervisionDelegate(),
        )

        self.assertEqual("DISTRICT OFFICE 6, POCATELLO", level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

        supervision_period = attr.evolve(
            DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_IX",
            supervision_site="PAROLE COMMISSION",
        )

        (
            level_1_supervision_location,
            level_2_supervision_location,
        ) = supervising_location_info(
            supervision_period,
            UsIxSupervisionDelegate(),
        )

        self.assertEqual("PAROLE COMMISSION", level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

        supervision_period = attr.evolve(
            DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_IX",
            supervision_site="DISTRICT OFFICE 4, BOISE",
        )

        (
            level_1_supervision_location,
            level_2_supervision_location,
        ) = supervising_location_info(
            supervision_period,
            UsIxSupervisionDelegate(),
        )

        self.assertEqual("DISTRICT OFFICE 4, BOISE", level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

        supervision_period = attr.evolve(
            DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_IX",
            supervision_site=None,
        )

        (
            level_1_supervision_location,
            level_2_supervision_location,
        ) = supervising_location_info(
            supervision_period,
            UsIxSupervisionDelegate(),
        )

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
            level_1_supervision_location,
            level_2_supervision_location,
        ) = supervising_location_info(
            supervision_period,
            UsMoSupervisionDelegate(),
        )

        self.assertEqual("04C", level_1_supervision_location)
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
            level_1_supervision_location,
            level_2_supervision_location,
        ) = supervising_location_info(
            supervision_period,
            UsPaSupervisionDelegate(),
        )

        self.assertEqual("CO - CENTRAL OFFICE", level_1_supervision_location)
        self.assertEqual("CO", level_2_supervision_location)

        supervision_period = attr.evolve(
            DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_PA",
            supervision_site=None,
        )

        (
            level_1_supervision_location,
            level_2_supervision_location,
        ) = supervising_location_info(
            supervision_period,
            UsPaSupervisionDelegate(),
        )

        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)


class TestGetPostIncarcerationSupervisionType(unittest.TestCase):
    """Tests the get_post_incarceration_supervision_type function."""

    def setUp(self) -> None:
        self.admission_date = date(2020, 1, 3)
        self.release_date = date(2020, 1, 31)
        self.incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            external_id="ip1",
            state_code="US_XX",
            admission_date=self.admission_date,
            release_date=self.release_date,
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )
        self.supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=1,
            external_id="sp1",
            sequence_num=0,
            state_code="US_XX",
            start_date=self.release_date,
            termination_date=date(2020, 4, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )
        self.supervision_period_index = default_normalized_sp_index_for_tests(
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
                default_normalized_sp_index_for_tests(
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
            sequence_num=1,
            start_date=self.release_date + relativedelta(days=40),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )
        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PAROLE,
            get_post_incarceration_supervision_type(
                self.incarceration_period,
                default_normalized_sp_index_for_tests(
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
            self,
            incarceration_period: StateIncarcerationPeriod
            | NormalizedStateIncarcerationPeriod,
        ) -> Optional[StateSupervisionPeriodSupervisionType]:
            return StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT

    def test_get_post_incarceration_supervision_type_sorting_criteria_delegate(
        self,
    ) -> None:
        second_supervision_period = attr.evolve(
            self.supervision_period,
            supervision_type=StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT,
            sequence_num=1,
        )
        self.assertEqual(
            StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT,
            get_post_incarceration_supervision_type(
                self.incarceration_period,
                default_normalized_sp_index_for_tests(
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
            sequence_num=1,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            termination_date=date(2020, 6, 1),
        )
        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PROBATION,
            get_post_incarceration_supervision_type(
                self.incarceration_period,
                default_normalized_sp_index_for_tests(
                    supervision_periods=[
                        self.supervision_period,
                        second_supervision_period,
                    ]
                ),
                self.supervision_delegate,
            ),
        )


class TestFindSupervisionPeriodsOverlappingWithDate(unittest.TestCase):
    """Tests the supervision_periods_overlapping_with_date function."""

    def test_supervision_periods_overlapping_with_date(self) -> None:
        intersection_date = date(2013, 3, 1)

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2015, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_periods = [supervision_period]

        supervision_periods_during_referral = supervision_periods_overlapping_with_date(
            intersection_date, supervision_periods
        )

        self.assertEqual(supervision_periods, supervision_periods_during_referral)

    def test_supervision_periods_overlapping_with_date_no_termination(
        self,
    ) -> None:
        intersection_date = date(2013, 3, 1)

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2002, 11, 5),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_periods = [supervision_period]

        supervision_periods_during_referral = supervision_periods_overlapping_with_date(
            intersection_date, supervision_periods
        )

        self.assertEqual(supervision_periods, supervision_periods_during_referral)

    def test_supervision_periods_overlapping_with_date_no_overlap(self) -> None:
        intersection_date = date(2019, 3, 1)

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2015, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_periods = [supervision_period]

        supervision_periods_during_referral = supervision_periods_overlapping_with_date(
            intersection_date, supervision_periods
        )

        self.assertEqual([], supervision_periods_during_referral)

    def test_supervision_periods_overlapping_with_date_start_on_intersection(
        self,
    ) -> None:
        intersection_date = date(2013, 3, 1)

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=intersection_date,
            termination_date=date(2015, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_periods = [supervision_period]

        supervision_periods_during_referral = supervision_periods_overlapping_with_date(
            intersection_date, supervision_periods
        )

        self.assertEqual(supervision_periods, supervision_periods_during_referral)


class TestFilterOutSpTypesExcludedFromPreAdmissionSearch(unittest.TestCase):
    """Tests the filter_out_supervision_period_types_excluded_from_pre_admission_search
    function."""

    def test_filter_out_supervision_period_types_excluded_from_pre_admission_search(
        self,
    ) -> None:
        sp = NormalizedStateSupervisionPeriod(
            supervision_period_id=1,
            external_id="sp1",
            sequence_num=0,
            state_code="US_XX",
            start_date=date(2020, 1, 1),
            termination_date=date(2020, 4, 1),
        )

        # Assert all StateSupervisionPeriodSupervisionType values are covered
        for supervision_type in StateSupervisionPeriodSupervisionType:
            sp.supervision_type = supervision_type

            _ = filter_out_supervision_period_types_excluded_from_pre_admission_search(
                [sp]
            )

    def test_filter_out_unset_type_excluded_from_pre_admission_search(
        self,
    ) -> None:
        sp = NormalizedStateSupervisionPeriod(
            supervision_period_id=1,
            external_id="sp1",
            sequence_num=0,
            state_code="US_XX",
            start_date=date(2020, 1, 1),
            termination_date=date(2020, 4, 1),
        )

        # Unset supervision_type should be excluded
        filtered_sps = (
            filter_out_supervision_period_types_excluded_from_pre_admission_search([sp])
        )

        self.assertEqual([], filtered_sps)
