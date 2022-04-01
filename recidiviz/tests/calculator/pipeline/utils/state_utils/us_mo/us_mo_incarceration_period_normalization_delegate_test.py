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
"""Tests the US_MO-specific UsMoIncarcerationNormalizationManager."""
import unittest
from datetime import date
from itertools import permutations
from typing import List, Optional

import attr

from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.incarceration_period_normalization_manager import (
    IncarcerationPeriodNormalizationManager,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_incarceration_period_normalization_delegate import (
    UsMoIncarcerationNormalizationDelegate,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.tests.calculator.pipeline.utils.entity_normalization.normalization_testing_utils import (
    default_normalized_sp_index_for_tests,
)


class TestNormalizedIncarcerationPeriodsForCalculations(unittest.TestCase):
    """Tests the US_MO-specific aspects of the
    normalized_incarceration_periods_and_additional_attributes function on the
    IncarcerationNormalizationManager when a UsMoIncarcerationNormalizationDelegate
    is provided."""

    @staticmethod
    def _normalized_incarceration_periods_for_calculations(
        incarceration_periods: List[StateIncarcerationPeriod],
        earliest_death_date: Optional[date] = None,
    ) -> List[StateIncarcerationPeriod]:
        # IP pre-processing for US_MO does not rely on violation responses or
        # supervision periods
        violation_responses: Optional[
            List[NormalizedStateSupervisionViolationResponse]
        ] = []
        sp_index = default_normalized_sp_index_for_tests()

        ip_normalization_manager = IncarcerationPeriodNormalizationManager(
            incarceration_periods=incarceration_periods,
            normalization_delegate=UsMoIncarcerationNormalizationDelegate(),
            normalized_supervision_period_index=sp_index,
            normalized_violation_responses=violation_responses,
            incarceration_sentences=None,
            field_index=CoreEntityFieldIndex(),
            earliest_death_date=earliest_death_date,
        )

        (
            ips,
            _,
        ) = (
            ip_normalization_manager.normalized_incarceration_periods_and_additional_attributes()
        )

        return ips

    def test_prepare_incarceration_periods_for_calculations_multiple_jail_and_valid(
        self,
    ) -> None:
        state_code = "US_MO"
        jail_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="111",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.ESCAPE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        jail_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=112,
            external_id="222",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            state_code=state_code,
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.ESCAPE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="333",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2016, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2017, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [
            jail_period_1,
            jail_period_2,
            valid_incarceration_period,
        ]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._normalized_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test
                )
            )

            self.assertEqual(
                [valid_incarceration_period], validated_incarceration_periods
            )

    def test_prepare_incarceration_periods_for_calculations_valid_then_jail(
        self,
    ) -> None:
        state_code = "US_MO"
        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        jail_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            external_id="2",
            state_code=state_code,
            admission_date=date(2010, 1, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2010, 1, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [valid_incarceration_period, jail_period]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._normalized_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test
                )
            )

            self.assertEqual(
                [valid_incarceration_period], validated_incarceration_periods
            )

    def test_prepare_incarceration_periods_for_calculations_parole_board_hold(
        self,
    ) -> None:
        state_code = "US_MO"

        board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="222",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2016, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="40I0050",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2017, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=333,
            external_id="333",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2017, 12, 4),
            admission_reason_raw_text="50N1020",
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [board_hold, revocation_period]

        updated_board_hold = attr.evolve(
            board_hold,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._normalized_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test
                )
            )

            self.assertEqual(
                validated_incarceration_periods, [updated_board_hold, revocation_period]
            )

    def test_prepare_incarceration_periods_for_calculations_sanction_admission_treatment(
        self,
    ) -> None:
        state_code = "US_MO"

        board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="222",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2016, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="40I0050",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2017, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        treatment_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=333,
            external_id="333",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2017, 12, 4),
            # This status indicates a sanction admission for treatment
            admission_reason_raw_text="50N1060",
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            # It's common for this status to not be correctly associated with a
            # TREATMENT pfi
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [board_hold, treatment_period]

        updated_board_hold = attr.evolve(
            board_hold,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        updated_treatment_period = attr.evolve(
            treatment_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
        )

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._normalized_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test
                )
            )

            self.assertEqual(
                validated_incarceration_periods,
                [updated_board_hold, updated_treatment_period],
            )

    def test_prepare_incarceration_periods_for_calculations_sanction_admission_shock(
        self,
    ) -> None:
        state_code = "US_MO"

        shock_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=333,
            external_id="333",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2017, 12, 4),
            # This status indicates a sanction admission for shock incarceration
            admission_reason_raw_text="20I1010,40I7000,45O7000",
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            # It's common for this status to not be correctly associated with a
            # SHOCK_INCARCERATION pfi
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [shock_period]

        updated_shock_period = attr.evolve(
            shock_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._normalized_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test
                )
            )

            self.assertEqual(
                [updated_shock_period],
                validated_incarceration_periods,
            )

    def test_prepare_incarceration_periods_for_calculations_temp_custody_not_parole_board_hold(
        self,
    ) -> None:
        state_code = "US_MO"

        # This is an admission to TREATMENT_IN_PRISON, where all of the admission
        # statuses indicate that this person is in on a board hold. These should get an
        # admission_reason and release_reason override of INTERNAL_UNKNOWN
        not_board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="222",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2016, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="40I0050,45O0050,65N9999",
            release_date=date(2017, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
        )

        updated_period = attr.evolve(
            not_board_hold,
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
        )

        incarceration_periods = [not_board_hold]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._normalized_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test
                )
            )

            self.assertEqual([updated_period], validated_incarceration_periods)

    def test_prepare_incarceration_periods_for_calculations_irregular_board_hold(
        self,
    ) -> None:
        state_code = "US_MO"

        board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="111",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2017, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="50N1010",
            release_date=date(2017, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="222",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2017, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="50N1050",
            release_date=date(2020, 1, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        updated_period = attr.evolve(
            board_hold,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        incarceration_periods = [board_hold, revocation_period]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._normalized_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test
                )
            )

            self.assertEqual(
                [updated_period, revocation_period], validated_incarceration_periods
            )

    def test_prepare_incarceration_periods_for_calculations_irregular_board_hold_before_other_hold(
        self,
    ) -> None:
        state_code = "US_MO"

        # This gets cast as a parole board hold because it has a release
        # reason of RELEASED_FROM_TEMPORARY_CUSTODY and it immediately precedes a
        # standard parole board hold
        irregular_board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="111",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2017, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="50N1010",
            release_date=date(2017, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        regular_board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="222",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2017, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="40I0050,45O0050,65N9999",
            release_date=date(2017, 12, 31),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=333,
            external_id="333",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2017, 12, 31),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="50N1050",
            release_date=date(2020, 1, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        updated_irregular_board_hold_period = attr.evolve(
            irregular_board_hold,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        updated_regular_board_hold_period = attr.evolve(
            regular_board_hold,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        incarceration_periods = [
            irregular_board_hold,
            regular_board_hold,
            revocation_period,
        ]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._normalized_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test
                )
            )

            self.assertEqual(
                [
                    updated_irregular_board_hold_period,
                    updated_regular_board_hold_period,
                    revocation_period,
                ],
                validated_incarceration_periods,
            )

    def test_prepare_incarceration_periods_for_calculations_irregular_board_hold_before_multiple_holds_revocation(
        self,
    ) -> None:
        state_code = "US_MO"

        # This gets cast as a parole board hold because it has a release
        # reason of RELEASED_FROM_TEMPORARY_CUSTODY and it immediately precedes another
        # irregular board hold
        irregular_board_hold_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="111",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2017, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="50N1010",
            release_date=date(2017, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        # This gets cast as a parole board hold because it has a release
        # reason of RELEASED_FROM_TEMPORARY_CUSTODY and it immediately precedes a
        # standard parole board hold
        irregular_board_hold_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="222",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2017, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="50N1010",
            release_date=date(2017, 12, 10),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        regular_board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=333,
            external_id="333",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2017, 12, 10),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="40I0050,45O0050,65N9999",
            release_date=date(2017, 12, 31),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=333,
            external_id="333",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2017, 12, 31),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="50N1050",
            release_date=date(2020, 1, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        updated_irregular_board_hold_1_period = attr.evolve(
            irregular_board_hold_1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        updated_irregular_board_hold_2_period = attr.evolve(
            irregular_board_hold_2,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        updated_regular_board_hold_period = attr.evolve(
            regular_board_hold,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        incarceration_periods = [
            irregular_board_hold_1,
            irregular_board_hold_2,
            regular_board_hold,
            revocation_period,
        ]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._normalized_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test
                )
            )

            self.assertEqual(
                [
                    updated_irregular_board_hold_1_period,
                    updated_irregular_board_hold_2_period,
                    updated_regular_board_hold_period,
                    revocation_period,
                ],
                validated_incarceration_periods,
            )

    def test_prepare_incarceration_periods_for_calculations_irregular_board_hold_before_multipls_holds(
        self,
    ) -> None:
        """Tests that the recursive logic that identifies board holds in US_MO can
        safely iterate through all periods."""
        state_code = "US_MO"

        # This gets cast as a parole board hold because it has a release
        # reason of RELEASED_FROM_TEMPORARY_CUSTODY and it immediately precedes another
        # irregular board hold
        irregular_board_hold_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="111",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2017, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="50N1010",
            release_date=date(2017, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        # This gets cast as a parole board hold because it has a release
        # reason of RELEASED_FROM_TEMPORARY_CUSTODY and it immediately precedes a
        # standard parole board hold
        irregular_board_hold_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="222",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2017, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="50N1010",
            release_date=date(2017, 12, 10),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        regular_board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=333,
            external_id="333",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2017, 12, 10),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="40I0050,45O0050,65N9999",
            release_date=date(2017, 12, 31),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        updated_irregular_board_hold_1_period = attr.evolve(
            irregular_board_hold_1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        updated_irregular_board_hold_2_period = attr.evolve(
            irregular_board_hold_2,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        updated_regular_board_hold_period = attr.evolve(
            regular_board_hold,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        incarceration_periods = [
            irregular_board_hold_1,
            irregular_board_hold_2,
            regular_board_hold,
        ]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._normalized_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test
                )
            )

            self.assertEqual(
                [
                    updated_irregular_board_hold_1_period,
                    updated_irregular_board_hold_2_period,
                    updated_regular_board_hold_period,
                ],
                validated_incarceration_periods,
            )

    def test_prepare_incarceration_periods_for_calculations_irregular_board_hold_before_sanction(
        self,
    ) -> None:
        state_code = "US_MO"

        # This gets cast as a parole board hold because it has a release
        # reason of RELEASED_FROM_TEMPORARY_CUSTODY and it immediately precedes a
        # sanction admission
        irregular_board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="111",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2017, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="40I1010,45O1010",
            release_date=date(2017, 12, 31),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            release_reason_raw_text="50N1060",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="222",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2017, 12, 31),
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            admission_reason_raw_text="50N1060",
            release_date=date(2020, 1, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="IC-IC",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        updated_irregular_board_hold_period = attr.evolve(
            irregular_board_hold,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        updated_revocation_period = attr.evolve(
            revocation_period,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
        )

        incarceration_periods = [
            irregular_board_hold,
            revocation_period,
        ]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._normalized_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test
                )
            )

            self.assertEqual(
                [updated_irregular_board_hold_period, updated_revocation_period],
                validated_incarceration_periods,
            )
