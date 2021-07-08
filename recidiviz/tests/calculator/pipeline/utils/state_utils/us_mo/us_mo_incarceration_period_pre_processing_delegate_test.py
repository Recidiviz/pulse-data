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
"""Tests the US_MO-specific UsMoIncarcerationPreProcessingManager."""
import unittest
from datetime import date
from itertools import permutations
from typing import List, Optional

import attr

from recidiviz.calculator.pipeline.utils.incarceration_period_pre_processing_manager import (
    IncarcerationPreProcessingManager,
)
from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_incarceration_period_pre_processing_delegate import (
    UsMoIncarcerationPreProcessingDelegate,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionViolationResponse,
)


class TestPreProcessedIncarcerationPeriodsForCalculations(unittest.TestCase):
    """Tests the US_MO-specific aspects of the
    pre_processed_incarceration_periods_for_calculations function on the
    UsNdIncarcerationPreProcessingManager."""

    @staticmethod
    def _pre_processed_incarceration_periods_for_calculations(
        incarceration_periods: List[StateIncarcerationPeriod],
        collapse_transfers: bool = True,
        overwrite_facility_information_in_transfers: bool = True,
        earliest_death_date: Optional[date] = None,
    ) -> List[StateIncarcerationPeriod]:
        # IP pre-processing for US_MO does not rely on violation responses or
        # supervision periods
        violation_responses: Optional[List[StateSupervisionViolationResponse]] = []
        sp_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=[],
        )

        ip_pre_processing_manager = IncarcerationPreProcessingManager(
            incarceration_periods=incarceration_periods,
            delegate=UsMoIncarcerationPreProcessingDelegate(),
            pre_processed_supervision_period_index=sp_index,
            violation_responses=violation_responses,
            earliest_death_date=earliest_death_date,
        )

        return ip_pre_processing_manager.pre_processed_incarceration_period_index_for_calculations(
            collapse_transfers=collapse_transfers,
            overwrite_facility_information_in_transfers=overwrite_facility_information_in_transfers,
        ).incarceration_periods

    def test_prepare_incarceration_periods_for_calculations_multiple_jail_and_valid(
        self,
    ) -> None:
        state_code = "US_MO"
        jail_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="111",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
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
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
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
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
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
                self._pre_processed_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test,
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
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
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
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
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
                self._pre_processed_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test,
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
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
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
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2017, 12, 4),
            admission_reason_raw_text="50N1020",
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
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
                self._pre_processed_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test,
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
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
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
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2017, 12, 4),
            # This status indicates a sanction admission for treatment
            admission_reason_raw_text="50N1060",
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
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
                self._pre_processed_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test,
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
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2017, 12, 4),
            # This status indicates a sanction admission for shock incarceration
            admission_reason_raw_text="20I1010,40I7000,45O7000",
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
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
                self._pre_processed_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test,
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

        not_board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="222",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2016, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            # This admission reason will get mapped as a PAROLE_REVOCATION. Periods
            # that were previously mapped to TEMPORARY_CUSTODY that are now mapped to
            # PAROLE_REVOCATION should get an override of INTERNAL_UNKNOWN.
            admission_reason_raw_text="50N1020",
            release_date=date(2017, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        updated_period = attr.evolve(
            not_board_hold,
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
        )

        incarceration_periods = [not_board_hold]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._pre_processed_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test,
                )
            )

            self.assertEqual([updated_period], validated_incarceration_periods)
