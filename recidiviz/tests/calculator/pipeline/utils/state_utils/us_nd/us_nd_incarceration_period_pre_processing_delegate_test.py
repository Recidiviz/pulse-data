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
"""Tests the US_ND-specific UsNdIncarcerationPreProcessingManager."""
import unittest
from datetime import date
from itertools import permutations
from typing import List, Optional

import attr

from recidiviz.calculator.pipeline.utils.incarceration_period_pre_processing_manager import (
    IncarcerationPreProcessingManager,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_incarceration_period_pre_processing_delegate import (
    UsNdIncarcerationPreProcessingDelegate,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason as AdmissionReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason as ReleaseReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodStatus,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


class TestPreProcessedIncarcerationPeriodsForCalculations(unittest.TestCase):
    """Tests the US_ND-specific aspects of the
    pre_processed_incarceration_periods_for_calculations function on the
    UsNdIncarcerationPreProcessingManager."""

    @staticmethod
    def _pre_processed_incarceration_periods_for_calculations(
        incarceration_periods: List[StateIncarcerationPeriod],
        collapse_transfers: bool = True,
        overwrite_facility_information_in_transfers: bool = True,
        earliest_death_date: Optional[date] = None,
    ) -> List[StateIncarcerationPeriod]:
        ip_pre_processing_manager = IncarcerationPreProcessingManager(
            incarceration_periods=incarceration_periods,
            delegate=UsNdIncarcerationPreProcessingDelegate(),
            # US_ND IP pre-processing does not rely on supervision periods
            pre_processed_supervision_period_index=None,
            earliest_death_date=earliest_death_date,
        )

        return ip_pre_processing_manager.pre_processed_incarceration_period_index_for_calculations(
            collapse_transfers=collapse_transfers,
            overwrite_facility_information_in_transfers=overwrite_facility_information_in_transfers,
        ).incarceration_periods

    def test_pre_processed_incarceration_periods_for_calculations_multiple_temporary_and_valid(
        self,
    ):
        state_code = "US_ND"
        temporary_custody_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="111",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        temporary_custody_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=112,
            external_id="222",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2008, 12, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2008, 12, 24),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="333",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2016, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2017, 12, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            temporary_custody_1,
            temporary_custody_2,
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
                validated_incarceration_periods, [valid_incarceration_period]
            )

    def test_pre_processed_incarceration_periods_for_calculations_multiple_temporary_and_transfer(
        self,
    ):
        state_code = "US_ND"
        temporary_custody_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        temporary_custody_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=112,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="2",
            state_code=state_code,
            admission_date=date(2008, 12, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2008, 12, 24),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="3",
            state_code=state_code,
            admission_date=date(2011, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 4),
            release_reason=ReleaseReason.TRANSFER,
        )

        valid_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="4",
            state_code=state_code,
            admission_date=date(2012, 12, 4),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2012, 12, 24),
            release_reason=ReleaseReason.TRANSFER,
        )

        valid_incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="5",
            state_code=state_code,
            admission_date=date(2012, 12, 24),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2012, 12, 30),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            temporary_custody_1,
            temporary_custody_2,
            valid_incarceration_period_1,
            valid_incarceration_period_2,
            valid_incarceration_period_3,
        ]

        collapsed_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=valid_incarceration_period_1.incarceration_period_id,
            external_id=valid_incarceration_period_1.external_id,
            status=valid_incarceration_period_3.status,
            state_code=valid_incarceration_period_1.state_code,
            admission_date=valid_incarceration_period_1.admission_date,
            admission_reason=valid_incarceration_period_1.admission_reason,
            release_date=valid_incarceration_period_3.release_date,
            release_reason=valid_incarceration_period_3.release_reason,
        )

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._pre_processed_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test,
                )
            )

            self.assertEqual(
                validated_incarceration_periods, [collapsed_incarceration_period]
            )

    def test_pre_processed_incarceration_periods_for_calculations_valid_then_temporary(
        self,
    ):
        state_code = "US_ND"
        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 12, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        temporary_custody = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="2",
            state_code=state_code,
            admission_date=date(2010, 1, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2010, 1, 24),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        incarceration_periods = [valid_incarceration_period, temporary_custody]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._pre_processed_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test,
                )
            )

            self.assertEqual(
                validated_incarceration_periods, [valid_incarceration_period]
            )

    def test_pre_processed_incarceration_periods_for_calculations_invalid_board_hold(
        self,
    ):
        state_code = "US_ND"
        invalid_board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2009, 12, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [invalid_board_hold]

        with self.assertRaises(ValueError):
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods,
            )
