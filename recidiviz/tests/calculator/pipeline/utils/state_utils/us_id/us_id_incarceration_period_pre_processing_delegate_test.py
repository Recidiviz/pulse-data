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
"""Tests the US_ID-specific UsIdIncarcerationPreProcessingManager."""
import unittest
from datetime import date
from itertools import permutations
from typing import List, Optional

import attr

from recidiviz.calculator.pipeline.utils.incarceration_period_pre_processing_manager import (
    IncarcerationPreProcessingManager,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_incarceration_period_pre_processing_delegate import (
    UsIdIncarcerationPreProcessingDelegate,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodStatus,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


class TestPreProcessedIncarcerationPeriodsForCalculations(unittest.TestCase):
    """Tests the US_ID-specific aspects of the
    pre_processed_incarceration_periods_for_calculations function on the
    UsIdIncarcerationPreProcessingManager."""

    @staticmethod
    def _pre_processed_incarceration_periods_for_calculations(
        incarceration_periods: List[StateIncarcerationPeriod],
        collapse_transfers: bool = True,
        overwrite_facility_information_in_transfers: bool = True,
        earliest_death_date: Optional[date] = None,
    ) -> List[StateIncarcerationPeriod]:
        ip_pre_processing_manager = IncarcerationPreProcessingManager(
            incarceration_periods=incarceration_periods,
            delegate=UsIdIncarcerationPreProcessingDelegate(),
            earliest_death_date=earliest_death_date,
        )

        return ip_pre_processing_manager.pre_processed_incarceration_period_index_for_calculations(
            collapse_transfers=collapse_transfers,
            overwrite_facility_information_in_transfers=overwrite_facility_information_in_transfers,
        ).incarceration_periods

    def test_pre_processed_incarceration_periods_for_calculations_different_pfi_do_not_collapse(
        self,
    ):
        """Tests the pre-processing function does not collapse two adjacent TRANSFER
        edges in US_ID when they have different specialized_purpose_for_incarceration
        values.
        """
        state_code = "US_ID"
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            release_date=date(2012, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2012, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            initial_incarceration_period,
            second_incarceration_period,
        ]

        updated_periods = [
            attr.evolve(
                initial_incarceration_period,
                release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
            ),
            attr.evolve(
                second_incarceration_period,
                admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            ),
        ]

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods,
                collapse_transfers=True,
            )
        )

        self.assertEqual(updated_periods, validated_incarceration_periods)

    def test_pre_processed_incarceration_periods_for_calculations_same_pfi_transfer(
        self,
    ):
        """Tests the pre-processing function does collapse two adjacent TRANSFER
        edges in US_ID when they have the same specialized_purpose_for_incarceration
        values.
        """
        state_code = "US_ID"
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2012, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            initial_incarceration_period,
            second_incarceration_period,
        ]

        collapsed_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=initial_incarceration_period.incarceration_period_id,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=initial_incarceration_period.admission_date,
            admission_reason=initial_incarceration_period.admission_reason,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=second_incarceration_period.release_date,
            release_reason=second_incarceration_period.release_reason,
        )

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._pre_processed_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test,
                    collapse_transfers=True,
                )
            )

            self.assertEqual(validated_incarceration_periods, [collapsed_period])
