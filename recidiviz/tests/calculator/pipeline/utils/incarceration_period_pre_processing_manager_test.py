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
# pylint: disable=protected-access
"""Tests for incarceration_period_pre_processing_manager.py."""

import unittest
from datetime import date
from itertools import permutations
from typing import List, Optional

import attr
import mock
from freezegun import freeze_time

from recidiviz.calculator.pipeline.utils.incarceration_period_pre_processing_manager import (
    ATTRIBUTES_TRIGGERING_STATUS_CHANGE,
    IncarcerationPreProcessingManager,
    PurposeForIncarcerationInfo,
)
from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_incarceration_delegate import (
    UsXxIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_incarceration_period_pre_processing_delegate import (
    UsXxIncarcerationPreProcessingDelegate,
)
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationFacilitySecurityLevel,
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


class TestPreProcessedIncarcerationPeriodsForCalculations(unittest.TestCase):
    """Tests the pre_processed_incarceration_periods_for_calculations function."""

    @staticmethod
    def _pre_processed_incarceration_periods_for_calculations(
        incarceration_periods: List[StateIncarcerationPeriod],
        collapse_transfers: bool,
        overwrite_facility_information_in_transfers: bool = False,
        earliest_death_date: Optional[date] = None,
    ) -> List[StateIncarcerationPeriod]:
        # None of the state-agnostic tests rely on supervision periods
        sp_index = None
        # None of the state-agnostic tests rely on violation responses
        violation_responses = None

        ip_pre_processing_manager = IncarcerationPreProcessingManager(
            incarceration_periods=incarceration_periods,
            pre_processing_delegate=UsXxIncarcerationPreProcessingDelegate(),
            incarceration_delegate=UsXxIncarcerationDelegate(),
            pre_processed_supervision_period_index=sp_index,
            violation_responses=violation_responses,
            earliest_death_date=earliest_death_date,
            field_index=CoreEntityFieldIndex(),
        )

        return ip_pre_processing_manager.pre_processed_incarceration_period_index_for_calculations(
            collapse_transfers=collapse_transfers,
            overwrite_facility_information_in_transfers=overwrite_facility_information_in_transfers,
        ).incarceration_periods

    def test_pre_processed_incarceration_periods_for_calculations(self):
        state_code = "US_XX"
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        first_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2011, 3, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        second_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2012, 2, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2016, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [
            initial_incarceration_period,
            first_reincarceration_period,
            second_reincarceration_period,
        ]

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods,
                collapse_transfers=True,
                overwrite_facility_information_in_transfers=True,
            )
        )

        self.assertEqual(validated_incarceration_periods, incarceration_periods)

    def test_pre_processed_incarceration_periods_for_calculations_standardize_parole_board_hold(
        self,
    ):
        state_code = "US_XX"
        board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2010, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2015, 2, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2016, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [
            board_hold,
            revocation_period,
            reincarceration_period,
        ]

        updated_board_hold = attr.evolve(
            board_hold,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods,
                collapse_transfers=True,
                overwrite_facility_information_in_transfers=True,
            )
        )

        self.assertEqual(
            [updated_board_hold, revocation_period, reincarceration_period],
            validated_incarceration_periods,
        )

    def test_pre_processed_incarceration_periods_for_calculations_standardize_parole_board_hold_no_release(
        self,
    ):
        state_code = "US_XX"
        board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        incarceration_periods = [
            board_hold,
        ]

        updated_board_hold = attr.evolve(
            board_hold,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
        )

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods,
                collapse_transfers=True,
                overwrite_facility_information_in_transfers=True,
            )
        )

        self.assertEqual(
            [updated_board_hold],
            validated_incarceration_periods,
        )

    def test_pre_processed_incarceration_periods_for_calculations_board_hold_other_temp_custody(
        self,
    ):
        state_code = "US_XX"

        temporary_custody_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2008, 11, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        temporary_custody_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            release_date=date(2008, 11, 22),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 22),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_periods = [
            board_hold,
            temporary_custody_period_1,
            temporary_custody_period_2,
        ]

        updated_temporary_custody_1 = attr.evolve(
            temporary_custody_period_1,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        )

        updated_temporary_custody_2 = attr.evolve(
            temporary_custody_period_2,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        updated_board_hold = attr.evolve(
            board_hold,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods,
                collapse_transfers=False,
                overwrite_facility_information_in_transfers=True,
            )
        )

        self.assertEqual(
            [
                updated_temporary_custody_1,
                updated_temporary_custody_2,
                updated_board_hold,
            ],
            validated_incarceration_periods,
        )

    def test_pre_processed_incarceration_periods_for_calculations_not_board_hold_other_temp_custody(
        self,
    ):
        state_code = "US_XX"

        not_board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        incarceration_periods = [not_board_hold]

        updated_period = attr.evolve(
            not_board_hold,
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
        )

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods,
                collapse_transfers=True,
                overwrite_facility_information_in_transfers=True,
            )
        )

        self.assertEqual(
            [updated_period],
            validated_incarceration_periods,
        )

    def test_parole_board_hold_edges_stay_as_transfers(self):
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2019, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2019, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        ip_periods_copied = [
            attr.evolve(incarceration_period_1),
            attr.evolve(incarceration_period_2),
        ]

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        updated_periods = self._pre_processed_incarceration_periods_for_calculations(
            incarceration_periods=incarceration_periods,
            collapse_transfers=False,
            overwrite_facility_information_in_transfers=True,
        )

        self.assertEqual(ip_periods_copied, updated_periods)

    def test_parole_board_hold_edges_updated_as_transfers(self):
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2019, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2019, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        ip_periods_copied = [
            attr.evolve(
                incarceration_period_1,
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            ),
            attr.evolve(
                incarceration_period_2,
                admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            ),
        ]

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        updated_periods = self._pre_processed_incarceration_periods_for_calculations(
            incarceration_periods=incarceration_periods,
            collapse_transfers=False,
            overwrite_facility_information_in_transfers=True,
        )

        self.assertEqual(ip_periods_copied, updated_periods)

    def test_parole_board_hold_should_not_be_updated_as_transfers(self):
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2019, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2019, 12, 28),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        ip_periods_copied = [
            attr.evolve(
                incarceration_period_1,
            ),
            attr.evolve(
                incarceration_period_2,
            ),
        ]

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        updated_periods = self._pre_processed_incarceration_periods_for_calculations(
            incarceration_periods=incarceration_periods,
            collapse_transfers=True,
            overwrite_facility_information_in_transfers=True,
        )

        self.assertEqual(ip_periods_copied, updated_periods)

    def test_update_edges_to_transfers(self):
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=None,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2019, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=None,
            release_date=date(2019, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        ip_periods_copied = [
            attr.evolve(
                incarceration_period_1,
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            ),
            attr.evolve(
                incarceration_period_2,
                admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            ),
        ]

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        updated_periods = self._pre_processed_incarceration_periods_for_calculations(
            incarceration_periods=incarceration_periods,
            collapse_transfers=False,
            overwrite_facility_information_in_transfers=True,
        )

        self.assertEqual(ip_periods_copied, updated_periods)

    def test_no_update_for_edges_to_transfers_when_gap_in_between(self):
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=None,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2020, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=None,
            release_date=date(2020, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        ip_periods_copied = [
            attr.evolve(
                incarceration_period_1,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            ),
            attr.evolve(
                incarceration_period_2,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            ),
        ]

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        updated_periods = self._pre_processed_incarceration_periods_for_calculations(
            incarceration_periods=incarceration_periods,
            collapse_transfers=True,
            overwrite_facility_information_in_transfers=True,
        )

        self.assertEqual(ip_periods_copied, updated_periods)

    def test_pre_processed_incarceration_periods_for_calculations_deepcopy(self):
        """Tests that the same IncarcerationPreProcessingManager instance can be
        re-used for different configurations of pre-processing, since the original
        incarceration_periods are deep-copied and stored."""
        state_code = "US_XX"
        ip_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            facility="A",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        ip_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2010, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            facility="B",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        ip_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2014, 4, 14),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2016, 9, 4),
            release_reason=None,
            facility="C",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [ip_1, ip_2, ip_3]

        stored_raw_copies = [attr.evolve(ip_1), attr.evolve(ip_2), attr.evolve(ip_3)]

        # This tests does not rely on supervision periods or violation responses
        sp_index = None
        violation_responses = None

        ip_pre_processing_manager = IncarcerationPreProcessingManager(
            incarceration_periods=incarceration_periods,
            pre_processing_delegate=UsXxIncarcerationPreProcessingDelegate(),
            incarceration_delegate=UsXxIncarcerationDelegate(),
            pre_processed_supervision_period_index=sp_index,
            violation_responses=violation_responses,
            earliest_death_date=None,
            field_index=CoreEntityFieldIndex(),
        )

        collapsed_incarceration_periods = ip_pre_processing_manager.pre_processed_incarceration_period_index_for_calculations(
            collapse_transfers=True,
            overwrite_facility_information_in_transfers=True,
        ).incarceration_periods

        collapsed_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2016, 9, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            facility="C",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        self.assertEqual([collapsed_period], collapsed_incarceration_periods)

        not_collapsed_incarceration_periods = ip_pre_processing_manager.pre_processed_incarceration_period_index_for_calculations(
            collapse_transfers=False,
            overwrite_facility_information_in_transfers=False,
        ).incarceration_periods

        expected_non_collapsed_periods = [
            attr.evolve(ip_1),
            attr.evolve(ip_2),
            attr.evolve(
                ip_3,
                release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            ),
        ]

        # Without collapsing, the pre-processed periods should match the original
        # ones, with the release_reason updated on ip_3
        self.assertEqual(
            expected_non_collapsed_periods, not_collapsed_incarceration_periods
        )
        # Assert that the original incarceration periods are unchanged
        self.assertEqual(
            stored_raw_copies, ip_pre_processing_manager._original_incarceration_periods
        )
        # Assert that the previously-processed periods are unchanged
        self.assertEqual([collapsed_period], collapsed_incarceration_periods)

    def test_pre_processed_incarceration_periods_for_calculations_empty_admission_data(
        self,
    ):
        """Tests that the incarceration periods are correctly collapsed when there's an empty admission_date and
        admission_reason following a transfer out..
        """
        state_code = "US_XX"
        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id="99983-1|99983-2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_period_id=1111,
            state_code=state_code,
            admission_date=date(2004, 1, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id="99983-3",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_period_id=2222,
            state_code=state_code,
            admission_date=None,
            admission_reason=None,
            release_date=date(2010, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [
            first_incarceration_period,
            second_incarceration_period,
        ]

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods,
                collapse_transfers=True,
                overwrite_facility_information_in_transfers=True,
            )
        )

        self.assertEqual(
            [
                StateIncarcerationPeriod.new_with_defaults(
                    external_id="99983-1|99983-2",
                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                    incarceration_period_id=1111,
                    state_code=state_code,
                    admission_date=date(2004, 1, 3),
                    admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                    release_date=date(2010, 4, 14),
                    release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                )
            ],
            validated_incarceration_periods,
        )

    def test_pre_processed_incarceration_periods_for_calculations_drop_invalid_zero_day(
        self,
    ):
        state_code = "US_XX"
        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        invalid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2009, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2009, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        valid_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="3",
            state_code=state_code,
            admission_date=date(2009, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2014, 12, 19),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [
            valid_incarceration_period_1,
            invalid_incarceration_period,
            valid_incarceration_period_2,
        ]

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods,
                collapse_transfers=True,
                overwrite_facility_information_in_transfers=True,
            )
        )

        self.assertEqual(
            [valid_incarceration_period_1, valid_incarceration_period_2],
            validated_incarceration_periods,
        )

    def test_pre_processed_incarceration_periods_for_calculations_drop_periods_for_deceased(
        self,
    ):
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2009, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.DEATH,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2009, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2009, 12, 5),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2009, 12, 6),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [
            incarceration_period_1,
            incarceration_period_2,
            incarceration_period_3,
        ]

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods,
                collapse_transfers=True,
                overwrite_facility_information_in_transfers=True,
                earliest_death_date=incarceration_period_1.release_date,
            )
        )

        self.assertEqual(
            [incarceration_period_1],
            validated_incarceration_periods,
        )

    def test_pre_processed_incarceration_periods_for_calculations_drop_invalid_zero_day_after_transfer(
        self,
    ):
        state_code = "US_XX"
        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        valid_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="2",
            state_code=state_code,
            admission_date=date(2009, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2009, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        invalid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="3",
            state_code=state_code,
            admission_date=date(2009, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2009, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [
            valid_incarceration_period_1,
            invalid_incarceration_period,
            valid_incarceration_period_2,
        ]

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods,
                collapse_transfers=True,
                overwrite_facility_information_in_transfers=True,
            )
        )

        collapsed_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        self.assertEqual(
            [collapsed_period],
            validated_incarceration_periods,
        )

    def test_pre_processed_incarceration_periods_for_calculations_drop_invalid_zero_day_border(
        self,
    ):
        zero_day_start = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 11, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        valid_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        zero_day_end_different_reason = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2010, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [
            zero_day_start,
            valid_period,
            zero_day_end_different_reason,
        ]

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods,
                collapse_transfers=True,
                overwrite_facility_information_in_transfers=True,
            )
        )

        self.assertEqual(
            [valid_period, zero_day_end_different_reason],
            validated_incarceration_periods,
        )

    def test_standardize_purpose_for_incarceration_values(self):
        state_code = "US_XX"
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        expected_period = attr.evolve(
            incarceration_period,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period],
                collapse_transfers=True,
                overwrite_facility_information_in_transfers=True,
            )
        )

        self.assertEqual(len(validated_incarceration_periods), 1)
        self.assertEqual([expected_period], validated_incarceration_periods)

    @mock.patch(
        "recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_incarceration_period_pre_processing_delegate.UsXxIncarcerationPreProcessingDelegate.get_pfi_info_for_period_if_commitment_from_supervision"
    )
    @mock.patch(
        "recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_incarceration_period_pre_processing_delegate.UsXxIncarcerationPreProcessingDelegate.normalize_period_if_commitment_from_supervision"
    )
    def test_standardize_purpose_for_incarceration_values_propagate_pfi(
        self, mock_normalization, mock_get_pfi_info
    ):
        """Tests that when the purpose_for_incarceration value of a period is
        updated, that value is propagated to all subsequent IPs along transfer edges."""
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2010, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2010, 12, 10),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2010, 12, 10),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2011, 1, 19),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_period_4 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=4444,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2015, 6, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2015, 11, 19),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        mock_normalization.side_effect = [
            attr.evolve(
                incarceration_period_1,
                admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            ),
            incarceration_period_2,
            incarceration_period_3,
            incarceration_period_4,
        ]

        mock_get_pfi_info.side_effect = [
            PurposeForIncarcerationInfo(
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
            ),
            PurposeForIncarcerationInfo(),
            PurposeForIncarcerationInfo(),
            PurposeForIncarcerationInfo(),
        ]

        expected_periods = [
            attr.evolve(
                incarceration_period_1,
                admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
            attr.evolve(
                incarceration_period_2,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
            attr.evolve(
                incarceration_period_3,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
            attr.evolve(
                incarceration_period_4,
            ),
        ]

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[
                    incarceration_period_1,
                    incarceration_period_2,
                    incarceration_period_3,
                    incarceration_period_4,
                ],
                collapse_transfers=False,
            )
        )

        self.assertEqual(expected_periods, validated_incarceration_periods)

    @mock.patch(
        "recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_incarceration_period_pre_processing_delegate.UsXxIncarcerationPreProcessingDelegate.get_pfi_info_for_period_if_commitment_from_supervision"
    )
    @mock.patch(
        "recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_incarceration_period_pre_processing_delegate.UsXxIncarcerationPreProcessingDelegate.normalize_period_if_commitment_from_supervision"
    )
    def test_standardize_purpose_for_incarceration_values_propagate_pfi_avoid_status_changes(
        self, mock_normalization, mock_get_pfi_info
    ):
        """Tests that when the purpose_for_incarceration value of a period is
        updated, that value is propagated to all subsequent IPs along transfer edges,
        excluding edges that are set as STATUS_CHANGE edges mid-processing."""
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2010, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2010, 12, 10),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2010, 12, 10),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2011, 1, 19),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_period_4 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=4444,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2015, 6, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2015, 11, 19),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        def _mock_normalization_fn(
            incarceration_period_list_index: int,
            sorted_incarceration_periods: List[StateIncarcerationPeriod],
            # pylint: disable=unused-argument
            original_sorted_incarceration_periods: List[StateIncarcerationPeriod],
            supervision_period_index: Optional[PreProcessedSupervisionPeriodIndex],
        ) -> StateIncarcerationPeriod:
            ip = sorted_incarceration_periods[incarceration_period_list_index]

            if (
                ip.incarceration_period_id
                == incarceration_period_1.incarceration_period_id
            ):
                return attr.evolve(
                    ip,
                    admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
                )
            return ip

        mock_normalization.side_effect = _mock_normalization_fn

        mock_get_pfi_info.side_effect = [
            PurposeForIncarcerationInfo(
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
            ),
            PurposeForIncarcerationInfo(),
            PurposeForIncarcerationInfo(),
            PurposeForIncarcerationInfo(),
        ]

        expected_periods = [
            attr.evolve(
                incarceration_period_1,
                admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
            attr.evolve(
                incarceration_period_2,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
                release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
            ),
            attr.evolve(
                incarceration_period_3,
                admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            ),
            attr.evolve(
                incarceration_period_4,
            ),
        ]

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[
                    incarceration_period_1,
                    incarceration_period_2,
                    incarceration_period_3,
                    incarceration_period_4,
                ],
                collapse_transfers=False,
            )
        )

        self.assertEqual(expected_periods, validated_incarceration_periods)

    def test_collapse_incarceration_periods(self):
        state_code = "US_XX"
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        first_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2010, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [
            initial_incarceration_period,
            first_reincarceration_period,
        ]

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods,
                collapse_transfers=True,
                overwrite_facility_information_in_transfers=True,
            )
        )

        self.assertEqual(len(validated_incarceration_periods), 1)

        self.assertEqual(
            validated_incarceration_periods,
            [
                StateIncarcerationPeriod.new_with_defaults(
                    incarceration_period_id=initial_incarceration_period.incarceration_period_id,
                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                    state_code=initial_incarceration_period.state_code,
                    admission_date=initial_incarceration_period.admission_date,
                    admission_reason=initial_incarceration_period.admission_reason,
                    release_date=first_reincarceration_period.release_date,
                    release_reason=first_reincarceration_period.release_reason,
                    specialized_purpose_for_incarceration=first_reincarceration_period.specialized_purpose_for_incarceration,
                )
            ],
        )

    def test_collapse_incarceration_periods_missing_transfer_in(self):
        state_code = "US_XX"
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2003, 1, 2),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2007, 4, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        first_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        second_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=None,
            admission_reason=None,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [
            initial_incarceration_period,
            first_reincarceration_period,
            second_reincarceration_period,
        ]

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods,
                collapse_transfers=True,
                overwrite_facility_information_in_transfers=True,
            )
        )

        expected_collapsed_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        self.assertEqual(
            [initial_incarceration_period, expected_collapsed_period],
            validated_incarceration_periods,
        )

    def test_apply_overrides_sanction_admission(self):
        sanction_admission = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="1",
            state_code="US_XX",
            admission_date=date(2012, 12, 24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            release_date=date(2012, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        updated_period = attr.evolve(
            sanction_admission,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        )

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[sanction_admission],
                collapse_transfers=True,
                overwrite_facility_information_in_transfers=True,
            )
        )

        self.assertEqual([updated_period], validated_incarceration_periods)

    def test_apply_overrides_not_sanction_admission(self):
        regular_admission = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="1",
            state_code="US_XX",
            admission_date=date(2012, 12, 24),
            # This person was sentenced to complete treatment in prison
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            release_date=date(2012, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        period_copy = attr.evolve(
            regular_admission,
        )

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[regular_admission],
                collapse_transfers=True,
                overwrite_facility_information_in_transfers=True,
            )
        )

        self.assertEqual([period_copy], validated_incarceration_periods)


class TestCollapseIncarcerationPeriods(unittest.TestCase):
    """Tests the _collapse_incarceration_period_transfers function on the
    IncarcerationPreProcessingManager."""

    @staticmethod
    def _collapse_incarceration_period_transfers(
        incarceration_periods: List[StateIncarcerationPeriod],
        overwrite_facility_information_in_transfers: bool = False,
    ):
        # None of the state-agnostic tests rely on supervision periods
        sp_index = None
        # None of the state-agnostic tests rely on violation responses
        violation_responses = None

        ip_pre_processing_manager = IncarcerationPreProcessingManager(
            incarceration_periods=incarceration_periods,
            pre_processing_delegate=UsXxIncarcerationPreProcessingDelegate(),
            incarceration_delegate=UsXxIncarcerationDelegate(),
            pre_processed_supervision_period_index=sp_index,
            violation_responses=violation_responses,
            earliest_death_date=None,
            field_index=CoreEntityFieldIndex(),
        )

        return ip_pre_processing_manager._collapse_incarceration_period_transfers(
            incarceration_periods=incarceration_periods,
            overwrite_facility_information_in_transfers=overwrite_facility_information_in_transfers,
        )

    def test_collapse_incarceration_period_transfers(self):
        """Tests _collapse_incarceration_period_transfers for two incarceration periods
        linked by a transfer."""

        state_code = "US_XX"
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        first_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            state_code=state_code,
            admission_date=date(2010, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            initial_incarceration_period,
            first_reincarceration_period,
        ]

        updated_periods = self._collapse_incarceration_period_transfers(
            incarceration_periods=incarceration_periods,
        )

        self.assertListEqual(
            [
                StateIncarcerationPeriod.new_with_defaults(
                    incarceration_period_id=initial_incarceration_period.incarceration_period_id,
                    state_code=initial_incarceration_period.state_code,
                    admission_date=initial_incarceration_period.admission_date,
                    admission_reason=initial_incarceration_period.admission_reason,
                    release_date=first_reincarceration_period.release_date,
                    release_reason=first_reincarceration_period.release_reason,
                )
            ],
            updated_periods,
        )

    def test_collapse_incarceration_period_transfers_no_transfers(self):
        """Tests _collapse_incarceration_period_transfers for two incarceration
        periods not linked by a transfer."""

        state_code = "US_XX"
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        first_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            state_code=state_code,
            admission_date=date(2010, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            initial_incarceration_period,
            first_reincarceration_period,
        ]

        updated_periods = self._collapse_incarceration_period_transfers(
            incarceration_periods=incarceration_periods,
        )

        self.assertListEqual(incarceration_periods, updated_periods)

    def test_collapse_incarceration_period_transfers_multiple_transfers(self):
        """Tests _collapse_incarceration_period_transfers for a person who was repeatedly
        transferred between facilities. All of these incarceration periods
        should collapse into a single incarceration period."""

        state_code = "US_XX"
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        first_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            state_code=state_code,
            admission_date=date(2011, 3, 2),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        second_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            state_code=state_code,
            admission_date=date(2012, 2, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        third_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=4444,
            state_code=state_code,
            admission_date=date(2016, 6, 2),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2017, 3, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            initial_incarceration_period,
            first_reincarceration_period,
            second_reincarceration_period,
            third_reincarceration_period,
        ]

        updated_periods = self._collapse_incarceration_period_transfers(
            incarceration_periods=incarceration_periods,
        )

        self.assertListEqual(
            [
                StateIncarcerationPeriod.new_with_defaults(
                    incarceration_period_id=initial_incarceration_period.incarceration_period_id,
                    state_code=initial_incarceration_period.state_code,
                    admission_date=initial_incarceration_period.admission_date,
                    admission_reason=initial_incarceration_period.admission_reason,
                    release_date=third_reincarceration_period.release_date,
                    release_reason=third_reincarceration_period.release_reason,
                ),
            ],
            updated_periods,
        )

    def test_collapse_incarceration_period_transfers_between_periods(self):
        """Tests _collapse_incarceration_period_transfers for two incarceration
        periods linked by a transfer preceded and followed by regular
        incarceration periods."""

        state_code = "US_XX"
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        first_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            state_code=state_code,
            admission_date=date(2011, 3, 2),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        second_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            state_code=state_code,
            admission_date=date(2012, 2, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        third_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=4444,
            state_code=state_code,
            admission_date=date(2016, 6, 2),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2017, 3, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            initial_incarceration_period,
            first_reincarceration_period,
            second_reincarceration_period,
            third_reincarceration_period,
        ]

        updated_periods = self._collapse_incarceration_period_transfers(
            incarceration_periods=incarceration_periods,
        )

        self.assertListEqual(
            [
                initial_incarceration_period,
                StateIncarcerationPeriod.new_with_defaults(
                    incarceration_period_id=first_reincarceration_period.incarceration_period_id,
                    state_code=first_reincarceration_period.state_code,
                    admission_date=first_reincarceration_period.admission_date,
                    admission_reason=first_reincarceration_period.admission_reason,
                    release_date=second_reincarceration_period.release_date,
                    release_reason=second_reincarceration_period.release_reason,
                ),
                third_reincarceration_period,
            ],
            updated_periods,
        )

    def test_collapse_incarceration_period_transfers_no_incarcerations(self):
        """Tests _collapse_incarceration_period_transfers for an empty list of
        incarceration periods."""

        updated_periods = self._collapse_incarceration_period_transfers(
            incarceration_periods=[],
        )
        assert not updated_periods

    def test_collapse_incarceration_period_transfers_one_incarceration(self):
        """Tests _collapse_incarceration_period_transfers for a person with only
        one incarceration period that ended with a sentence served."""

        state_code = "US_XX"
        only_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [only_incarceration_period]

        updated_periods = self._collapse_incarceration_period_transfers(
            incarceration_periods=incarceration_periods,
        )

        self.assertListEqual(updated_periods, incarceration_periods)

    def test_collapse_incarceration_period_transfers_one_incarceration_transferred(
        self,
    ):
        """Tests _collapse_incarceration_period_transfers for a person with only
        one incarceration period that ended with a transfer out of state
        prison."""

        state_code = "US_XX"
        only_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_periods = [only_incarceration_period]

        updated_periods = self._collapse_incarceration_period_transfers(
            incarceration_periods=incarceration_periods,
        )

        self.assertListEqual(updated_periods, incarceration_periods)

    def test_collapse_incarceration_period_transfers_transfer_out_then_in(self):
        """Tests _collapse_incarceration_period_transfers for a person who was
        transferred elsewhere, potentially out of state, and then reappeared
        in the state later as a new admission. These two periods should not
        be collapsed, because it's possible that this person was transferred
        to another state or to a federal prison, was completely released from
        that facility, and then has a new crime admission back into this state's
        facility.

        Then, this person was conditionally released, and returned later due to
        a transfer. These two period should also not be collapsed, because we
        don't have enough knowledge to make a safe assumption that this last
        incarceration period is connected to any of the previous ones.
        """

        state_code = "US_XX"
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        first_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            state_code=state_code,
            admission_date=date(2011, 3, 2),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        second_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            state_code=state_code,
            admission_date=date(2013, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            initial_incarceration_period,
            first_reincarceration_period,
            second_reincarceration_period,
        ]

        updated_periods = self._collapse_incarceration_period_transfers(
            incarceration_periods=incarceration_periods,
        )

        self.assertListEqual(incarceration_periods, updated_periods)

    def test_collapse_incarceration_period_transfers_transfer_back_then_transfer(self):
        """Tests _collapse_incarceration_period_transfers for a person who was transferred
        elsewhere, perhaps out of state, and then later reappears in the
        system as a new admission. These two incarceration periods should not
        be collapsed, because it's possible that this person was transferred
        to another state or to a federal prison, was completely released from
        that facility, and then has a new crime admission back into this state's
        facility.

        Then, this person was transferred out of this period and into a new
        incarceration period. These two periods should be collapsed because
        they are connected by a transfer.
        """

        state_code = "US_XX"
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        first_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            state_code=state_code,
            admission_date=date(2011, 3, 2),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        second_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            state_code=state_code,
            admission_date=date(2012, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            initial_incarceration_period,
            first_reincarceration_period,
            second_reincarceration_period,
        ]

        updated_periods = self._collapse_incarceration_period_transfers(
            incarceration_periods=incarceration_periods,
        )

        self.assertListEqual(
            [
                initial_incarceration_period,
                StateIncarcerationPeriod.new_with_defaults(
                    incarceration_period_id=first_reincarceration_period.incarceration_period_id,
                    state_code=first_reincarceration_period.state_code,
                    admission_date=first_reincarceration_period.admission_date,
                    admission_reason=first_reincarceration_period.admission_reason,
                    release_date=second_reincarceration_period.release_date,
                    release_reason=second_reincarceration_period.release_reason,
                ),
            ],
            updated_periods,
        )


class TestCombineIncarcerationPeriods(unittest.TestCase):
    """Tests for _combine_incarceration_periods function."""

    def test_combineIncarcerationPeriods(self):
        """Tests for combining two incarceration periods connected by a
        transfer."""

        state_code = "US_XX"
        start_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            state_code=state_code,
            facility="Green",
            housing_unit="House19",
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MEDIUM,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        end_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            state_code=state_code,
            facility="Jones",
            housing_unit="HouseUnit3",
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            admission_date=date(2010, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 10),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        combined_incarceration_period = (
            IncarcerationPreProcessingManager._combine_incarceration_periods(
                start_incarceration_period,
                end_incarceration_period,
                overwrite_facility_information=True,
            )
        )

        assert combined_incarceration_period == StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=start_incarceration_period.incarceration_period_id,
            state_code=start_incarceration_period.state_code,
            facility=end_incarceration_period.facility,
            housing_unit=end_incarceration_period.housing_unit,
            facility_security_level=end_incarceration_period.facility_security_level,
            projected_release_reason=end_incarceration_period.projected_release_reason,
            admission_date=start_incarceration_period.admission_date,
            admission_reason=start_incarceration_period.admission_reason,
            release_date=end_incarceration_period.release_date,
            release_reason=end_incarceration_period.release_reason,
        )

    def test_combineIncarcerationPeriods_overwriteFacilityInformation(self):
        """Tests for combining two incarceration periods connected by a transfer, where the facility information
        (facility, housing unit, security level, purpose for incarceration) should be taken from the
        end_incarceration_period."""
        state_code = "US_XX"
        start_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            state_code=state_code,
            facility="Green",
            housing_unit="House19",
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MEDIUM,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        end_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            state_code=state_code,
            facility="Jones",
            housing_unit="HouseUnit3",
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            admission_date=date(2010, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 10),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        combined_incarceration_period = (
            IncarcerationPreProcessingManager._combine_incarceration_periods(
                start_incarceration_period,
                end_incarceration_period,
                overwrite_facility_information=True,
            )
        )

        expected_combined_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=start_incarceration_period.incarceration_period_id,
            state_code=start_incarceration_period.state_code,
            facility=end_incarceration_period.facility,
            housing_unit=end_incarceration_period.housing_unit,
            facility_security_level=end_incarceration_period.facility_security_level,
            specialized_purpose_for_incarceration=end_incarceration_period.specialized_purpose_for_incarceration,
            projected_release_reason=end_incarceration_period.projected_release_reason,
            admission_date=start_incarceration_period.admission_date,
            admission_reason=start_incarceration_period.admission_reason,
            release_date=end_incarceration_period.release_date,
            release_reason=end_incarceration_period.release_reason,
        )
        self.assertEqual(expected_combined_period, combined_incarceration_period)

    def test_combineIncarcerationPeriods_doNotOverwriteFacilityInformation(self):
        """Tests for combining two incarceration periods connected by a transfer, where the facility information
        (facility, housing unit, security level, purpose for incarceration) should be taken from the
        start_incarceration_period."""
        state_code = "US_XX"
        start_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            state_code=state_code,
            facility="Green",
            housing_unit="House19",
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MEDIUM,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        end_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            state_code=state_code,
            facility="Jones",
            housing_unit="HouseUnit3",
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            specialized_purpose_for_incarceration=None,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            admission_date=date(2010, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 10),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        combined_incarceration_period = (
            IncarcerationPreProcessingManager._combine_incarceration_periods(
                start_incarceration_period,
                end_incarceration_period,
                overwrite_facility_information=True,
            )
        )

        expected_combined_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=start_incarceration_period.incarceration_period_id,
            state_code=start_incarceration_period.state_code,
            facility=end_incarceration_period.facility,
            housing_unit=end_incarceration_period.housing_unit,
            facility_security_level=end_incarceration_period.facility_security_level,
            specialized_purpose_for_incarceration=start_incarceration_period.specialized_purpose_for_incarceration,
            projected_release_reason=end_incarceration_period.projected_release_reason,
            admission_date=start_incarceration_period.admission_date,
            admission_reason=start_incarceration_period.admission_reason,
            release_date=end_incarceration_period.release_date,
            release_reason=end_incarceration_period.release_reason,
        )
        self.assertEqual(expected_combined_period, combined_incarceration_period)


class TestSortAndInferMissingDatesAndStatuses(unittest.TestCase):
    """Tests the _infer_missing_dates_and_statuses."""

    @staticmethod
    def _sort_and_infer_missing_dates_and_statuses(
        incarceration_periods: List[StateIncarcerationPeriod],
    ):
        # None of the state-agnostic tests rely on supervision periods
        sp_index = None
        # None of the state-agnostic tests rely on violation responses
        violation_responses = None

        ip_pre_processing_manager = IncarcerationPreProcessingManager(
            incarceration_periods=incarceration_periods,
            pre_processing_delegate=UsXxIncarcerationPreProcessingDelegate(),
            incarceration_delegate=UsXxIncarcerationDelegate(),
            pre_processed_supervision_period_index=sp_index,
            violation_responses=violation_responses,
            earliest_death_date=None,
            field_index=CoreEntityFieldIndex(),
        )

        return ip_pre_processing_manager._sort_and_infer_missing_dates_and_statuses(
            incarceration_periods=incarceration_periods,
        )

    def test_sort_and_infer_missing_dates_and_statuses(self):
        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        valid_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            state_code="US_XX",
            admission_date=date(2012, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        valid_incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            external_id="3",
            state_code="US_XX",
            admission_date=date(2012, 12, 24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        # Invalid open period with same admission date as valid_incarceration_period_1
        invalid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="0",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
        )

        incarceration_periods = [
            valid_incarceration_period_3,
            valid_incarceration_period_1,
            valid_incarceration_period_2,
            invalid_open_period,
        ]

        updated_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="0",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            # Release date set to the admission date of valid_incarceration_period_1
            release_date=valid_incarceration_period_1.admission_date,
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
        )

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(
                [
                    updated_open_period,
                    valid_incarceration_period_1,
                    valid_incarceration_period_2,
                    valid_incarceration_period_3,
                ],
                updated_periods,
            )

    def test_sort_and_infer_missing_dates_and_statuses_all_valid(self):
        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="3",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        valid_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="4",
            state_code="US_XX",
            admission_date=date(2012, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        valid_incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            external_id="5",
            state_code="US_XX",
            admission_date=date(2012, 12, 24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        # Valid open period
        valid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="6",
            state_code="US_XX",
            admission_date=date(2015, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        incarceration_periods = [
            valid_open_period,
            valid_incarceration_period_1,
            valid_incarceration_period_2,
            valid_incarceration_period_3,
        ]

        ordered_periods = [
            valid_incarceration_period_1,
            valid_incarceration_period_2,
            valid_incarceration_period_3,
            valid_open_period,
        ]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(ordered_periods, updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_all_valid_shared_release(self):
        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="3",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        nested_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="4",
            state_code="US_XX",
            admission_date=date(2012, 11, 29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_periods = [
            valid_incarceration_period,
            nested_incarceration_period,
        ]

        ordered_periods = [valid_incarceration_period]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(ordered_periods, updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_set_empty_reasons(self):
        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="3",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=None,
            release_date=date(2012, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        valid_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="4",
            state_code="US_XX",
            admission_date=date(2012, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        valid_incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            external_id="5",
            state_code="US_XX",
            admission_date=date(2012, 12, 24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 30),
            release_reason=None,
        )

        # Valid open period
        valid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="5",
            state_code="US_XX",
            admission_date=date(2015, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        incarceration_periods = [
            valid_open_period,
            valid_incarceration_period_1,
            valid_incarceration_period_2,
            valid_incarceration_period_3,
        ]

        expected_output = [
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                external_id="3",
                state_code="US_XX",
                admission_date=date(2011, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
                release_date=date(2012, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            ),
            valid_incarceration_period_2,
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1113,
                external_id="5",
                state_code="US_XX",
                admission_date=date(2012, 12, 24),
                admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
                release_date=date(2012, 12, 30),
                release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            ),
            valid_open_period,
        ]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(expected_output, updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_only_open(self):
        valid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="5",
            state_code="US_XX",
            admission_date=date(2015, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        incarceration_periods = [valid_open_period]

        updated_periods = self._sort_and_infer_missing_dates_and_statuses(
            incarceration_periods=incarceration_periods
        )

        self.assertEqual(incarceration_periods, updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_invalid_dates(self):
        # We drop any periods with a release_date that precedes the admission_date
        valid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="5",
            state_code="US_XX",
            admission_date=date(2015, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2015, 1, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_periods = [valid_open_period]

        updated_periods = self._sort_and_infer_missing_dates_and_statuses(
            incarceration_periods=incarceration_periods
        )

        self.assertEqual([], updated_periods)

    @freeze_time("2000-01-01")
    def test_sort_and_infer_missing_dates_and_statuses_invalid_admission_date_in_future(
        self,
    ):
        # We drop any periods with an admission_date in the future
        invalid_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="5",
            state_code="US_XX",
            admission_date=date(2015, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2015, 1, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_periods = [invalid_period]

        updated_periods = self._sort_and_infer_missing_dates_and_statuses(
            incarceration_periods=incarceration_periods
        )

        self.assertEqual([], updated_periods)

    @freeze_time("2000-01-01")
    def test_sort_and_infer_missing_dates_and_statuses_invalid_release_date_in_future(
        self,
    ):
        # We clear the release information for release_dates in the future
        invalid_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="5",
            state_code="US_XX",
            admission_date=date(1990, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2015, 1, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_periods = [invalid_period]

        updated_periods = self._sort_and_infer_missing_dates_and_statuses(
            incarceration_periods=incarceration_periods
        )

        updated_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="5",
            state_code="US_XX",
            admission_date=date(1990, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=None,
            release_reason=None,
        )

        self.assertEqual([updated_period], updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_open_with_release_reason(self):
        valid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="5",
            state_code="US_XX",
            admission_date=date(2015, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=None,
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [valid_open_period]

        updated_periods = self._sort_and_infer_missing_dates_and_statuses(
            incarceration_periods=incarceration_periods
        )

        updated_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="5",
            state_code="US_XX",
            admission_date=date(2015, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2015, 11, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        self.assertEqual([updated_period], updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_only_one_closed(self):
        closed_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="5",
            state_code="US_XX",
            release_date=date(2015, 11, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [closed_period]

        updated_periods = self._sort_and_infer_missing_dates_and_statuses(
            incarceration_periods=incarceration_periods
        )

        updated_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="5",
            state_code="US_XX",
            admission_date=date(2015, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            release_date=date(2015, 11, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        self.assertEqual([updated_period], updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_missing_admission(self):
        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        valid_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            state_code="US_XX",
            admission_date=date(2012, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        valid_incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            external_id="3",
            state_code="US_XX",
            admission_date=date(2012, 12, 24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        # Invalid period without an admission_date, where the release_date is the same as the release_date on
        # valid_incarceration_period_3
        invalid_period_no_admission = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="0",
            state_code="US_XX",
            release_date=date(2012, 12, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            valid_incarceration_period_3,
            valid_incarceration_period_1,
            valid_incarceration_period_2,
            invalid_period_no_admission,
        ]

        updated_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="0",
            state_code="US_XX",
            admission_date=valid_incarceration_period_3.release_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            release_date=date(2012, 12, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(
                [
                    valid_incarceration_period_1,
                    valid_incarceration_period_2,
                    valid_incarceration_period_3,
                    updated_period,
                ],
                updated_periods,
            )

    def test_sort_and_infer_missing_dates_and_statuses_missing_admission_same_day_transfer(
        self,
    ):
        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="5-6",
            state_code="US_XX",
            admission_date=date(2015, 12, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2016, 2, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        # Invalid period without an admission_date, where the release_date is the same as the release_date on
        invalid_period_no_admission = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="7",
            state_code="US_XX",
            release_date=date(2016, 2, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        valid_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            external_id="8-9",
            state_code="US_XX",
            admission_date=date(2016, 2, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2016, 2, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        valid_incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1114,
            external_id="10-11",
            state_code="US_XX",
            admission_date=date(2016, 2, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2016, 4, 5),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            invalid_period_no_admission,
            valid_incarceration_period_3,
            valid_incarceration_period_1,
            valid_incarceration_period_2,
        ]

        updated_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="7",
            state_code="US_XX",
            admission_date=valid_incarceration_period_1.release_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2016, 2, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(
                [
                    valid_incarceration_period_1,
                    updated_period,
                    valid_incarceration_period_2,
                    valid_incarceration_period_3,
                ],
                updated_periods,
            )

    def test_sort_and_infer_missing_dates_and_statuses_mismatch_admission_release(self):
        # Open incarceration period with no release_date
        invalid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            external_id="0",
            state_code="US_XX",
            admission_date=date(2012, 12, 24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
        )

        # Invalid period without an admission_date
        invalid_closed_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="1",
            state_code="US_XX",
            release_date=date(2012, 12, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [invalid_closed_period, invalid_open_period]

        updated_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            external_id="0",
            state_code="US_XX",
            admission_date=date(2012, 12, 24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
        )

        updated_closed_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="1",
            state_code="US_XX",
            admission_date=date(2012, 12, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            release_date=date(2012, 12, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(
                [updated_open_period, updated_closed_period],
                updated_periods,
            )

    def test_sort_and_infer_missing_dates_and_statuses_valid_open_admission(self):
        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2011, 11, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        # Invalid open period with same admission date as valid_incarceration_period_1
        valid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="0",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
        )

        incarceration_periods = [valid_incarceration_period_1, valid_open_period]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )
            self.assertEqual(
                incarceration_periods,
                updated_periods,
            )

    def test_sort_and_infer_missing_dates_and_statuses_two_open_two_invalid_closed(
        self,
    ):
        open_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            state_code="US_XX",
            admission_date=date(2001, 6, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
        )

        open_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="2",
            state_code="US_XX",
            admission_date=date(2001, 6, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
        )

        closed_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            external_id="3",
            state_code="US_XX",
            release_date=date(2001, 6, 19),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        closed_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=4444,
            external_id="4",
            state_code="US_XX",
            release_date=date(2001, 7, 17),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_periods = [
            open_incarceration_period_1,
            open_incarceration_period_2,
            closed_incarceration_period_1,
            closed_incarceration_period_2,
        ]

        updated_open_incarceration_period_1 = (
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                external_id="1",
                state_code="US_XX",
                admission_date=date(2001, 6, 11),
                admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
                release_date=date(2001, 6, 19),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            )
        )

        updated_open_incarceration_period_2 = (
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                external_id="2",
                state_code="US_XX",
                admission_date=date(2001, 6, 19),
                admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
                release_date=date(2001, 6, 19),
                release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            )
        )

        updated_closed_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            external_id="3",
            state_code="US_XX",
            admission_date=date(2001, 6, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            release_date=date(2001, 6, 19),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        updated_closed_incarceration_period_2 = (
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=4444,
                external_id="4",
                state_code="US_XX",
                admission_date=date(2001, 6, 19),
                admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
                release_date=date(2001, 7, 17),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            )
        )

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(
                [
                    updated_open_incarceration_period_1,
                    updated_open_incarceration_period_2,
                    updated_closed_incarceration_period_1,
                    updated_closed_incarceration_period_2,
                ],
                updated_periods,
            )

    def test_sort_and_infer_missing_dates_and_statuses_multiple_open_periods(self):
        open_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            state_code="US_XX",
            admission_date=date(2001, 6, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
        )

        open_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="2",
            state_code="US_XX",
            admission_date=date(2001, 6, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
        )

        open_incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            external_id="3",
            state_code="US_XX",
            admission_date=date(2001, 6, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
        )

        incarceration_periods = [
            open_incarceration_period_1,
            open_incarceration_period_2,
            open_incarceration_period_3,
        ]

        # Tests that only one period is left open
        for ip_order_combo in permutations(incarceration_periods.copy()):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            num_open_periods = len(
                [ip for ip in updated_periods if ip.release_date is None]
            )

            self.assertEqual(1, num_open_periods)

    def test_sort_and_infer_missing_dates_and_statuses_no_periods(self):
        incarceration_periods = []

        updated_periods = self._sort_and_infer_missing_dates_and_statuses(
            incarceration_periods=incarceration_periods
        )

        self.assertEqual([], updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_set_missing_admission_data_after_transfer(
        self,
    ):
        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2004, 1, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id="2",
            incarceration_period_id=2222,
            state_code="US_XX",
            admission_date=None,
            admission_reason=None,
            release_date=date(2010, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            first_incarceration_period,
            second_incarceration_period,
        ]

        expected_output = [
            StateIncarcerationPeriod.new_with_defaults(
                external_id="1",
                incarceration_period_id=1111,
                state_code="US_XX",
                admission_date=date(2004, 1, 3),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2008, 4, 14),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            ),
            StateIncarcerationPeriod.new_with_defaults(
                external_id="2",
                incarceration_period_id=2222,
                state_code="US_XX",
                admission_date=date(2008, 4, 14),
                admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
                release_date=date(2010, 4, 14),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            ),
        ]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(expected_output, updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_same_dates_sort_by_transfer_start(
        self,
    ):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2004, 1, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_period_copy = StateIncarcerationPeriod.new_with_defaults(
            external_id="2",
            incarceration_period_id=2222,
            state_code="US_XX",
            admission_date=date(2004, 1, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2008, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_periods = [incarceration_period, incarceration_period_copy]

        updated_order = [incarceration_period]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(updated_order, updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_same_admission_dates_sort_by_external_ids(
        self,
    ):
        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2004, 1, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id="2",
            incarceration_period_id=2222,
            state_code="US_XX",
            admission_date=date(2004, 1, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        incarceration_periods = [
            second_incarceration_period,
            first_incarceration_period,
        ]

        updated_first_incarceration_period = attr.evolve(
            first_incarceration_period,
            release_date=date(2004, 1, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
        )

        expected_output = [
            updated_first_incarceration_period,
            second_incarceration_period,
        ]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(expected_output, updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_no_admission_dates(self):
        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            release_date=date(2004, 1, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id="2",
            incarceration_period_id=2222,
            state_code="US_XX",
            release_date=date(2004, 1, 10),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            second_incarceration_period,
            first_incarceration_period,
        ]

        updated_first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2004, 1, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            release_date=date(2004, 1, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id="2",
            incarceration_period_id=2222,
            state_code="US_XX",
            admission_date=date(2004, 1, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            release_date=date(2004, 1, 10),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        expected_output = [
            updated_first_incarceration_period,
            second_incarceration_period,
        ]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(expected_output, updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_nested_period(self):
        outer_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 2, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        nested_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id="2",
            incarceration_period_id=2222,
            state_code="US_XX",
            admission_date=date(2002, 3, 13),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 3, 13),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_periods = [outer_ip, nested_ip]

        expected_output = [outer_ip]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(expected_output, updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_multiple_nested_periods(self):
        outer_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 2, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        nested_ip_1 = StateIncarcerationPeriod.new_with_defaults(
            external_id="2",
            incarceration_period_id=2222,
            state_code="US_XX",
            admission_date=date(2002, 2, 13),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 2, 18),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        nested_ip_2 = StateIncarcerationPeriod.new_with_defaults(
            external_id="3",
            incarceration_period_id=3333,
            state_code="US_XX",
            admission_date=date(2002, 2, 18),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 6, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        nested_ip_3 = StateIncarcerationPeriod.new_with_defaults(
            external_id="4",
            incarceration_period_id=4444,
            state_code="US_XX",
            admission_date=date(2002, 6, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 6, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        nested_ip_4 = StateIncarcerationPeriod.new_with_defaults(
            external_id="5",
            incarceration_period_id=5555,
            state_code="US_XX",
            admission_date=date(2002, 6, 29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 9, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_periods = [
            outer_ip,
            nested_ip_1,
            nested_ip_2,
            nested_ip_3,
            nested_ip_4,
        ]

        expected_output = [outer_ip]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(expected_output, updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_partial_overlap_period(self):
        ip_1 = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 2, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        ip_2 = StateIncarcerationPeriod.new_with_defaults(
            external_id="2",
            incarceration_period_id=2222,
            state_code="US_XX",
            admission_date=date(2002, 3, 13),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 10, 22),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_periods = [ip_1, ip_2]

        expected_output = [ip_1, ip_2]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(expected_output, updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_borders_edges(self):
        zero_day_period_start = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2011, 11, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
        )

        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        zero_day_period_end = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            external_id="3",
            state_code="US_XX",
            admission_date=date(2012, 12, 24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        incarceration_periods = [
            zero_day_period_start,
            valid_incarceration_period,
            zero_day_period_end,
        ]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(
                [
                    zero_day_period_start,
                    valid_incarceration_period,
                    zero_day_period_end,
                ],
                updated_periods,
            )


class TestIsZeroDayErroneousPeriod(unittest.TestCase):
    """Tests the _is_zero_day_erroneous_period function on the
    IncarcerationPreProcessingManager."""

    def test_drop_zero_day_erroneous_periods(self):
        state_code = "US_XX"
        invalid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2008, 11, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
        )

        self.assertTrue(
            IncarcerationPreProcessingManager._is_zero_day_erroneous_period(
                invalid_incarceration_period, None, None
            )
        )

    def test_drop_zero_day_erroneous_periods_valid(self):
        state_code = "US_XX"
        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            # Don't drop a period if admission_date != release_date
            release_date=date(2008, 11, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
        )

        self.assertFalse(
            IncarcerationPreProcessingManager._is_zero_day_erroneous_period(
                valid_incarceration_period, None, None
            )
        )

    def test_drop_zero_day_erroneous_periods_transfer_adm(self):
        state_code = "US_XX"
        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2008, 11, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
        )

        self.assertFalse(
            IncarcerationPreProcessingManager._is_zero_day_erroneous_period(
                valid_incarceration_period, None, None
            )
        )

    def test_drop_zero_day_erroneous_periods_non_erroneous_admission(self):
        state_code = "US_XX"
        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2008, 11, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        self.assertFalse(
            IncarcerationPreProcessingManager._is_zero_day_erroneous_period(
                valid_incarceration_period, None, None
            )
        )

    def test_drop_zero_day_erroneous_periods_borders_edges(self):
        zero_day_period_start = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2011, 11, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
        )

        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        zero_day_period_end = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            external_id="3",
            state_code="US_XX",
            admission_date=date(2012, 12, 24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN,
            release_date=date(2012, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        self.assertTrue(
            IncarcerationPreProcessingManager._is_zero_day_erroneous_period(
                zero_day_period_start, None, valid_incarceration_period
            )
        )

        self.assertTrue(
            IncarcerationPreProcessingManager._is_zero_day_erroneous_period(
                zero_day_period_end, valid_incarceration_period, None
            )
        )

        self.assertFalse(
            IncarcerationPreProcessingManager._is_zero_day_erroneous_period(
                valid_incarceration_period,
                zero_day_period_start,
                zero_day_period_end,
            )
        )


class TestStatusChangeEdges(unittest.TestCase):
    """Tests the various functionality related to setting accurate STATUS_CHANGE
    edges on periods."""

    def test_update_transfers_to_status_changes(self):
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2019, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2019, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        updated_periods = (
            IncarcerationPreProcessingManager._update_transfers_to_status_changes(
                incarceration_periods
            )
        )

        expected_updated_periods = [
            attr.evolve(
                incarceration_period_1,
                release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
            ),
            attr.evolve(
                incarceration_period_2,
                admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            ),
        ]

        self.assertEqual(expected_updated_periods, updated_periods)

    def test_update_transfers_to_status_changes_diff_custodial_authority(self):
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2019, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2019, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        updated_periods = (
            IncarcerationPreProcessingManager._update_transfers_to_status_changes(
                incarceration_periods
            )
        )

        expected_updated_periods = [
            attr.evolve(
                incarceration_period_1,
                release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
            ),
            attr.evolve(
                incarceration_period_2,
                admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            ),
        ]

        self.assertEqual(expected_updated_periods, updated_periods)

    def test_update_transfers_to_status_changes_not_status_change(self):
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2019, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2019, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        ip_periods_copied = [
            attr.evolve(
                incarceration_period_1,
            ),
            attr.evolve(
                incarceration_period_2,
            ),
        ]

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        updated_periods = (
            IncarcerationPreProcessingManager._update_transfers_to_status_changes(
                incarceration_periods
            )
        )

        self.assertEqual(ip_periods_copied, updated_periods)

    def test_update_transfers_to_status_changes_null_to_set(self):
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=None,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2019, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2019, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        expected_updated_periods = [
            attr.evolve(
                incarceration_period_1,
                release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
            ),
            attr.evolve(
                incarceration_period_2,
                admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            ),
        ]

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        updated_periods = (
            IncarcerationPreProcessingManager._update_transfers_to_status_changes(
                incarceration_periods
            )
        )

        self.assertEqual(expected_updated_periods, updated_periods)

    def test_update_transfers_to_status_changes_both_null(self):
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=None,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2019, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=None,
            release_date=date(2019, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        ip_periods_copied = [
            attr.evolve(
                incarceration_period_1,
            ),
            attr.evolve(
                incarceration_period_2,
            ),
        ]

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        updated_periods = (
            IncarcerationPreProcessingManager._update_transfers_to_status_changes(
                incarceration_periods
            )
        )

        self.assertEqual(ip_periods_copied, updated_periods)

    def test_is_status_change_edge_valid_attributes(self):
        """Tests that all attributes listed in ATTRIBUTES_TRIGGERING_STATUS_CHANGE
        are valid attributes on the StateIncarcerationPeriod class."""
        for attribute in ATTRIBUTES_TRIGGERING_STATUS_CHANGE:
            if attribute not in StateIncarcerationPeriod.__dict__.get(
                "__annotations__"
            ):
                raise ValueError(
                    "Attribute in ATTRIBUTES_TRIGGERING_STATUS_CHANGE "
                    "not valid attribute on the StateIncarcerationPeriod "
                    "class."
                )


class TestValidateIpInvariants(unittest.TestCase):
    """Tests the validate_ip_invariants function."""

    def setUp(self) -> None:
        self.valid_ip = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            external_id="3",
            state_code="US_XX",
            admission_date=date(2010, 1, 13),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

    def test_validate_ip_invariants_valid(self):
        # Assert no error
        IncarcerationPreProcessingManager.validate_ip_invariants([self.valid_ip])

    def test_validate_ip_invariants_missing_admission_reason(self):
        invalid_ip_missing = attr.evolve(self.valid_ip, admission_reason=None)

        with self.assertRaises(ValueError):
            IncarcerationPreProcessingManager.validate_ip_invariants(
                [invalid_ip_missing]
            )

    def test_validate_ip_invariants_missing_admission_date(self):
        invalid_ip_missing = attr.evolve(self.valid_ip, admission_date=None)

        with self.assertRaises(ValueError):
            IncarcerationPreProcessingManager.validate_ip_invariants(
                [invalid_ip_missing]
            )

    def test_validate_ip_invariants_missing_pfi(self):
        invalid_ip_missing = attr.evolve(
            self.valid_ip, specialized_purpose_for_incarceration=None
        )

        with self.assertRaises(ValueError):
            IncarcerationPreProcessingManager.validate_ip_invariants(
                [invalid_ip_missing]
            )

    def test_validate_ip_invariants_invalid_admission_reason(self):
        invalid_ip_missing = attr.evolve(
            self.valid_ip,
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
        )

        with self.assertRaises(ValueError):
            IncarcerationPreProcessingManager.validate_ip_invariants(
                [invalid_ip_missing]
            )
