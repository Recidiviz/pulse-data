# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests the functions in us_id_revocation_identification.py"""
import unittest
from datetime import date

from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_revocation_identification import \
    us_id_revoked_supervision_period_if_revocation_occurred, \
    us_id_filter_supervision_periods_for_revocation_identification, us_id_get_pre_revocation_supervision_type
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodStatus, \
    StateIncarcerationPeriodAdmissionReason, StateSpecializedPurposeForIncarceration, \
    StateIncarcerationPeriodReleaseReason
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodStatus, \
    StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod, StateSupervisionPeriod


class TestUsIdIncarcerationAdmissionDateIfRevocationOccurred(unittest.TestCase):
    """Tests the us_id_revoked_supervision_period_if_revocation_occurred function."""
    def test_us_id_revoked_supervision_period_if_revocation_occurred_probation_revocation(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_periods = [supervision_period]

        admission_is_revocation, revoked_period = us_id_revoked_supervision_period_if_revocation_occurred(
            incarceration_revocation_period, supervision_periods, None)

        self.assertTrue(admission_is_revocation)
        self.assertEqual(revoked_period, supervision_period)

    def test_us_id_revoked_supervision_period_if_revocation_occurred_treatment(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
        )

        supervision_periods = [supervision_period]

        admission_is_revocation, revoked_period = us_id_revoked_supervision_period_if_revocation_occurred(
            incarceration_revocation_period, supervision_periods, None)

        self.assertTrue(admission_is_revocation)
        self.assertEqual(revoked_period, supervision_period)

    def test_us_id_not_revoked_supervision_period_if_shock_incarceration(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION
        )

        supervision_periods = [supervision_period]

        admission_is_revocation, revoked_period = us_id_revoked_supervision_period_if_revocation_occurred(
            incarceration_revocation_period, supervision_periods, None)

        self.assertFalse(admission_is_revocation)
        self.assertIsNone(revoked_period)

    def test_us_id_revoked_supervision_period_if_revocation_occurred_parole_board_revocation(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        board_hold_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2017, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD
        )

        incarceration_revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_periods = [supervision_period]

        admission_is_revocation, revoked_period = us_id_revoked_supervision_period_if_revocation_occurred(
            incarceration_revocation_period, supervision_periods, board_hold_period)
        self.assertTrue(admission_is_revocation)
        self.assertEqual(revoked_period, supervision_period)

    def test_us_id_revoked_supervision_period_if_revocation_occurred_treatment_transfer_not_revocation(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        treatment_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2017, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
        )

        transfer_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_periods = [supervision_period]

        admission_is_revocation, revoked_period = us_id_revoked_supervision_period_if_revocation_occurred(
            transfer_incarceration_period, supervision_periods, treatment_period)
        self.assertFalse(admission_is_revocation)
        self.assertIsNone(revoked_period)

    def test_us_id_revoked_supervision_period_if_revocation_occurred_transfers_not_same_day(self):
        treatment_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2017, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
        )

        incarceration_revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2018, 9, 29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        admission_is_revocation, revoked_period = us_id_revoked_supervision_period_if_revocation_occurred(
            incarceration_revocation_period, [], treatment_period)

        self.assertFalse(admission_is_revocation)
        self.assertIsNone(revoked_period)

    def test_us_id_revoked_supervision_period_if_revocation_occurred_no_revocation(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2019, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        admission_is_revocation, revoked_period = us_id_revoked_supervision_period_if_revocation_occurred(
            incarceration_period, [], None)

        self.assertFalse(admission_is_revocation)
        self.assertIsNone(revoked_period)

    def test_us_id_revoked_supervision_period_if_revocation_occurred_transfer_admission(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2019, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        admission_is_revocation, revoked_period = us_id_revoked_supervision_period_if_revocation_occurred(
            incarceration_period, [], None)

        self.assertFalse(admission_is_revocation)
        self.assertIsNone(revoked_period)

    def test_us_id_revoked_supervision_period_if_revocation_occurred_regular_transfer(self):
        board_hold_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2017, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        incarceration_revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        admission_is_revocation, revoked_period = us_id_revoked_supervision_period_if_revocation_occurred(
            incarceration_revocation_period, [], board_hold_period)

        self.assertFalse(admission_is_revocation)
        self.assertIsNone(revoked_period)

    def test_us_id_revoked_supervision_period_if_revocation_occurred_investigation_not_revocation(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        )

        incarceration_revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_periods = [supervision_period]

        admission_is_revocation, revoked_period = us_id_revoked_supervision_period_if_revocation_occurred(
            incarceration_revocation_period, supervision_periods, None)

        self.assertFalse(admission_is_revocation)
        self.assertIsNone(revoked_period)


class TestSupervisionFiltering(unittest.TestCase):
    """Tests the us_id_filter_supervision_periods_for_revocation_identification function."""
    def test_us_id_filter_supervision_periods_for_revocation_identification(self):
        supervision_period_set = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ID',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
        )

        supervision_period_unset = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ID',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=None
        )

        supervision_periods = [supervision_period_set, supervision_period_unset]

        self.assertEqual([supervision_period_set],
                         us_id_filter_supervision_periods_for_revocation_identification(supervision_periods))

    def test_us_id_filter_supervision_periods_for_revocation_identification_internal_unknown(self):
        supervision_period_set = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ID',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
        )

        supervision_period_unset = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ID',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
        )

        supervision_periods = [supervision_period_set, supervision_period_unset]

        self.assertEqual([supervision_period_set],
                         us_id_filter_supervision_periods_for_revocation_identification(supervision_periods))

    def test_us_id_filter_supervision_periods_for_revocation_identification_empty_list(self):
        supervision_periods = []

        self.assertEqual([],
                         us_id_filter_supervision_periods_for_revocation_identification(supervision_periods))


class TestGetPreRevocationSupervisionType(unittest.TestCase):
    """Tests the us_id_get_pre_revocation_supervision_type function."""
    def test_us_id_get_pre_revocation_supervision_type(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ID',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
        )

        self.assertEqual(StateSupervisionPeriodSupervisionType.PROBATION,
                         us_id_get_pre_revocation_supervision_type(supervision_period))

    def test_us_id_get_pre_revocation_supervision_type_Empty(self):
        self.assertIsNone(us_id_get_pre_revocation_supervision_type(None))
