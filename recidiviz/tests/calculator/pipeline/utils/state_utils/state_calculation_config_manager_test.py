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
"""Tests the functions in the state_calculation_config_manager file."""
from datetime import date

import unittest

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.utils.state_utils import state_calculation_config_manager
from recidiviz.calculator.pipeline.utils.supervision_period_utils import SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason as AdmissionReason, \
    StateIncarcerationPeriodReleaseReason as ReleaseReason, \
    StateIncarcerationPeriodStatus, StateSpecializedPurposeForIncarceration, StateIncarcerationPeriodAdmissionReason
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodStatus, StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod, StateIncarcerationPeriod


class TestRevokedSupervisionPeriodsIfRevocationOccurred(unittest.TestCase):
    """Tests the state-specific revoked_supervision_periods_if_revocation_occurred function."""
    def test_revoked_supervision_periods_if_revocation_occurred(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_XX',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            release_date=date(2019, 5, 29),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_periods = []

        admission_is_revocation, revoked_periods = \
            state_calculation_config_manager.revoked_supervision_periods_if_revocation_occurred(
                incarceration_period, supervision_periods, None
            )

        self.assertTrue(admission_is_revocation)
        self.assertEqual([], revoked_periods)

    def test_revoked_supervision_periods_if_revocation_occured_with_general_purpose_US_ID(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2019, 5, 29),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_periods = []

        admission_is_revocation, revoked_periods = \
            state_calculation_config_manager.revoked_supervision_periods_if_revocation_occurred(
                incarceration_period, supervision_periods, None
            )

        self.assertTrue(admission_is_revocation)
        self.assertEqual([], revoked_periods)

    def test_revoked_supervision_periods_if_revocation_occurred_with_treatment_US_ID(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=AdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2019, 5, 29),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
        )

        supervision_periods = []

        admission_is_revocation, revoked_periods = \
            state_calculation_config_manager.revoked_supervision_periods_if_revocation_occurred(
                incarceration_period, supervision_periods, None
            )

        self.assertTrue(admission_is_revocation)
        self.assertEqual([], revoked_periods)

    def test_revoked_supervision_periods_if_revocation_did_not_occur_with_treatment_transfer_US_ID(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        treatment_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=AdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2017, 5, 29),
            release_reason=ReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON)

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 29),
            admission_reason=AdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_periods = [supervision_period]

        admission_is_revocation, revoked_periods = \
            state_calculation_config_manager.revoked_supervision_periods_if_revocation_occurred(
                incarceration_period, supervision_periods, treatment_period
            )

        self.assertFalse(admission_is_revocation)
        self.assertEqual([], revoked_periods)

    def test_revoked_supervision_periods_if_revocation_occurred_US_ID_NoRecentSupervision(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ID',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
        )

        # Incarceration period that occurred more than SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT months after
        # the most recent supervision period ended
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=
            supervision_period.termination_date + relativedelta(months=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT + 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_periods = [supervision_period]

        admission_is_revocation, revoked_periods = \
            state_calculation_config_manager.revoked_supervision_periods_if_revocation_occurred(
                incarceration_period, supervision_periods, None
            )

        self.assertTrue(admission_is_revocation)
        self.assertEqual([], revoked_periods)

    def test_revoked_supervision_periods_if_revocation_occurred_US_ID_InvestigationSupervision(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ID',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 9),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_periods = [supervision_period]

        admission_is_revocation, revoked_periods = \
            state_calculation_config_manager.revoked_supervision_periods_if_revocation_occurred(
                incarceration_period, supervision_periods, None
            )

        self.assertFalse(admission_is_revocation)
        self.assertEqual([], revoked_periods)
