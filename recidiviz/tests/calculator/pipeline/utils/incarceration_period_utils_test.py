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

"""Tests for incarceration_period_utils.py."""

import unittest
from datetime import date
from itertools import permutations

import attr
import pytest
from freezegun import freeze_time

from recidiviz.calculator.pipeline.utils.incarceration_period_utils import \
    drop_placeholder_periods, \
    prepare_incarceration_periods_for_calculations, \
    collapse_temporary_custody_and_revocation_periods, drop_periods_not_under_state_custodial_authority, \
    _infer_missing_dates_and_statuses, _ip_is_nested_in_previous_period
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationFacilitySecurityLevel, StateSpecializedPurposeForIncarceration
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason as AdmissionReason
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodReleaseReason as ReleaseReason
from recidiviz.persistence.entity.state.entities import \
    StateIncarcerationPeriod
from recidiviz.calculator.pipeline.utils import \
    incarceration_period_utils as utils


class TestDropPeriods(unittest.TestCase):
    """Tests the drop_placeholder_periods and drop_periods_not_under_state_custodial_authority function."""

    def test_drop_placeholder_periods(self):
        state_code = 'US_XX'
        placeholder_custody = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
            state_code=state_code)

        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2016, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2017, 12, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [placeholder_custody, valid_incarceration_period]

        validated_incarceration_periods = drop_placeholder_periods(incarceration_periods)

        self.assertEqual([valid_incarceration_period], validated_incarceration_periods)

    def test_dropPeriodsNotUnderStateCustodialAuthority(self):
        state_code = 'US_XX'
        temporary_custody_jail = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY)
        temporary_custody_prison = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=112,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2008, 12, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2008, 12, 24),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY)
        new_admission_prison = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2016, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2017, 12, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED)
        incarceration_periods = [temporary_custody_jail, temporary_custody_prison, new_admission_prison]

        validated_incarceration_periods = drop_periods_not_under_state_custodial_authority(state_code,
                                                                                           incarceration_periods)

        self.assertCountEqual([temporary_custody_prison, new_admission_prison], validated_incarceration_periods)

    def test_dropPeriodsNotUnderStateCustodialAuthority_usNd(self):
        state_code = 'US_ND'
        temporary_custody_jail = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY)
        temporary_custody_prison = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=112,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2008, 12, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2008, 12, 24),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY)
        new_admission_prison = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2016, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2017, 12, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED)
        incarceration_periods = [temporary_custody_jail, temporary_custody_prison, new_admission_prison]

        validated_incarceration_periods = drop_periods_not_under_state_custodial_authority(state_code,
                                                                                           incarceration_periods)

        self.assertCountEqual([new_admission_prison], validated_incarceration_periods)

    def test_dropPeriodsNotUnderStateCustodialAuthority_usMo(self):
        state_code = 'US_MO'
        temporary_custody_jail = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY)
        temporary_custody_prison = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=112,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2008, 12, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2008, 12, 24),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY)
        new_admission_prison = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2016, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2017, 12, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED)
        incarceration_periods = [temporary_custody_jail, temporary_custody_prison, new_admission_prison]

        validated_incarceration_periods = drop_periods_not_under_state_custodial_authority(state_code,
                                                                                           incarceration_periods)

        self.assertCountEqual([temporary_custody_prison, new_admission_prison], validated_incarceration_periods)

    def test_dropPeriodsNotUnderStateCustodialAuthority_usId(self):
        state_code = 'US_ID'
        temporary_custody_jail = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY)
        temporary_custody_prison = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=112,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2008, 12, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2008, 12, 24),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY)
        new_admission_prison = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2016, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2017, 12, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED)
        incarceration_periods = [temporary_custody_jail, temporary_custody_prison, new_admission_prison]

        validated_incarceration_periods = drop_periods_not_under_state_custodial_authority(state_code,
                                                                                           incarceration_periods)

        self.assertCountEqual(
            [temporary_custody_jail, temporary_custody_prison, new_admission_prison], validated_incarceration_periods)


class TestCollapseIncarcerationPeriods(unittest.TestCase):
    """Tests the collapse_incarceration_periods function."""

    def test_collapse_incarceration_periods(self):
        """Tests collapse_incarceration_periods for two incarceration periods linked by a transfer."""

        state_code = 'US_XX'
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2010, 12, 4),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period]

        collapsed_incarceration_periods = \
            utils.collapse_incarceration_period_transfers(incarceration_periods)

        self.assertListEqual([
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=initial_incarceration_period.incarceration_period_id,
                status=first_reincarceration_period.status,
                state_code=initial_incarceration_period.state_code,
                admission_date=initial_incarceration_period.admission_date,
                admission_reason=initial_incarceration_period.admission_reason,
                release_date=first_reincarceration_period.release_date,
                release_reason=first_reincarceration_period.release_reason)
        ], collapsed_incarceration_periods)

    def test_collapse_incarceration_periods_no_transfers(self):
        """Tests collapse_incarceration_periods for two incarceration
        periods not linked by a transfer."""

        state_code = 'US_XX'
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2010, 12, 4),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period]

        collapsed_incarceration_periods = \
            utils.collapse_incarceration_period_transfers(incarceration_periods)

        self.assertListEqual(incarceration_periods, collapsed_incarceration_periods)

    def test_collapse_incarceration_periods_multiple_transfers(self):
        """Tests collapse_incarceration_periods for a person who was repeatedly
        transferred between facilities. All of these incarceration periods
        should collapse into a single incarceration period."""

        state_code = 'US_XX'
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2011, 3, 2),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2012, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2012, 2, 4),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.TRANSFER)

        third_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=4444,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2016, 6, 2),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2017, 3, 1),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period,
                                 second_reincarceration_period,
                                 third_reincarceration_period]

        collapsed_incarceration_periods = \
            utils.collapse_incarceration_period_transfers(incarceration_periods)

        self.assertListEqual([
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=initial_incarceration_period.
                incarceration_period_id,
                status=third_reincarceration_period.status,
                state_code=initial_incarceration_period.state_code,
                admission_date=initial_incarceration_period.admission_date,
                admission_reason=initial_incarceration_period.admission_reason,
                release_date=third_reincarceration_period.release_date,
                release_reason=third_reincarceration_period.release_reason),
        ], collapsed_incarceration_periods)

    def test_collapse_incarceration_periods_between_periods(self):
        """Tests collapse_incarceration_periods for two incarceration
        periods linked by a transfer preceded and followed by regular
        incarceration periods."""

        state_code = 'US_XX'
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2011, 3, 2),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2012, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2012, 2, 4),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        third_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=4444,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2016, 6, 2),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2017, 3, 1),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period,
                                 second_reincarceration_period,
                                 third_reincarceration_period]

        collapsed_incarceration_periods = \
            utils.collapse_incarceration_period_transfers(incarceration_periods)

        self.assertListEqual([
            initial_incarceration_period,
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=first_reincarceration_period.
                incarceration_period_id,
                status=second_reincarceration_period.status,
                state_code=first_reincarceration_period.state_code,
                admission_date=first_reincarceration_period.admission_date,
                admission_reason=first_reincarceration_period.admission_reason,
                release_date=second_reincarceration_period.release_date,
                release_reason=second_reincarceration_period.release_reason),
            third_reincarceration_period
        ], collapsed_incarceration_periods)

    def test_collapse_incarceration_periods_no_incarcerations(self):
        """Tests collapse_incarceration_periods for an empty list of
        incarceration periods."""

        collapsed_incarceration_periods = \
            utils.collapse_incarceration_period_transfers([])

        assert not collapsed_incarceration_periods

    def test_collapse_incarceration_periods_one_incarceration(self):
        """Tests collapse_incarceration_periods for a person with only
        one incarceration period that ended with a sentence served."""

        state_code = 'US_XX'
        only_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [only_incarceration_period]

        collapsed_incarceration_periods = \
            utils.collapse_incarceration_period_transfers(incarceration_periods)

        self.assertListEqual(collapsed_incarceration_periods, incarceration_periods)

    def test_collapse_incarceration_periods_one_incarceration_transferred(self):
        """Tests collapse_incarceration_periods for a person with only
        one incarceration period that ended with a transfer out of state
        prison."""

        state_code = 'US_XX'
        only_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.TRANSFER)

        incarceration_periods = [only_incarceration_period]

        collapsed_incarceration_periods = utils.collapse_incarceration_period_transfers(incarceration_periods)

        self.assertListEqual(collapsed_incarceration_periods, incarceration_periods)

    def test_collapse_incarceration_periods_transfer_out_then_in(self):
        """Tests collapse_incarceration_periods for a person who was
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

        state_code = 'US_XX'
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2011, 3, 2),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2012, 12, 4),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2013, 12, 4),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period,
                                 second_reincarceration_period]

        collapsed_incarceration_periods = \
            utils.collapse_incarceration_period_transfers(incarceration_periods)

        self.assertListEqual(incarceration_periods, collapsed_incarceration_periods)

    def test_collapse_incarceration_periods_transfer_back_then_transfer(self):
        """Tests collapse_incarceration_periods for a person who was transferred
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

        state_code = 'US_XX'
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2011, 3, 2),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2012, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2012, 12, 4),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period,
                                 second_reincarceration_period]

        collapsed_incarceration_periods = \
            utils.collapse_incarceration_period_transfers(incarceration_periods)

        self.assertListEqual([
            initial_incarceration_period,
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=first_reincarceration_period.
                incarceration_period_id,
                status=second_reincarceration_period.status,
                state_code=first_reincarceration_period.state_code,
                admission_date=first_reincarceration_period.admission_date,
                admission_reason=first_reincarceration_period.admission_reason,
                release_date=second_reincarceration_period.release_date,
                release_reason=second_reincarceration_period.release_reason),
        ], collapsed_incarceration_periods)

    def test_collapse_incarceration_periods_transfer_different_pfi_do_not_collapse(self):
        """Tests collapse_incarceration_periods when the collapse_transfers_with_different_pfi flag is False, and
        the two periods connected by a transfer have different specialized_purpose_for_incarceration values.
        """
        state_code = 'US_XX'
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2012, 12, 4),
                admission_reason=AdmissionReason.TRANSFER,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 second_incarceration_period]

        collapsed_incarceration_periods = \
            utils.collapse_incarceration_period_transfers(incarceration_periods,
                                                          collapse_transfers_with_different_pfi=False)

        self.assertListEqual(incarceration_periods, collapsed_incarceration_periods)

    def test_collapse_incarceration_periods_transfer_different_pfi_collapse(self):
        """Tests collapse_incarceration_periods when the collapse_transfers_with_different_pfi flag is True, and
        the two periods connected by a transfer have different specialized_purpose_for_incarceration values.
        """
        state_code = 'US_XX'
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2012, 12, 4),
                admission_reason=AdmissionReason.TRANSFER,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 second_incarceration_period]

        collapsed_incarceration_periods = \
            utils.collapse_incarceration_period_transfers(incarceration_periods,
                                                          collapse_transfers_with_different_pfi=True)

        self.assertListEqual([
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=initial_incarceration_period.incarceration_period_id,
                status=second_incarceration_period.status,
                state_code=initial_incarceration_period.state_code,
                admission_date=initial_incarceration_period.admission_date,
                admission_reason=initial_incarceration_period.admission_reason,
                specialized_purpose_for_incarceration=second_incarceration_period.specialized_purpose_for_incarceration,
                release_date=second_incarceration_period.release_date,
                release_reason=second_incarceration_period.release_reason)
        ], collapsed_incarceration_periods)


class TestCombineIncarcerationPeriods(unittest.TestCase):
    """Tests for collapse_incarceration_period_transfers function."""

    def test_combineIncarcerationPeriods(self):
        """Tests for combining two incarceration periods connected by a
        transfer."""

        state_code = 'US_XX'
        start_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            facility='Green',
            housing_unit='House19',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.
            MEDIUM,
            projected_release_reason=ReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.TRANSFER)

        end_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            facility='Jones',
            housing_unit='HouseUnit3',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.
            MAXIMUM,
            projected_release_reason=ReleaseReason.SENTENCE_SERVED,
            admission_date=date(2010, 12, 4),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2012, 12, 10),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE)

        combined_incarceration_period = utils.combine_incarceration_periods(
            start_incarceration_period, end_incarceration_period,
            overwrite_facility_information=True)

        assert combined_incarceration_period == \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=start_incarceration_period.
                incarceration_period_id,
                status=end_incarceration_period.status,
                state_code=start_incarceration_period.state_code,
                facility=end_incarceration_period.facility,
                housing_unit=end_incarceration_period.housing_unit,
                facility_security_level=end_incarceration_period.
                facility_security_level,
                projected_release_reason=end_incarceration_period.
                projected_release_reason,
                admission_date=start_incarceration_period.admission_date,
                admission_reason=start_incarceration_period.admission_reason,
                release_date=end_incarceration_period.release_date,
                release_reason=end_incarceration_period.release_reason

            )

    def test_combineIncarcerationPeriods_overwriteAdmissionReason(self):
        """Tests for combining two incarceration periods connected by a transfer, where the admission reason on the
        start_incarceration_period should be overwritten by the admission reason on the end_incarceration_period."""

        state_code = 'US_XX'
        start_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            facility='Green',
            housing_unit='House19',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MEDIUM,
            projected_release_reason=ReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.TRANSFER)

        end_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            facility='Jones',
            housing_unit='HouseUnit3',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            projected_release_reason=ReleaseReason.SENTENCE_SERVED,
            admission_date=date(2010, 12, 4),
            admission_reason=AdmissionReason.DUAL_REVOCATION,
            release_date=date(2012, 12, 10),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE)

        combined_incarceration_period = utils.combine_incarceration_periods(
            start_incarceration_period,
            end_incarceration_period,
            overwrite_admission_reason=True,
            overwrite_facility_information=False)

        expected_combined_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=start_incarceration_period.incarceration_period_id,
            status=end_incarceration_period.status,
            state_code=start_incarceration_period.state_code,
            facility=start_incarceration_period.facility,
            housing_unit=start_incarceration_period.housing_unit,
            facility_security_level=start_incarceration_period.facility_security_level,
            projected_release_reason=end_incarceration_period.projected_release_reason,
            admission_date=start_incarceration_period.admission_date,
            admission_reason=end_incarceration_period.admission_reason,
            release_date=end_incarceration_period.release_date,
            release_reason=end_incarceration_period.release_reason)
        self.assertEqual(expected_combined_period, combined_incarceration_period)

    def test_combineIncarcerationPeriods_overwriteFacilityInformation(self):
        """Tests for combining two incarceration periods connected by a transfer, where the facility information
        (facility, housing unit, security level, purpose for incarceration) should be taken from the
        end_incarceration_period."""
        state_code = 'US_XX'
        start_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            facility='Green',
            housing_unit='House19',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MEDIUM,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            projected_release_reason=ReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.TRANSFER)

        end_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            facility='Jones',
            housing_unit='HouseUnit3',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            projected_release_reason=ReleaseReason.SENTENCE_SERVED,
            admission_date=date(2010, 12, 4),
            admission_reason=AdmissionReason.DUAL_REVOCATION,
            release_date=date(2012, 12, 10),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE)

        combined_incarceration_period = utils.combine_incarceration_periods(
            start_incarceration_period,
            end_incarceration_period,
            overwrite_admission_reason=True,
            overwrite_facility_information=True)

        expected_combined_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=start_incarceration_period.incarceration_period_id,
            status=end_incarceration_period.status,
            state_code=start_incarceration_period.state_code,
            facility=end_incarceration_period.facility,
            housing_unit=end_incarceration_period.housing_unit,
            facility_security_level=end_incarceration_period.facility_security_level,
            specialized_purpose_for_incarceration=end_incarceration_period.specialized_purpose_for_incarceration,
            projected_release_reason=end_incarceration_period.projected_release_reason,
            admission_date=start_incarceration_period.admission_date,
            admission_reason=end_incarceration_period.admission_reason,
            release_date=end_incarceration_period.release_date,
            release_reason=end_incarceration_period.release_reason)
        self.assertEqual(expected_combined_period, combined_incarceration_period)

    def test_combineIncarcerationPeriods_doNotOverwriteFacilityInformation(self):
        """Tests for combining two incarceration periods connected by a transfer, where the facility information
        (facility, housing unit, security level, purpose for incarceration) should be taken from the
        start_incarceration_period."""
        state_code = 'US_XX'
        start_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            facility='Green',
            housing_unit='House19',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MEDIUM,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            projected_release_reason=ReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.TRANSFER)

        end_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            facility='Jones',
            housing_unit='HouseUnit3',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            specialized_purpose_for_incarceration=None,
            projected_release_reason=ReleaseReason.SENTENCE_SERVED,
            admission_date=date(2010, 12, 4),
            admission_reason=AdmissionReason.DUAL_REVOCATION,
            release_date=date(2012, 12, 10),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE)

        combined_incarceration_period = utils.combine_incarceration_periods(
            start_incarceration_period,
            end_incarceration_period,
            overwrite_admission_reason=True,
            overwrite_facility_information=True)

        expected_combined_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=start_incarceration_period.incarceration_period_id,
            status=end_incarceration_period.status,
            state_code=start_incarceration_period.state_code,
            facility=end_incarceration_period.facility,
            housing_unit=end_incarceration_period.housing_unit,
            facility_security_level=end_incarceration_period.facility_security_level,
            specialized_purpose_for_incarceration=start_incarceration_period.specialized_purpose_for_incarceration,
            projected_release_reason=end_incarceration_period.projected_release_reason,
            admission_date=start_incarceration_period.admission_date,
            admission_reason=end_incarceration_period.admission_reason,
            release_date=end_incarceration_period.release_date,
            release_reason=end_incarceration_period.release_reason)
        self.assertEqual(expected_combined_period, combined_incarceration_period)


class TestCollapseTemporaryCustodyAndRevocationPeriods(unittest.TestCase):
    """Tests for collapse_temporary_custody_and_revocation_periods function."""

    def test_collapseTemporaryCustodyAndRevocationPeriods(self):
        # Arrange
        state_code = 'US_XX'
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=state_code,
            admission_date=date(2011, 1, 1),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2011, 2, 1),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY)
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=state_code,
            admission_date=date(2011, 2, 1),
            admission_reason=AdmissionReason.DUAL_REVOCATION,
            release_date=date(2011, 3, 1),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE)

        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            state_code=state_code,
            admission_date=date(2012, 1, 1),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2012, 2, 1),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY)
        incarceration_period_4 = StateIncarcerationPeriod.new_with_defaults(
            state_code=state_code,
            admission_date=date(2012, 3, 1),
            admission_reason=AdmissionReason.DUAL_REVOCATION,
            release_date=date(2012, 4, 1),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE)

        incarceration_period_5 = StateIncarcerationPeriod.new_with_defaults(
            state_code=state_code,
            admission_date=date(2013, 1, 1),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2013, 2, 1),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY)
        incarceration_period_6 = StateIncarcerationPeriod.new_with_defaults(
            state_code=state_code,
            admission_date=date(2013, 2, 1),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2013, 3, 1),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE)
        incarceration_periods = [
            incarceration_period,
            incarceration_period_2,
            incarceration_period_3,
            incarceration_period_4,
            incarceration_period_5,
            incarceration_period_6
        ]

        expected_incarceration_period_merged = attr.evolve(
            incarceration_period,
            admission_reason=incarceration_period_2.admission_reason,
            release_date=incarceration_period_2.release_date,
            release_reason=incarceration_period_2.release_reason)
        expected_incarceration_period_3 = attr.evolve(incarceration_period_3)
        expected_incarceration_period_4 = attr.evolve(incarceration_period_4)
        expected_incarceration_period_5 = attr.evolve(incarceration_period_5)
        expected_incarceration_period_6 = attr.evolve(incarceration_period_6)

        expected_incarceration_periods = [
            expected_incarceration_period_merged,
            expected_incarceration_period_3,
            expected_incarceration_period_4,
            expected_incarceration_period_5,
            expected_incarceration_period_6
        ]

        # Act
        collapsed_periods = collapse_temporary_custody_and_revocation_periods(incarceration_periods)

        # Assert
        self.assertCountEqual(expected_incarceration_periods, collapsed_periods)

    def test_collapseTemporaryCustodyAndRevocationPeriods_noTempCustodyPreceedingRevocation(self):
        # Arrange
        state_code = 'US_XX'
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=state_code,
            admission_date=date(2011, 1, 1),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2011, 2, 1),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE)
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=state_code,
            admission_date=date(2011, 2, 1),
            admission_reason=AdmissionReason.DUAL_REVOCATION,
            release_date=date(2011, 3, 1),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE)
        incarceration_periods = [incarceration_period, incarceration_period_2]

        expected_incarceration_period = attr.evolve(incarceration_period)
        expected_incarceration_period_2 = attr.evolve(incarceration_period_2)

        expected_incarceration_periods = [expected_incarceration_period, expected_incarceration_period_2]

        # Act
        collapsed_periods = collapse_temporary_custody_and_revocation_periods(incarceration_periods)

        # Assert
        self.assertCountEqual(expected_incarceration_periods, collapsed_periods)


class TestPrepareIncarcerationPeriodsForCalculations(unittest.TestCase):
    """Tests the prepare_incarceration_periods_for_calculations function."""

    def test_prepare_incarceration_periods_for_calculations(self):
        state_code = 'US_XX'
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2011, 3, 4),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2012, 2, 4),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2016, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        input_incarceration_periods = [
            initial_incarceration_period, first_reincarceration_period, second_reincarceration_period]

        validated_incarceration_periods = prepare_incarceration_periods_for_calculations(
            state_code,
            input_incarceration_periods,
            collapse_transfers=True,
            collapse_temporary_custody_periods_with_revocation=False,
            collapse_transfers_with_different_pfi=True,
            overwrite_facility_information_in_transfers=True
        )

        self.assertEqual(validated_incarceration_periods, input_incarceration_periods)

    def test_prepare_incarceration_periods_for_calculations_empty_admission_data(self):
        """Tests that the incarceration periods are correctly collapsed when there's an empty admission_date and
        admission_reason following a transfer out..
        """
        state_code = 'US_ND'
        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id='99983-1|99983-2',
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2004, 1, 3),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2008, 4, 14),
                release_reason=ReleaseReason.TRANSFER)

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id='99983-3',
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=None,
                admission_reason=None,
                release_date=date(2010, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        input_incarceration_periods = [first_incarceration_period, second_incarceration_period]

        collapsed_incarceration_periods = prepare_incarceration_periods_for_calculations(
            state_code,
            input_incarceration_periods,
            collapse_transfers=True,
            collapse_temporary_custody_periods_with_revocation=False,
            collapse_transfers_with_different_pfi=True,
            overwrite_facility_information_in_transfers=True
        )

        self.assertEqual(collapsed_incarceration_periods, [
            StateIncarcerationPeriod.new_with_defaults(
                external_id='99983-1|99983-2',
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2004, 1, 3),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)
        ])

    def test_prepare_incarceration_periods_for_calculations_temporary_between_valid(self):
        state_code = 'US_ND'
        valid_incarceration_period_1 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                external_id='1',
                state_code=state_code,
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2009, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        temporary_custody = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='2',
            state_code=state_code,
            admission_date=date(2010, 1, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2010, 1, 24),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY
        )

        valid_incarceration_period_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=112,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                external_id='3',
                state_code=state_code,
                admission_date=date(2014, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2014, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [valid_incarceration_period_1, temporary_custody, valid_incarceration_period_2]

        validated_incarceration_periods = prepare_incarceration_periods_for_calculations(
            state_code,
            incarceration_periods,
            collapse_transfers=True,
            collapse_temporary_custody_periods_with_revocation=False,
            collapse_transfers_with_different_pfi=True,
            overwrite_facility_information_in_transfers=True
        )

        self.assertEqual(validated_incarceration_periods, [valid_incarceration_period_1, valid_incarceration_period_2])

    def test_sort_incarceration_periods(self):
        state_code = 'US_XX'
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2011, 3, 4),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2012, 2, 4),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2016, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        input_incarceration_periods = [
            initial_incarceration_period, second_reincarceration_period, first_reincarceration_period]

        validated_incarceration_periods = prepare_incarceration_periods_for_calculations(
            state_code,
            input_incarceration_periods,
            collapse_transfers=True,
            collapse_temporary_custody_periods_with_revocation=False,
            collapse_transfers_with_different_pfi=True,
            overwrite_facility_information_in_transfers=True
        )

        self.assertEqual(validated_incarceration_periods, [
            initial_incarceration_period,
            first_reincarceration_period,
            second_reincarceration_period])

    def test_sort_incarceration_periods_empty_release_dates(self):
        state_code = 'US_XX'
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2011, 3, 4),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2012, 2, 4),
                admission_reason=AdmissionReason.NEW_ADMISSION)

        input_incarceration_periods = [
            initial_incarceration_period, second_reincarceration_period, first_reincarceration_period]

        validated_incarceration_periods = prepare_incarceration_periods_for_calculations(
            state_code,
            input_incarceration_periods,
            collapse_transfers=True,
            collapse_temporary_custody_periods_with_revocation=False,
            collapse_transfers_with_different_pfi=True,
            overwrite_facility_information_in_transfers=True
        )

        self.assertEqual(validated_incarceration_periods, [
            initial_incarceration_period,
            first_reincarceration_period,
            second_reincarceration_period])

    def test_sort_incarceration_periods_two_admissions_same_day(self):
        state_code = 'US_XX'
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2008, 11, 20),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        third_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2012, 2, 4),
                admission_reason=AdmissionReason.NEW_ADMISSION)

        input_incarceration_periods = [third_incarceration_period, second_incarceration_period,
                                       initial_incarceration_period]

        validated_incarceration_periods = prepare_incarceration_periods_for_calculations(
            state_code,
            input_incarceration_periods,
            collapse_transfers=True,
            collapse_temporary_custody_periods_with_revocation=False,
            collapse_transfers_with_different_pfi=True,
            overwrite_facility_information_in_transfers=True
        )

        self.assertEqual(validated_incarceration_periods, [
            initial_incarceration_period,
            second_incarceration_period,
            third_incarceration_period])

    def test_collapse_incarceration_periods(self):
        state_code = 'US_XX'
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code=state_code,
                admission_date=date(2010, 12, 4),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period]

        collapsed_incarceration_periods = prepare_incarceration_periods_for_calculations(
            state_code,
            incarceration_periods,
            collapse_transfers=True,
            collapse_temporary_custody_periods_with_revocation=False,
            collapse_transfers_with_different_pfi=True,
            overwrite_facility_information_in_transfers=True
        )

        self.assertEqual(len(collapsed_incarceration_periods), 1)

        self.assertEqual(collapsed_incarceration_periods, [
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=initial_incarceration_period.incarceration_period_id,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=first_reincarceration_period.status,
                state_code=initial_incarceration_period.state_code,
                admission_date=initial_incarceration_period.admission_date,
                admission_reason=initial_incarceration_period.admission_reason,
                release_date=first_reincarceration_period.release_date,
                release_reason=first_reincarceration_period.release_reason)
        ])

    def test_collapse_incarceration_periods_missing_transfer_in(self):
        state_code = 'US_XX'
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2003, 1, 2),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2007, 4, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        first_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.TRANSFER)

        second_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=None,
            admission_reason=None,
            release_date=date(2014, 4, 14),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period,
                                 second_reincarceration_period]

        collapsed_incarceration_periods = prepare_incarceration_periods_for_calculations(
            state_code,
            incarceration_periods,
            collapse_transfers=True,
            collapse_temporary_custody_periods_with_revocation=False,
            collapse_transfers_with_different_pfi=True,
            overwrite_facility_information_in_transfers=True
        )

        expected_collapsed_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2014, 4, 14),
            release_reason=ReleaseReason.SENTENCE_SERVED
        )

        self.assertEqual([initial_incarceration_period, expected_collapsed_period], collapsed_incarceration_periods)

    def test_prepare_incarceration_periods_for_calculations_multiple_jail_and_valid(self):
        state_code = 'US_XX'
        jail_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id='111',
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.ESCAPE,
        )

        jail_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=112,
            external_id='222',
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2008, 12, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 12, 24),
            release_reason=ReleaseReason.ESCAPE,
        )

        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id='333',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2016, 11, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2017, 12, 4),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY)

        incarceration_periods = [jail_period_1, jail_period_2, valid_incarceration_period]

        validated_incarceration_periods = prepare_incarceration_periods_for_calculations(
            state_code,
            incarceration_periods,
            collapse_transfers=True,
            collapse_temporary_custody_periods_with_revocation=False,
            collapse_transfers_with_different_pfi=True,
            overwrite_facility_information_in_transfers=True
        )

        self.assertEqual(validated_incarceration_periods, [valid_incarceration_period])

    def test_prepare_incarceration_periods_for_calculations_multiple_jail_and_transfer(self):
        state_code = 'US_XX'
        jail_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='1',
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.ESCAPE
        )

        jail_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=112,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='2',
            state_code=state_code,
            admission_date=date(2008, 12, 20),
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 12, 24),
            release_reason=ReleaseReason.ESCAPE
        )

        valid_incarceration_period_1 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                external_id='3',
                state_code=state_code,
                admission_date=date(2011, 11, 20),
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2012, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        valid_incarceration_period_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1112,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                external_id='4',
                state_code=state_code,
                admission_date=date(2012, 12, 4),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2012, 12, 24),
                release_reason=ReleaseReason.TRANSFER)

        valid_incarceration_period_3 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1113,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                external_id='5',
                state_code=state_code,
                admission_date=date(2012, 12, 24),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2012, 12, 30),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [jail_period_1,
                                 jail_period_2,
                                 valid_incarceration_period_1,
                                 valid_incarceration_period_2,
                                 valid_incarceration_period_3]

        validated_incarceration_periods = prepare_incarceration_periods_for_calculations(
            state_code,
            incarceration_periods,
            collapse_transfers=True,
            collapse_temporary_custody_periods_with_revocation=False,
            collapse_transfers_with_different_pfi=True,
            overwrite_facility_information_in_transfers=True
        )

        collapsed_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=valid_incarceration_period_1.incarceration_period_id,
                incarceration_type=valid_incarceration_period_1.incarceration_type,
                external_id=valid_incarceration_period_1.external_id,
                status=valid_incarceration_period_3.status,
                state_code=valid_incarceration_period_1.state_code,
                admission_date=valid_incarceration_period_1.admission_date,
                admission_reason=valid_incarceration_period_1.admission_reason,
                release_date=valid_incarceration_period_3.release_date,
                release_reason=valid_incarceration_period_3.release_reason
            )

        self.assertEqual(validated_incarceration_periods, [collapsed_incarceration_period])

    def test_prepare_incarceration_periods_for_calculations_valid_then_jail(self):
        state_code = 'US_XX'
        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='1',
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 12, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        jail_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            external_id='2',
            state_code=state_code,
            admission_date=date(2010, 1, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2010, 1, 24),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY
        )

        incarceration_periods = [valid_incarceration_period, jail_period]

        validated_incarceration_periods = prepare_incarceration_periods_for_calculations(
            state_code,
            incarceration_periods,
            collapse_transfers=True,
            collapse_temporary_custody_periods_with_revocation=False,
            collapse_transfers_with_different_pfi=True,
            overwrite_facility_information_in_transfers=True
        )

        self.assertEqual(validated_incarceration_periods, [valid_incarceration_period])


class TestUsNdPrepareIncarcerationPeriodsForCalculations(unittest.TestCase):
    """Tests the prepare_incarceration_periods_for_calculations function for ND specific functions."""

    def test_prepare_incarceration_periods_for_calculations_multiple_temporary_and_valid(self):
        state_code = 'US_ND'
        temporary_custody_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id='111',
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
            external_id='222',
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2008, 12, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2008, 12, 24),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY
        )

        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id='333',
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2016, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2017, 12, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [temporary_custody_1, temporary_custody_2, valid_incarceration_period]

        for ip_order_combo in permutations(incarceration_periods):
            validated_incarceration_periods = prepare_incarceration_periods_for_calculations(
                state_code,
                ip_order_combo,
                collapse_transfers=True,
                collapse_temporary_custody_periods_with_revocation=False,
                collapse_transfers_with_different_pfi=True,
                overwrite_facility_information_in_transfers=True
            )

            self.assertEqual(validated_incarceration_periods, [valid_incarceration_period])

    def test_prepare_incarceration_periods_for_calculations_multiple_temporary_and_transfer(self):
        state_code = 'US_ND'
        temporary_custody_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='1',
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY
        )

        temporary_custody_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=112,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='2',
            state_code=state_code,
            admission_date=date(2008, 12, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2008, 12, 24),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY
        )

        valid_incarceration_period_1 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                external_id='3',
                state_code=state_code,
                admission_date=date(2011, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2012, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        valid_incarceration_period_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1112,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                external_id='4',
                state_code=state_code,
                admission_date=date(2012, 12, 4),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2012, 12, 24),
                release_reason=ReleaseReason.TRANSFER)

        valid_incarceration_period_3 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1113,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                external_id='5',
                state_code=state_code,
                admission_date=date(2012, 12, 24),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2012, 12, 30),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [temporary_custody_1,
                                 temporary_custody_2,
                                 valid_incarceration_period_1,
                                 valid_incarceration_period_2,
                                 valid_incarceration_period_3]

        collapsed_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=valid_incarceration_period_1.incarceration_period_id,
                external_id=valid_incarceration_period_1.external_id,
                status=valid_incarceration_period_3.status,
                state_code=valid_incarceration_period_1.state_code,
                admission_date=valid_incarceration_period_1.admission_date,
                admission_reason=valid_incarceration_period_1.admission_reason,
                release_date=valid_incarceration_period_3.release_date,
                release_reason=valid_incarceration_period_3.release_reason
            )

        for ip_order_combo in permutations(incarceration_periods):
            validated_incarceration_periods = prepare_incarceration_periods_for_calculations(
                state_code,
                ip_order_combo,
                collapse_transfers=True,
                collapse_temporary_custody_periods_with_revocation=False,
                collapse_transfers_with_different_pfi=True,
                overwrite_facility_information_in_transfers=True
            )

            self.assertEqual(validated_incarceration_periods, [collapsed_incarceration_period])

    def test_prepare_incarceration_periods_for_calculations_valid_then_temporary(self):
        state_code = 'US_ND'
        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='1',
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 12, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        temporary_custody = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='2',
            state_code=state_code,
            admission_date=date(2010, 1, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2010, 1, 24),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY
        )

        incarceration_periods = [valid_incarceration_period, temporary_custody]

        for ip_order_combo in permutations(incarceration_periods):
            validated_incarceration_periods = prepare_incarceration_periods_for_calculations(
                state_code,
                ip_order_combo,
                collapse_transfers=True,
                collapse_temporary_custody_periods_with_revocation=False,
                collapse_transfers_with_different_pfi=True,
                overwrite_facility_information_in_transfers=True
            )

            self.assertEqual(validated_incarceration_periods, [valid_incarceration_period])


class TestInferMissingDatesAndStatuses(unittest.TestCase):
    """Tests the _infer_missing_dates_and_statuses."""
    def test_infer_missing_dates_and_statuses(self):
        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='1',
            state_code='US_ND',
            admission_date=date(2011, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 4),
            release_reason=ReleaseReason.TRANSFER)

        valid_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='2',
            state_code='US_ND',
            admission_date=date(2012, 12, 4),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2012, 12, 24),
            release_reason=ReleaseReason.TRANSFER)

        valid_incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='3',
            state_code='US_ND',
            admission_date=date(2012, 12, 24),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2012, 12, 30),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        # Invalid open period with same admission date as valid_incarceration_period_1
        invalid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            external_id='0',
            state_code='US_ND',
            admission_date=date(2011, 11, 20),
            admission_reason=AdmissionReason.TRANSFER,
        )

        incarceration_periods = [valid_incarceration_period_3,
                                 valid_incarceration_period_1,
                                 valid_incarceration_period_2,
                                 invalid_open_period]

        updated_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='0',
            state_code='US_ND',
            admission_date=date(2011, 11, 20),
            admission_reason=AdmissionReason.TRANSFER,
            # Release date set to the admission date of valid_incarceration_period_1
            release_date=valid_incarceration_period_1.admission_date,
            release_reason=ReleaseReason.INTERNAL_UNKNOWN
        )

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [
                attr.evolve(ip) for ip in ip_order_combo
            ]

            updated_incarceration_periods = _infer_missing_dates_and_statuses(ips_for_test)

            self.assertEqual([
                updated_open_period,
                valid_incarceration_period_1,
                valid_incarceration_period_2,
                valid_incarceration_period_3
            ], updated_incarceration_periods)

    def test_infer_missing_dates_and_statuses_all_valid(self):
        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='3',
            state_code='US_ND',
            admission_date=date(2011, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 4),
            release_reason=ReleaseReason.TRANSFER)

        valid_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='4',
            state_code='US_ND',
            admission_date=date(2012, 12, 4),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2012, 12, 24),
            release_reason=ReleaseReason.TRANSFER)

        valid_incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='5',
            state_code='US_ND',
            admission_date=date(2012, 12, 24),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2012, 12, 30),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        # Valid open period
        valid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            external_id='6',
            state_code='US_ND',
            admission_date=date(2015, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
        )

        incarceration_periods = [valid_open_period,
                                 valid_incarceration_period_1,
                                 valid_incarceration_period_2,
                                 valid_incarceration_period_3]

        ordered_periods = [valid_incarceration_period_1,
                           valid_incarceration_period_2,
                           valid_incarceration_period_3,
                           valid_open_period]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [
                attr.evolve(ip) for ip in ip_order_combo
            ]

            updated_incarceration_periods = _infer_missing_dates_and_statuses(ips_for_test)

            self.assertEqual(ordered_periods, updated_incarceration_periods)

    def test_infer_missing_dates_and_statuses_all_valid_shared_release(self):
        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='3',
            state_code='US_ND',
            admission_date=date(2011, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 4),
            release_reason=ReleaseReason.TRANSFER)

        nested_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='4',
            state_code='US_ND',
            admission_date=date(2012, 11, 29),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2012, 12, 4),
            release_reason=ReleaseReason.TRANSFER)

        incarceration_periods = [valid_incarceration_period,
                                 nested_incarceration_period]

        ordered_periods = [valid_incarceration_period]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [
                attr.evolve(ip) for ip in ip_order_combo
            ]

            updated_incarceration_periods = _infer_missing_dates_and_statuses(ips_for_test)

            self.assertEqual(ordered_periods, updated_incarceration_periods)

    def test_infer_missing_dates_and_statuses_set_empty_reasons(self):
        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='3',
            state_code='US_ND',
            admission_date=date(2011, 11, 20),
            admission_reason=None,
            release_date=date(2012, 12, 4),
            release_reason=ReleaseReason.TRANSFER)

        valid_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='4',
            state_code='US_ND',
            admission_date=date(2012, 12, 4),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2012, 12, 24),
            release_reason=ReleaseReason.TRANSFER)

        valid_incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='5',
            state_code='US_ND',
            admission_date=date(2012, 12, 24),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2012, 12, 30),
            release_reason=None)

        # Valid open period
        valid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            external_id='5',
            state_code='US_ND',
            admission_date=date(2015, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
        )

        incarceration_periods = [valid_open_period,
                                 valid_incarceration_period_1,
                                 valid_incarceration_period_2,
                                 valid_incarceration_period_3]

        expected_output = [
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                external_id='3',
                state_code='US_ND',
                admission_date=date(2011, 11, 20),
                admission_reason=AdmissionReason.INTERNAL_UNKNOWN,
                release_date=date(2012, 12, 4),
                release_reason=ReleaseReason.TRANSFER),
            valid_incarceration_period_2,
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1113,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                external_id='5',
                state_code='US_ND',
                admission_date=date(2012, 12, 24),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2012, 12, 30),
                release_reason=ReleaseReason.INTERNAL_UNKNOWN),
            valid_open_period
        ]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [
                attr.evolve(ip) for ip in ip_order_combo
            ]

            updated_incarceration_periods = _infer_missing_dates_and_statuses(ips_for_test)

            self.assertEqual(expected_output, updated_incarceration_periods)

    def test_infer_missing_dates_and_statuses_only_open(self):
        valid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            external_id='5',
            state_code='US_ND',
            admission_date=date(2015, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
        )

        incarceration_periods = [valid_open_period]

        updated_incarceration_periods = _infer_missing_dates_and_statuses(incarceration_periods)

        self.assertEqual(incarceration_periods, updated_incarceration_periods)

    def test_infer_missing_dates_and_statuses_invalid_dates(self):
        # We drop any periods with a release_date that precedes the admission_date
        valid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            external_id='5',
            state_code='US_ND',
            admission_date=date(2015, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2015, 1, 1),
            release_reason=ReleaseReason.TRANSFER
        )

        incarceration_periods = [valid_open_period]

        updated_incarceration_periods = _infer_missing_dates_and_statuses(incarceration_periods)

        self.assertEqual([], updated_incarceration_periods)

    @freeze_time('2000-01-01')
    def test_infer_missing_dates_and_statuses_invalid_admission_date_in_future(self):
        # We drop any periods with an admission_date in the future
        invalid_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            external_id='5',
            state_code='US_ND',
            admission_date=date(2015, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2015, 1, 1),
            release_reason=ReleaseReason.TRANSFER
        )

        incarceration_periods = [invalid_period]

        updated_incarceration_periods = _infer_missing_dates_and_statuses(incarceration_periods)

        self.assertEqual([], updated_incarceration_periods)

    @freeze_time('2000-01-01')
    def test_infer_missing_dates_and_statuses_invalid_release_date_in_future(self):
        # We clear the release information for release_dates in the future
        invalid_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='5',
            state_code='US_ND',
            admission_date=date(1990, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2015, 1, 1),
            release_reason=ReleaseReason.TRANSFER
        )

        incarceration_periods = [invalid_period]

        updated_incarceration_periods = _infer_missing_dates_and_statuses(incarceration_periods)

        updated_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            external_id='5',
            state_code='US_ND',
            admission_date=date(1990, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=None,
            release_reason=None
        )

        self.assertEqual([updated_period], updated_incarceration_periods)

    def test_infer_missing_dates_and_statuses_open_with_release_reason(self):
        valid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            external_id='5',
            state_code='US_ND',
            admission_date=date(2015, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=None,
            release_reason=ReleaseReason.SENTENCE_SERVED
        )

        incarceration_periods = [valid_open_period]

        updated_incarceration_periods = _infer_missing_dates_and_statuses(incarceration_periods)

        updated_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='5',
            state_code='US_ND',
            admission_date=date(2015, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2015, 11, 20),
            release_reason=ReleaseReason.SENTENCE_SERVED
        )

        self.assertEqual([updated_period], updated_incarceration_periods)

    def test_infer_missing_dates_and_statuses_only_one_closed(self):
        closed_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='5',
            state_code='US_ND',
            release_date=date(2015, 11, 20),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [closed_period]

        updated_incarceration_periods = _infer_missing_dates_and_statuses(incarceration_periods)

        updated_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='5',
            state_code='US_ND',
            admission_date=date(2015, 11, 20),
            admission_reason=AdmissionReason.INTERNAL_UNKNOWN,
            release_date=date(2015, 11, 20),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        self.assertEqual([updated_period], updated_incarceration_periods)

    def test_infer_missing_dates_and_statuses_missing_admission(self):
        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='1',
            state_code='US_ND',
            admission_date=date(2011, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 4),
            release_reason=ReleaseReason.TRANSFER)

        valid_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='2',
            state_code='US_ND',
            admission_date=date(2012, 12, 4),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2012, 12, 24),
            release_reason=ReleaseReason.TRANSFER)

        valid_incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='3',
            state_code='US_ND',
            admission_date=date(2012, 12, 24),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2012, 12, 30),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        # Invalid period without an admission_date, where the release_date is the same as the release_date on
        # valid_incarceration_period_3
        invalid_period_no_admission = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='0',
            state_code='US_ND',
            release_date=date(2012, 12, 30),
            release_reason=ReleaseReason.SENTENCE_SERVED
        )

        incarceration_periods = [valid_incarceration_period_3,
                                 valid_incarceration_period_1,
                                 valid_incarceration_period_2,
                                 invalid_period_no_admission]

        updated_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='0',
            state_code='US_ND',
            admission_date=valid_incarceration_period_3.release_date,
            admission_reason=AdmissionReason.INTERNAL_UNKNOWN,
            release_date=date(2012, 12, 30),
            release_reason=ReleaseReason.SENTENCE_SERVED
        )

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [
                attr.evolve(ip) for ip in ip_order_combo
            ]

            updated_incarceration_periods = _infer_missing_dates_and_statuses(ips_for_test)

            self.assertEqual([
                valid_incarceration_period_1,
                valid_incarceration_period_2,
                valid_incarceration_period_3,
                updated_period
            ], updated_incarceration_periods)

    def test_infer_missing_dates_and_statuses_missing_admission_same_day_transfer(self):
        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='5-6',
            state_code='US_XX',
            admission_date=date(2015, 12, 3),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2016, 2, 11),
            release_reason=ReleaseReason.TRANSFER)

        # Invalid period without an admission_date, where the release_date is the same as the release_date on
        invalid_period_no_admission = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='7',
            state_code='US_XX',
            release_date=date(2016, 2, 11),
            release_reason=ReleaseReason.TRANSFER
        )

        valid_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='8-9',
            state_code='US_XX',
            admission_date=date(2016, 2, 11),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2016, 2, 11),
            release_reason=ReleaseReason.TRANSFER)

        valid_incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1114,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='10-11',
            state_code='US_XX',
            admission_date=date(2016, 2, 11),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2016, 4, 5),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [invalid_period_no_admission,
                                 valid_incarceration_period_3,
                                 valid_incarceration_period_1,
                                 valid_incarceration_period_2,
                                 ]

        updated_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='7',
            state_code='US_XX',
            admission_date=valid_incarceration_period_1.release_date,
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2016, 2, 11),
            release_reason=ReleaseReason.TRANSFER
        )

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [
                attr.evolve(ip) for ip in ip_order_combo
            ]

            updated_incarceration_periods = _infer_missing_dates_and_statuses(ips_for_test)

            self.assertEqual([
                valid_incarceration_period_1,
                updated_period,
                valid_incarceration_period_2,
                valid_incarceration_period_3,
            ], updated_incarceration_periods)

    def test_infer_missing_dates_and_statuses_mismatch_admission_release(self):
        # Open incarceration period with no release_date
        invalid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            external_id='0',
            state_code='US_ND',
            admission_date=date(2012, 12, 24),
            admission_reason=AdmissionReason.TRANSFER)

        # Invalid period without an admission_date
        invalid_closed_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='1',
            state_code='US_ND',
            release_date=date(2012, 12, 30),
            release_reason=ReleaseReason.SENTENCE_SERVED
        )

        incarceration_periods = [invalid_closed_period, invalid_open_period]

        updated_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='0',
            state_code='US_ND',
            admission_date=date(2012, 12, 24),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2012, 12, 30),
            release_reason=ReleaseReason.INTERNAL_UNKNOWN
        )

        updated_closed_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='1',
            state_code='US_ND',
            admission_date=date(2012, 12, 30),
            admission_reason=AdmissionReason.INTERNAL_UNKNOWN,
            release_date=date(2012, 12, 30),
            release_reason=ReleaseReason.SENTENCE_SERVED
        )

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [
                attr.evolve(ip) for ip in ip_order_combo
            ]

            updated_incarceration_periods = _infer_missing_dates_and_statuses(ips_for_test)

            self.assertEqual([
                updated_open_period,
                updated_closed_period
            ], updated_incarceration_periods)

    def test_infer_missing_dates_and_statuses_valid_open_admission(self):
        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='1',
            state_code='US_ND',
            admission_date=date(2011, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2011, 11, 20),
            release_reason=ReleaseReason.TRANSFER)

        # Invalid open period with same admission date as valid_incarceration_period_1
        valid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            external_id='0',
            state_code='US_ND',
            admission_date=date(2011, 11, 20),
            admission_reason=AdmissionReason.TRANSFER,
        )

        incarceration_periods = [valid_incarceration_period_1, valid_open_period]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [
                attr.evolve(ip) for ip in ip_order_combo
            ]

            updated_incarceration_periods = _infer_missing_dates_and_statuses(ips_for_test)
            self.assertEqual(incarceration_periods, updated_incarceration_periods)

    def test_infer_missing_dates_and_statuses_two_open_two_invalid_closed(self):
        open_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            external_id='1',
            state_code='US_ND',
            admission_date=date(2001, 6, 11),
            admission_reason=AdmissionReason.TRANSFER)

        open_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            external_id='2',
            state_code='US_ND',
            admission_date=date(2001, 6, 19),
            admission_reason=AdmissionReason.TRANSFER)

        closed_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='3',
            state_code='US_ND',
            release_date=date(2001, 6, 19),
            release_reason=ReleaseReason.TRANSFER)

        closed_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=4444,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='4',
            state_code='US_ND',
            release_date=date(2001, 7, 17),
            release_reason=ReleaseReason.TRANSFER)

        incarceration_periods = [open_incarceration_period_1,
                                 open_incarceration_period_2,
                                 closed_incarceration_period_1,
                                 closed_incarceration_period_2]

        updated_open_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='1',
            state_code='US_ND',
            admission_date=date(2001, 6, 11),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2001, 6, 19),
            release_reason=ReleaseReason.TRANSFER
        )

        updated_open_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='2',
            state_code='US_ND',
            admission_date=date(2001, 6, 19),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2001, 6, 19),
            release_reason=ReleaseReason.INTERNAL_UNKNOWN
        )

        updated_closed_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='3',
            state_code='US_ND',
            admission_date=date(2001, 6, 19),
            admission_reason=AdmissionReason.INTERNAL_UNKNOWN,
            release_date=date(2001, 6, 19),
            release_reason=ReleaseReason.TRANSFER
        )

        updated_closed_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=4444,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='4',
            state_code='US_ND',
            admission_date=date(2001, 6, 19),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2001, 7, 17),
            release_reason=ReleaseReason.TRANSFER)

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [
                attr.evolve(ip) for ip in ip_order_combo
            ]

            updated_incarceration_periods = _infer_missing_dates_and_statuses(ips_for_test)

            self.assertEqual([
                updated_open_incarceration_period_1,
                updated_open_incarceration_period_2,
                updated_closed_incarceration_period_1,
                updated_closed_incarceration_period_2
            ], updated_incarceration_periods)

    def test_infer_missing_dates_and_statuses_multiple_open_periods(self):
        open_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            external_id='1',
            state_code='US_ND',
            admission_date=date(2001, 6, 19),
            admission_reason=AdmissionReason.TRANSFER)

        open_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            external_id='2',
            state_code='US_ND',
            admission_date=date(2001, 6, 19),
            admission_reason=AdmissionReason.TRANSFER)

        open_incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='3',
            state_code='US_ND',
            admission_date=date(2001, 6, 19),
            admission_reason=AdmissionReason.TRANSFER)

        incarceration_periods = [open_incarceration_period_1,
                                 open_incarceration_period_2,
                                 open_incarceration_period_3]

        updated_open_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='1',
            state_code='US_ND',
            admission_date=date(2001, 6, 19),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2001, 6, 19),
            release_reason=ReleaseReason.TRANSFER
        )

        updated_open_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            external_id='2',
            state_code='US_ND',
            admission_date=date(2001, 6, 19),
            admission_reason=AdmissionReason.TRANSFER
        )

        updated_open_incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id='3',
            state_code='US_ND',
            admission_date=date(2001, 6, 19),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2001, 6, 19),
            release_reason=ReleaseReason.TRANSFER
        )

        for ip_order_combo in permutations(incarceration_periods.copy()):
            ips_for_test = [
                attr.evolve(ip) for ip in ip_order_combo
            ]

            updated_incarceration_periods = _infer_missing_dates_and_statuses(ips_for_test)

            self.assertEqual([
                updated_open_incarceration_period_3,
                updated_open_incarceration_period_1,
                updated_open_incarceration_period_2
            ], updated_incarceration_periods)

    def test_infer_missing_dates_and_statuses_no_periods(self):
        incarceration_periods = []

        updated_incarceration_periods = _infer_missing_dates_and_statuses(incarceration_periods)

        self.assertEqual(updated_incarceration_periods, [])

    def test_infer_missing_dates_and_statuses_set_missing_admission_data_after_transfer(self):
        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id='1',
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2004, 1, 3),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 4, 14),
            release_reason=ReleaseReason.TRANSFER)

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id='2',
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=None,
            admission_reason=None,
            release_date=date(2010, 4, 14),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [first_incarceration_period,
                                 second_incarceration_period]

        expected_output = [
            StateIncarcerationPeriod.new_with_defaults(
                external_id='1',
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2004, 1, 3),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2008, 4, 14),
                release_reason=ReleaseReason.TRANSFER),
            StateIncarcerationPeriod.new_with_defaults(
                external_id='2',
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2008, 4, 14),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2010, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)
        ]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [
                attr.evolve(ip) for ip in ip_order_combo
            ]

            updated_incarceration_periods = _infer_missing_dates_and_statuses(ips_for_test)

            self.assertEqual(expected_output, updated_incarceration_periods)

    def test_infer_missing_dates_and_statuses_same_dates_sort_by_external_id(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id='1',
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2004, 1, 3),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 4, 14),
            release_reason=ReleaseReason.TRANSFER)

        incarceration_period_copy = StateIncarcerationPeriod.new_with_defaults(
            external_id='2',
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2004, 1, 3),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 4, 14),
            release_reason=ReleaseReason.TRANSFER)

        incarceration_periods = [incarceration_period, incarceration_period_copy]

        updated_order = [incarceration_period]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [
                attr.evolve(ip) for ip in ip_order_combo
            ]

            updated_incarceration_periods = _infer_missing_dates_and_statuses(ips_for_test)

            self.assertEqual(updated_order, updated_incarceration_periods)

    def test_infer_missing_dates_and_statuses_same_admission_dates_sort_by_external_id(self):
        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id='1',
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2004, 1, 3),
            admission_reason=AdmissionReason.NEW_ADMISSION)

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id='2',
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2004, 1, 3),
            admission_reason=AdmissionReason.NEW_ADMISSION)

        incarceration_periods = [second_incarceration_period,
                                 first_incarceration_period]

        updated_first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id='1',
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2004, 1, 3),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2004, 1, 3),
            release_reason=ReleaseReason.INTERNAL_UNKNOWN
        )

        expected_output = [updated_first_incarceration_period, second_incarceration_period]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [
                attr.evolve(ip) for ip in ip_order_combo
            ]

            updated_incarceration_periods = _infer_missing_dates_and_statuses(ips_for_test)

            self.assertEqual(expected_output, updated_incarceration_periods)

    def test_infer_missing_dates_and_statuses_same_admission_dates_sort_by_statuses(self):
        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id='1',
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2004, 1, 3),
            admission_reason=AdmissionReason.NEW_ADMISSION
        )

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id='2',
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.EXTERNAL_UNKNOWN,
            state_code='US_ND',
            admission_date=date(2004, 1, 3),
            admission_reason=AdmissionReason.NEW_ADMISSION)

        incarceration_periods = [second_incarceration_period,
                                 first_incarceration_period]

        updated_second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id='2',
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2004, 1, 3),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2004, 1, 3),
            release_reason=ReleaseReason.INTERNAL_UNKNOWN
        )

        expected_output = [updated_second_incarceration_period, first_incarceration_period]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [
                attr.evolve(ip) for ip in ip_order_combo
            ]

            updated_incarceration_periods = _infer_missing_dates_and_statuses(ips_for_test)

            self.assertEqual(expected_output, updated_incarceration_periods)

    def test_infer_missing_dates_and_statuses_no_admission_dates(self):
        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id='1',
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            release_date=date(2004, 1, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id='2',
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            release_date=date(2004, 1, 10),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [second_incarceration_period,
                                 first_incarceration_period]

        updated_first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id='1',
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2004, 1, 3),
            admission_reason=AdmissionReason.INTERNAL_UNKNOWN,
            release_date=date(2004, 1, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED
        )

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id='2',
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2004, 1, 3),
            admission_reason=AdmissionReason.INTERNAL_UNKNOWN,
            release_date=date(2004, 1, 10),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        expected_output = [updated_first_incarceration_period, second_incarceration_period]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [
                attr.evolve(ip) for ip in ip_order_combo
            ]

            updated_incarceration_periods = _infer_missing_dates_and_statuses(ips_for_test)

            self.assertEqual(expected_output, updated_incarceration_periods)

    def test_infer_missing_dates_and_statuses_nested_period(self):
        outer_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id='1',
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 2, 5),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        nested_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id='2',
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 3, 13),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2002, 3, 13),
            release_reason=ReleaseReason.TRANSFER)

        incarceration_periods = [outer_ip,
                                 nested_ip]

        expected_output = [outer_ip]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [
                attr.evolve(ip) for ip in ip_order_combo
            ]

            updated_incarceration_periods = _infer_missing_dates_and_statuses(ips_for_test)

            self.assertEqual(expected_output, updated_incarceration_periods)

    def test_infer_missing_dates_and_statuses_multiple_nested_periods(self):
        outer_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id='1',
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 2, 5),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        nested_ip_1 = StateIncarcerationPeriod.new_with_defaults(
            external_id='2',
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 2, 13),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2002, 2, 18),
            release_reason=ReleaseReason.TRANSFER)

        nested_ip_2 = StateIncarcerationPeriod.new_with_defaults(
            external_id='3',
            incarceration_period_id=3333,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 2, 18),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2002, 6, 20),
            release_reason=ReleaseReason.TRANSFER)

        nested_ip_3 = StateIncarcerationPeriod.new_with_defaults(
            external_id='4',
            incarceration_period_id=4444,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 6, 20),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2002, 6, 29),
            release_reason=ReleaseReason.TRANSFER)

        nested_ip_4 = StateIncarcerationPeriod.new_with_defaults(
            external_id='5',
            incarceration_period_id=5555,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 6, 29),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2002, 9, 11),
            release_reason=ReleaseReason.TRANSFER)

        incarceration_periods = [outer_ip, nested_ip_1, nested_ip_2, nested_ip_3, nested_ip_4]

        expected_output = [outer_ip]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [
                attr.evolve(ip) for ip in ip_order_combo
            ]

            updated_incarceration_periods = _infer_missing_dates_and_statuses(ips_for_test)

            self.assertEqual(expected_output, updated_incarceration_periods)

    def test_infer_missing_dates_and_statuses_partial_overlap_period(self):
        ip_1 = StateIncarcerationPeriod.new_with_defaults(
            external_id='1',
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 2, 5),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        ip_2 = StateIncarcerationPeriod.new_with_defaults(
            external_id='2',
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 3, 13),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2002, 10, 22),
            release_reason=ReleaseReason.TRANSFER)

        incarceration_periods = [ip_1, ip_2]

        expected_output = [ip_1, ip_2]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [
                attr.evolve(ip) for ip in ip_order_combo
            ]

            updated_incarceration_periods = _infer_missing_dates_and_statuses(ips_for_test)

            self.assertEqual(expected_output, updated_incarceration_periods)


class TestIpIsNestedInPreviousPeriod(unittest.TestCase):
    """Tests the _ip_is_nested_in_previous_period function."""
    def test_ip_is_nested_in_previous_period(self):
        previous_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id='1',
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 2, 5),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        ip = StateIncarcerationPeriod.new_with_defaults(
            external_id='2',
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 3, 13),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2002, 5, 22),
            release_reason=ReleaseReason.TRANSFER)

        is_nested = _ip_is_nested_in_previous_period(ip, previous_ip)

        self.assertTrue(is_nested)

    def test_ip_is_nested_in_previous_period_not_nested(self):
        previous_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id='1',
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 2, 5),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        ip = StateIncarcerationPeriod.new_with_defaults(
            external_id='2',
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 10, 13),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2002, 12, 22),
            release_reason=ReleaseReason.TRANSFER)

        is_nested = _ip_is_nested_in_previous_period(ip, previous_ip)

        self.assertFalse(is_nested)

    def test_ip_is_nested_in_previous_period_share_release(self):
        previous_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id='1',
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 2, 5),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        ip = StateIncarcerationPeriod.new_with_defaults(
            external_id='2',
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 3, 13),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2002, 9, 11),
            release_reason=ReleaseReason.TRANSFER)

        is_nested = _ip_is_nested_in_previous_period(ip, previous_ip)

        self.assertTrue(is_nested)

    def test_ip_is_nested_in_previous_period_single_day_period(self):
        previous_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id='1',
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 2, 5),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        ip = StateIncarcerationPeriod.new_with_defaults(
            external_id='2',
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 3, 13),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2002, 3, 13),
            release_reason=ReleaseReason.TRANSFER)

        is_nested = _ip_is_nested_in_previous_period(ip, previous_ip)

        self.assertTrue(is_nested)

    def test_ip_is_nested_in_previous_period_two_single_day_periods(self):
        previous_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id='1',
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 3, 13),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2002, 3, 13),
            release_reason=ReleaseReason.TRANSFER)

        ip = StateIncarcerationPeriod.new_with_defaults(
            external_id='2',
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 3, 13),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2002, 3, 13),
            release_reason=ReleaseReason.TRANSFER)

        is_nested = _ip_is_nested_in_previous_period(ip, previous_ip)

        self.assertFalse(is_nested)

    def test_ip_is_nested_in_previous_period_single_day_period_on_release(self):
        previous_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id='1',
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 1, 13),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2002, 3, 13),
            release_reason=ReleaseReason.TRANSFER)

        ip = StateIncarcerationPeriod.new_with_defaults(
            external_id='2',
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 3, 13),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2002, 3, 13),
            release_reason=ReleaseReason.TRANSFER)

        is_nested = _ip_is_nested_in_previous_period(ip, previous_ip)

        self.assertFalse(is_nested)

    def test_ip_is_nested_in_previous_period_bad_sort(self):
        # This period should not have been sorted before ip
        previous_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id='1',
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 2, 5),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        ip = StateIncarcerationPeriod.new_with_defaults(
            external_id='2',
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2002, 1, 1),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2002, 5, 22),
            release_reason=ReleaseReason.TRANSFER)

        with pytest.raises(ValueError):
            _ip_is_nested_in_previous_period(ip, previous_ip)
