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
"""Tests the us_nd_utils.py file."""
import unittest
from datetime import date

from recidiviz.calculator.pipeline.utils.us_nd_utils import \
    set_missing_admission_data
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason as AdmissionReason
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodReleaseReason as ReleaseReason
from recidiviz.persistence.entity.entity_utils import is_placeholder
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


class TestSetMissingAdmissionData(unittest.TestCase):
    """Tests for the set_missing_admission_data function."""
    def test_set_missing_admission_data(self):
        """Tests that the admission data on an incarceration period is set when
        there's an empty admission_date and admission_reason following a
        transfer out.
        """
        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id='99983-1|99983-2',
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2004, 1, 3),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2008, 4, 14),
                release_reason=ReleaseReason.TRANSFER)

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id='99983-3',
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_ND',
                admission_date=None,
                admission_reason=None,
                release_date=date(2010, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [first_incarceration_period,
                                 second_incarceration_period]

        updated_incarceration_periods = \
            set_missing_admission_data(incarceration_periods)

        assert updated_incarceration_periods == [
            StateIncarcerationPeriod.new_with_defaults(
                external_id='99983-1|99983-2',
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2004, 1, 3),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2008, 4, 14),
                release_reason=ReleaseReason.TRANSFER),
            StateIncarcerationPeriod.new_with_defaults(
                external_id='99983-3',
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2008, 4, 14),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2010, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)
        ]

    def test_set_missing_admission_data_all_valid(self):
        """Tests that no information is changed when all incarceration periods
        have valid data."""
        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id='99983-1|99983-2',
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2004, 1, 3),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2008, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id='99983-3',
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2009, 3, 19),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [first_incarceration_period,
                                 second_incarceration_period]

        updated_incarceration_periods = \
            set_missing_admission_data(incarceration_periods)

        assert updated_incarceration_periods == incarceration_periods

    def test_set_missing_admission_data_invalid_missing_data(self):
        """Tests that the admission data is not updated when there's not
        a valid transfer out preceding the empty admission data."""
        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id='99983-1|99983-2',
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_ND',
                admission_date=None,
                admission_reason=None,
                release_date=date(2008, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [first_incarceration_period]

        updated_incarceration_periods = \
            set_missing_admission_data(incarceration_periods)

        assert updated_incarceration_periods == incarceration_periods

    def test_set_missing_admission_data_unsorted_periods(self):
        """Tests that the admission data on an incarceration period is set when
        there's an empty admission_date and admission_reason following a
        transfer out, and the incarceration periods are not sent in a sorted
        order.
        """
        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id='99983-1|99983-2',
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2004, 1, 3),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2008, 4, 14),
                release_reason=ReleaseReason.TRANSFER)

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id='99983-3',
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_ND',
                admission_date=None,
                admission_reason=None,
                release_date=date(2010, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [second_incarceration_period,
                                 first_incarceration_period]

        updated_incarceration_periods = \
            set_missing_admission_data(incarceration_periods)

        assert updated_incarceration_periods == [
            StateIncarcerationPeriod.new_with_defaults(
                external_id='99983-1|99983-2',
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2004, 1, 3),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2008, 4, 14),
                release_reason=ReleaseReason.TRANSFER),
            StateIncarcerationPeriod.new_with_defaults(
                external_id='99983-3',
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2008, 4, 14),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2010, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)
        ]

    def test_set_missing_admission_data_empty_input(self):
        """Tests that the function does not fail when there are no
        incarceration periods sent to it."""
        incarceration_periods = []

        updated_incarceration_periods = \
            set_missing_admission_data(incarceration_periods)

        assert updated_incarceration_periods == incarceration_periods

    def test_set_missing_admission_data_no_external_id(self):
        """Tests that the function removes incarceration periods without
        external_ids."""
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code='US_ND',
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2008, 4, 14),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2010, 4, 14),
            release_reason=ReleaseReason.SENTENCE_SERVED
        )

        incarceration_periods = [incarceration_period]

        assert not is_placeholder(incarceration_period)
        assert incarceration_period.external_id is None

        updated_incarceration_periods = \
            set_missing_admission_data(incarceration_periods)

        assert updated_incarceration_periods == []

    def test_set_missing_admission_data_placeholders(self):
        """Tests that the function removes placeholder incarceration periods."""
        placeholder = StateIncarcerationPeriod.new_with_defaults(
            state_code='US_ND'
        )

        incarceration_periods = [placeholder]

        assert is_placeholder(placeholder)

        updated_incarceration_periods = \
            set_missing_admission_data(incarceration_periods)

        assert updated_incarceration_periods == []
