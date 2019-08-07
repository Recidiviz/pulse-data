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

"""Tests for recidivism/identifier.py."""

import pytest
import unittest
from datetime import date
from typing import Dict

from recidiviz.calculator.recidivism import identifier
from recidiviz.calculator.recidivism.release_event import ReleaseEvent, \
    RecidivismReleaseEvent, NonRecidivismReleaseEvent, ReincarcerationReturnType
from recidiviz.calculator.recidivism.release_event import \
    ReincarcerationReturnFromSupervisionType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationFacilitySecurityLevel
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason as AdmissionReason
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodReleaseReason as ReleaseReason
from recidiviz.persistence.entity.state.entities import \
    StateIncarcerationPeriod, StatePerson


class TestClassifyReleaseEvents(unittest.TestCase):
    """Tests for the find_release_events_by_cohort_year function."""

    def test_find_release_events_by_cohort_year(self):
        """Tests the find_release_events_by_cohort_year function path where the
        person did recidivate."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 4, 5),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        subsequent_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='TX',
                admission_date=date(2017, 1, 4),
                admission_reason=AdmissionReason.NEW_ADMISSION)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period,
                                 subsequent_reincarceration_period]

        release_events_by_cohort = \
            identifier.find_release_events_by_cohort_year(
                incarceration_periods)

        assert len(release_events_by_cohort) == 2

        assert release_events_by_cohort[2010] == [RecidivismReleaseEvent(
            state_code='TX',
            original_admission_date=initial_incarceration_period.admission_date,
            release_date=initial_incarceration_period.release_date,
            release_facility=None,
            reincarceration_date=first_reincarceration_period.admission_date,
            reincarceration_facility=None,
            return_type=ReincarcerationReturnType.NEW_ADMISSION)]

        assert release_events_by_cohort[2014] == [RecidivismReleaseEvent(
            state_code='TX',
            original_admission_date=first_reincarceration_period.admission_date,
            release_date=first_reincarceration_period.release_date,
            release_facility=None,
            reincarceration_date=subsequent_reincarceration_period.
            admission_date,
            reincarceration_facility=None,
            return_type=ReincarcerationReturnType.NEW_ADMISSION)]

    def test_find_release_events_by_cohort_year_no_incarcerations_at_all(self):
        """Tests the find_release_events_by_cohort_year function when the person
        has no StateIncarcerationPeriods."""
        release_events_by_cohort = \
            identifier.find_release_events_by_cohort_year([])

        assert not release_events_by_cohort

    def test_find_release_events_by_cohort_year_no_recidivism_after_first(self):
        """Tests the find_release_events_by_cohort_year function when the person
        does not have any StateIncarcerationPeriods after their first."""
        only_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2000, 1, 9),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2003, 12, 8),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        release_events_by_cohort = \
            identifier.find_release_events_by_cohort_year(
                [only_incarceration_period])

        assert len(release_events_by_cohort) == 1

        assert release_events_by_cohort[2003] == [
            NonRecidivismReleaseEvent(
                state_code='TX',
                original_admission_date=only_incarceration_period.
                admission_date,
                release_date=only_incarceration_period.release_date,
                release_facility=None)]

    def test_find_release_events_by_cohort_year_still_incarcerated(self):
        """Tests the find_release_events_by_cohort_year function where the
        person is still incarcerated on their very first
         StateIncarcerationPeriod."""
        only_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION)

        release_events_by_cohort = \
            identifier.find_release_events_by_cohort_year(
                [only_incarceration_period])

        assert not release_events_by_cohort

    def test_find_release_events_by_cohort_year_no_recid_cond_release(self):
        """Tests the find_release_events_by_cohort_year function when the person
        does not have any StateIncarcerationPeriods after their first, and they
        were released conditionally."""
        only_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2000, 1, 9),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2003, 12, 8),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE)

        release_events_by_cohort = \
            identifier.find_release_events_by_cohort_year(
                [only_incarceration_period])

        assert len(release_events_by_cohort) == 1

        assert release_events_by_cohort[2003] == [
            NonRecidivismReleaseEvent(
                state_code='TX',
                original_admission_date=only_incarceration_period.
                admission_date,
                release_date=only_incarceration_period.release_date,
                release_facility=None)]

    def test_find_release_events_by_cohort_year_parole_revocation(self):
        """Tests the find_release_events_by_cohort_year function path where the
        person was conditionally released on parole and returned for a parole
        revocation."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 4, 5),
                admission_reason=AdmissionReason.PAROLE_REVOCATION,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period]

        release_events_by_cohort = \
            identifier.find_release_events_by_cohort_year(
                incarceration_periods=incarceration_periods)

        assert len(release_events_by_cohort) == 2

        assert release_events_by_cohort[2010] == [RecidivismReleaseEvent(
            state_code='TX',
            original_admission_date=initial_incarceration_period.admission_date,
            release_date=initial_incarceration_period.release_date,
            release_facility=None,
            reincarceration_date=first_reincarceration_period.admission_date,
            reincarceration_facility=None,
            return_type=ReincarcerationReturnType.REVOCATION,
            from_supervision_type=ReincarcerationReturnFromSupervisionType.
            PAROLE)]

        assert release_events_by_cohort[2014] == [
            NonRecidivismReleaseEvent(
                state_code='TX',
                original_admission_date=first_reincarceration_period.
                admission_date,
                release_date=first_reincarceration_period.release_date,
                release_facility=None)]

    def test_find_release_events_by_cohort_year_probation_revocation(self):
        """Tests the find_release_events_by_cohort_year function path where the
        person was conditionally released and returned for a probation
        revocation."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 4, 5),
                admission_reason=AdmissionReason.PROBATION_REVOCATION,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period]

        release_events_by_cohort = \
            identifier.find_release_events_by_cohort_year(
                incarceration_periods=incarceration_periods)

        assert len(release_events_by_cohort) == 2

        assert release_events_by_cohort[2010] == [RecidivismReleaseEvent(
            state_code='TX',
            original_admission_date=initial_incarceration_period.admission_date,
            release_date=initial_incarceration_period.release_date,
            release_facility=None,
            reincarceration_date=first_reincarceration_period.admission_date,
            reincarceration_facility=None,
            return_type=ReincarcerationReturnType.REVOCATION,
            from_supervision_type=ReincarcerationReturnFromSupervisionType.
            PROBATION)]

        assert release_events_by_cohort[2014] == [
            NonRecidivismReleaseEvent(
                state_code='TX',
                original_admission_date=first_reincarceration_period.
                admission_date,
                release_date=first_reincarceration_period.release_date,
                release_facility=None)]

    def test_find_release_events_by_cohort_year_cond_release_new_admit(self):
        """Tests the find_release_events_by_cohort_year function path where the
        person was conditionally released on parole but returned as a new
         admission."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 4, 5),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period]

        release_events_by_cohort = \
            identifier.find_release_events_by_cohort_year(
                incarceration_periods=incarceration_periods)

        assert len(release_events_by_cohort) == 2

        assert release_events_by_cohort[2010] == [RecidivismReleaseEvent(
            state_code='TX',
            original_admission_date=initial_incarceration_period.admission_date,
            release_date=initial_incarceration_period.release_date,
            release_facility=None,
            reincarceration_date=first_reincarceration_period.admission_date,
            reincarceration_facility=None,
            return_type=ReincarcerationReturnType.NEW_ADMISSION)]

        assert release_events_by_cohort[2014] == [
            NonRecidivismReleaseEvent(
                state_code='TX',
                original_admission_date=first_reincarceration_period.
                admission_date,
                release_date=first_reincarceration_period.release_date,
                release_facility=None)]

    def test_find_release_events_by_cohort_year_sentence_served_prob_rev(self):
        """Tests the find_release_events_by_cohort_year function path where the
        person served their first sentence, then later returned on a probation
        revocation."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 4, 5),
                admission_reason=AdmissionReason.PROBATION_REVOCATION,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period]

        release_events_by_cohort = \
            identifier.find_release_events_by_cohort_year(
                incarceration_periods=incarceration_periods)

        assert len(release_events_by_cohort) == 2

        assert release_events_by_cohort[2010] == [RecidivismReleaseEvent(
            state_code='TX',
            original_admission_date=initial_incarceration_period.admission_date,
            release_date=initial_incarceration_period.release_date,
            release_facility=None,
            reincarceration_date=first_reincarceration_period.admission_date,
            reincarceration_facility=None,
            return_type=ReincarcerationReturnType.REVOCATION,
            from_supervision_type=ReincarcerationReturnFromSupervisionType.
            PROBATION)]

        assert release_events_by_cohort[2014] == [
            NonRecidivismReleaseEvent(
                state_code='TX',
                original_admission_date=first_reincarceration_period.
                admission_date,
                release_date=first_reincarceration_period.release_date,
                release_facility=None)]

    def test_find_release_events_by_cohort_year_transfer_no_recidivism(self):
        """Tests the find_release_events_by_cohort_year function path where the
        person was transferred between two incarceration periods."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2010, 12, 4),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period]

        release_events_by_cohort = \
            identifier.find_release_events_by_cohort_year(
                incarceration_periods=incarceration_periods)

        assert len(release_events_by_cohort) == 1

        assert release_events_by_cohort[2014] == [
            NonRecidivismReleaseEvent(
                state_code='TX',
                original_admission_date=initial_incarceration_period.
                admission_date,
                release_date=first_reincarceration_period.release_date,
                release_facility=None)]

    def test_find_release_events_by_cohort_year_transfer_out_but_recid(self):
        """Tests the find_release_events_by_cohort_year function path where the
        person was transferred out of state, then later returned on a new
        admission."""
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 4, 5),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period]

        release_events_by_cohort = \
            identifier.find_release_events_by_cohort_year(
                incarceration_periods=incarceration_periods)

        assert len(release_events_by_cohort) == 1

        assert release_events_by_cohort[2014] == [
            NonRecidivismReleaseEvent(
                state_code='TX',
                original_admission_date=first_reincarceration_period.
                admission_date,
                release_date=first_reincarceration_period.release_date,
                release_facility=None)]


class TestValidateSortAndCollapseIncarcerationPeriods:
    """Tests the validate_sort_and_collapse_incarceration_periods function."""

    def test_validate_incarceration_periods(self):
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 3, 4),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2012, 2, 4),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        input_incarceration_periods = [
            initial_incarceration_period, first_reincarceration_period,
            second_reincarceration_period]

        validated_incarceration_periods = \
            identifier.validate_sort_and_collapse_incarceration_periods(
                input_incarceration_periods)

        assert validated_incarceration_periods == input_incarceration_periods

    def test_validate_incarceration_periods_empty_admission_date(self):
        """Tests that there are no incarceration periods returned when there's
        an empty admission_date.
        """
        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2008, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        input_incarceration_periods = [first_incarceration_period]

        collapsed_incarceration_periods = \
            identifier.validate_sort_and_collapse_incarceration_periods(
                input_incarceration_periods)

        assert not collapsed_incarceration_periods

    def test_validate_incarceration_periods_empty_admission_reason(self):
        """Tests that there are no incarceration periods returned when there's
        an empty admission_reason.
        """
        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2006, 1, 7),
                release_date=date(2008, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        input_incarceration_periods = [first_incarceration_period]

        collapsed_incarceration_periods = \
            identifier.validate_sort_and_collapse_incarceration_periods(
                input_incarceration_periods)

        assert not collapsed_incarceration_periods

    def test_validate_incarceration_periods_empty_admission_data(self):
        """Tests that there are no incarceration periods returned when there's
        an empty admission_date and admission_reason.
        """
        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=None,
                admission_reason=None,
                release_date=date(2008, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        input_incarceration_periods = [first_incarceration_period]

        collapsed_incarceration_periods = \
            identifier.validate_sort_and_collapse_incarceration_periods(
                input_incarceration_periods)

        assert not collapsed_incarceration_periods

    def test_validate_incarceration_periods_empty_release_date(self):
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 3, 4),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2012, 2, 4),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_reason=ReleaseReason.SENTENCE_SERVED)

        input_incarceration_periods = [
            initial_incarceration_period, first_reincarceration_period,
            second_reincarceration_period]

        collapsed_incarceration_periods = \
            identifier.validate_sort_and_collapse_incarceration_periods(
                input_incarceration_periods)

        assert not collapsed_incarceration_periods

    def test_validate_incarceration_periods_empty_release_reason(self):
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 3, 4),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2012, 2, 4),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2014, 4, 14))

        input_incarceration_periods = [
            initial_incarceration_period, first_reincarceration_period,
            second_reincarceration_period]

        collapsed_incarceration_periods = \
            identifier.validate_sort_and_collapse_incarceration_periods(
                input_incarceration_periods)

        assert not collapsed_incarceration_periods

    def test_validate_incarceration_periods_empty_release_in_custody(self):
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 3, 4),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='TX',
                admission_date=date(2012, 2, 4),
                admission_reason=AdmissionReason.NEW_ADMISSION)

        input_incarceration_periods = [
            initial_incarceration_period, first_reincarceration_period,
            second_reincarceration_period]

        validated_incarceration_periods = \
            identifier.validate_sort_and_collapse_incarceration_periods(
                input_incarceration_periods)

        assert validated_incarceration_periods == input_incarceration_periods

    def test_validate_incarceration_periods_placeholder(self):
        incarceration_periods = [StateIncarcerationPeriod.new_with_defaults(
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
            state_code='XX',
            person=[StatePerson.new_with_defaults()],
            incarceration_period_id=1)]

        validated_incarceration_periods = \
            identifier.validate_sort_and_collapse_incarceration_periods(
                incarceration_periods)

        assert validated_incarceration_periods == []

    def test_validate_incarceration_periods_placeholder_and_valid(self):
        placeholder = StateIncarcerationPeriod.new_with_defaults(
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
            state_code='XX',
            person=[StatePerson.new_with_defaults()],
            incarceration_period_id=1)

        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [placeholder, valid_incarceration_period]

        validated_incarceration_periods = \
            identifier.validate_sort_and_collapse_incarceration_periods(
                incarceration_periods)

        assert validated_incarceration_periods == [valid_incarceration_period]

    def test_sort_incarceration_periods(self):
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 3, 4),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2012, 2, 4),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        input_incarceration_periods = [
            initial_incarceration_period, second_reincarceration_period,
            first_reincarceration_period]

        validated_incarceration_periods = \
            identifier.validate_sort_and_collapse_incarceration_periods(
                input_incarceration_periods)

        assert validated_incarceration_periods == [
            initial_incarceration_period,
            first_reincarceration_period,
            second_reincarceration_period]

    def test_collapse_incarceration_periods(self):
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2010, 12, 4),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period]

        collapsed_incarceration_periods = \
            identifier.validate_sort_and_collapse_incarceration_periods(
                incarceration_periods)

        assert len(collapsed_incarceration_periods) == 1

        assert collapsed_incarceration_periods == [
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=initial_incarceration_period.
                incarceration_period_id,
                status=first_reincarceration_period.status,
                state_code=initial_incarceration_period.state_code,
                admission_date=initial_incarceration_period.admission_date,
                admission_reason=initial_incarceration_period.admission_reason,
                release_date=first_reincarceration_period.release_date,
                release_reason=first_reincarceration_period.release_reason)
        ]

    def test_collapse_incarceration_periods_missing_transfer_in(self):
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2003, 1, 2),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2007, 4, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=None,
                admission_reason=None,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period,
                                 second_reincarceration_period]

        collapsed_incarceration_periods = \
            identifier.validate_sort_and_collapse_incarceration_periods(
                incarceration_periods)

        assert not collapsed_incarceration_periods

    def test_collapse_incarceration_periods_missing_transfer_in_twice(self):
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2003, 1, 2),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2007, 4, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=None,
                admission_reason=None,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        third_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=None,
                admission_reason=None,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period,
                                 second_reincarceration_period,
                                 third_reincarceration_period]

        collapsed_incarceration_periods = \
            identifier.validate_sort_and_collapse_incarceration_periods(
                incarceration_periods)

        assert not collapsed_incarceration_periods


class TestCollapseIncarcerationPeriods:
    """Tests the collapse_incarceration_periods function."""

    def test_collapse_incarceration_periods(self):
        """Tests collapse_incarceration_periods for two incarceration
        periods linked by a transfer."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2010, 12, 4),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period]

        collapsed_incarceration_periods = \
            identifier.collapse_incarceration_periods(incarceration_periods)

        assert len(collapsed_incarceration_periods) == 1

        assert collapsed_incarceration_periods == [
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=initial_incarceration_period.
                incarceration_period_id,
                status=first_reincarceration_period.status,
                state_code=initial_incarceration_period.state_code,
                admission_date=initial_incarceration_period.admission_date,
                admission_reason=initial_incarceration_period.admission_reason,
                release_date=first_reincarceration_period.release_date,
                release_reason=first_reincarceration_period.release_reason)
        ]

    def test_collapse_incarceration_periods_no_transfers(self):
        """Tests collapse_incarceration_periods for two incarceration
        periods not linked by a transfer."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2010, 12, 4),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period]

        collapsed_incarceration_periods = \
            identifier.collapse_incarceration_periods(incarceration_periods)

        assert len(collapsed_incarceration_periods) == 2

        assert collapsed_incarceration_periods == incarceration_periods

    def test_collapse_incarceration_periods_multiple_transfers(self):
        """Tests collapse_incarceration_periods for a person who was repeatedly
        transferred between facilities. All of these incarceration periods
        should collapse into a single incarceration period."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 3, 2),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2012, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2012, 2, 4),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.TRANSFER)

        third_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=4444,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2016, 6, 2),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2017, 3, 1),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period,
                                 second_reincarceration_period,
                                 third_reincarceration_period]

        collapsed_incarceration_periods = \
            identifier.collapse_incarceration_periods(incarceration_periods)

        assert len(collapsed_incarceration_periods) == 1

        assert collapsed_incarceration_periods == [
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=initial_incarceration_period.
                incarceration_period_id,
                status=third_reincarceration_period.status,
                state_code=initial_incarceration_period.state_code,
                admission_date=initial_incarceration_period.admission_date,
                admission_reason=initial_incarceration_period.admission_reason,
                release_date=third_reincarceration_period.release_date,
                release_reason=third_reincarceration_period.release_reason),
        ]

    def test_collapse_incarceration_periods_between_periods(self):
        """Tests collapse_incarceration_periods for two incarceration
        periods linked by a transfer preceded and followed by regular
        incarceration periods."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 3, 2),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2012, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2012, 2, 4),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        third_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=4444,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2016, 6, 2),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2017, 3, 1),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period,
                                 second_reincarceration_period,
                                 third_reincarceration_period]

        collapsed_incarceration_periods = \
            identifier.collapse_incarceration_periods(incarceration_periods)

        assert len(collapsed_incarceration_periods) == 3

        assert collapsed_incarceration_periods == [
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
        ]

    def test_collapse_incarceration_periods_no_incarcerations(self):
        """Tests collapse_incarceration_periods for an empty list of
        incarceration periods."""

        collapsed_incarceration_periods = \
            identifier.collapse_incarceration_periods([])

        assert not collapsed_incarceration_periods

    def test_collapse_incarceration_periods_one_incarceration(self):
        """Tests collapse_incarceration_periods for a person with only
        one incarceration period that ended with a sentence served."""

        only_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [only_incarceration_period]

        collapsed_incarceration_periods = \
            identifier.collapse_incarceration_periods(incarceration_periods)

        assert collapsed_incarceration_periods == incarceration_periods

    def test_collapse_incarceration_periods_one_incarceration_transferred(self):
        """Tests collapse_incarceration_periods for a person with only
        one incarceration period that ended with a transfer out of state
        prison."""

        only_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        incarceration_periods = [only_incarceration_period]

        collapsed_incarceration_periods = \
            identifier.collapse_incarceration_periods(incarceration_periods)

        assert collapsed_incarceration_periods == incarceration_periods

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

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 3, 2),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2012, 12, 4),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2013, 12, 4),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period,
                                 second_reincarceration_period]

        collapsed_incarceration_periods = \
            identifier.collapse_incarceration_periods(incarceration_periods)

        assert len(collapsed_incarceration_periods) == 3

        assert collapsed_incarceration_periods == incarceration_periods

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

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 3, 2),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2012, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2012, 12, 4),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period,
                                 second_reincarceration_period]

        collapsed_incarceration_periods = \
            identifier.collapse_incarceration_periods(incarceration_periods)

        assert len(collapsed_incarceration_periods) == 2

        assert collapsed_incarceration_periods == [
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
        ]


class TestCombineIncarcerationPeriods:
    """Tests for combine_incarceration_periods function."""

    def test_combine_incarceration_periods(self):
        """Tests for combining two incarceration periods connected by a
        transfer."""

        start_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
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
            state_code='TX',
            facility='Jones',
            housing_unit='HouseUnit3',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.
            MAXIMUM,
            projected_release_reason=ReleaseReason.SENTENCE_SERVED,
            admission_date=date(2010, 12, 4),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2012, 12, 10),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE)

        combined_incarceration_period = \
            identifier.combine_incarceration_periods(
                start_incarceration_period, end_incarceration_period
            )

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


_RETURN_TYPES_BY_STANDARD_ADMISSION: Dict[
    AdmissionReason, ReincarcerationReturnType] = {
        AdmissionReason.ADMITTED_IN_ERROR:
            ReincarcerationReturnType.NEW_ADMISSION,
        AdmissionReason.EXTERNAL_UNKNOWN:
            ReincarcerationReturnType.NEW_ADMISSION,
        AdmissionReason.NEW_ADMISSION:
            ReincarcerationReturnType.NEW_ADMISSION,
        AdmissionReason.PAROLE_REVOCATION:
            ReincarcerationReturnType.REVOCATION,
        AdmissionReason.PROBATION_REVOCATION:
            ReincarcerationReturnType.REVOCATION,
        AdmissionReason.TRANSFER:
            ReincarcerationReturnType.NEW_ADMISSION
    }


# Stores the return types for the combinations of release reason and
# admission reason that should be included in a release cohort
SHOULD_INCLUDE_WITH_RETURN_TYPE: \
    Dict[ReleaseReason, Dict[AdmissionReason, ReincarcerationReturnType]] = \
    {ReleaseReason.COMMUTED: _RETURN_TYPES_BY_STANDARD_ADMISSION,
     ReleaseReason.CONDITIONAL_RELEASE: _RETURN_TYPES_BY_STANDARD_ADMISSION,
     ReleaseReason.COURT_ORDER: {},
     ReleaseReason.DEATH: {},
     ReleaseReason.ESCAPE: {},
     ReleaseReason.EXTERNAL_UNKNOWN: _RETURN_TYPES_BY_STANDARD_ADMISSION,
     ReleaseReason.RELEASED_IN_ERROR: {},
     ReleaseReason.SENTENCE_SERVED: _RETURN_TYPES_BY_STANDARD_ADMISSION,
     ReleaseReason.TRANSFER: {}}


class TestShouldIncludeInReleaseCohort(unittest.TestCase):
    """Tests the should_include_in_release_cohort function."""

    def test_should_include_in_release_cohort(self):
        """Tests the should_include_in_release_cohort_function for all
        possible combinations of release reason and admission reason."""
        for release_reason in ReleaseReason:
            for admission_reason in AdmissionReason:
                should_include = \
                    identifier.should_include_in_release_cohort(
                        release_reason, admission_reason)

                if admission_reason in \
                        SHOULD_INCLUDE_WITH_RETURN_TYPE[release_reason].keys():
                    assert should_include
                else:
                    assert not should_include

    def test_coverage_of_should_include_map(self):
        release_reason_keys = SHOULD_INCLUDE_WITH_RETURN_TYPE.keys()

        for release_reason in ReleaseReason:
            self.assertTrue(release_reason in release_reason_keys,
                            "StateIncarcerationPeriodReleaseReason enum not "
                            "handled in SHOULD_INCLUDE_WITH_RETURN_TYPE.")

    def test_should_include_in_release_cohort_coverage(self):
        for admission_reason in AdmissionReason:
            for release_reason in ReleaseReason:
                # Assert that no error is raised
                identifier.should_include_in_release_cohort(
                    release_reason,
                    admission_reason)


class TestGetReturnType:
    """Tests the get_return_type function."""

    def test_get_return_type(self):
        """Tests the get_return_type function for all possible admission
        reasons."""
        with pytest.raises(ValueError) as e:

            for admission_reason in AdmissionReason:
                return_type = identifier.get_return_type(admission_reason)

                if admission_reason in (
                        AdmissionReason.RETURN_FROM_ESCAPE,
                        AdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE,
                        AdmissionReason.TRANSFER):
                    assert str(e) == (f"should_include_in_release_cohort is not"
                                      f" effectively filtering. "
                                      f"Found unexpected admission_reason of:"
                                      f" {admission_reason}")
                elif admission_reason in (AdmissionReason.ADMITTED_IN_ERROR,
                                          AdmissionReason.EXTERNAL_UNKNOWN,
                                          AdmissionReason.NEW_ADMISSION):
                    assert return_type == \
                        ReincarcerationReturnType.NEW_ADMISSION
                elif admission_reason in (AdmissionReason.PAROLE_REVOCATION,
                                          AdmissionReason.PROBATION_REVOCATION):
                    assert return_type == ReincarcerationReturnType.REVOCATION
                else:
                    # StateIncarcerationPeriodAdmissionReason enum type not
                    # handled in get_return_type
                    assert False

    def test_get_return_type_valid_combinations(self):
        """Tests the get_return_type function for all valid combinations of
        release reason and admission reason."""
        for release_reason in ReleaseReason:
            for valid_admission_reason in \
                    SHOULD_INCLUDE_WITH_RETURN_TYPE[release_reason]:
                return_type = \
                    identifier.get_return_type(valid_admission_reason)

                assert return_type == \
                    SHOULD_INCLUDE_WITH_RETURN_TYPE[release_reason][
                        valid_admission_reason]

    def test_get_return_type_invalid(self):
        """Tests the get_return_type function with an invalid admission
        reason."""
        with pytest.raises(ValueError) as e:

            _ = identifier.get_return_type('INVALID')

        assert str(e.value) == (f"Enum case not handled for "
                                f"StateIncarcerationPeriodAdmissionReason of"
                                f" type: INVALID.")


class TestForLastIncarcerationPeriod:
    """Tests the for_last_incarceration_period function."""

    def test_for_last_incarceration_period(self):
        state_code = 'CA'
        admission_date = date(2000, 12, 1)
        status = StateIncarcerationPeriodStatus.NOT_IN_CUSTODY
        release_date = date(2005, 3, 8)
        release_facility = 'Facility'

        for release_reason in ReleaseReason:
            # Will raise ValueError if enum case isn't handled
            event = identifier.for_last_incarceration_period(state_code,
                                                             admission_date,
                                                             status,
                                                             release_date,
                                                             release_reason,
                                                             release_facility)

            if release_reason in [ReleaseReason.COMMUTED,
                                  ReleaseReason.CONDITIONAL_RELEASE,
                                  ReleaseReason.EXTERNAL_UNKNOWN,
                                  ReleaseReason.SENTENCE_SERVED]:
                assert event == NonRecidivismReleaseEvent(state_code,
                                                          admission_date,
                                                          release_date,
                                                          release_facility)
            else:
                assert not event


class TestGetFromSupervisionType:
    """Tests the get_from_supervision_type function."""

    def test_get_from_supervision_type(self):
        """Tests the get_from_supervision_type function for all possible
        admission reasons."""
        with pytest.raises(ValueError) as e:
            for admission_reason in AdmissionReason:
                from_supervision_type = \
                    identifier.get_from_supervision_type(admission_reason)

                if admission_reason in \
                        [AdmissionReason.RETURN_FROM_ESCAPE,
                         AdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE,
                         AdmissionReason.TRANSFER]:
                    assert str(e) == (f"should_include_in_release_cohort is not"
                                      f" effectively filtering. "
                                      f"Found unexpected admission_reason of:"
                                      f" {admission_reason}")
                elif admission_reason in [AdmissionReason.ADMITTED_IN_ERROR,
                                          AdmissionReason.EXTERNAL_UNKNOWN,
                                          AdmissionReason.NEW_ADMISSION]:
                    assert not from_supervision_type
                elif admission_reason in [AdmissionReason.PAROLE_REVOCATION,
                                          AdmissionReason.PROBATION_REVOCATION]:
                    assert from_supervision_type
                else:
                    assert str(e.value) == (f"Enum case not handled for "
                                            f"StateIncarcerationPeriodAdmission"
                                            f"Reason of type: INVALID.")

    def test_get_from_supervision_type_invalid(self):
        """Tests the get_from_supervision_type function for an invalid
        admission reason."""
        with pytest.raises(ValueError) as e:

            _ = identifier.get_from_supervision_type('INVALID')

        assert str(e.value) == (f"Enum case not handled for "
                                f"StateIncarcerationPeriodAdmissionReason of"
                                f" type: INVALID.")
