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
from typing import Dict, List

from recidiviz.calculator.pipeline.recidivism import identifier
from recidiviz.calculator.pipeline.recidivism.release_event import \
    RecidivismReleaseEvent, NonRecidivismReleaseEvent, ReincarcerationReturnType
from recidiviz.calculator.pipeline.recidivism.metrics import \
    StateSupervisionPeriodSupervisionType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason as AdmissionReason
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodReleaseReason as ReleaseReason
from recidiviz.persistence.entity.state.entities import \
    StateIncarcerationPeriod


_COUNTY_OF_RESIDENCE = 'county'


class TestClassifyReleaseEvents(unittest.TestCase):
    """Tests for the find_release_events_by_cohort_year function."""

    def testFindReleaseEventsByCohortYear_usNd_ignoreTemporaryCustody(self):
        """Tests the find_release_events_by_cohort_year function for US_ND where temporary custody periods are
        completely ignored. In this test case the person did recidivate.
        """

        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id='1',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        temporary_custody_reincarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id='2',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2011, 4, 5),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2014, 4, 14),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY)

        revocation_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            external_id='3',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2020, 4, 14),
            admission_reason=AdmissionReason.PROBATION_REVOCATION)

        temporary_custody_reincarceration_standalone = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=4444,
            external_id='4',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2019, 4, 5),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2019, 4, 14),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY)

        incarceration_periods = [
            initial_incarceration_period,
            temporary_custody_reincarceration,
            revocation_incarceration_period,
            temporary_custody_reincarceration_standalone]

        release_events_by_cohort = identifier.find_release_events_by_cohort_year(
            incarceration_periods, _COUNTY_OF_RESIDENCE)

        self.assertEqual(1, len(release_events_by_cohort))

        self.assertCountEqual(
            [RecidivismReleaseEvent(
                state_code='US_ND',
                original_admission_date=initial_incarceration_period.admission_date,
                release_date=initial_incarceration_period.release_date,
                release_facility=None,
                reincarceration_date=revocation_incarceration_period.admission_date,
                reincarceration_facility=None,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                from_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                return_type=ReincarcerationReturnType.REVOCATION)],
            release_events_by_cohort[2010])

    def testFindReleaseEventsByCohortYear_collapseTemporaryCustodyAndRevocation(self):
        """Tests the find_release_events_by_cohort_year function where a temporary custody incarceration period
        is followed by a revocation period. In this test case the person did recidivate.
        """

        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id='1',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        temporary_custody_reincarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id='2',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            admission_date=date(2011, 4, 5),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2014, 4, 14),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY)

        revocation_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            external_id='3',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='TX',
            admission_date=date(2014, 4, 14),
            admission_reason=AdmissionReason.PROBATION_REVOCATION)

        incarceration_periods = [
            initial_incarceration_period,
            temporary_custody_reincarceration,
            revocation_incarceration_period]

        release_events_by_cohort = identifier.find_release_events_by_cohort_year(
            incarceration_periods, _COUNTY_OF_RESIDENCE)

        self.assertEqual(1, len(release_events_by_cohort))

        self.assertCountEqual(
            [RecidivismReleaseEvent(
                state_code='TX',
                original_admission_date=initial_incarceration_period.admission_date,
                release_date=initial_incarceration_period.release_date,
                release_facility=None,
                reincarceration_date=temporary_custody_reincarceration.admission_date,
                reincarceration_facility=None,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                from_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                return_type=ReincarcerationReturnType.REVOCATION)],
            release_events_by_cohort[2010])

    def test_find_release_events_by_cohort_year(self):
        """Tests the find_release_events_by_cohort_year function path where the
        person did recidivate."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 4, 5),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2014, 4, 14),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        subsequent_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='TX',
                admission_date=date(2017, 1, 4),
                admission_reason=AdmissionReason.NEW_ADMISSION)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period,
                                 subsequent_reincarceration_period]

        release_events_by_cohort = \
            identifier.find_release_events_by_cohort_year(
                incarceration_periods,
                _COUNTY_OF_RESIDENCE)

        self.assertEqual(2, len(release_events_by_cohort))

        self.assertCountEqual(
            [RecidivismReleaseEvent(
                state_code='TX',
                original_admission_date=initial_incarceration_period.admission_date,
                release_date=initial_incarceration_period.release_date,
                release_facility=None,
                reincarceration_date=first_reincarceration_period.admission_date,
                reincarceration_facility=None,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                return_type=ReincarcerationReturnType.NEW_ADMISSION)],
            release_events_by_cohort[2010])

        self.assertCountEqual(
            [RecidivismReleaseEvent(
                state_code='TX',
                original_admission_date=first_reincarceration_period.admission_date,
                release_date=first_reincarceration_period.release_date,
                release_facility=None,
                reincarceration_date=subsequent_reincarceration_period.admission_date,
                reincarceration_facility=None,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                return_type=ReincarcerationReturnType.NEW_ADMISSION)]
            ,
            release_events_by_cohort[2014]
        )

    def test_find_release_events_by_cohort_year_no_incarcerations_at_all(self):
        """Tests the find_release_events_by_cohort_year function when the person
        has no StateIncarcerationPeriods."""
        release_events_by_cohort = \
            identifier.find_release_events_by_cohort_year(
                [], _COUNTY_OF_RESIDENCE)

        assert not release_events_by_cohort

    def test_find_release_events_by_cohort_year_no_recidivism_after_first(self):
        """Tests the find_release_events_by_cohort_year function when the person
        does not have any StateIncarcerationPeriods after their first."""
        only_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code='TX',
                admission_date=date(2000, 1, 9),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2003, 12, 8),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        release_events_by_cohort = \
            identifier.find_release_events_by_cohort_year(
                [only_incarceration_period],
                _COUNTY_OF_RESIDENCE)

        assert len(release_events_by_cohort) == 1

        assert release_events_by_cohort[2003] == [
            NonRecidivismReleaseEvent(
                state_code='TX',
                original_admission_date=only_incarceration_period.
                admission_date,
                release_date=only_incarceration_period.release_date,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_facility=None)]

    def test_find_release_events_by_cohort_year_still_incarcerated(self):
        """Tests the find_release_events_by_cohort_year function where the
        person is still incarcerated on their very first
         StateIncarcerationPeriod."""
        only_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION)

        release_events_by_cohort = \
            identifier.find_release_events_by_cohort_year(
                [only_incarceration_period],
                _COUNTY_OF_RESIDENCE)

        assert not release_events_by_cohort

    def test_find_release_events_by_cohort_year_invalid_open_period(self):
        """Tests the find_release_events_by_cohort_year function where the person has an open IN_CUSTODY period that is
        invalid because the person was released elsewhere after the admission to the period."""
        invalid_open_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='TX',
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION)

        closed_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 3, 14),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        release_events_by_cohort = \
            identifier.find_release_events_by_cohort_year(
                [invalid_open_incarceration_period, closed_incarceration_period],
                _COUNTY_OF_RESIDENCE)

        assert release_events_by_cohort[2009] == [
            NonRecidivismReleaseEvent(
                state_code='TX',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                original_admission_date=closed_incarceration_period.admission_date,
                release_date=closed_incarceration_period.release_date,
                release_facility=None)]

    def test_find_release_events_by_cohort_year_no_recid_cond_release(self):
        """Tests the find_release_events_by_cohort_year function when the person
        does not have any StateIncarcerationPeriods after their first, and they
        were released conditionally."""
        only_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2000, 1, 9),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2003, 12, 8),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE)

        release_events_by_cohort = \
            identifier.find_release_events_by_cohort_year(
                [only_incarceration_period], _COUNTY_OF_RESIDENCE)

        assert len(release_events_by_cohort) == 1

        assert release_events_by_cohort[2003] == [
            NonRecidivismReleaseEvent(
                state_code='TX',
                county_of_residence=_COUNTY_OF_RESIDENCE,
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
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
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
                incarceration_periods=incarceration_periods,
                county_of_residence=_COUNTY_OF_RESIDENCE)

        assert len(release_events_by_cohort) == 2

        assert release_events_by_cohort[2010] == [RecidivismReleaseEvent(
            state_code='TX',
            original_admission_date=initial_incarceration_period.admission_date,
            release_date=initial_incarceration_period.release_date,
            release_facility=None,
            county_of_residence=_COUNTY_OF_RESIDENCE,
            reincarceration_date=first_reincarceration_period.admission_date,
            reincarceration_facility=None,
            return_type=ReincarcerationReturnType.REVOCATION,
            from_supervision_type=StateSupervisionPeriodSupervisionType.
            PAROLE)]

        assert release_events_by_cohort[2014] == [
            NonRecidivismReleaseEvent(
                state_code='TX',
                original_admission_date=first_reincarceration_period.
                admission_date,
                release_date=first_reincarceration_period.release_date,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_facility=None)]

    def test_find_release_events_by_cohort_year_probation_revocation(self):
        """Tests the find_release_events_by_cohort_year function path where the
        person was conditionally released and returned for a probation
        revocation."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
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
                incarceration_periods=incarceration_periods,
                county_of_residence=_COUNTY_OF_RESIDENCE)

        assert len(release_events_by_cohort) == 2

        assert release_events_by_cohort[2010] == [RecidivismReleaseEvent(
            state_code='TX',
            original_admission_date=initial_incarceration_period.admission_date,
            release_date=initial_incarceration_period.release_date,
            release_facility=None,
            reincarceration_date=first_reincarceration_period.admission_date,
            reincarceration_facility=None,
            county_of_residence=_COUNTY_OF_RESIDENCE,
            return_type=ReincarcerationReturnType.REVOCATION,
            from_supervision_type=StateSupervisionPeriodSupervisionType.
            PROBATION)]

        assert release_events_by_cohort[2014] == [
            NonRecidivismReleaseEvent(
                state_code='TX',
                original_admission_date=first_reincarceration_period.
                admission_date,
                release_date=first_reincarceration_period.release_date,
                release_facility=None,
                county_of_residence=_COUNTY_OF_RESIDENCE)]

    def test_find_release_events_by_cohort_year_cond_release_new_admit(self):
        """Tests the find_release_events_by_cohort_year function path where the
        person was conditionally released on parole but returned as a new
         admission."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
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
                incarceration_periods=incarceration_periods,
                county_of_residence=_COUNTY_OF_RESIDENCE)

        assert len(release_events_by_cohort) == 2

        assert release_events_by_cohort[2010] == [RecidivismReleaseEvent(
            state_code='TX',
            original_admission_date=initial_incarceration_period.admission_date,
            release_date=initial_incarceration_period.release_date,
            release_facility=None,
            reincarceration_date=first_reincarceration_period.admission_date,
            reincarceration_facility=None,
            county_of_residence=_COUNTY_OF_RESIDENCE,
            return_type=ReincarcerationReturnType.NEW_ADMISSION)]

        assert release_events_by_cohort[2014] == [
            NonRecidivismReleaseEvent(
                state_code='TX',
                original_admission_date=first_reincarceration_period.
                admission_date,
                release_date=first_reincarceration_period.release_date,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_facility=None)]

    def test_find_release_events_by_cohort_year_sentence_served_prob_rev(self):
        """Tests the find_release_events_by_cohort_year function path where the
        person served their first sentence, then later returned on a probation
        revocation."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
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
                incarceration_periods=incarceration_periods,
                county_of_residence=_COUNTY_OF_RESIDENCE)

        assert len(release_events_by_cohort) == 2

        assert release_events_by_cohort[2010] == [RecidivismReleaseEvent(
            state_code='TX',
            original_admission_date=initial_incarceration_period.admission_date,
            release_date=initial_incarceration_period.release_date,
            release_facility=None,
            reincarceration_date=first_reincarceration_period.admission_date,
            reincarceration_facility=None,
            county_of_residence=_COUNTY_OF_RESIDENCE,
            return_type=ReincarcerationReturnType.REVOCATION,
            from_supervision_type=StateSupervisionPeriodSupervisionType.
            PROBATION)]

        assert release_events_by_cohort[2014] == [
            NonRecidivismReleaseEvent(
                state_code='TX',
                original_admission_date=first_reincarceration_period.
                admission_date,
                release_date=first_reincarceration_period.release_date,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_facility=None)]

    def test_find_release_events_by_cohort_year_transfer_no_recidivism(self):
        """Tests the find_release_events_by_cohort_year function path where the
        person was transferred between two incarceration periods."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
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
                incarceration_periods=incarceration_periods,
                county_of_residence=_COUNTY_OF_RESIDENCE)

        assert len(release_events_by_cohort) == 1

        assert release_events_by_cohort[2014] == [
            NonRecidivismReleaseEvent(
                state_code='TX',
                original_admission_date=initial_incarceration_period.
                admission_date,
                release_date=first_reincarceration_period.release_date,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_facility=None)]

    def test_find_release_events_by_cohort_year_transfer_out_but_recid(self):
        """Tests the find_release_events_by_cohort_year function path where the
        person was transferred out of state, then later returned on a new
        admission."""
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.TRANSFER)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
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
                incarceration_periods=incarceration_periods,
                county_of_residence=_COUNTY_OF_RESIDENCE)

        assert len(release_events_by_cohort) == 1

        assert release_events_by_cohort[2014] == [
            NonRecidivismReleaseEvent(
                state_code='TX',
                original_admission_date=first_reincarceration_period.
                admission_date,
                release_date=first_reincarceration_period.release_date,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_facility=None)]

    def test_find_release_events_by_cohort_year_only_placeholder_periods(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            state_code='US_XX')

        release_events_by_cohort = \
            identifier.find_release_events_by_cohort_year(
                incarceration_periods=[incarceration_period],
                county_of_residence=_COUNTY_OF_RESIDENCE)

        assert len(release_events_by_cohort) == 0


_RETURN_TYPES_BY_STANDARD_ADMISSION: Dict[
    AdmissionReason, ReincarcerationReturnType] = {
        AdmissionReason.ADMITTED_IN_ERROR: ReincarcerationReturnType.NEW_ADMISSION,
        AdmissionReason.EXTERNAL_UNKNOWN: ReincarcerationReturnType.NEW_ADMISSION,
        AdmissionReason.INTERNAL_UNKNOWN: ReincarcerationReturnType.NEW_ADMISSION,
        AdmissionReason.NEW_ADMISSION: ReincarcerationReturnType.NEW_ADMISSION,
        AdmissionReason.RETURN_FROM_SUPERVISION: ReincarcerationReturnType.REVOCATION,
        AdmissionReason.PAROLE_REVOCATION: ReincarcerationReturnType.REVOCATION,
        AdmissionReason.PROBATION_REVOCATION: ReincarcerationReturnType.REVOCATION,
        AdmissionReason.DUAL_REVOCATION: ReincarcerationReturnType.REVOCATION,
        AdmissionReason.TRANSFER: ReincarcerationReturnType.NEW_ADMISSION,
        AdmissionReason.TRANSFERRED_FROM_OUT_OF_STATE: ReincarcerationReturnType.NEW_ADMISSION
    }


_SHOULD_BE_FILTERED_OUT_IN_VALIDATION_ADMISSION: List[AdmissionReason] = \
    [AdmissionReason.TEMPORARY_CUSTODY]

_SHOULD_BE_FILTERED_OUT_IN_VALIDATION_RELEASE: List[ReleaseReason] = \
    [ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY]


# Stores the return types for the combinations of release reason and
# admission reason that should be included in a release cohort
SHOULD_INCLUDE_WITH_RETURN_TYPE: \
    Dict[ReleaseReason, Dict[AdmissionReason, ReincarcerationReturnType]] = \
    {ReleaseReason.COMMUTED: _RETURN_TYPES_BY_STANDARD_ADMISSION,
     ReleaseReason.COMPASSIONATE: _RETURN_TYPES_BY_STANDARD_ADMISSION,
     ReleaseReason.CONDITIONAL_RELEASE: _RETURN_TYPES_BY_STANDARD_ADMISSION,
     ReleaseReason.COURT_ORDER: {},
     ReleaseReason.DEATH: {},
     ReleaseReason.ESCAPE: {},
     ReleaseReason.EXECUTION: {},
     ReleaseReason.EXTERNAL_UNKNOWN: _RETURN_TYPES_BY_STANDARD_ADMISSION,
     ReleaseReason.INTERNAL_UNKNOWN: _RETURN_TYPES_BY_STANDARD_ADMISSION,
     ReleaseReason.PARDONED: _RETURN_TYPES_BY_STANDARD_ADMISSION,
     ReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION: _RETURN_TYPES_BY_STANDARD_ADMISSION,
     ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY: {},
     ReleaseReason.RELEASED_IN_ERROR: {},
     ReleaseReason.SENTENCE_SERVED: _RETURN_TYPES_BY_STANDARD_ADMISSION,
     ReleaseReason.TRANSFER: {},
     ReleaseReason.TRANSFERRED_OUT_OF_STATE: {},
     ReleaseReason.VACATED: _RETURN_TYPES_BY_STANDARD_ADMISSION}


class TestShouldIncludeInReleaseCohort(unittest.TestCase):
    """Tests the should_include_in_release_cohort function."""

    def test_should_include_in_release_cohort(self):
        """Tests the should_include_in_release_cohort_function for all
        possible combinations of release reason and admission reason."""
        for release_reason in ReleaseReason:
            for admission_reason in AdmissionReason:
                if release_reason in _SHOULD_BE_FILTERED_OUT_IN_VALIDATION_RELEASE:
                    with pytest.raises(ValueError) as e:
                        _ = identifier.should_include_in_release_cohort(release_reason, admission_reason)
                        assert str(e) == ("validate_sort_and_collapse_incarceration_periods is not effectively "
                                          "filtering. Found unexpected release_reason of: {release_reason}")

                elif admission_reason in _SHOULD_BE_FILTERED_OUT_IN_VALIDATION_ADMISSION \
                        and SHOULD_INCLUDE_WITH_RETURN_TYPE[release_reason].keys():
                    with pytest.raises(ValueError) as e:
                        _ = identifier.should_include_in_release_cohort(release_reason, admission_reason)
                        assert str(e) == ("validate_sort_and_collapse_incarceration_periods is not effectively "
                                          "filtering. Found unexpected admission_reason of: {admission_reason}")
                else:
                    should_include = identifier.should_include_in_release_cohort(release_reason, admission_reason)
                    if admission_reason in SHOULD_INCLUDE_WITH_RETURN_TYPE[release_reason].keys():
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
            if admission_reason in \
                    _SHOULD_BE_FILTERED_OUT_IN_VALIDATION_ADMISSION:
                continue

            for release_reason in ReleaseReason:
                if release_reason in \
                        _SHOULD_BE_FILTERED_OUT_IN_VALIDATION_RELEASE:
                    continue

                # Assert that no error is raised
                identifier.should_include_in_release_cohort(
                    release_reason,
                    admission_reason)


class TestGetReturnType(unittest.TestCase):
    """Tests the get_return_type function."""

    def test_get_return_type(self):
        """Tests the get_return_type function for all possible admission
        reasons."""

        for admission_reason in AdmissionReason:
            if admission_reason in (
                    AdmissionReason.RETURN_FROM_ESCAPE,
                    AdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE):
                with pytest.raises(ValueError) as e:
                    _ = identifier.get_return_type(admission_reason)
                    assert str(e) == (f"should_include_in_release_cohort is not"
                                      f" effectively filtering. "
                                      f"Found unexpected admission_reason of:"
                                      f" {admission_reason}")
            elif admission_reason in \
                    _SHOULD_BE_FILTERED_OUT_IN_VALIDATION_ADMISSION:
                with pytest.raises(ValueError) as e:
                    _ = identifier.get_return_type(admission_reason)
                    assert str(e) == ("validate_sort_and_collapse_"
                                      "incarceration_periods is "
                                      "not effectively filtering."
                                      " Found unexpected admission_reason"
                                      f" of: {admission_reason}")
            else:
                return_type = identifier.get_return_type(admission_reason)
                if admission_reason in (AdmissionReason.ADMITTED_IN_ERROR,
                                        AdmissionReason.EXTERNAL_UNKNOWN,
                                        AdmissionReason.INTERNAL_UNKNOWN,
                                        AdmissionReason.NEW_ADMISSION,
                                        AdmissionReason.TRANSFER,
                                        AdmissionReason.TRANSFERRED_FROM_OUT_OF_STATE):
                    assert return_type == \
                        ReincarcerationReturnType.NEW_ADMISSION
                elif admission_reason in (
                        AdmissionReason.RETURN_FROM_SUPERVISION,
                        AdmissionReason.PAROLE_REVOCATION,
                        AdmissionReason.PROBATION_REVOCATION,
                        AdmissionReason.DUAL_REVOCATION):
                    assert return_type == ReincarcerationReturnType.REVOCATION
                else:
                    # StateIncarcerationPeriodAdmissionReason enum type not
                    # handled in get_return_type
                    self.fail()

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
        """Tests the get_return_type function with an invalid admission reason."""
        with pytest.raises(ValueError):
            _ = identifier.get_return_type('INVALID')


class TestForLastIncarcerationPeriod(unittest.TestCase):
    """Tests the for_last_incarceration_period function."""

    def test_for_last_incarceration_period(self):
        state_code = 'CA'
        admission_date = date(2000, 12, 1)
        status = StateIncarcerationPeriodStatus.NOT_IN_CUSTODY
        release_date = date(2005, 3, 8)
        release_facility = 'Facility'

        for release_reason in ReleaseReason:
            if release_reason in _SHOULD_BE_FILTERED_OUT_IN_VALIDATION_RELEASE:
                with pytest.raises(ValueError) as e:
                    _ = identifier.for_last_incarceration_period(
                        state_code,
                        admission_date,
                        status,
                        release_date,
                        release_reason,
                        release_facility,
                        _COUNTY_OF_RESIDENCE)
                    assert str(e) == ("validate_sort_and_collapse_"
                                      "incarceration_periods is "
                                      "not effectively filtering."
                                      " Found unexpected release_reason"
                                      f" of: {release_reason}")
            else:
                # Will raise ValueError if enum case isn't handled
                event = identifier.for_last_incarceration_period(
                    state_code,
                    admission_date,
                    status,
                    release_date,
                    release_reason,
                    release_facility,
                    _COUNTY_OF_RESIDENCE)

                if release_reason in [ReleaseReason.COMMUTED,
                                      ReleaseReason.COMPASSIONATE,
                                      ReleaseReason.CONDITIONAL_RELEASE,
                                      ReleaseReason.INTERNAL_UNKNOWN,
                                      ReleaseReason.EXTERNAL_UNKNOWN,
                                      ReleaseReason.PARDONED,
                                      ReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
                                      ReleaseReason.SENTENCE_SERVED,
                                      ReleaseReason.VACATED]:
                    assert event == NonRecidivismReleaseEvent(
                        state_code,
                        admission_date,
                        release_date,
                        release_facility,
                        _COUNTY_OF_RESIDENCE)
                else:
                    assert not event


class TestGetFromSupervisionType(unittest.TestCase):
    """Tests the get_from_supervision_type function."""

    def test_get_from_supervision_type(self):
        """Tests the get_from_supervision_type function for all possible
        admission reasons."""
        for admission_reason in AdmissionReason:
            if admission_reason in \
                    [AdmissionReason.RETURN_FROM_ESCAPE,
                     AdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE]:
                with pytest.raises(ValueError) as e:
                    _ = identifier.get_from_supervision_type(admission_reason)
                    assert str(e) == (f"should_include_in_release_cohort is not"
                                      f" effectively filtering. "
                                      f"Found unexpected admission_reason of:"
                                      f" {admission_reason}")
            elif admission_reason in \
                    _SHOULD_BE_FILTERED_OUT_IN_VALIDATION_ADMISSION:
                with pytest.raises(ValueError) as e:
                    _ = identifier.get_from_supervision_type(admission_reason)
                    assert str(e) == ("validate_sort_and_collapse_"
                                      "incarceration_periods is "
                                      "not effectively filtering."
                                      " Found unexpected admission_reason"
                                      f" of: {admission_reason}")
            else:
                from_supervision_type = \
                    identifier.get_from_supervision_type(admission_reason)
                if admission_reason in [AdmissionReason.ADMITTED_IN_ERROR,
                                        AdmissionReason.EXTERNAL_UNKNOWN,
                                        AdmissionReason.INTERNAL_UNKNOWN,
                                        AdmissionReason.NEW_ADMISSION,
                                        AdmissionReason.TRANSFER,
                                        AdmissionReason.TRANSFERRED_FROM_OUT_OF_STATE]:
                    assert not from_supervision_type
                elif admission_reason in [AdmissionReason.RETURN_FROM_SUPERVISION,
                                          AdmissionReason.PAROLE_REVOCATION,
                                          AdmissionReason.PROBATION_REVOCATION,
                                          AdmissionReason.DUAL_REVOCATION]:
                    assert from_supervision_type
                else:
                    assert str(e.value) == (
                        "Enum case not handled for StateIncarcerationPeriodAdmissionReason of type: INVALID.")

    def test_get_from_supervision_type_invalid(self):
        """Tests the get_from_supervision_type function for an invalid
        admission reason."""
        with pytest.raises(ValueError) as e:

            _ = identifier.get_from_supervision_type('INVALID')

        assert str(e.value) == ("Enum case not handled for StateIncarcerationPeriodAdmissionReason of type: INVALID.")
