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
# pylint: disable=unused-import,wrong-import-order,protected-access

"""Tests for incarceration/identifier.py."""

from datetime import date

import unittest
from typing import List

from dateutil.relativedelta import relativedelta
from freezegun import freeze_time

from recidiviz.calculator.pipeline.incarceration import identifier
from recidiviz.calculator.pipeline.incarceration.incarceration_event import \
    IncarcerationAdmissionEvent, IncarcerationReleaseEvent, \
    IncarcerationStayEvent
from recidiviz.calculator.pipeline.utils.calculator_utils import \
    last_day_of_month
from recidiviz.common.constants.state.state_incarceration import \
    StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason as AdmissionReason, \
    StateIncarcerationPeriodReleaseReason as ReleaseReason
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod

_COUNTY_OF_RESIDENCE = 'county'


class TestFindIncarcerationEvents(unittest.TestCase):
    """Tests the find_incarceration_events function."""

    def test_find_incarceration_events(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2009, 1, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [incarceration_period]

        incarceration_events = identifier.find_incarceration_events(
            incarceration_periods, _COUNTY_OF_RESIDENCE)

        self.assertEqual(4, len(incarceration_events))

        self.assertEqual([
            IncarcerationStayEvent(
                state_code=incarceration_period.state_code,
                event_date=last_day_of_month(
                    incarceration_period.admission_date),
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
            ),
            IncarcerationStayEvent(
                state_code=incarceration_period.state_code,
                event_date=last_day_of_month(
                    incarceration_period.admission_date +
                    relativedelta(months=1)),
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
            ),
            IncarcerationAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=AdmissionReason.NEW_ADMISSION
            ),
            IncarcerationReleaseEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.release_date,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_reason=ReleaseReason.SENTENCE_SERVED
            )
        ], incarceration_events)

    def test_find_incarceration_events_transfer(self):
        incarceration_period_1 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2009, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2009, 12, 1),
                release_reason=ReleaseReason.TRANSFER)

        incarceration_period_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON 10',
                admission_date=date(2009, 12, 1),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2010, 2, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [incarceration_period_1,
                                 incarceration_period_2]

        incarceration_events = identifier.find_incarceration_events(
            incarceration_periods, _COUNTY_OF_RESIDENCE)

        self.assertEqual(5, len(incarceration_events))

        self.assertEqual([
            IncarcerationStayEvent(
                state_code=incarceration_period_1.state_code,
                event_date=last_day_of_month(
                    incarceration_period_1.admission_date),
                facility=incarceration_period_1.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
            ),
            IncarcerationStayEvent(
                state_code=incarceration_period_2.state_code,
                event_date=last_day_of_month(
                    incarceration_period_1.admission_date +
                    relativedelta(months=1)),
                facility=incarceration_period_2.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
            ),
            IncarcerationStayEvent(
                state_code=incarceration_period_2.state_code,
                event_date=last_day_of_month(
                    incarceration_period_1.admission_date +
                    relativedelta(months=2)),
                facility=incarceration_period_2.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
            ),
            IncarcerationAdmissionEvent(
                state_code=incarceration_period_1.state_code,
                event_date=incarceration_period_1.admission_date,
                facility=incarceration_period_2.facility,
                admission_reason=AdmissionReason.NEW_ADMISSION,
                county_of_residence=_COUNTY_OF_RESIDENCE,
            ),
            IncarcerationReleaseEvent(
                state_code=incarceration_period_2.state_code,
                event_date=incarceration_period_2.release_date,
                facility=incarceration_period_2.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_reason=ReleaseReason.SENTENCE_SERVED
            )
        ], incarceration_events)


class TestFindEndOfMonthStatePrisonStays(unittest.TestCase):
    """Tests the find_end_of_month_state_prison_stays function."""
    def test_find_end_of_month_state_prison_stays(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2000, 1, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 1),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_events = \
            identifier.find_end_of_month_state_prison_stays(
                incarceration_period, _COUNTY_OF_RESIDENCE
            )

        expected_month_count = 131

        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period, expected_month_count
        )

        self.assertEqual(expected_month_count, len(incarceration_events))
        self.assertEqual(expected_incarceration_events, incarceration_events)

    @freeze_time('2019-11-01')
    def test_find_end_of_month_state_prison_stays_no_release(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2018, 1, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION)

        incarceration_events = \
            identifier.find_end_of_month_state_prison_stays(
                incarceration_period, _COUNTY_OF_RESIDENCE
            )

        expected_month_count = 22
        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period, expected_month_count
        )

        self.assertEqual(expected_month_count, len(incarceration_events))
        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_end_of_month_state_prison_stays_no_admission(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3')

        incarceration_events = \
            identifier.find_end_of_month_state_prison_stays(
                incarceration_period, _COUNTY_OF_RESIDENCE
            )

        self.assertEqual(0, len(incarceration_events))
        self.assertEqual([], incarceration_events)

    def test_find_end_of_month_state_prison_stays_admitted_end_of_month(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2000, 1, 31),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2000, 2, 13),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_events = \
            identifier.find_end_of_month_state_prison_stays(
                incarceration_period, _COUNTY_OF_RESIDENCE
            )

        expected_month_count = 1
        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period, expected_month_count
        )

        self.assertEqual(expected_month_count, len(incarceration_events))
        self.assertEqual(expected_incarceration_events, incarceration_events)

    @freeze_time('2019-12-02')
    def test_find_end_of_month_state_prison_stays_still_in_custody(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2019, 11, 30),
                admission_reason=AdmissionReason.NEW_ADMISSION)

        incarceration_events = \
            identifier.find_end_of_month_state_prison_stays(
                incarceration_period, _COUNTY_OF_RESIDENCE
            )

        expected_month_count = 1
        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period, expected_month_count
        )

        self.assertEqual(expected_month_count, len(incarceration_events))
        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_end_of_month_state_prison_stays_released_end_of_month(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2019, 11, 29),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2019, 11, 30),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_events = \
            identifier.find_end_of_month_state_prison_stays(
                incarceration_period, _COUNTY_OF_RESIDENCE
            )

        expected_month_count = 1
        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period, expected_month_count
        )

        self.assertEqual(expected_month_count, len(incarceration_events))
        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_end_of_month_state_prison_stays_released_first_of_month(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2019, 11, 15),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2019, 12, 1),
                release_reason=ReleaseReason.TRANSFER)

        incarceration_events = \
            identifier.find_end_of_month_state_prison_stays(
                incarceration_period, _COUNTY_OF_RESIDENCE
            )

        expected_month_count = 1
        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period, expected_month_count
        )

        self.assertEqual(expected_month_count, len(incarceration_events))
        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_end_of_month_state_prison_stays_only_one_day(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2019, 7, 31),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2019, 7, 31),
                release_reason=ReleaseReason.TRANSFER)

        incarceration_events = \
            identifier.find_end_of_month_state_prison_stays(
                incarceration_period, _COUNTY_OF_RESIDENCE
            )

        expected_month_count = 1
        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period, expected_month_count
        )

        self.assertEqual(expected_month_count, len(incarceration_events))
        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_end_of_month_state_prison_stays_county_jail(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.COUNTY_JAIL,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2000, 1, 31),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2000, 2, 13),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_events = \
            identifier.find_end_of_month_state_prison_stays(
                incarceration_period, _COUNTY_OF_RESIDENCE
            )

        expected_month_count = 0
        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period, expected_month_count
        )

        self.assertEqual(expected_month_count, len(incarceration_events))
        self.assertEqual(expected_incarceration_events, incarceration_events)


class TestDeDuplicatedAdmissions(unittest.TestCase):
    """Tests the de_duplicated_admissions function."""

    def test_de_duplicated_admissions(self):
        incarceration_period_1 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2008, 11, 20),
                release_reason=ReleaseReason.TRANSFER)

        incarceration_period_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [incarceration_period_1,
                                 incarceration_period_2]

        de_duplicated_incarceration_admissions = \
            identifier.de_duplicated_admissions(
                incarceration_periods
            )

        self.assertEqual([incarceration_period_1],
                         de_duplicated_incarceration_admissions)

    def test_de_duplicated_admissions_different_reason(self):
        incarceration_period_1 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2008, 11, 20),
                release_reason=ReleaseReason.TRANSFER)

        incarceration_period_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.PAROLE_REVOCATION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [incarceration_period_1,
                                 incarceration_period_2]

        de_duplicated_incarceration_admissions = \
            identifier.de_duplicated_admissions(
                incarceration_periods
            )

        self.assertEqual(incarceration_periods,
                         de_duplicated_incarceration_admissions)


class TestDeDuplicatedReleases(unittest.TestCase):
    """Tests the de_duplicated_releases function."""

    def test_de_duplicated_releases(self):
        incarceration_period_1 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2008, 11, 19),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_period_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_periods = [incarceration_period_1,
                                 incarceration_period_2]

        de_duplicated_incarceration_releases = \
            identifier.de_duplicated_releases(
                incarceration_periods
            )

        self.assertEqual([incarceration_period_1],
                         de_duplicated_incarceration_releases)

    def test_de_duplicated_releases_different_reason(self):
        incarceration_period_1 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_period_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE)

        incarceration_periods = [incarceration_period_1,
                                 incarceration_period_2]

        de_duplicated_incarceration_releases = \
            identifier.de_duplicated_releases(
                incarceration_periods
            )

        self.assertEqual(incarceration_periods,
                         de_duplicated_incarceration_releases)


class TestAdmissionEventForPeriod(unittest.TestCase):
    """Tests the admission_event_for_period function."""

    def test_admission_event_for_period(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        admission_event = identifier.admission_event_for_period(
            incarceration_period, _COUNTY_OF_RESIDENCE)

        self.assertEqual(IncarcerationAdmissionEvent(
            state_code=incarceration_period.state_code,
            event_date=incarceration_period.admission_date,
            facility='PRISON3',
            county_of_residence=_COUNTY_OF_RESIDENCE,
            admission_reason=incarceration_period.admission_reason
        ), admission_event)

    def test_admission_event_for_period_all_admission_reasons(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2013, 11, 20),
                release_date=date(2019, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        for admission_reason in AdmissionReason:
            incarceration_period.admission_reason = admission_reason

            admission_event = identifier.admission_event_for_period(
                incarceration_period, _COUNTY_OF_RESIDENCE)

            self.assertIsNotNone(admission_event)

    def test_admission_event_for_period_county_jail(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.COUNTY_JAIL,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2013, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2019, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        admission_event = identifier.admission_event_for_period(
            incarceration_period, _COUNTY_OF_RESIDENCE
        )

        self.assertIsNone(admission_event)


class TestReleaseEventForPeriod(unittest.TestCase):
    """Tests the release_event_for_period function."""

    def test_release_event_for_period(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        release_event = identifier.release_event_for_period(
            incarceration_period, _COUNTY_OF_RESIDENCE)

        self.assertEqual(IncarcerationReleaseEvent(
            state_code=incarceration_period.state_code,
            event_date=incarceration_period.release_date,
            facility='PRISON3',
            county_of_residence=_COUNTY_OF_RESIDENCE,
            release_reason=incarceration_period.release_reason
        ), release_event)

    def test_admission_event_for_period_all_release_reasons(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2013, 11, 20),
                release_date=date(2019, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        for release_reason in ReleaseReason:
            incarceration_period.release_reason = release_reason

            release_event = identifier.release_event_for_period(
                incarceration_period, _COUNTY_OF_RESIDENCE)

            self.assertIsNotNone(release_event)

    def test_release_event_for_period_county_jail(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.COUNTY_JAIL,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2013, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2019, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        release_event = identifier.release_event_for_period(
            incarceration_period, _COUNTY_OF_RESIDENCE
        )

        self.assertIsNone(release_event)


def expected_incarceration_stay_events(
        incarceration_period: StateIncarcerationPeriod,
        expected_month_count: int
) -> List[IncarcerationStayEvent]:

    expected_incarceration_events = []
    months_incarcerated_eom_range = range(0, expected_month_count, 1)

    if incarceration_period.admission_date:
        for month in months_incarcerated_eom_range:
            event = IncarcerationStayEvent(
                state_code=incarceration_period.state_code,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                event_date=last_day_of_month(
                    incarceration_period.admission_date +
                    relativedelta(months=month))
            )

            expected_incarceration_events.append(event)

    return expected_incarceration_events
