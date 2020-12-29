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
from typing import List, Optional

import attr
import pytest
from dateutil.relativedelta import relativedelta
from freezegun import freeze_time

from recidiviz.calculator.pipeline.incarceration import identifier
from recidiviz.calculator.pipeline.incarceration.incarceration_event import \
    IncarcerationAdmissionEvent, IncarcerationReleaseEvent, \
    IncarcerationStayEvent, IncarcerationEvent
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_sentence_classification import SupervisionTypeSpan
from recidiviz.common.constants.state.state_incarceration import \
    StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import StateSpecializedPurposeForIncarceration, \
    StateIncarcerationPeriodAdmissionReason, StateIncarcerationPeriodReleaseReason
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod, StateSentenceGroup, \
    StateIncarcerationSentence, StateSupervisionSentence, StateCharge, StateSupervisionPeriod
from recidiviz.tests.calculator.pipeline.utils.us_mo_fakes import FakeUsMoSupervisionSentence, \
    FakeUsMoIncarcerationSentence

_COUNTY_OF_RESIDENCE = 'county'
_COUNTY_OF_RESIDENCE_ROWS = [
    {'state_code': 'US_XX',
     'person_id': 123,
     'county_of_residence': _COUNTY_OF_RESIDENCE}
]

_DEFAULT_IP_ID = 123
_DEFAULT_SP_ID = 456

_DEFAULT_INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION = [
    {'incarceration_period_id': _DEFAULT_IP_ID, 'judicial_district_code': 'NW'}
]


class TestFindIncarcerationEvents(unittest.TestCase):
    """Tests the find_incarceration_events function."""

    def test_find_incarceration_events(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=_DEFAULT_IP_ID,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                admission_reason_raw_text='INCARCERATION_ADMISSION',
                release_date=date(2009, 1, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            start_date=date(2008, 10, 11),
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2007, 12, 11),
                    ncic_code='0901',
                    statute='9999'
                )
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX',
            incarceration_sentences=[incarceration_sentence]
        )

        incarceration_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = identifier.find_incarceration_events(
            sentence_groups, _DEFAULT_INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION, _COUNTY_OF_RESIDENCE_ROWS)

        expected_events: List[IncarcerationEvent] = expected_incarceration_stay_events(
            incarceration_period,
            most_serious_offense_ncic_code='0901',
            most_serious_offense_statute='9999',
            judicial_district_code='NW'
        )

        expected_events.extend(
            [IncarcerationAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                admission_reason_raw_text='INCARCERATION_ADMISSION',
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            ),
             IncarcerationReleaseEvent(
                 state_code=incarceration_period.state_code,
                 event_date=incarceration_period.release_date,
                 facility=incarceration_period.facility,
                 county_of_residence=_COUNTY_OF_RESIDENCE,
                 release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
                 admission_reason=incarceration_period.admission_reason,
                 total_days_incarcerated=(incarceration_period.release_date - incarceration_period.admission_date).days,
                 purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
             )
            ]
        )

        self.assertCountEqual(expected_events, incarceration_events)

    def test_find_incarceration_events_transfer(self):
        incarceration_period_1 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=_DEFAULT_IP_ID,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2009, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                admission_reason_raw_text='NA',
                release_date=date(2009, 12, 1),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)

        incarceration_period_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON 10',
                admission_date=date(2009, 12, 1),
                admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
                release_date=date(2010, 2, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            start_date=date(2008, 1, 11),
            incarceration_periods=[incarceration_period_1, incarceration_period_2],
            charges=[
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2007, 12, 11),
                    ncic_code='5511',
                    statute='9999'
                )
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX',
            incarceration_sentences=[incarceration_sentence]
        )

        incarceration_period_1.incarceration_sentences = [incarceration_sentence]
        incarceration_period_2.incarceration_sentences = [incarceration_sentence]
        incarceration_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = identifier.find_incarceration_events(
            sentence_groups, _DEFAULT_INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION, _COUNTY_OF_RESIDENCE_ROWS)

        expected_events: List[IncarcerationEvent] = expected_incarceration_stay_events(
            incarceration_period_1,
            most_serious_offense_ncic_code='5511',
            most_serious_offense_statute='9999',
            judicial_district_code='NW'
        )

        expected_events.extend(expected_incarceration_stay_events(
            incarceration_period_2,
            original_admission_reason=incarceration_period_1.admission_reason,
            original_admission_reason_raw_text=incarceration_period_1.admission_reason_raw_text,
            most_serious_offense_ncic_code='5511',
            most_serious_offense_statute='9999'
        ))

        expected_events.extend(
            [
                IncarcerationAdmissionEvent(
                    state_code=incarceration_period_1.state_code,
                    event_date=incarceration_period_1.admission_date,
                    facility=incarceration_period_1.facility,
                    admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                    admission_reason_raw_text='NA',
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                ),
                IncarcerationReleaseEvent(
                    state_code=incarceration_period_2.state_code,
                    event_date=incarceration_period_2.release_date,
                    facility=incarceration_period_2.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
                    admission_reason=incarceration_period_1.admission_reason,
                    total_days_incarcerated=(
                        incarceration_period_2.release_date - incarceration_period_1.admission_date).days
                )
            ]
        )

        self.assertCountEqual(expected_events, incarceration_events)

    def test_find_incarceration_events_multiple_sentences(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_DEFAULT_IP_ID,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_XX',
            facility='PRISON3',
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            start_date=date(2008, 10, 11),
            incarceration_periods=[incarceration_period]
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code='US_XX',
            start_date=date(2008, 10, 11),
            incarceration_periods=[incarceration_period]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX',
            incarceration_sentences=[incarceration_sentence],
            supervision_sentences=[supervision_sentence]
        )

        incarceration_sentence.sentence_group = sentence_group
        supervision_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = identifier.find_incarceration_events(
            sentence_groups, _DEFAULT_INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION, _COUNTY_OF_RESIDENCE_ROWS)

        expected_events: List[IncarcerationEvent] = expected_incarceration_stay_events(
            incarceration_period,
            judicial_district_code='NW'
        )

        expected_events.extend(
            [IncarcerationAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION
            ),
             IncarcerationReleaseEvent(
                 state_code=incarceration_period.state_code,
                 event_date=incarceration_period.release_date,
                 facility=incarceration_period.facility,
                 county_of_residence=_COUNTY_OF_RESIDENCE,
                 release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
                 admission_reason=incarceration_period.admission_reason,
                 total_days_incarcerated=(incarceration_period.release_date - incarceration_period.admission_date).days
             )])

        self.assertCountEqual(expected_events, incarceration_events)

    def test_find_incarceration_events_multiple_sentences_with_investigative_supervision_period_us_id(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_DEFAULT_IP_ID,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ID',
            facility='PRISON3',
            admission_date=date(2018, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2018, 11, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_ID',
            start_date=date(2018, 11, 20),
            incarceration_periods=[incarceration_period]
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1111,
            state_code='US_ID',
            start_date=date(2018, 11, 1),
            termination_date=date(2018, 11, 19),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION)

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code='US_ID',
            start_date=date(2018, 11, 1),
            incarceration_periods=[incarceration_period],
            supervision_periods=[supervision_period]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_ID',
            incarceration_sentences=[incarceration_sentence],
            supervision_sentences=[supervision_sentence]
        )

        incarceration_sentence.sentence_group = sentence_group
        supervision_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = identifier.find_incarceration_events(
            sentence_groups, _DEFAULT_INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION, _COUNTY_OF_RESIDENCE_ROWS)

        expected_events: List[IncarcerationEvent] = expected_incarceration_stay_events(
            incarceration_period,
            judicial_district_code='NW',
            supervision_type_at_admission=StateSupervisionPeriodSupervisionType.INVESTIGATION,
            original_admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        expected_events.extend(
            [IncarcerationAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                supervision_type_at_admission=StateSupervisionPeriodSupervisionType.INVESTIGATION
            ),
             IncarcerationReleaseEvent(
                 state_code=incarceration_period.state_code,
                 event_date=incarceration_period.release_date,
                 facility=incarceration_period.facility,
                 county_of_residence=_COUNTY_OF_RESIDENCE,
                 release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
                 admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                 total_days_incarcerated=(incarceration_period.release_date - incarceration_period.admission_date).days
             )])

        self.assertCountEqual(expected_events, incarceration_events)

    def testFindIncarcerationEvents_only_placeholder_ips_and_sps(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SP_ID,
            state_code='US_XX')
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_DEFAULT_IP_ID,
            state_code='US_XX')

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            start_date=date(2008, 10, 11),
            incarceration_periods=[incarceration_period],
            supervision_periods=[supervision_period]
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code='US_XX',
            start_date=date(2008, 10, 11),
            incarceration_periods=[incarceration_period]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX',
            incarceration_sentences=[incarceration_sentence],
            supervision_sentences=[supervision_sentence]
        )

        incarceration_sentence.sentence_group = sentence_group
        supervision_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = identifier.find_incarceration_events(
            sentence_groups, _DEFAULT_INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION, _COUNTY_OF_RESIDENCE_ROWS)

        self.assertCountEqual([], incarceration_events)

    def testFindIncarcerationEvents_usNd_tempCustodyFollowedByRevocation(self):
        """Tests that with state code US_ND, temporary custody periods are dropped before finding all incarceration
        events.
        """
        temp_custody_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id='1',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            facility='PRISON',
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text='INCARCERATION_ADMISSION',
            release_date=date(2008, 12, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY)
        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id='2',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            facility='PRISON',
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            admission_reason_raw_text='Revocation',
            release_date=date(2008, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text='RPRB'
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_ND',
            start_date=date(2008, 12, 11),
            incarceration_periods=[temp_custody_period, revocation_period],
            charges=[
                StateCharge.new_with_defaults(
                    state_code='US_ND',
                    offense_date=date(2007, 12, 11),
                    ncic_code='0901',
                    statute='9999'
                )
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(state_code='US_ND',
                                                              sentence_group_id=9797,
                                                              incarceration_sentences=[incarceration_sentence])

        incarceration_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = identifier.find_incarceration_events(
            sentence_groups, _DEFAULT_INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION, _COUNTY_OF_RESIDENCE_ROWS)

        self.assertCountEqual([
            IncarcerationStayEvent(
                admission_reason=revocation_period.admission_reason,
                admission_reason_raw_text=revocation_period.admission_reason_raw_text,
                supervision_type_at_admission=StateSupervisionPeriodSupervisionType.PROBATION,
                state_code=revocation_period.state_code,
                event_date=revocation_period.admission_date,
                facility=revocation_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                most_serious_offense_ncic_code='0901',
                most_serious_offense_statute='9999'
            ),
            IncarcerationAdmissionEvent(
                state_code=revocation_period.state_code,
                event_date=revocation_period.admission_date,
                facility=revocation_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=revocation_period.admission_reason,
                admission_reason_raw_text=revocation_period.admission_reason_raw_text,
                supervision_type_at_admission=StateSupervisionPeriodSupervisionType.PROBATION,
            ),
            IncarcerationReleaseEvent(
                state_code=revocation_period.state_code,
                event_date=revocation_period.release_date,
                facility=revocation_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_reason=revocation_period.release_reason,
                release_reason_raw_text=revocation_period.release_reason_raw_text,
                supervision_type_at_release=StateSupervisionPeriodSupervisionType.PROBATION,
                admission_reason=revocation_period.admission_reason,
                total_days_incarcerated=(revocation_period.release_date - revocation_period.admission_date).days
            )
        ], incarceration_events)

    def testFindIncarcerationEvents_usMo_tempCustodyFollowedByRevocation(self):
        """Tests that when a temporary custody period is followed by a revocation period, we collapse the two when
        generating IncarcerationAdmissionEvents, but do not when creating IncarcerationStayEvents.
        """
        temp_custody_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id='1',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_MO',
            facility='PRISON',
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text='Temporary Custody',
            release_date=date(2008, 11, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY)
        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id='2',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_MO',
            facility='PRISON',
            admission_date=date(2008, 11, 21),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            admission_reason_raw_text='Revocation',
            release_date=date(2008, 11, 22),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE)

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_MO',
            supervision_period_id=1313,
            external_id='3',
            start_date=date(2008, 1, 1))

        incarceration_sentence = \
            FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
                StateIncarcerationSentence.new_with_defaults(
                    state_code='US_MO',
                    incarceration_sentence_id=123,
                    incarceration_periods=[temp_custody_period, revocation_period],
                    supervision_periods=[supervision_period],
                    start_date=date(2008, 1, 1),
                    charges=[
                        StateCharge.new_with_defaults(
                            state_code='US_MO',
                            offense_date=date(2007, 12, 11),
                            ncic_code='0901',
                            statute='9999'
                        )
                    ]
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=supervision_period.start_date,
                        end_date=temp_custody_period.admission_date,
                        supervision_type=StateSupervisionType.PAROLE
                    ),
                    SupervisionTypeSpan(
                        start_date=temp_custody_period.admission_date,
                        end_date=revocation_period.admission_date,
                        supervision_type=None
                    ),
                    SupervisionTypeSpan(
                        start_date=revocation_period.admission_date,
                        end_date=None,
                        supervision_type=None
                    )
                ]
            )

        supervision_sentence = \
            FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
                StateSupervisionSentence.new_with_defaults(
                    state_code='US_MO',
                    supervision_sentence_id=123,
                    incarceration_periods=[temp_custody_period, revocation_period],
                    supervision_periods=[supervision_period],
                    start_date=date(2008, 1, 1)),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=supervision_period.start_date,
                        end_date=temp_custody_period.admission_date,
                        supervision_type=StateSupervisionType.PROBATION
                    ),
                    SupervisionTypeSpan(
                        start_date=temp_custody_period.admission_date,
                        end_date=revocation_period.admission_date,
                        supervision_type=None
                    ),
                    SupervisionTypeSpan(
                        start_date=revocation_period.admission_date,
                        end_date=None,
                        supervision_type=None
                    )
                ]
            )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_MO',
            supervision_sentences=[supervision_sentence],
            incarceration_sentences=[incarceration_sentence])

        incarceration_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = identifier.find_incarceration_events(
            sentence_groups, _DEFAULT_INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION, _COUNTY_OF_RESIDENCE_ROWS)

        self.maxDiff = None
        self.assertCountEqual([
            IncarcerationStayEvent(
                admission_reason=temp_custody_period.admission_reason,
                admission_reason_raw_text=temp_custody_period.admission_reason_raw_text,
                supervision_type_at_admission=StateSupervisionPeriodSupervisionType.DUAL,
                state_code=temp_custody_period.state_code,
                event_date=temp_custody_period.admission_date,
                facility=temp_custody_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                most_serious_offense_ncic_code='0901',
                most_serious_offense_statute='9999'
            ),
            IncarcerationStayEvent(
                admission_reason=revocation_period.admission_reason,
                admission_reason_raw_text=revocation_period.admission_reason_raw_text,
                supervision_type_at_admission=StateSupervisionPeriodSupervisionType.DUAL,
                state_code=revocation_period.state_code,
                event_date=revocation_period.admission_date,
                facility=revocation_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                most_serious_offense_ncic_code='0901',
                most_serious_offense_statute='9999'
            ),
            IncarcerationAdmissionEvent(
                state_code=temp_custody_period.state_code,
                event_date=temp_custody_period.admission_date,
                facility=temp_custody_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=revocation_period.admission_reason,
                admission_reason_raw_text=revocation_period.admission_reason_raw_text,
                supervision_type_at_admission=StateSupervisionPeriodSupervisionType.DUAL,
            ),
            IncarcerationReleaseEvent(
                state_code=revocation_period.state_code,
                event_date=revocation_period.release_date,
                facility=revocation_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_reason=revocation_period.release_reason,
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                total_days_incarcerated=(revocation_period.release_date - temp_custody_period.admission_date).days
            )
        ], incarceration_events)


class TestFindEndOfMonthStatePrisonStays(unittest.TestCase):
    """Tests the find_incarceration_stays function."""

    @staticmethod
    def _run_find_incarceration_stays_with_no_sentences(incarceration_period: StateIncarcerationPeriod,
                                                        county_of_residence: str):
        """Runs `find_incarceration_stays` without providing sentence information. Sentence information
        is only used in `US_MO` to inform supervision_type_at_admission. All tests using this method should not require
        that state specific logic.
        """
        incarceration_sentences: List[StateIncarcerationSentence] = []
        supervision_sentences: List[StateSupervisionSentence] = []
        default_incarceration_period_judicial_district_association = {
            123: _DEFAULT_INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION[0]
        }

        original_admission_reasons_by_period_id = \
            identifier._original_admission_reasons_by_period_id([incarceration_period])

        return identifier.find_incarceration_stays(
            incarceration_sentences,
            supervision_sentences,
            incarceration_period,
            original_admission_reasons_by_period_id,
            default_incarceration_period_judicial_district_association,
            county_of_residence)

    def test_find_incarceration_stays_type_us_mo(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_MO',
            facility='PRISON3',
            admission_date=date(2010, 1, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 3, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1111,
            state_code='US_MO',
            start_date=date(2010, 1, 1),
            termination_date=date(2010, 2, 15))

        supervision_sentence = \
            FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
                StateSupervisionSentence.new_with_defaults(
                    supervision_sentence_id=1111,
                    state_code='US_MO',
                    supervision_type=StateSupervisionType.PROBATION,
                    start_date=date(2010, 1, 1),
                    supervision_periods=[supervision_period],
                    incarceration_periods=[incarceration_period]
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=date(2010, 1, 1),
                        end_date=None,
                        supervision_type=StateSupervisionType.PROBATION
                    )
                ]
            )

        incarceration_period.supervision_sentences = [supervision_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_MO', sentence_group_id=6666, external_id='12345')
        supervision_sentence.sentence_group = sentence_group

        incarceration_period_judicial_district_association = {
            incarceration_period.incarceration_period_id: {
                'incarceration_period_id': incarceration_period.incarceration_period_id,
                'judicial_district_code': 'XXX'
            }
        }

        original_admission_reasons_by_period_id = \
            identifier._original_admission_reasons_by_period_id([incarceration_period])

        incarceration_sentences = []
        incarceration_events = identifier.find_incarceration_stays(
            incarceration_sentences,
            [supervision_sentence],
            incarceration_period,
            original_admission_reasons_by_period_id,
            incarceration_period_judicial_district_association,
            _COUNTY_OF_RESIDENCE)

        expected_incarceration_events = expected_incarceration_stay_events(incarceration_period)

        updated_expected_events = []
        for expected_event in expected_incarceration_events:
            updated_expected_events.append(
                attr.evolve(expected_event,
                            judicial_district_code='XXX',
                            supervision_type_at_admission=StateSupervisionPeriodSupervisionType.PROBATION))

        self.assertEqual(updated_expected_events, incarceration_events)

    def test_find_incarceration_stays(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2000, 1, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                release_date=date(2010, 12, 1),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period]
        )
        incarceration_period.supervision_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX', sentence_group_id=6666, external_id='12345')
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = \
            self._run_find_incarceration_stays_with_no_sentences(
                incarceration_period, _COUNTY_OF_RESIDENCE
            )

        expected_incarceration_events = expected_incarceration_stay_events(incarceration_period)

        self.assertEqual(expected_incarceration_events, incarceration_events)

    @freeze_time('2019-11-01')
    def test_find_incarceration_stays_no_release(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2018, 1, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period]
        )
        incarceration_period.supervision_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX', sentence_group_id=6666, external_id='12345')
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = \
            self._run_find_incarceration_stays_with_no_sentences(
                incarceration_period, _COUNTY_OF_RESIDENCE
            )

        expected_incarceration_events = expected_incarceration_stay_events(incarceration_period)

        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_incarceration_stays_no_admission_or_release(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3')

        with pytest.raises(ValueError):
            _ = self._run_find_incarceration_stays_with_no_sentences(
                incarceration_period, _COUNTY_OF_RESIDENCE)

    def test_find_incarceration_stays_no_release_reason(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2000, 1, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                release_date=date(2010, 12, 1),
                release_reason=None)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period]
        )
        incarceration_period.supervision_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX', sentence_group_id=6666, external_id='12345')
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = \
            self._run_find_incarceration_stays_with_no_sentences(
                incarceration_period, _COUNTY_OF_RESIDENCE)

        expected_incarceration_events = expected_incarceration_stay_events(incarceration_period)

        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_incarceration_stays_admitted_end_of_month(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2000, 1, 31),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2000, 2, 13),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period]
        )
        incarceration_period.supervision_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX', sentence_group_id=6666, external_id='12345')
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = self._run_find_incarceration_stays_with_no_sentences(
            incarceration_period, _COUNTY_OF_RESIDENCE)

        expected_incarceration_events = expected_incarceration_stay_events(incarceration_period)

        self.assertEqual(expected_incarceration_events, incarceration_events)

    @freeze_time('2019-12-02')
    def test_find_incarceration_stays_still_in_custody(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2019, 11, 30),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period]
        )
        incarceration_period.supervision_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX', sentence_group_id=6666, external_id='12345')
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = self._run_find_incarceration_stays_with_no_sentences(
            incarceration_period, _COUNTY_OF_RESIDENCE)

        expected_incarceration_events = expected_incarceration_stay_events(incarceration_period)

        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_incarceration_stays_released_end_of_month(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2019, 11, 29),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2019, 11, 30),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period]
        )
        incarceration_period.supervision_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX', sentence_group_id=6666, external_id='12345')
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = \
            self._run_find_incarceration_stays_with_no_sentences(
                incarceration_period, _COUNTY_OF_RESIDENCE
            )

        # We do not count the termination date of an incarceration period as a day the person is incarcerated.
        expected_incarceration_events = expected_incarceration_stay_events(incarceration_period)

        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_incarceration_stays_transfers_end_of_month(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2019, 11, 29),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2019, 11, 30),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        incarceration_period_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON4',
                admission_date=date(2019, 11, 30),
                admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
                release_date=date(2019, 12, 1),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period, incarceration_period_2]
        )
        incarceration_period.supervision_sentences = [incarceration_sentence]
        incarceration_period_2.supervision_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX', sentence_group_id=6666, external_id='12345')
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events_period_1 = \
            self._run_find_incarceration_stays_with_no_sentences(
                incarceration_period, _COUNTY_OF_RESIDENCE

            )

        expected_incarceration_events = expected_incarceration_stay_events(incarceration_period)

        self.assertEqual(expected_incarceration_events, incarceration_events_period_1)

        incarceration_events_period_2 = \
            self._run_find_incarceration_stays_with_no_sentences(
                incarceration_period_2, _COUNTY_OF_RESIDENCE
            )

        expected_incarceration_events = expected_incarceration_stay_events(incarceration_period_2)

        self.assertEqual(expected_incarceration_events, incarceration_events_period_2)

    def test_find_incarceration_stays_released_first_of_month(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2019, 11, 15),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2019, 12, 1),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period]
        )
        incarceration_period.supervision_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX', sentence_group_id=6666, external_id='12345')
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = \
            self._run_find_incarceration_stays_with_no_sentences(
                incarceration_period, _COUNTY_OF_RESIDENCE
            )

        expected_incarceration_events = expected_incarceration_stay_events(incarceration_period)

        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_incarceration_stays_only_one_day(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2019, 7, 31),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2019, 7, 31),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period]
        )
        incarceration_period.supervision_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX', sentence_group_id=6666, external_id='12345')
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = \
            self._run_find_incarceration_stays_with_no_sentences(
                incarceration_period, _COUNTY_OF_RESIDENCE
            )

        # We do not count people who were released on the last day of the month as being incarcerated on that last day.
        # In normal circumstances, if this person remained incarcerated but had a quick, one-day transfer, there will
        # be another incarceration period that opens on the last day of the month with a later termination date - we
        # *will* count this one.
        expected_incarceration_events = expected_incarceration_stay_events(incarceration_period)

        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_incarceration_stays_county_jail(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.COUNTY_JAIL,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2000, 1, 31),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2000, 2, 13),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period]
        )
        incarceration_period.incarceration_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX', sentence_group_id=6666, external_id='12345')
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = \
            self._run_find_incarceration_stays_with_no_sentences(
                incarceration_period, _COUNTY_OF_RESIDENCE
            )

        expected_incarceration_events = expected_incarceration_stay_events(incarceration_period)

        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_incarceration_stays_original_admission_reason(self):
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_XX',
            facility='PRISON3',
            admission_date=date(2010, 1, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 3, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_XX',
            facility='PRISON3',
            admission_date=date(2010, 3, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2010, 3, 31),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=9797,
            incarceration_periods=incarceration_periods
        )

        incarceration_period_2.incarceration_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX', sentence_group_id=6666, external_id='12345')
        incarceration_sentence.sentence_group = sentence_group

        incarceration_period_judicial_district_association = {
            incarceration_period_1.incarceration_period_id: {
                'incarceration_period_id': incarceration_period_1.incarceration_period_id,
                'judicial_district_code': 'XXX'
            }
        }

        original_admission_reasons_by_period_id = \
            identifier._original_admission_reasons_by_period_id([incarceration_period_1, incarceration_period_2])

        incarceration_sentences = []
        incarceration_events = identifier.find_incarceration_stays(
            incarceration_sentences,
            [],
            incarceration_period_2,
            original_admission_reasons_by_period_id,
            incarceration_period_judicial_district_association,
            _COUNTY_OF_RESIDENCE)

        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period_2,
            original_admission_reason=incarceration_period_1.admission_reason,
            original_admission_reason_raw_text=incarceration_period_1.admission_reason_raw_text
        )

        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_incarceration_stays_two_official_admission_reasons(self):
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_XX',
            facility='PRISON3',
            admission_date=date(2010, 1, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 3, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_XX',
            facility='PRISON3',
            admission_date=date(2010, 3, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            release_date=date(2010, 3, 31),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=9797,
            incarceration_periods=incarceration_periods
        )

        incarceration_period_2.incarceration_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX', sentence_group_id=6666, external_id='12345')
        incarceration_sentence.sentence_group = sentence_group

        incarceration_period_judicial_district_association = {
            incarceration_period_1.incarceration_period_id: {
                'incarceration_period_id': incarceration_period_1.incarceration_period_id,
                'judicial_district_code': 'XXX'
            }
        }

        original_admission_reasons_by_period_id = \
            identifier._original_admission_reasons_by_period_id([incarceration_period_1, incarceration_period_2])

        incarceration_sentences = []
        incarceration_events = identifier.find_incarceration_stays(
            incarceration_sentences,
            [],
            incarceration_period_2,
            original_admission_reasons_by_period_id,
            incarceration_period_judicial_district_association,
            _COUNTY_OF_RESIDENCE)

        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period_2
        )

        self.assertEqual(expected_incarceration_events, incarceration_events)


class TestDeDuplicatedAdmissions(unittest.TestCase):
    """Tests the de_duplicated_admissions function."""

    def test_de_duplicated_admissions(self):
        incarceration_period_1 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2008, 11, 20),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)

        incarceration_period_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

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
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2008, 11, 20),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)

        incarceration_period_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

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
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2008, 11, 19),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        incarceration_period_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

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
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        incarceration_period_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE)

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

    def _run_admission_event_for_period_with_no_sentences(self, incarceration_period: StateIncarcerationPeriod,
                                                          county_of_residence: str):
        """Runs `admission_event_for_period` without providing sentence information. Sentence information
        is only used in `US_MO` to inform supervision_type_at_admission. All tests using this method should not require
        that state specific logic.
        """
        incarceration_sentences: List[StateIncarcerationSentence] = []
        supervision_sentences: List[StateSupervisionSentence] = []
        return identifier.admission_event_for_period(
            incarceration_sentences,
            supervision_sentences,
            incarceration_period,
            county_of_residence)

    def test_admission_event_for_period_us_mo(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_MO',
            facility='PRISON3',
            admission_date=date(2010, 1, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 3, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1111,
            state_code='US_MO',
            start_date=date(2010, 1, 1),
            termination_date=date(2010, 2, 15))
        supervision_sentence = \
            FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
                StateSupervisionSentence.new_with_defaults(
                    supervision_sentence_id=1111,
                    state_code='US_MO',
                    supervision_type=StateSupervisionType.PROBATION,
                    start_date=date(2010, 1, 1),
                    supervision_periods=[supervision_period],
                    incarceration_periods=[incarceration_period]),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=date(2010, 1, 1),
                        end_date=None,
                        supervision_type=StateSupervisionType.PROBATION
                    )
                ]
            )
        incarceration_sentences = []

        admission_event = identifier.admission_event_for_period(
            incarceration_sentences,
            [supervision_sentence],
            incarceration_period,
            _COUNTY_OF_RESIDENCE)

        self.assertEqual(IncarcerationAdmissionEvent(
            state_code=incarceration_period.state_code,
            event_date=incarceration_period.admission_date,
            facility='PRISON3',
            county_of_residence=_COUNTY_OF_RESIDENCE,
            admission_reason=incarceration_period.admission_reason,
            supervision_type_at_admission=StateSupervisionPeriodSupervisionType.PROBATION,
        ), admission_event)

    def test_admission_event_after_investigative_supervision_period_us_id(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ID',
            facility='PRISON3',
            admission_date=date(2010, 1, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2010, 3, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1111,
            state_code='US_ID',
            start_date=date(2010, 1, 1),
            termination_date=date(2010, 2, 15),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION)
        supervision_sentences = [StateSupervisionSentence.new_with_defaults(
            state_code='US_ID',
            supervision_periods=[supervision_period]
        )]
        incarceration_sentences = []

        admission_event = identifier.admission_event_for_period(
            incarceration_sentences,
            supervision_sentences,
            incarceration_period,
            _COUNTY_OF_RESIDENCE)

        self.assertEqual(IncarcerationAdmissionEvent(
            state_code=incarceration_period.state_code,
            event_date=incarceration_period.admission_date,
            facility='PRISON3',
            county_of_residence=_COUNTY_OF_RESIDENCE,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            supervision_type_at_admission=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        ), admission_event)

    def test_admission_event_for_period(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_XX',
            facility='PRISON3',
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.DUAL_REVOCATION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        admission_event = self._run_admission_event_for_period_with_no_sentences(
            incarceration_period, _COUNTY_OF_RESIDENCE)

        self.assertEqual(IncarcerationAdmissionEvent(
            state_code=incarceration_period.state_code,
            event_date=incarceration_period.admission_date,
            facility='PRISON3',
            county_of_residence=_COUNTY_OF_RESIDENCE,
            admission_reason=incarceration_period.admission_reason,
            supervision_type_at_admission=StateSupervisionPeriodSupervisionType.DUAL,
        ), admission_event)

    def test_admission_event_for_period_all_admission_reasons(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2013, 11, 20),
                release_date=date(2019, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        for admission_reason in StateIncarcerationPeriodAdmissionReason:
            incarceration_period.admission_reason = admission_reason

            admission_event = self._run_admission_event_for_period_with_no_sentences(
                incarceration_period, _COUNTY_OF_RESIDENCE)

            self.assertIsNotNone(admission_event)

    def test_admission_event_for_period_specialized_pfi(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        admission_event = self._run_admission_event_for_period_with_no_sentences(
            incarceration_period, _COUNTY_OF_RESIDENCE)

        self.assertEqual(IncarcerationAdmissionEvent(
            state_code=incarceration_period.state_code,
            event_date=incarceration_period.admission_date,
            facility='PRISON3',
            county_of_residence=_COUNTY_OF_RESIDENCE,
            admission_reason=incarceration_period.admission_reason,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
        ), admission_event)

    def test_admission_event_for_period_county_jail(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.COUNTY_JAIL,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='CJ10',
                admission_date=date(2013, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2019, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        admission_event = self._run_admission_event_for_period_with_no_sentences(
            incarceration_period, _COUNTY_OF_RESIDENCE
        )

        self.assertEqual(IncarcerationAdmissionEvent(
            state_code=incarceration_period.state_code,
            event_date=incarceration_period.admission_date,
            facility='CJ10',
            county_of_residence=_COUNTY_OF_RESIDENCE,
            admission_reason=incarceration_period.admission_reason,
        ), admission_event)


class TestReleaseEventForPeriod(unittest.TestCase):
    """Tests the release_event_for_period function."""

    @staticmethod
    def _run_release_for_period_with_no_sentences(incarceration_period, county_of_residence):
        """Runs `release_event_for_period` without providing sentence information. Sentence information
        is only used to inform supervision_type_at_release for US_MO and US_ID. All tests using this method should
        not require that state specific logic.
        """
        incarceration_sentences = []
        supervision_sentences = []
        return identifier.release_event_for_period(
            incarceration_sentences,
            supervision_sentences,
            incarceration_period,
            county_of_residence)

    def test_release_event_for_period(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
                release_reason_raw_text='SS')

        release_event = self._run_release_for_period_with_no_sentences(
            incarceration_period, _COUNTY_OF_RESIDENCE)

        self.assertEqual(IncarcerationReleaseEvent(
            state_code=incarceration_period.state_code,
            event_date=incarceration_period.release_date,
            facility='PRISON3',
            county_of_residence=_COUNTY_OF_RESIDENCE,
            release_reason=incarceration_period.release_reason,
            release_reason_raw_text=incarceration_period.release_reason_raw_text,
            purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration,
            admission_reason=incarceration_period.admission_reason,
            total_days_incarcerated=(incarceration_period.release_date - incarceration_period.admission_date).days
        ), release_event)

    def test_release_event_for_period_all_release_reasons(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2013, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2019, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        for release_reason in StateIncarcerationPeriodReleaseReason:
            incarceration_period.release_reason = release_reason

            release_event = self._run_release_for_period_with_no_sentences(
                incarceration_period, _COUNTY_OF_RESIDENCE)

            self.assertIsNotNone(release_event)

    def test_release_event_for_period_county_jail(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.COUNTY_JAIL,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='CJ19',
                admission_date=date(2013, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2019, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        release_event = self._run_release_for_period_with_no_sentences(
            incarceration_period, _COUNTY_OF_RESIDENCE)

        self.assertEqual(IncarcerationReleaseEvent(
            state_code=incarceration_period.state_code,
            event_date=incarceration_period.release_date,
            facility='CJ19',
            county_of_residence=_COUNTY_OF_RESIDENCE,
            release_reason=incarceration_period.release_reason,
            admission_reason=incarceration_period.admission_reason,
            total_days_incarcerated=(incarceration_period.release_date - incarceration_period.admission_date).days
        ), release_event)

    def test_release_event_for_period_us_id(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_ID',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
                release_reason_raw_text='SS')

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=111,
            start_date=incarceration_period.release_date,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
        )

        supervision_sentences = [StateSupervisionSentence.new_with_defaults(
            state_code='US_ID',
            supervision_periods=[supervision_period]
        )]

        incarceration_sentences = []

        release_event = identifier.release_event_for_period(
            incarceration_sentences=incarceration_sentences,
            supervision_sentences=supervision_sentences,
            incarceration_period=incarceration_period,
            county_of_residence=_COUNTY_OF_RESIDENCE
        )

        self.assertEqual(IncarcerationReleaseEvent(
            state_code=incarceration_period.state_code,
            event_date=incarceration_period.release_date,
            facility='PRISON3',
            county_of_residence=_COUNTY_OF_RESIDENCE,
            release_reason=incarceration_period.release_reason,
            release_reason_raw_text=incarceration_period.release_reason_raw_text,
            purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration,
            supervision_type_at_release=StateSupervisionPeriodSupervisionType.PROBATION,
            admission_reason=incarceration_period.admission_reason,
            total_days_incarcerated=(incarceration_period.release_date - incarceration_period.admission_date).days
        ), release_event)

    def test_release_event_for_period_us_mo(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_MO',
                facility='PRISON3',
                admission_date=date(2013, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2019, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1111,
            state_code='US_MO',
            start_date=date(2019, 12, 4))
        supervision_sentence = \
            FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
                StateSupervisionSentence.new_with_defaults(
                    supervision_sentence_id=1111,
                    state_code='US_MO',
                    supervision_type=StateSupervisionType.PROBATION,
                    start_date=date(2019, 11, 24),
                    supervision_periods=[supervision_period],
                    incarceration_periods=[incarceration_period]),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=date(2019, 12, 4),
                        end_date=None,
                        supervision_type=StateSupervisionType.PROBATION
                    )
                ]
            )
        incarceration_sentences = []

        release_event = identifier.release_event_for_period(
            incarceration_sentences=incarceration_sentences,
            supervision_sentences=[supervision_sentence],
            incarceration_period=incarceration_period,
            county_of_residence=_COUNTY_OF_RESIDENCE
        )

        self.assertEqual(IncarcerationReleaseEvent(
            state_code=incarceration_period.state_code,
            event_date=incarceration_period.release_date,
            facility='PRISON3',
            county_of_residence=_COUNTY_OF_RESIDENCE,
            release_reason=incarceration_period.release_reason,
            release_reason_raw_text=incarceration_period.release_reason_raw_text,
            purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration,
            supervision_type_at_release=StateSupervisionPeriodSupervisionType.PROBATION,
            admission_reason=incarceration_period.admission_reason,
            total_days_incarcerated=(incarceration_period.release_date - incarceration_period.admission_date).days
        ), release_event)

    def test_release_event_for_period_us_nd(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_ND',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
                release_reason_raw_text='RPAR')

        release_event = self._run_release_for_period_with_no_sentences(incarceration_period, _COUNTY_OF_RESIDENCE)

        self.assertEqual(IncarcerationReleaseEvent(
            state_code=incarceration_period.state_code,
            event_date=incarceration_period.release_date,
            facility='PRISON3',
            county_of_residence=_COUNTY_OF_RESIDENCE,
            release_reason=incarceration_period.release_reason,
            release_reason_raw_text=incarceration_period.release_reason_raw_text,
            purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration,
            supervision_type_at_release=StateSupervisionPeriodSupervisionType.PAROLE,
            admission_reason=incarceration_period.admission_reason,
            total_days_incarcerated=(incarceration_period.release_date - incarceration_period.admission_date).days
        ), release_event)


class TestGetUniquePeriodsFromSentenceGroupAndAddBackedges(unittest.TestCase):
    """Tests the get_unique_periods_from_sentence_groups_and_add_backedges function."""
    def test_get_unique_periods_from_sentence_groups_and_add_backedges(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1112,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            state_code='US_XX',
            start_date=date(2008, 11, 20),
            termination_date=date(2010, 12, 4))

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_XX',
            facility='PRISON3',
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_XX',
            facility='PRISON3',
            admission_date=date(2011, 1, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE)

        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_XX',
            facility='PRISON3',
            admission_date=date(2013, 5, 22),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2015, 11, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        sentence_groups = [
            StateSentenceGroup.new_with_defaults(
                state_code='US_XX',
                incarceration_sentences=[
                    StateIncarcerationSentence.new_with_defaults(
                        state_code='US_XX',
                        incarceration_periods=[incarceration_period_1, incarceration_period_2]
                    )
                ],
                supervision_sentences=[
                    StateSupervisionSentence.new_with_defaults(
                        state_code='US_XX',
                        incarceration_periods=[incarceration_period_1, incarceration_period_3]
                    )
                ]
            ),
            StateSentenceGroup.new_with_defaults(
                state_code='US_XX',
                incarceration_sentences=[
                    StateIncarcerationSentence.new_with_defaults(
                        state_code='US_XX',
                        supervision_periods=[supervision_period],
                        incarceration_periods=[incarceration_period_3]
                    )
                ]
            )
        ]

        incarceration_periods, supervision_periods = \
            identifier.get_unique_periods_from_sentence_groups_and_add_backedges(sentence_groups)

        expected_incarceration_periods = [incarceration_period_1, incarceration_period_2, incarceration_period_3]
        expected_supervision_periods = [supervision_period]

        self.assertEqual(expected_incarceration_periods, incarceration_periods)
        self.assertEqual(expected_supervision_periods, supervision_periods)

    def test_get_unique_periods_from_sentence_groups_and_add_backedges_no_periods(self):
        sentence_groups = [
            StateSentenceGroup.new_with_defaults(
                state_code='US_XX'
            ),
            StateSentenceGroup.new_with_defaults(
                state_code='US_XX'
            )
        ]

        incarceration_periods, supervision_periods = \
            identifier.get_unique_periods_from_sentence_groups_and_add_backedges(sentence_groups)

        expected_incarceration_periods = []
        expected_supervision_periods = []

        self.assertEqual(expected_incarceration_periods, incarceration_periods)
        self.assertEqual(expected_supervision_periods, supervision_periods)

    def test_get_unique_periods_from_sentence_groups_and_add_backedges_no_sentence_groups(self):
        incarceration_periods, supervision_periods = \
            identifier.get_unique_periods_from_sentence_groups_and_add_backedges([])

        expected_incarceration_periods = []
        expected_supervision_periods = []

        self.assertEqual(expected_incarceration_periods, incarceration_periods)
        self.assertEqual(expected_supervision_periods, supervision_periods)


class TestFindMostSeriousOffenseStatuteInSentenceGroup(unittest.TestCase):
    """Tests the find_most_serious_prior_charge_in_sentence_group function,"""
    def test_find_most_serious_prior_charge_in_sentence_group(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2009, 1, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            start_date=date(2007, 12, 10),
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2007, 12, 11),
                    ncic_code='2703',
                    statute='9999'
                ),
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2007, 12, 11),
                    ncic_code='1316',
                    statute='8888'
                ),
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2007, 12, 11),
                    ncic_code='3619',
                    statute='7777'
                )
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX',
            incarceration_sentences=[incarceration_sentence]
        )

        incarceration_period.incarceration_sentences = [incarceration_sentence]
        incarceration_sentence.sentence_group = sentence_group

        most_serious_statute = identifier.find_most_serious_prior_charge_in_sentence_group(
            incarceration_period, date(2008, 12, 31)).statute

        self.assertEqual(most_serious_statute, '8888')

    def test_find_most_serious_prior_charge_in_sentence_group_multiple_sentences(self):
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_XX',
            facility='PRISON3',
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        incarceration_sentence_1 = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            start_date=date(2007, 12, 10),
            incarceration_periods=[incarceration_period_1],
            charges=[
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2007, 12, 11),
                    ncic_code='3606',
                    statute='3606',
                ),
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2007, 12, 11),
                    ncic_code='3611',
                    statute='3611',
                ),
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2007, 12, 11),
                    ncic_code='3623',
                    statute='3623',
                )
            ]
        )

        incarceration_period_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2003, 1, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2009, 1, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        incarceration_sentence_2 = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            start_date=date(2007, 12, 10),
            incarceration_periods=[incarceration_period_2],
            charges=[
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2001, 12, 11),
                    ncic_code='3907',
                    statute='3907'
                ),
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2001, 12, 11),
                    ncic_code='3909',
                    statute='3909'
                ),
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2001, 12, 11),
                    ncic_code='3912',
                    statute='3912'
                )
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX',
            incarceration_sentences=[incarceration_sentence_1, incarceration_sentence_2]
        )

        incarceration_period_1.incarceration_sentences = [incarceration_sentence_1]
        incarceration_sentence_1.sentence_group = sentence_group

        incarceration_period_2.incarceration_sentences = [incarceration_sentence_2]
        incarceration_sentence_2.sentence_group = sentence_group

        most_serious_statute = identifier.find_most_serious_prior_charge_in_sentence_group(
            incarceration_period_1, date(2008, 12, 31)).statute

        self.assertEqual(most_serious_statute, '3606')

    def test_find_most_serious_prior_charge_in_sentence_group_offense_after_date(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2009, 1, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        incarceration_sentence_1 = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            start_date=date(2007, 12, 10),
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2007, 12, 11),
                    ncic_code='3611',
                    statute='1111'
                ),
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2007, 12, 11),
                    ncic_code='3623',
                    statute='3333'
                )
            ]
        )

        incarceration_sentence_2 = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            start_date=date(2010, 12, 10),
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2010, 12, 11),
                    ncic_code='3606',
                    statute='9999'
                ),
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX',
            incarceration_sentences=[incarceration_sentence_1, incarceration_sentence_2]
        )

        incarceration_period.incarceration_sentences = sentence_group.incarceration_sentences
        incarceration_sentence_1.sentence_group = sentence_group
        incarceration_sentence_2.sentence_group = sentence_group

        most_serious_statute = identifier.find_most_serious_prior_charge_in_sentence_group(
            incarceration_period, date(2008, 12, 31)).statute

        self.assertEqual(most_serious_statute, '1111')

    def test_find_most_serious_prior_charge_in_sentence_group_charges_no_ncic(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2009, 1, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2010, 12, 11),
                    statute='9999'
                ),
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2007, 12, 11),
                    statute='1111'
                ),
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2007, 12, 11),
                    statute='3333'
                )
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX',
            incarceration_sentences=[incarceration_sentence]
        )

        incarceration_period.incarceration_sentences = sentence_group.incarceration_sentences
        incarceration_sentence.sentence_group = sentence_group

        most_serious_charge = identifier.find_most_serious_prior_charge_in_sentence_group(
            incarceration_period, date(2008, 12, 31))

        self.assertIsNone(most_serious_charge)

    def test_find_most_serious_prior_charge_in_sentence_group_includes_chars(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2009, 1, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            start_date=date(2007, 12, 10),
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2007, 12, 11),
                    ncic_code='040A',
                    statute='xxxx'
                ),
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2007, 12, 11),
                    ncic_code='0101',
                    statute='9999'
                ),
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2007, 12, 11),
                    ncic_code='5301',
                    statute='1111'
                )
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX',
            incarceration_sentences=[incarceration_sentence]
        )

        incarceration_period.incarceration_sentences = sentence_group.incarceration_sentences
        incarceration_sentence.sentence_group = sentence_group

        most_serious_statute = identifier.find_most_serious_prior_charge_in_sentence_group(
            incarceration_period, date(2008, 12, 31)).statute

        self.assertEqual(most_serious_statute, '9999')

    def test_find_most_serious_prior_offense_statute_no_offense_dates(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2009, 1, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            start_date=date(2007, 12, 10),
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    ncic_code='2703',
                    statute='9999'
                ),
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    ncic_code='1316',
                    statute='8888'
                ),
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    ncic_code='3619',
                    statute='7777'
                )
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX',
            incarceration_sentences=[incarceration_sentence]
        )

        incarceration_period.incarceration_sentences = [incarceration_sentence]
        incarceration_sentence.sentence_group = sentence_group

        most_serious_statute = identifier.find_most_serious_prior_charge_in_sentence_group(
            incarceration_period, date(2008, 12, 31)).statute

        self.assertEqual(most_serious_statute, '8888')

    def test_find_most_serious_prior_offense_statute_offense_but_not_sentence_dates_before_cutoff(self):
        """We only look at the sentence date to determine the cutoff for most serious offense calculations."""
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2009, 1, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            start_date=date(2009, 12, 10),
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2007, 12, 10),
                    ncic_code='2703',
                    statute='9999'
                ),
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2007, 12, 10),
                    ncic_code='1316',
                    statute='8888'
                ),
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2007, 12, 10),
                    ncic_code='3619',
                    statute='7777'
                )
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX',
            incarceration_sentences=[incarceration_sentence]
        )

        incarceration_period.incarceration_sentences = [incarceration_sentence]
        incarceration_sentence.sentence_group = sentence_group

        most_serious_charge = identifier.find_most_serious_prior_charge_in_sentence_group(
            incarceration_period, date(2008, 12, 31))

        self.assertEqual(most_serious_charge, None)

    def test_find_most_serious_prior_offense_statute_sentence_but_not_offense_dates_before_cutoff(self):
        """We only look at the sentence date to determine the cutoff for most serious offense calculations."""
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2009, 1, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            start_date=date(2007, 12, 10),
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2009, 12, 10),
                    ncic_code='2703',
                    statute='9999'
                ),
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2009, 12, 10),
                    ncic_code='1316',
                    statute='8888'
                ),
                StateCharge.new_with_defaults(
                    state_code='US_XX',
                    offense_date=date(2009, 12, 10),
                    ncic_code='3619',
                    statute='7777'
                )
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX',
            incarceration_sentences=[incarceration_sentence]
        )

        incarceration_period.incarceration_sentences = [incarceration_sentence]
        incarceration_sentence.sentence_group = sentence_group

        most_serious_statute = identifier.find_most_serious_prior_charge_in_sentence_group(
            incarceration_period, date(2008, 12, 31)).statute

        self.assertEqual(most_serious_statute, '8888')


class TestOriginalAdmissionReasonsByPeriodID(unittest.TestCase):
    """Tests the _original_admission_reasons_by_period_id function in the identifier."""
    def test_original_admission_reasons_by_period_id(self):
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code='US_XX',
            incarceration_period_id=111,
            admission_date=date(2000, 1, 1),
            release_date=date(2000, 10, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code='US_XX',
            incarceration_period_id=222,
            admission_date=date(2000, 10, 3),
            release_date=date(2000, 10, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        original_admission_reasons_by_period_id = \
            identifier._original_admission_reasons_by_period_id(incarceration_periods)

        reason_tuple = (incarceration_period_1.admission_reason, incarceration_period_1.admission_reason_raw_text)

        expected_output = {
            incarceration_period_1.incarceration_period_id: reason_tuple,
            incarceration_period_2.incarceration_period_id: reason_tuple
        }

        self.assertEqual(expected_output, original_admission_reasons_by_period_id)

    def test_original_admission_reasons_by_period_id_multiple_official_admissions(self):
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code='US_XX',
            incarceration_period_id=111,
            admission_date=date(2000, 1, 1),
            release_date=date(2000, 10, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code='US_XX',
            incarceration_period_id=222,
            admission_date=date(2000, 10, 3),
            release_date=date(2000, 10, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED
        )

        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            state_code='US_XX',
            incarceration_period_id=333,
            admission_date=date(2020, 5, 1),
            release_date=date(2020, 10, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER
        )

        incarceration_period_4 = StateIncarcerationPeriod.new_with_defaults(
            state_code='US_XX',
            incarceration_period_id=444,
            admission_date=date(2020, 10, 3),
            release_date=date(2020, 10, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED
        )

        incarceration_periods = [incarceration_period_1,
                                 incarceration_period_2,
                                 incarceration_period_3,
                                 incarceration_period_4]

        original_admission_reasons_by_period_id = \
            identifier._original_admission_reasons_by_period_id(incarceration_periods)

        first_admission_tuple = (incarceration_period_1.admission_reason,
                                 incarceration_period_1.admission_reason_raw_text)
        second_admission_tuple = (incarceration_period_3.admission_reason,
                                  incarceration_period_3.admission_reason_raw_text)

        expected_output = {
            incarceration_period_1.incarceration_period_id: first_admission_tuple,
            incarceration_period_2.incarceration_period_id: first_admission_tuple,
            incarceration_period_3.incarceration_period_id: second_admission_tuple,
            incarceration_period_4.incarceration_period_id: second_admission_tuple
        }

        self.assertEqual(expected_output, original_admission_reasons_by_period_id)

    def test_original_admission_reasons_by_period_id_multiple_transfer_periods(self):
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code='US_XX',
            incarceration_period_id=111,
            admission_date=date(2000, 1, 1),
            release_date=date(2000, 10, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code='US_XX',
            incarceration_period_id=222,
            admission_date=date(2000, 10, 3),
            release_date=date(2000, 10, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER
        )

        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            state_code='US_XX',
            incarceration_period_id=333,
            admission_date=date(2000, 10, 12),
            release_date=date(2001, 1, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER
        )

        incarceration_period_4 = StateIncarcerationPeriod.new_with_defaults(
            state_code='US_XX',
            incarceration_period_id=444,
            admission_date=date(2001, 1, 4),
            release_date=date(2001, 10, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER
        )

        incarceration_periods = [incarceration_period_1,
                                 incarceration_period_2,
                                 incarceration_period_3,
                                 incarceration_period_4]

        original_admission_reasons_by_period_id = \
            identifier._original_admission_reasons_by_period_id(incarceration_periods)

        admission_tuple = (incarceration_period_1.admission_reason, incarceration_period_1.admission_reason_raw_text)

        expected_output = {
            incarceration_period_1.incarceration_period_id: admission_tuple,
            incarceration_period_2.incarceration_period_id: admission_tuple,
            incarceration_period_3.incarceration_period_id: admission_tuple,
            incarceration_period_4.incarceration_period_id: admission_tuple
        }

        self.assertEqual(expected_output, original_admission_reasons_by_period_id)

    def test_original_admission_reasons_by_period_id_no_official_admission(self):
        # The first incarceration period always counts as the official start of incarceration
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code='US_XX',
            incarceration_period_id=111,
            admission_date=date(2000, 1, 1),
            release_date=date(2000, 10, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_IN_ERROR
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code='US_XX',
            incarceration_period_id=222,
            admission_date=date(2000, 10, 3),
            release_date=date(2000, 10, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        original_admission_reasons_by_period_id = \
            identifier._original_admission_reasons_by_period_id(incarceration_periods)

        admission_tuple = (incarceration_period_1.admission_reason, incarceration_period_1.admission_reason_raw_text)

        expected_output = {
            incarceration_period_1.incarceration_period_id: admission_tuple,
            incarceration_period_2.incarceration_period_id: admission_tuple
        }

        self.assertEqual(expected_output, original_admission_reasons_by_period_id)

    def test_original_admission_reasons_by_period_id_not_official_admission_after_official_release(self):
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code='US_XX',
            incarceration_period_id=111,
            admission_date=date(2000, 1, 1),
            release_date=date(2000, 10, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            # Because this is an official release, the next period should be mapped to its own admission reason
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code='US_XX',
            incarceration_period_id=222,
            admission_date=date(2015, 10, 3),
            release_date=date(2015, 10, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        original_admission_reasons_by_period_id = \
            identifier._original_admission_reasons_by_period_id(incarceration_periods)

        expected_output = {
            incarceration_period_1.incarceration_period_id:
                (incarceration_period_1.admission_reason, incarceration_period_1.admission_reason_raw_text),
            incarceration_period_2.incarceration_period_id:
                (incarceration_period_2.admission_reason, incarceration_period_2.admission_reason_raw_text)
        }

        self.assertEqual(expected_output, original_admission_reasons_by_period_id)

    def test_original_admission_reasons_by_period_id_not_official_admission_after_not_official_release(self):
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code='US_XX',
            incarceration_period_id=111,
            admission_date=date(2000, 1, 1),
            release_date=date(2000, 10, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code='US_XX',
            incarceration_period_id=222,
            admission_date=date(2015, 10, 3),
            release_date=date(2015, 10, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        original_admission_reasons_by_period_id = \
            identifier._original_admission_reasons_by_period_id(incarceration_periods)

        expected_output = {
            incarceration_period_1.incarceration_period_id:
                (incarceration_period_1.admission_reason, incarceration_period_1.admission_reason_raw_text),
            incarceration_period_2.incarceration_period_id:
                (incarceration_period_1.admission_reason, incarceration_period_1.admission_reason_raw_text)
        }

        self.assertEqual(expected_output, original_admission_reasons_by_period_id)


def expected_incarceration_stay_events(incarceration_period: StateIncarcerationPeriod,
                                       original_admission_reason:
                                       Optional[StateIncarcerationPeriodAdmissionReason] = None,
                                       original_admission_reason_raw_text: Optional[str] = None,
                                       most_serious_offense_statute: Optional[str] = None,
                                       most_serious_offense_ncic_code: Optional[str] = None,
                                       judicial_district_code: Optional[str] = None,
                                       supervision_type_at_admission:
                                       Optional[StateSupervisionPeriodSupervisionType] = None) -> \
        List[IncarcerationStayEvent]:
    """Returns the expected incarceration stay events based on the provided |incarceration_period|."""

    expected_incarceration_events = []

    original_admission_reason = (original_admission_reason if original_admission_reason
                                 else incarceration_period.admission_reason)

    original_admission_reason_raw_text = (original_admission_reason_raw_text if original_admission_reason_raw_text
                                          else incarceration_period.admission_reason_raw_text)

    if incarceration_period.admission_date:
        release_date = (incarceration_period.release_date if incarceration_period.release_date
                        else date.today() + relativedelta(days=1))

        days_incarcerated = [incarceration_period.admission_date + relativedelta(days=x)
                             for x in range((release_date - incarceration_period.admission_date).days)]

        if days_incarcerated:
            # Ensuring we're not counting the release date as a day spent incarcerated
            assert max(days_incarcerated) < release_date

        for stay_date in days_incarcerated:
            # Update the `supervision_type_at_admission` if it is not specified.
            if supervision_type_at_admission is None:
                if incarceration_period.admission_reason == StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION:
                    supervision_type_at_admission = StateSupervisionPeriodSupervisionType.PAROLE
                if incarceration_period.admission_reason == \
                        StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION:
                    supervision_type_at_admission = StateSupervisionPeriodSupervisionType.PROBATION
                if incarceration_period.admission_reason == StateIncarcerationPeriodAdmissionReason.DUAL_REVOCATION:
                    supervision_type_at_admission = StateSupervisionPeriodSupervisionType.DUAL

            event = IncarcerationStayEvent(
                admission_reason=original_admission_reason,
                admission_reason_raw_text=original_admission_reason_raw_text,
                state_code=incarceration_period.state_code,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                event_date=stay_date,
                supervision_type_at_admission=supervision_type_at_admission,
                most_serious_offense_statute=most_serious_offense_statute,
                most_serious_offense_ncic_code=most_serious_offense_ncic_code,
                judicial_district_code=judicial_district_code,
                specialized_purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration
            )

            expected_incarceration_events.append(event)

    return expected_incarceration_events
