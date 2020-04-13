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
from recidiviz.calculator.pipeline.utils.us_mo_sentence_classification import SupervisionTypeSpan
from recidiviz.common.constants.state.state_incarceration import \
    StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason as AdmissionReason, \
    StateIncarcerationPeriodReleaseReason as ReleaseReason, StateSpecializedPurposeForIncarceration, \
    StateIncarcerationPeriodAdmissionReason
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod, StateSentenceGroup, \
    StateIncarcerationSentence, StateSupervisionSentence, StateCharge, StateSupervisionPeriod
from recidiviz.tests.calculator.pipeline.utils.us_mo_fakes import FakeUsMoSupervisionSentence, \
    FakeUsMoIncarcerationSentence

_COUNTY_OF_RESIDENCE = 'county'


class TestFindIncarcerationEvents(unittest.TestCase):
    """Tests the find_incarceration_events function."""

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
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text='ADMISSION',
            release_date=date(2008, 12, 20),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY)
        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id='2',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            facility='PRISON',
            admission_date=date(2008, 12, 20),
            admission_reason=AdmissionReason.PROBATION_REVOCATION,
            admission_reason_raw_text='Revocation',
            release_date=date(2009, 1, 20),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            start_date=date(2008, 12, 11),
            incarceration_periods=[temp_custody_period, revocation_period],
            charges=[
                StateCharge.new_with_defaults(
                    offense_date=date(2007, 12, 11),
                    ncic_code='0901',
                    statute='9999'
                )
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(sentence_group_id=9797,
                                                              incarceration_sentences=[incarceration_sentence])

        incarceration_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = identifier.find_incarceration_events(sentence_groups, _COUNTY_OF_RESIDENCE)

        self.assertCountEqual([
            IncarcerationStayEvent(
                admission_reason=revocation_period.admission_reason,
                admission_reason_raw_text=revocation_period.admission_reason_raw_text,
                supervision_type_at_admission=StateSupervisionPeriodSupervisionType.PROBATION,
                state_code=revocation_period.state_code,
                event_date=last_day_of_month(revocation_period.admission_date),
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
                release_reason=revocation_period.release_reason
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
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text='Temporary Custody',
            release_date=date(2008, 12, 20),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY)
        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id='2',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_MO',
            facility='PRISON',
            admission_date=date(2008, 12, 20),
            admission_reason=AdmissionReason.PROBATION_REVOCATION,
            admission_reason_raw_text='Revocation',
            release_date=date(2009, 1, 20),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE)

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1313,
            external_id='3',
            start_date=date(2008, 1, 1))

        incarceration_sentence = \
            FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
                StateIncarcerationSentence.new_with_defaults(
                    incarceration_sentence_id=123,
                    incarceration_periods=[temp_custody_period, revocation_period],
                    supervision_periods=[supervision_period],
                    start_date=date(2008, 1, 1),
                    charges=[
                        StateCharge.new_with_defaults(
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
            supervision_sentences=[supervision_sentence],
            incarceration_sentences=[incarceration_sentence])

        incarceration_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = identifier.find_incarceration_events(sentence_groups, _COUNTY_OF_RESIDENCE)

        self.maxDiff = None
        self.assertCountEqual([
            IncarcerationStayEvent(
                admission_reason=temp_custody_period.admission_reason,
                admission_reason_raw_text=temp_custody_period.admission_reason_raw_text,
                supervision_type_at_admission=StateSupervisionPeriodSupervisionType.DUAL,
                state_code=temp_custody_period.state_code,
                event_date=last_day_of_month(temp_custody_period.admission_date),
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
                event_date=last_day_of_month(revocation_period.admission_date),
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
                release_reason=revocation_period.release_reason
            )
        ], incarceration_events)

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
                admission_reason_raw_text='ADMISSION',
                release_date=date(2009, 1, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            start_date=date(2008, 10, 11),
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    offense_date=date(2007, 12, 11),
                    ncic_code='0901',
                    statute='9999'
                )
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            incarceration_sentences=[incarceration_sentence]
        )

        incarceration_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = identifier.find_incarceration_events(sentence_groups, _COUNTY_OF_RESIDENCE)

        self.assertCountEqual([
            IncarcerationStayEvent(
                admission_reason=AdmissionReason.NEW_ADMISSION,
                admission_reason_raw_text='ADMISSION',
                state_code=incarceration_period.state_code,
                event_date=last_day_of_month(incarceration_period.admission_date),
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                most_serious_offense_ncic_code='0901',
                most_serious_offense_statute='9999'
            ),
            IncarcerationStayEvent(
                admission_reason=AdmissionReason.NEW_ADMISSION,
                admission_reason_raw_text='ADMISSION',
                state_code=incarceration_period.state_code,
                event_date=last_day_of_month(incarceration_period.admission_date + relativedelta(months=1)),
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                most_serious_offense_ncic_code='0901',
                most_serious_offense_statute='9999'
            ),
            IncarcerationAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=AdmissionReason.NEW_ADMISSION,
                admission_reason_raw_text='ADMISSION',
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
                incarceration_period_id=2222,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON 10',
                admission_date=date(2009, 12, 1),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2010, 2, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            start_date=date(2008, 1, 11),
            incarceration_periods=[incarceration_period_1, incarceration_period_2],
            charges=[
                StateCharge.new_with_defaults(
                    offense_date=date(2007, 12, 11),
                    ncic_code='5511',
                    statute='9999'
                )
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            incarceration_sentences=[incarceration_sentence]
        )

        incarceration_period_1.incarceration_sentences = [incarceration_sentence]
        incarceration_period_2.incarceration_sentences = [incarceration_sentence]
        incarceration_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = identifier.find_incarceration_events(sentence_groups, _COUNTY_OF_RESIDENCE)

        # self.assertEqual(5, len(incarceration_events))
        self.maxDiff = None

        self.assertCountEqual([
            IncarcerationStayEvent(
                admission_reason=AdmissionReason.NEW_ADMISSION,
                state_code=incarceration_period_1.state_code,
                event_date=last_day_of_month(incarceration_period_1.admission_date),
                facility=incarceration_period_1.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                most_serious_offense_ncic_code='5511',
                most_serious_offense_statute='9999'
            ),
            IncarcerationStayEvent(
                admission_reason=AdmissionReason.TRANSFER,
                state_code=incarceration_period_2.state_code,
                event_date=last_day_of_month(incarceration_period_1.admission_date + relativedelta(months=1)),
                facility=incarceration_period_2.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                most_serious_offense_ncic_code='5511',
                most_serious_offense_statute='9999'
            ),
            IncarcerationStayEvent(
                admission_reason=AdmissionReason.TRANSFER,
                state_code=incarceration_period_2.state_code,
                event_date=last_day_of_month(incarceration_period_1.admission_date + relativedelta(months=2)),
                facility=incarceration_period_2.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                most_serious_offense_ncic_code='5511',
                most_serious_offense_statute='9999'
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

    def test_find_incarceration_events_multiple_sentences(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            facility='PRISON3',
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 1, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            start_date=date(2008, 10, 11),
            incarceration_periods=[incarceration_period]
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            start_date=date(2008, 10, 11),
            incarceration_periods=[incarceration_period]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            incarceration_sentences=[incarceration_sentence],
            supervision_sentences=[supervision_sentence]
        )

        incarceration_sentence.sentence_group = sentence_group
        supervision_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = identifier.find_incarceration_events(sentence_groups, _COUNTY_OF_RESIDENCE)

        self.assertCountEqual([
            IncarcerationStayEvent(
                admission_reason=AdmissionReason.NEW_ADMISSION,
                state_code=incarceration_period.state_code,
                event_date=last_day_of_month(incarceration_period.admission_date),
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
            ),
            IncarcerationStayEvent(
                admission_reason=AdmissionReason.NEW_ADMISSION,
                state_code=incarceration_period.state_code,
                event_date=last_day_of_month(incarceration_period.admission_date + relativedelta(months=1)),
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


class TestFindEndOfMonthStatePrisonStays(unittest.TestCase):
    """Tests the find_end_of_month_state_prison_stays function."""

    def _run_find_end_of_month_state_prison_stays_with_no_sentences(self, incarceration_period, county_of_residence):
        """Runs `find_end_of_month_state_prison_stays` without providing sentence information. Sentence information
        is only used in `US_MO` to inform supervision_type_at_admission. All tests using this method should not require
        that state specific logic.
        """
        incarceration_sentences = []
        supervision_sentences = []
        return identifier.find_end_of_month_state_prison_stays(
            incarceration_sentences,
            supervision_sentences,
            incarceration_period,
            county_of_residence)

    def test_find_end_of_month_prison_stays_type_us_mo(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_MO',
            facility='PRISON3',
            admission_date=date(2010, 1, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 3, 1),
            release_reason=ReleaseReason.SENTENCE_SERVED)
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

        sentence_group = StateSentenceGroup.new_with_defaults(sentence_group_id=6666, external_id='12345')
        supervision_sentence.sentence_group = sentence_group

        incarceration_sentences = []
        incarceration_events = identifier.find_end_of_month_state_prison_stays(
            incarceration_sentences,
            [supervision_sentence],
            incarceration_period,
            _COUNTY_OF_RESIDENCE)

        expected_event = IncarcerationStayEvent(
            admission_reason=incarceration_period.admission_reason,
            admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
            state_code=incarceration_period.state_code,
            facility=incarceration_period.facility,
            county_of_residence=_COUNTY_OF_RESIDENCE,
            event_date=last_day_of_month(incarceration_period.admission_date),
            supervision_type_at_admission=StateSupervisionPeriodSupervisionType.PROBATION,
        )
        expected_event_2 = IncarcerationStayEvent(
            admission_reason=incarceration_period.admission_reason,
            admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
            state_code=incarceration_period.state_code,
            facility=incarceration_period.facility,
            county_of_residence=_COUNTY_OF_RESIDENCE,
            event_date=last_day_of_month(incarceration_period.admission_date + relativedelta(months=1)),
            supervision_type_at_admission=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        self.assertCountEqual([expected_event, expected_event_2], incarceration_events)

    def test_find_end_of_month_state_prison_stays(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2000, 1, 20),
                admission_reason=AdmissionReason.PROBATION_REVOCATION,
                release_date=date(2010, 12, 1),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period]
        )
        incarceration_period.supervision_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(sentence_group_id=6666, external_id='12345')
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = \
            self._run_find_end_of_month_state_prison_stays_with_no_sentences(
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

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period]
        )
        incarceration_period.supervision_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(sentence_group_id=6666, external_id='12345')
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = \
            self._run_find_end_of_month_state_prison_stays_with_no_sentences(
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
            self._run_find_end_of_month_state_prison_stays_with_no_sentences(
                incarceration_period, _COUNTY_OF_RESIDENCE)

        self.assertEqual(0, len(incarceration_events))
        self.assertEqual([], incarceration_events)

    def test_find_end_of_month_state_prison_stays_no_release_reason(self):
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2000, 1, 20),
                admission_reason=AdmissionReason.PROBATION_REVOCATION,
                release_date=date(2010, 12, 1),
                release_reason=None)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period]
        )
        incarceration_period.supervision_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(sentence_group_id=6666, external_id='12345')
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = \
            self._run_find_end_of_month_state_prison_stays_with_no_sentences(
                incarceration_period, _COUNTY_OF_RESIDENCE)

        expected_month_count = 131

        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period, expected_month_count
        )

        self.assertEqual(expected_month_count, len(incarceration_events))
        self.assertEqual(expected_incarceration_events, incarceration_events)

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

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period]
        )
        incarceration_period.supervision_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(sentence_group_id=6666, external_id='12345')
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = self._run_find_end_of_month_state_prison_stays_with_no_sentences(
            incarceration_period, _COUNTY_OF_RESIDENCE)

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
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period]
        )
        incarceration_period.supervision_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(sentence_group_id=6666, external_id='12345')
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = self._run_find_end_of_month_state_prison_stays_with_no_sentences(
            incarceration_period, _COUNTY_OF_RESIDENCE)

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
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period]
        )
        incarceration_period.supervision_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(sentence_group_id=6666, external_id='12345')
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = \
            self._run_find_end_of_month_state_prison_stays_with_no_sentences(
                incarceration_period, _COUNTY_OF_RESIDENCE
            )

        # We do not count the termination date of an incarceration period as a day the person is incarcerated.
        expected_month_count = 0
        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period, expected_month_count
        )

        self.assertEqual(expected_month_count, len(incarceration_events))
        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_end_of_month_state_prison_stays_transfers_end_of_month(self):
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
                release_reason=ReleaseReason.TRANSFER)
        incarceration_period_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                facility='PRISON4',
                admission_date=date(2019, 11, 30),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2019, 12, 1),
                release_reason=ReleaseReason.SENTENCE_SERVED)
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period, incarceration_period_2]
        )
        incarceration_period.supervision_sentences = [incarceration_sentence]
        incarceration_period_2.supervision_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(sentence_group_id=6666, external_id='12345')
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events_period_1 = \
            self._run_find_end_of_month_state_prison_stays_with_no_sentences(
                incarceration_period, _COUNTY_OF_RESIDENCE
            )

        expected_month_count = 0
        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period, expected_month_count
        )

        self.assertEqual(expected_month_count, len(incarceration_events_period_1))
        self.assertEqual(expected_incarceration_events, incarceration_events_period_1)

        incarceration_events_period_2 = \
            self._run_find_end_of_month_state_prison_stays_with_no_sentences(
                incarceration_period_2, _COUNTY_OF_RESIDENCE
            )

        expected_month_count = 1
        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period_2, expected_month_count
        )

        self.assertEqual(expected_month_count, len(incarceration_events_period_2))
        self.assertEqual(expected_incarceration_events, incarceration_events_period_2)

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
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period]
        )
        incarceration_period.supervision_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(sentence_group_id=6666, external_id='12345')
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = \
            self._run_find_end_of_month_state_prison_stays_with_no_sentences(
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

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period]
        )
        incarceration_period.supervision_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(sentence_group_id=6666, external_id='12345')
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = \
            self._run_find_end_of_month_state_prison_stays_with_no_sentences(
                incarceration_period, _COUNTY_OF_RESIDENCE
            )

        # We do not count people who were released on the last day of the month as being incarcerated on that last day.
        # In normal circumstances, if this person remained incarcerated but had a quick, one-day transfer, there will
        # be another incarceration period that opens on the last day of the month with a later termination date - we
        # *will* count this one.
        expected_month_count = 0
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
            self._run_find_end_of_month_state_prison_stays_with_no_sentences(
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

    def _run_admission_event_for_period_with_no_sentences(self, incarceration_period, county_of_residence):
        """Runs `admission_event_for_period` without providing sentence information. Sentence information
        is only used in `US_MO` to inform supervision_type_at_admission. All tests using this method should not require
        that state specific logic.
        """
        incarceration_sentences = []
        supervision_sentences = []
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
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 3, 1),
            release_reason=ReleaseReason.SENTENCE_SERVED)
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

    def test_admission_event_for_period(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            facility='PRISON3',
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.DUAL_REVOCATION,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED)

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
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2013, 11, 20),
                release_date=date(2019, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        for admission_reason in AdmissionReason:
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
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
                release_date=date(2010, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

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
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2013, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2019, 12, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        admission_event = self._run_admission_event_for_period_with_no_sentences(
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


class TestGetUniquePeriodsFromSentenceGroupAndAddBackedges(unittest.TestCase):
    """Tests the get_unique_periods_from_sentence_groups_and_add_backedges function."""
    def test_get_unique_periods_from_sentence_groups_and_add_backedges(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1112,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            state_code='TX',
            start_date=date(2008, 11, 20),
            termination_date=date(2010, 12, 4))

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            facility='PRISON3',
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            facility='PRISON3',
            admission_date=date(2011, 1, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 4),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE)

        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            facility='PRISON3',
            admission_date=date(2013, 5, 22),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2015, 11, 9),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        sentence_groups = [
            StateSentenceGroup.new_with_defaults(
                incarceration_sentences=[
                    StateIncarcerationSentence.new_with_defaults(
                        incarceration_periods=[incarceration_period_1, incarceration_period_2]
                    )
                ],
                supervision_sentences=[
                    StateSupervisionSentence.new_with_defaults(
                        incarceration_periods=[incarceration_period_1, incarceration_period_3]
                    )
                ]
            ),
            StateSentenceGroup.new_with_defaults(
                incarceration_sentences=[
                    StateIncarcerationSentence.new_with_defaults(
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
            StateSentenceGroup.new_with_defaults(),
            StateSentenceGroup.new_with_defaults()
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
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2009, 1, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            start_date=date(2007, 12, 10),
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    offense_date=date(2007, 12, 11),
                    ncic_code='2703',
                    statute='9999'
                ),
                StateCharge.new_with_defaults(
                    offense_date=date(2007, 12, 11),
                    ncic_code='1316',
                    statute='8888'
                ),
                StateCharge.new_with_defaults(
                    offense_date=date(2007, 12, 11),
                    ncic_code='3619',
                    statute='7777'
                )
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
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
            state_code='TX',
            facility='PRISON3',
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 1, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_sentence_1 = StateIncarcerationSentence.new_with_defaults(
            start_date=date(2007, 12, 10),
            incarceration_periods=[incarceration_period_1],
            charges=[
                StateCharge.new_with_defaults(
                    offense_date=date(2007, 12, 11),
                    ncic_code='3606',
                    statute='3606',
                ),
                StateCharge.new_with_defaults(
                    offense_date=date(2007, 12, 11),
                    ncic_code='3611',
                    statute='3611',
                ),
                StateCharge.new_with_defaults(
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
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2003, 1, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2009, 1, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_sentence_2 = StateIncarcerationSentence.new_with_defaults(
            start_date=date(2007, 12, 10),
            incarceration_periods=[incarceration_period_2],
            charges=[
                StateCharge.new_with_defaults(
                    offense_date=date(2001, 12, 11),
                    ncic_code='3907',
                    statute='3907'
                ),
                StateCharge.new_with_defaults(
                    offense_date=date(2001, 12, 11),
                    ncic_code='3909',
                    statute='3909'
                ),
                StateCharge.new_with_defaults(
                    offense_date=date(2001, 12, 11),
                    ncic_code='3912',
                    statute='3912'
                )
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
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
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2009, 1, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_sentence_1 = StateIncarcerationSentence.new_with_defaults(
            start_date=date(2007, 12, 10),
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    offense_date=date(2007, 12, 11),
                    ncic_code='3611',
                    statute='1111'
                ),
                StateCharge.new_with_defaults(
                    offense_date=date(2007, 12, 11),
                    ncic_code='3623',
                    statute='3333'
                )
            ]
        )

        incarceration_sentence_2 = StateIncarcerationSentence.new_with_defaults(
            start_date=date(2010, 12, 10),
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    offense_date=date(2010, 12, 11),
                    ncic_code='3606',
                    statute='9999'
                ),
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
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
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2009, 1, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    offense_date=date(2010, 12, 11),
                    statute='9999'
                ),
                StateCharge.new_with_defaults(
                    offense_date=date(2007, 12, 11),
                    statute='1111'
                ),
                StateCharge.new_with_defaults(
                    offense_date=date(2007, 12, 11),
                    statute='3333'
                )
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
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
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2009, 1, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            start_date=date(2007, 12, 10),
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    offense_date=date(2007, 12, 11),
                    ncic_code='040A',
                    statute='xxxx'
                ),
                StateCharge.new_with_defaults(
                    offense_date=date(2007, 12, 11),
                    ncic_code='0101',
                    statute='9999'
                ),
                StateCharge.new_with_defaults(
                    offense_date=date(2007, 12, 11),
                    ncic_code='5301',
                    statute='1111'
                )
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
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
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2009, 1, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            start_date=date(2007, 12, 10),
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    ncic_code='2703',
                    statute='9999'
                ),
                StateCharge.new_with_defaults(
                    ncic_code='1316',
                    statute='8888'
                ),
                StateCharge.new_with_defaults(
                    ncic_code='3619',
                    statute='7777'
                )
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
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
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2009, 1, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            start_date=date(2009, 12, 10),
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    offense_date=date(2007, 12, 10),
                    ncic_code='2703',
                    statute='9999'
                ),
                StateCharge.new_with_defaults(
                    offense_date=date(2007, 12, 10),
                    ncic_code='1316',
                    statute='8888'
                ),
                StateCharge.new_with_defaults(
                    offense_date=date(2007, 12, 10),
                    ncic_code='3619',
                    statute='7777'
                )
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
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
                state_code='TX',
                facility='PRISON3',
                admission_date=date(2008, 11, 20),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2009, 1, 4),
                release_reason=ReleaseReason.SENTENCE_SERVED)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            start_date=date(2007, 12, 10),
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    offense_date=date(2009, 12, 10),
                    ncic_code='2703',
                    statute='9999'
                ),
                StateCharge.new_with_defaults(
                    offense_date=date(2009, 12, 10),
                    ncic_code='1316',
                    statute='8888'
                ),
                StateCharge.new_with_defaults(
                    offense_date=date(2009, 12, 10),
                    ncic_code='3619',
                    statute='7777'
                )
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            incarceration_sentences=[incarceration_sentence]
        )

        incarceration_period.incarceration_sentences = [incarceration_sentence]
        incarceration_sentence.sentence_group = sentence_group

        most_serious_statute = identifier.find_most_serious_prior_charge_in_sentence_group(
            incarceration_period, date(2008, 12, 31)).statute

        self.assertEqual(most_serious_statute, '8888')


def expected_incarceration_stay_events(
        incarceration_period: StateIncarcerationPeriod,
        expected_month_count: int) -> List[IncarcerationStayEvent]:
    """Returns the expected incarceration stay events based on only the provided |incarceration_period| and the
    |expected_month_count.
    """

    expected_incarceration_events = []
    months_incarcerated_eom_range = range(0, expected_month_count, 1)

    if incarceration_period.admission_date:
        for month in months_incarcerated_eom_range:
            supervision_type = None
            if incarceration_period.admission_reason == StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION:
                supervision_type = StateSupervisionPeriodSupervisionType.PAROLE
            if incarceration_period.admission_reason == StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION:
                supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
            if incarceration_period.admission_reason == StateIncarcerationPeriodAdmissionReason.DUAL_REVOCATION:
                supervision_type = StateSupervisionPeriodSupervisionType.DUAL

            event = IncarcerationStayEvent(
                admission_reason=incarceration_period.admission_reason,
                admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
                state_code=incarceration_period.state_code,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                event_date=last_day_of_month(incarceration_period.admission_date + relativedelta(months=month)),
                supervision_type_at_admission=supervision_type,
            )

            expected_incarceration_events.append(event)

    return expected_incarceration_events
