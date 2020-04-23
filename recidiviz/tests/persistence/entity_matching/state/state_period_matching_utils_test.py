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
"""Tests for state_period_matching_utils.py"""
import datetime

import attr

from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodStatus
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_entity_converter import \
    schema_entity_converter as converter
from recidiviz.persistence.entity.state.entities import \
    StatePerson, StateSentenceGroup, StateSupervisionPeriod, StateSupervisionSentence, StateIncarcerationSentence, \
    StateIncarcerationPeriod
from recidiviz.persistence.entity_matching.state.state_period_matching_utils import \
    add_supervising_officer_to_open_supervision_periods, move_periods_onto_sentences_by_date, \
    _is_sentence_ended_by_status
from recidiviz.tests.persistence.database.schema.state.schema_test_utils \
    import generate_agent, generate_person, generate_external_id, \
    generate_supervision_period, generate_supervision_sentence, \
    generate_sentence_group
from recidiviz.tests.persistence.entity_matching.state.base_state_entity_matcher_test_classes import \
    BaseStateMatchingUtilsTest

_DATE_1 = datetime.date(year=2019, month=1, day=1)
_DATE_2 = datetime.date(year=2019, month=2, day=1)
_DATE_3 = datetime.date(year=2019, month=3, day=1)
_DATE_4 = datetime.date(year=2019, month=4, day=1)
_DATE_5 = datetime.date(year=2019, month=5, day=1)
_DATE_6 = datetime.date(year=2019, month=6, day=1)
_DATE_7 = datetime.date(year=2019, month=7, day=1)
_DATE_8 = datetime.date(year=2019, month=8, day=1)
_EXTERNAL_ID = 'EXTERNAL_ID-1'
_EXTERNAL_ID_2 = 'EXTERNAL_ID-2'
_EXTERNAL_ID_3 = 'EXTERNAL_ID-3'
_ID = 1
_ID_2 = 2
_ID_3 = 3
_STATE_CODE = 'NC'
_ID_TYPE = 'ID_TYPE'


# pylint: disable=protected-access
class TestStatePeriodMatchingUtils(BaseStateMatchingUtilsTest):
    """Tests for state period matching utils"""

    def test_addSupervisingOfficerToOpenSupervisionPeriods(self):
        # Arrange
        supervising_officer = generate_agent(agent_id=_ID, external_id=_EXTERNAL_ID, state_code=_STATE_CODE)
        person = generate_person(person_id=_ID, supervising_officer=supervising_officer)
        external_id = generate_external_id(
            person_external_id_id=_ID, external_id=_EXTERNAL_ID, state_code=_STATE_CODE, id_type=_ID_TYPE)
        open_supervision_period = generate_supervision_period(
            person=person,
            supervision_period_id=_ID,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_1,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE)
        placeholder_supervision_period = generate_supervision_period(
            person=person,
            supervision_period_id=_ID_2,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE)
        closed_supervision_period = generate_supervision_period(
            person=person,
            supervision_period_id=_ID_3,
            external_id=_EXTERNAL_ID_3,
            start_date=_DATE_3,
            termination_date=_DATE_4,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE)
        supervision_sentence = generate_supervision_sentence(
            person=person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            supervision_sentence_id=_ID,
            supervision_periods=[open_supervision_period, placeholder_supervision_period, closed_supervision_period])
        sentence_group = generate_sentence_group(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            sentence_group_id=_ID,
            supervision_sentences=[supervision_sentence])
        person.external_ids = [external_id]
        person.sentence_groups = [sentence_group]

        # Act
        add_supervising_officer_to_open_supervision_periods([person])

        # Assert
        self.assertEqual(open_supervision_period.supervising_officer, supervising_officer)
        self.assertIsNone(placeholder_supervision_period.supervising_officer, supervising_officer)
        self.assertIsNone(closed_supervision_period.supervising_officer, supervising_officer)

    def test_associatedPeriodsWithSentences_periodStartsBeforeAndEndsAfterSentence(self):
        sp = StateSupervisionPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID, start_date=_DATE_1, termination_date=_DATE_4)
        placeholder_ss = StateSupervisionSentence.new_with_defaults(supervision_periods=[sp])

        ss = StateSupervisionSentence.new_with_defaults(
            external_id=_EXTERNAL_ID, start_date=_DATE_2, completion_date=_DATE_3, status=StateSentenceStatus.SERVING)

        sg = StateSentenceGroup.new_with_defaults(supervision_sentences=[ss, placeholder_ss])

        state_person = StatePerson.new_with_defaults(sentence_groups=[sg])

        expected_sp = attr.evolve(sp)

        expected_placeholder_ss = attr.evolve(placeholder_ss, supervision_periods=[], incarceration_periods=[])
        expected_ss = attr.evolve(ss, supervision_periods=[expected_sp])
        expected_sg = attr.evolve(sg, supervision_sentences=[expected_ss, expected_placeholder_ss])
        expected_person = attr.evolve(state_person, sentence_groups=[expected_sg])

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_periods_onto_sentences_by_date(input_people)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_associatedPeriodsWithSentences_oneDayPeriodOverlapsWithStartOfSentence(self):
        sp = StateSupervisionPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID, start_date=_DATE_2, termination_date=_DATE_2)
        placeholder_ss = StateSupervisionSentence.new_with_defaults(supervision_periods=[sp])

        ss = StateSupervisionSentence.new_with_defaults(
            external_id=_EXTERNAL_ID, start_date=_DATE_2, completion_date=_DATE_3, status=StateSentenceStatus.SERVING)

        sg = StateSentenceGroup.new_with_defaults(supervision_sentences=[ss, placeholder_ss])

        state_person = StatePerson.new_with_defaults(sentence_groups=[sg])

        expected_sp = attr.evolve(sp)

        expected_placeholder_ss = attr.evolve(placeholder_ss, supervision_periods=[], incarceration_periods=[])
        expected_ss = attr.evolve(ss, supervision_periods=[expected_sp])
        expected_sg = attr.evolve(sg, supervision_sentences=[expected_ss, expected_placeholder_ss])
        expected_person = attr.evolve(state_person, sentence_groups=[expected_sg])

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_periods_onto_sentences_by_date(input_people)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_associatedPeriodsWithSentences(self):
        # Arrange
        sp_no_match = StateSupervisionPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID, start_date=_DATE_1, termination_date=_DATE_3)

        sp_1 = StateSupervisionPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID, start_date=_DATE_4, termination_date=_DATE_6)

        sp_2 = StateSupervisionPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_2, start_date=_DATE_6, termination_date=None)

        ip_1 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID, admission_date=_DATE_6, release_date=None)

        placeholder_ss = StateSupervisionSentence.new_with_defaults(
            supervision_periods=[sp_no_match, sp_1, sp_2], incarceration_periods=[ip_1])

        ss = StateSupervisionSentence.new_with_defaults(
            external_id=_EXTERNAL_ID, start_date=_DATE_4, completion_date=None, status=StateSentenceStatus.SERVING)
        inc_s = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID, start_date=_DATE_3, completion_date=_DATE_5, status=StateSentenceStatus.COMPLETED)

        sg = StateSentenceGroup.new_with_defaults(
            incarceration_sentences=[inc_s], supervision_sentences=[ss, placeholder_ss])

        state_person = StatePerson.new_with_defaults(sentence_groups=[sg])

        expected_sp_no_match = attr.evolve(sp_no_match)
        expected_sp_1 = attr.evolve(sp_1)
        expected_sp_2 = attr.evolve(sp_2)
        expected_ip_1 = attr.evolve(ip_1)

        expected_placeholder_ss = attr.evolve(
            placeholder_ss, supervision_periods=[expected_sp_no_match], incarceration_periods=[])
        expected_inc_s = attr.evolve(inc_s, supervision_periods=[expected_sp_1], incarceration_periods=[])
        expected_ss = attr.evolve(
            ss, incarceration_periods=[expected_ip_1], supervision_periods=[expected_sp_1, expected_sp_2])
        expected_sg = attr.evolve(
            sg, supervision_sentences=[expected_ss, expected_placeholder_ss], incarceration_sentences=[expected_inc_s])
        expected_person = attr.evolve(state_person, sentence_groups=[expected_sg])

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_periods_onto_sentences_by_date(input_people)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_associatedPeriodsWithSentences_justIncPeriods(self):
        # Arrange
        sp_1 = StateSupervisionPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID, start_date=_DATE_6, termination_date=None)

        ip_1 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID, admission_date=_DATE_6, release_date=None)

        placeholder_ss = StateSupervisionSentence.new_with_defaults(
            supervision_periods=[sp_1], incarceration_periods=[ip_1])
        ss = StateSupervisionSentence.new_with_defaults(
            external_id=_EXTERNAL_ID, start_date=_DATE_4, completion_date=None)
        inc_s = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID, start_date=_DATE_4, completion_date=None)

        sg = StateSentenceGroup.new_with_defaults(
            incarceration_sentences=[inc_s], supervision_sentences=[ss, placeholder_ss])

        state_person = StatePerson.new_with_defaults(sentence_groups=[sg])

        expected_sp_1 = attr.evolve(sp_1)
        expected_ip_1 = attr.evolve(ip_1)

        expected_placeholder_ss = attr.evolve(
            placeholder_ss, supervision_periods=[expected_sp_1], incarceration_periods=[])
        expected_inc_s = attr.evolve(inc_s, supervision_periods=[], incarceration_periods=[expected_ip_1])
        expected_ss = attr.evolve(
            ss, incarceration_periods=[expected_ip_1], supervision_periods=[])
        expected_sg = attr.evolve(
            sg, supervision_sentences=[expected_ss, expected_placeholder_ss], incarceration_sentences=[expected_inc_s])
        expected_person = attr.evolve(state_person, sentence_groups=[expected_sg])

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_periods_onto_sentences_by_date(input_people, period_filter=schema.StateIncarcerationPeriod)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_associatedPeriodsWithSentences_justSupervisionPeriods(self):
        # Arrange
        sp_1 = StateSupervisionPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID, start_date=_DATE_6, termination_date=None)

        ip_1 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID, admission_date=_DATE_6, release_date=None)

        placeholder_ss = StateSupervisionSentence.new_with_defaults(
            supervision_periods=[sp_1], incarceration_periods=[ip_1])
        ss = StateSupervisionSentence.new_with_defaults(
            external_id=_EXTERNAL_ID, start_date=_DATE_4, completion_date=None)
        inc_s = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID, start_date=_DATE_4, completion_date=None)

        sg = StateSentenceGroup.new_with_defaults(
            incarceration_sentences=[inc_s], supervision_sentences=[ss, placeholder_ss])

        state_person = StatePerson.new_with_defaults(sentence_groups=[sg])

        expected_sp_1 = attr.evolve(sp_1)
        expected_ip_1 = attr.evolve(ip_1)

        expected_placeholder_ss = attr.evolve(
            placeholder_ss, supervision_periods=[], incarceration_periods=[expected_ip_1])
        expected_inc_s = attr.evolve(inc_s, supervision_periods=[expected_sp_1], incarceration_periods=[])
        expected_ss = attr.evolve(ss, incarceration_periods=[], supervision_periods=[expected_sp_1])
        expected_sg = attr.evolve(
            sg, supervision_sentences=[expected_ss, expected_placeholder_ss], incarceration_sentences=[expected_inc_s])
        expected_person = attr.evolve(state_person, sentence_groups=[expected_sg])

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_periods_onto_sentences_by_date(input_people, period_filter=schema.StateSupervisionPeriod)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_associatedPeriodsWithSentences_doNotAssociateToClosedButUnterminatedSentences(self):
        # Arrange
        sp_1 = StateSupervisionPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID, start_date=_DATE_6, termination_date=None)

        ip_1 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID, admission_date=_DATE_6, release_date=None)

        placeholder_ss = StateSupervisionSentence.new_with_defaults(
            supervision_periods=[sp_1], incarceration_periods=[ip_1])
        ss = StateSupervisionSentence.new_with_defaults(
            external_id=_EXTERNAL_ID, status=StateSentenceStatus.COMPLETED, start_date=_DATE_4, completion_date=None)
        inc_s = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID, status=StateSentenceStatus.COMPLETED, start_date=_DATE_4, completion_date=None)

        sg = StateSentenceGroup.new_with_defaults(
            incarceration_sentences=[inc_s], supervision_sentences=[ss, placeholder_ss])

        state_person = StatePerson.new_with_defaults(sentence_groups=[sg])

        expected_sp_1 = attr.evolve(sp_1)
        expected_ip_1 = attr.evolve(ip_1)

        expected_placeholder_ss = attr.evolve(
            placeholder_ss, supervision_periods=[expected_sp_1], incarceration_periods=[expected_ip_1])
        expected_inc_s = attr.evolve(inc_s)
        expected_ss = attr.evolve(ss)
        expected_sg = attr.evolve(
            sg, supervision_sentences=[expected_ss, expected_placeholder_ss], incarceration_sentences=[expected_inc_s])
        expected_person = attr.evolve(state_person, sentence_groups=[expected_sg])

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_periods_onto_sentences_by_date(input_people)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_associatePeriodsWithSentence_doNotMatchSentenceWithNoStart(self):
        # Arrange
        placeholder_sp = StateSupervisionPeriod.new_with_defaults()

        sp = StateSupervisionPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_3, start_date=_DATE_2, termination_date=_DATE_3)
        ip = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_3, admission_date=_DATE_2, release_date=_DATE_3)

        inc_s_no_dates = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID, supervision_periods=[placeholder_sp])

        placeholder_inc_s = StateIncarcerationSentence.new_with_defaults(
            incarceration_periods=[ip], supervision_periods=[sp])

        sg = StateSentenceGroup.new_with_defaults(incarceration_sentences=[inc_s_no_dates, placeholder_inc_s])

        state_person = StatePerson.new_with_defaults(sentence_groups=[sg])

        # Should remain unchanged - the non-placeholder period should not get moved onto sentence with an id
        # but no start date
        expected_person = attr.evolve(state_person)

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_periods_onto_sentences_by_date(input_people)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_associatePeriodsWithSentence_periodNoLongerMatches(self):
        # Arrange
        sp_which_no_longer_overlaps = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_ID, external_id=_EXTERNAL_ID, state_code=_STATE_CODE, start_date=_DATE_6)
        ip_which_no_longer_overlaps = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_ID, external_id=_EXTERNAL_ID, state_code=_STATE_CODE, admission_date=_DATE_6)

        # This sentence, which has already been written to the DB, has presumably been updated so that the date range no
        # longer overlaps with the attached periods.
        inc_s_updated_dates = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=_ID_2,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            start_date=_DATE_3,
            completion_date=_DATE_5,
            incarceration_periods=[ip_which_no_longer_overlaps],
            supervision_periods=[sp_which_no_longer_overlaps])

        sg = StateSentenceGroup.new_with_defaults(
            sentence_group_id=_ID_3, state_code=_STATE_CODE, incarceration_sentences=[inc_s_updated_dates])

        state_person = StatePerson.new_with_defaults(person_id=_ID, sentence_groups=[sg])

        expected_sp = attr.evolve(sp_which_no_longer_overlaps)
        expected_ip = attr.evolve(ip_which_no_longer_overlaps)
        expected_is = attr.evolve(inc_s_updated_dates, incarceration_periods=[], supervision_periods=[])

        # We expect that a new placeholder supervision sentence has been created to hold on to the orphaned periods
        expected_placeholder_ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_periods=[expected_ip],
            supervision_periods=[expected_sp])
        expected_sg = attr.evolve(
            sg, incarceration_sentences=[expected_is], supervision_sentences=[expected_placeholder_ss])
        expected_person = attr.evolve(state_person, sentence_groups=[expected_sg])

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_periods_onto_sentences_by_date(input_people)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_associatePeriodsWithSentence_doNotMoveOntoPlaceholder(self):
        # Arrange
        placeholder_inc_s = StateIncarcerationSentence.new_with_defaults()

        sp = StateSupervisionPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID, start_date=_DATE_2, termination_date=_DATE_3)
        ip = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID, admission_date=_DATE_2, release_date=_DATE_3)

        inc_s = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            start_date=_DATE_1,
            completion_date=_DATE_8,
            incarceration_periods=[ip],
            supervision_periods=[sp])

        sg = StateSentenceGroup.new_with_defaults(incarceration_sentences=[inc_s, placeholder_inc_s])

        state_person = StatePerson.new_with_defaults(sentence_groups=[sg])

        # Should remain unchanged - periods should not get attached to other placeholder sentence.
        expected_person = attr.evolve(state_person)

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_periods_onto_sentences_by_date(input_people)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_associatePeriodsWithSentence_doNotMovePlaceholderPeriods(self):
        # Arrange
        placeholder_sp = StateSupervisionPeriod.new_with_defaults()
        placeholder_ip = StateIncarcerationPeriod.new_with_defaults()

        inc_s_2 = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID, start_date=_DATE_1, completion_date=_DATE_8)

        inc_s = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            start_date=_DATE_1,
            completion_date=_DATE_8,
            incarceration_periods=[placeholder_ip],
            supervision_periods=[placeholder_sp])

        sg = StateSentenceGroup.new_with_defaults(incarceration_sentences=[inc_s, inc_s_2])

        state_person = StatePerson.new_with_defaults(sentence_groups=[sg])

        # Should remain unchanged - placeholder period should not get attached to any other sentences
        expected_person = attr.evolve(state_person)

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_periods_onto_sentences_by_date(input_people)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_isSentenceEndedByStatus(self):
        sentence = StateSupervisionSentence.new_with_defaults()
        for status in StateSentenceStatus:
            sentence.status = status
            _is_sentence_ended_by_status(converter.convert_entity_to_schema_object(sentence))
