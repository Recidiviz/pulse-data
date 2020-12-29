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
"""Tests for state_date_based_matching_utils.py"""
import datetime

import attr

from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodStatus
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_entity_converter import \
    schema_entity_converter as converter
from recidiviz.persistence.entity.state.entities import \
    StatePerson, StateSentenceGroup, StateSupervisionPeriod, StateSupervisionSentence, StateIncarcerationSentence, \
    StateIncarcerationPeriod, StateSupervisionViolation, StateSupervisionViolationResponse, StateSupervisionContact
from recidiviz.persistence.entity_matching.state.state_date_based_matching_utils import \
    move_periods_onto_sentences_by_date, _is_sentence_ended_by_status, \
    move_violations_onto_supervision_periods_for_sentence, move_violations_onto_supervision_periods_for_person, \
    move_contacts_onto_supervision_periods_for_person
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
_EXTERNAL_ID_4 = 'EXTERNAL_ID-4'
_ID = 1
_ID_2 = 2
_ID_3 = 3
_STATE_CODE = 'NC'
_ID_TYPE = 'ID_TYPE'


# pylint: disable=protected-access
class TestStateDateBasedMatchingUtils(BaseStateMatchingUtilsTest):
    """Tests for state date based matching utils"""

    def test_associatedPeriodsWithSentences_periodStartsBeforeAndEndsAfterSentence(self) -> None:
        sp = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, start_date=_DATE_1, termination_date=_DATE_4)
        placeholder_ss = StateSupervisionSentence.new_with_defaults(state_code=_STATE_CODE, supervision_periods=[sp])

        ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_2,
            completion_date=_DATE_3,
            status=StateSentenceStatus.SERVING)

        sg = StateSentenceGroup.new_with_defaults(state_code=_STATE_CODE, supervision_sentences=[ss, placeholder_ss])

        state_person = StatePerson.new_with_defaults(state_code=_STATE_CODE, sentence_groups=[sg])

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

    def test_associatedPeriodsWithSentences_oneDayPeriodOverlapsWithStartOfSentence(self) -> None:
        sp = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, start_date=_DATE_2, termination_date=_DATE_2)
        placeholder_ss = StateSupervisionSentence.new_with_defaults(state_code=_STATE_CODE, supervision_periods=[sp])

        ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_2,
            completion_date=_DATE_3, status=StateSentenceStatus.SERVING)

        sg = StateSentenceGroup.new_with_defaults(state_code=_STATE_CODE, supervision_sentences=[ss, placeholder_ss])

        state_person = StatePerson.new_with_defaults(state_code=_STATE_CODE, sentence_groups=[sg])

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

    def test_associatedPeriodsWithSentences(self) -> None:
        # Arrange
        sp_no_match = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, start_date=_DATE_1, termination_date=_DATE_3)

        sp_1 = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, start_date=_DATE_4, termination_date=_DATE_6)

        sp_2 = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2, start_date=_DATE_6, termination_date=None)

        ip_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, admission_date=_DATE_6, release_date=None)

        placeholder_ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE, supervision_periods=[sp_no_match, sp_1, sp_2], incarceration_periods=[ip_1])

        ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_4,
            completion_date=None,
            status=StateSentenceStatus.SERVING)
        inc_s = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_3,
            completion_date=_DATE_5,
            status=StateSentenceStatus.COMPLETED)

        sg = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE, incarceration_sentences=[inc_s], supervision_sentences=[ss, placeholder_ss])

        state_person = StatePerson.new_with_defaults(state_code=_STATE_CODE, sentence_groups=[sg])

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

    def test_associatedPeriodsWithSentences_justIncPeriods(self) -> None:
        # Arrange
        sp_1 = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, start_date=_DATE_6, termination_date=None)

        ip_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, admission_date=_DATE_6, release_date=None)

        placeholder_ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE, supervision_periods=[sp_1], incarceration_periods=[ip_1])
        ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, start_date=_DATE_4, completion_date=None)
        inc_s = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, start_date=_DATE_4, completion_date=None)

        sg = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE, incarceration_sentences=[inc_s], supervision_sentences=[ss, placeholder_ss])

        state_person = StatePerson.new_with_defaults(state_code=_STATE_CODE, sentence_groups=[sg])

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

    def test_associatedPeriodsWithSentences_justSupervisionPeriods(self) -> None:
        # Arrange
        sp_1 = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, start_date=_DATE_6, termination_date=None)

        ip_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, admission_date=_DATE_6, release_date=None)

        placeholder_ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE, supervision_periods=[sp_1], incarceration_periods=[ip_1])
        ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, start_date=_DATE_4, completion_date=None)
        inc_s = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, start_date=_DATE_4, completion_date=None)

        sg = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE, incarceration_sentences=[inc_s], supervision_sentences=[ss, placeholder_ss])

        state_person = StatePerson.new_with_defaults(state_code=_STATE_CODE, sentence_groups=[sg])

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

    def test_associatedPeriodsWithSentences_doNotAssociateToClosedButUnterminatedSentences(self) -> None:
        # Arrange
        sp_1 = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, start_date=_DATE_6, termination_date=None)

        ip_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, admission_date=_DATE_6, release_date=None)

        placeholder_ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE, supervision_periods=[sp_1], incarceration_periods=[ip_1])
        ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.COMPLETED,
            start_date=_DATE_4,
            completion_date=None)
        inc_s = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.COMPLETED,
            start_date=_DATE_4,
            completion_date=None)

        sg = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE, incarceration_sentences=[inc_s], supervision_sentences=[ss, placeholder_ss])

        state_person = StatePerson.new_with_defaults(state_code=_STATE_CODE, sentence_groups=[sg])

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

    def test_associatePeriodsWithSentence_doNotMatchSentenceWithNoStart(self) -> None:
        # Arrange
        placeholder_sp = StateSupervisionPeriod.new_with_defaults(state_code=_STATE_CODE)

        sp = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_3, start_date=_DATE_2, termination_date=_DATE_3)
        ip = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_3, admission_date=_DATE_2, release_date=_DATE_3)

        inc_s_no_dates = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, supervision_periods=[placeholder_sp])

        placeholder_inc_s = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE, incarceration_periods=[ip], supervision_periods=[sp])

        sg = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE, incarceration_sentences=[inc_s_no_dates, placeholder_inc_s])

        state_person = StatePerson.new_with_defaults(state_code=_STATE_CODE, sentence_groups=[sg])

        # Should remain unchanged - the non-placeholder period should not get moved onto sentence with an id
        # but no start date
        expected_person = attr.evolve(state_person)

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_periods_onto_sentences_by_date(input_people)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_associatePeriodsWithSentence_periodNoLongerMatches(self) -> None:
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

        state_person = StatePerson.new_with_defaults(state_code=_STATE_CODE, person_id=_ID, sentence_groups=[sg])

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

    def test_associatePeriodsWithSentence_doNotMoveOntoPlaceholder(self) -> None:
        # Arrange
        placeholder_inc_s = StateIncarcerationSentence.new_with_defaults(state_code=_STATE_CODE)

        sp = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, start_date=_DATE_2, termination_date=_DATE_3)
        ip = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, admission_date=_DATE_2, release_date=_DATE_3)

        inc_s = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_1,
            completion_date=_DATE_8,
            incarceration_periods=[ip],
            supervision_periods=[sp])

        sg = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE, incarceration_sentences=[inc_s, placeholder_inc_s])

        state_person = StatePerson.new_with_defaults(state_code=_STATE_CODE, sentence_groups=[sg])

        # Should remain unchanged - periods should not get attached to other placeholder sentence.
        expected_person = attr.evolve(state_person)

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_periods_onto_sentences_by_date(input_people)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_associatePeriodsWithSentence_doNotMovePlaceholderPeriods(self) -> None:
        # Arrange
        placeholder_sp = StateSupervisionPeriod.new_with_defaults(state_code=_STATE_CODE)
        placeholder_ip = StateIncarcerationPeriod.new_with_defaults(state_code=_STATE_CODE)

        inc_s_2 = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, start_date=_DATE_1, completion_date=_DATE_8)

        inc_s = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2,
            start_date=_DATE_1,
            completion_date=_DATE_8,
            incarceration_periods=[placeholder_ip],
            supervision_periods=[placeholder_sp])

        sg = StateSentenceGroup.new_with_defaults(state_code=_STATE_CODE, incarceration_sentences=[inc_s, inc_s_2])

        state_person = StatePerson.new_with_defaults(state_code=_STATE_CODE, sentence_groups=[sg])

        # Should remain unchanged - placeholder period should not get attached to any other sentences
        expected_person = attr.evolve(state_person)

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_periods_onto_sentences_by_date(input_people)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_isSentenceEndedByStatus(self) -> None:
        sentence = StateSupervisionSentence.new_with_defaults(state_code=_STATE_CODE)
        for status in StateSentenceStatus:
            sentence.status = status
            _is_sentence_ended_by_status(converter.convert_entity_to_schema_object(sentence))

    def test_moveViolationsOntoSupervisionPeriodsForSentence(self) -> None:
        # Arrange
        sv_1 = StateSupervisionViolation.new_with_defaults(state_code=_STATE_CODE, violation_date=_DATE_2)
        svr = StateSupervisionViolationResponse.new_with_defaults(state_code=_STATE_CODE, response_date=_DATE_3)
        sv_2 = StateSupervisionViolation.new_with_defaults(state_code=_STATE_CODE,
                                                           supervision_violation_responses=[svr])
        sv_3 = StateSupervisionViolation.new_with_defaults(state_code=_STATE_CODE, violation_date=_DATE_8)

        placeholder_sp_ss = StateSupervisionPeriod.new_with_defaults(state_code=_STATE_CODE,
                                                                     supervision_violation_entries=[sv_1, sv_3])
        sp_ss = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, start_date=_DATE_1, termination_date=_DATE_3)
        sp_2_ss = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2, start_date=_DATE_7)
        ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, supervision_periods=[sp_ss, sp_2_ss, placeholder_sp_ss])

        placeholder_sp_is = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, supervision_violation_entries=[sv_2, sv_3])
        sp_is = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_3, start_date=_DATE_2, termination_date=_DATE_3)
        sp_2_is = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_4, start_date=_DATE_3, termination_date=_DATE_6)
        inc_s = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, supervision_periods=[sp_is, sp_2_is, placeholder_sp_is])

        sg = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE, incarceration_sentences=[inc_s], supervision_sentences=[ss])

        state_person = StatePerson.new_with_defaults(state_code=_STATE_CODE, sentence_groups=[sg])

        expected_sv_1 = attr.evolve(sv_1)
        expected_svr_2 = attr.evolve(svr)
        expected_sv_2 = attr.evolve(sv_2, supervision_violation_responses=[expected_svr_2])
        expected_sv_3 = attr.evolve(sv_3)

        expected_placeholder_sp_ss = attr.evolve(placeholder_sp_ss, supervision_violation_entries=[])
        expected_sp_ss = attr.evolve(sp_ss, supervision_violation_entries=[expected_sv_1])
        expected_sp_2_ss = attr.evolve(sp_2_ss, supervision_violation_entries=[expected_sv_3])
        expected_ss = attr.evolve(
            ss, supervision_periods=[expected_placeholder_sp_ss, expected_sp_ss, expected_sp_2_ss])

        expected_placeholder_sp_is = attr.evolve(placeholder_sp_is, supervision_violation_entries=[expected_sv_3])
        expected_sp_is = attr.evolve(sp_is, supervision_violation_entries=[])
        expected_sp_2_is = attr.evolve(sp_2_is, supervision_violation_entries=[expected_sv_2])
        expected_inc_s = attr.evolve(
            inc_s, supervision_periods=[expected_placeholder_sp_is, expected_sp_is, expected_sp_2_is])
        expected_sg = attr.evolve(sg, supervision_sentences=[expected_ss], incarceration_sentences=[expected_inc_s])
        expected_person = attr.evolve(state_person, sentence_groups=[expected_sg])

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_violations_onto_supervision_periods_for_sentence(input_people)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_moveViolationsOntoSupervisionPeriodsForSentence_violationNoLongerOverlaps(self) -> None:
        # Act
        sv_ss = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=_ID, state_code=_STATE_CODE, violation_date=_DATE_3)

        # This supervision period, which has already been written to the DB, has presumably been updated so that the
        # date range no longer overlaps with the attached violation (or the violation has been updated).
        sp_ss = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_ID,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            start_date=_DATE_1,
            termination_date=_DATE_3,
            supervision_violation_entries=[sv_ss])
        ss = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=_ID,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            supervision_periods=[sp_ss])

        sg = StateSentenceGroup.new_with_defaults(
            sentence_group_id=_ID, state_code=_STATE_CODE, supervision_sentences=[ss])

        state_person = StatePerson.new_with_defaults(state_code=_STATE_CODE, person_id=_ID, sentence_groups=[sg])

        expected_sv_ss = attr.evolve(sv_ss)
        expected_sp_ss = attr.evolve(sp_ss, supervision_violation_entries=[])
        expected_placeholder_sp_ss = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            supervision_violation_entries=[expected_sv_ss])
        expected_ss = attr.evolve(ss, supervision_periods=[expected_placeholder_sp_ss, expected_sp_ss])
        expected_sg = attr.evolve(sg, supervision_sentences=[expected_ss])
        expected_person = attr.evolve(state_person, sentence_groups=[expected_sg])

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_violations_onto_supervision_periods_for_sentence(input_people)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_moveViolationsOntoSupervisionPeriodsForPerson(self) -> None:
        # Arrange
        sv_1 = StateSupervisionViolation.new_with_defaults(state_code=_STATE_CODE, violation_date=_DATE_2)
        svr = StateSupervisionViolationResponse.new_with_defaults(state_code=_STATE_CODE, response_date=_DATE_3)
        sv_2 = StateSupervisionViolation.new_with_defaults(state_code=_STATE_CODE,
                                                           supervision_violation_responses=[svr])
        sv_3 = StateSupervisionViolation.new_with_defaults(state_code=_STATE_CODE, violation_date=_DATE_8)

        placeholder_sp_ss = StateSupervisionPeriod.new_with_defaults(state_code=_STATE_CODE,
                                                                     supervision_violation_entries=[sv_1, sv_3])
        sp_ss = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, start_date=_DATE_1, termination_date=_DATE_3)
        sp_2_ss = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2, start_date=_DATE_7)
        ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, supervision_periods=[sp_ss, sp_2_ss, placeholder_sp_ss])

        placeholder_sp_is = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, supervision_violation_entries=[sv_2, sv_3])
        sp_is = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_3, start_date=_DATE_2, termination_date=_DATE_3)
        sp_2_is = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_4, start_date=_DATE_3, termination_date=_DATE_6)
        inc_s = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, supervision_periods=[sp_is, sp_2_is, placeholder_sp_is])

        sg = StateSentenceGroup.new_with_defaults(state_code=_STATE_CODE, incarceration_sentences=[inc_s])
        sg_2 = StateSentenceGroup.new_with_defaults(state_code=_STATE_CODE, supervision_sentences=[ss])

        state_person = StatePerson.new_with_defaults(state_code=_STATE_CODE, sentence_groups=[sg, sg_2])

        expected_sv_1 = attr.evolve(sv_1)
        expected_svr_2 = attr.evolve(svr)
        expected_sv_2 = attr.evolve(sv_2, supervision_violation_responses=[expected_svr_2])
        expected_sv_3 = attr.evolve(sv_3)

        expected_placeholder_sp_ss = attr.evolve(placeholder_sp_ss, supervision_violation_entries=[])
        expected_sp_ss = attr.evolve(sp_ss, supervision_violation_entries=[expected_sv_1])
        expected_sp_2_ss = attr.evolve(sp_2_ss, supervision_violation_entries=[expected_sv_3])
        expected_ss = attr.evolve(
            ss, supervision_periods=[expected_placeholder_sp_ss, expected_sp_ss, expected_sp_2_ss])

        expected_placeholder_sp_is = attr.evolve(placeholder_sp_is, supervision_violation_entries=[])
        expected_sp_is = attr.evolve(sp_is, supervision_violation_entries=[expected_sv_1])
        expected_sp_2_is = attr.evolve(sp_2_is, supervision_violation_entries=[expected_sv_2])
        expected_inc_s = attr.evolve(
            inc_s, supervision_periods=[expected_placeholder_sp_is, expected_sp_is, expected_sp_2_is])
        expected_sg = attr.evolve(sg, incarceration_sentences=[expected_inc_s])
        expected_sg_2 = attr.evolve(sg_2, supervision_sentences=[expected_ss])

        expected_person = attr.evolve(state_person, sentence_groups=[expected_sg, expected_sg_2])

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_violations_onto_supervision_periods_for_person(input_people, _STATE_CODE)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_moveViolationsOntoSupervisionPeriodsForPerson_violationNoLongerOverlaps(self) -> None:
        # Act
        sv_ss = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=_ID, state_code=_STATE_CODE, violation_date=_DATE_4)

        # This supervision period, which has already been written to the DB, has presumably been updated so that the
        # date range no longer overlaps with the attached violation (or the violation has been updated).
        sp_ss = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_ID,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            start_date=_DATE_1,
            termination_date=_DATE_3,
            supervision_violation_entries=[sv_ss])
        ss = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=_ID,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            supervision_periods=[sp_ss])

        sg = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID, sentence_group_id=_ID, state_code=_STATE_CODE, supervision_sentences=[ss])

        state_person = StatePerson.new_with_defaults(state_code=_STATE_CODE, person_id=_ID, sentence_groups=[sg])

        expected_sp_ss = attr.evolve(sp_ss, supervision_violation_entries=[])
        expected_ss = attr.evolve(ss, supervision_periods=[expected_sp_ss])
        expected_sg = attr.evolve(sg, supervision_sentences=[expected_ss])

        expected_sv_ss = attr.evolve(sv_ss)
        expected_placeholder_sp = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            supervision_violation_entries=[expected_sv_ss])
        expected_placeholder_ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_periods=[expected_placeholder_sp])
        expected_placeholder_sg = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[expected_placeholder_ss])

        expected_person = attr.evolve(state_person, sentence_groups=[expected_sg, expected_placeholder_sg])

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_violations_onto_supervision_periods_for_person(input_people, _STATE_CODE)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_moveContactsOntoSupervisionPeriodsForPerson(self) -> None:
        self.maxDiff = None
        # Arrange
        sc_1 = StateSupervisionContact.new_with_defaults(state_code=_STATE_CODE, contact_date=_DATE_2)
        sc_2 = StateSupervisionContact.new_with_defaults(state_code=_STATE_CODE, contact_date=_DATE_3)
        sc_3 = StateSupervisionContact.new_with_defaults(state_code=_STATE_CODE, contact_date=_DATE_8)

        placeholder_sp_ss = StateSupervisionPeriod.new_with_defaults(state_code=_STATE_CODE,
                                                                     supervision_contacts=[sc_1, sc_3])
        sp_ss = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, start_date=_DATE_1, termination_date=_DATE_3)
        sp_2_ss = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2, start_date=_DATE_7)
        ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, supervision_periods=[sp_ss, sp_2_ss, placeholder_sp_ss])

        placeholder_sp_is = StateSupervisionPeriod.new_with_defaults(state_code=_STATE_CODE,
                                                                     supervision_contacts=[sc_2, sc_3])
        sp_is = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_3, start_date=_DATE_2, termination_date=_DATE_3)
        sp_2_is = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_4, start_date=_DATE_3, termination_date=_DATE_6)
        inc_s = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, supervision_periods=[sp_is, sp_2_is, placeholder_sp_is])

        sg = StateSentenceGroup.new_with_defaults(state_code=_STATE_CODE, incarceration_sentences=[inc_s])
        sg_2 = StateSentenceGroup.new_with_defaults(state_code=_STATE_CODE, supervision_sentences=[ss])

        state_person = StatePerson.new_with_defaults(state_code=_STATE_CODE, sentence_groups=[sg, sg_2])

        expected_sc_1 = attr.evolve(sc_1)
        expected_sc_2 = attr.evolve(sc_2)
        expected_sc_3 = attr.evolve(sc_3)

        expected_placeholder_sp_ss = attr.evolve(placeholder_sp_ss, supervision_contacts=[])
        expected_sp_ss = attr.evolve(sp_ss, supervision_contacts=[expected_sc_1])
        expected_sp_2_ss = attr.evolve(sp_2_ss, supervision_contacts=[expected_sc_3])
        expected_ss = attr.evolve(
            ss, supervision_periods=[expected_placeholder_sp_ss, expected_sp_ss, expected_sp_2_ss])

        expected_placeholder_sp_is = attr.evolve(placeholder_sp_is, supervision_contacts=[])
        expected_sp_is = attr.evolve(sp_is, supervision_contacts=[expected_sc_1])
        expected_sp_2_is = attr.evolve(sp_2_is, supervision_contacts=[expected_sc_2])
        expected_inc_s = attr.evolve(
            inc_s, supervision_periods=[expected_placeholder_sp_is, expected_sp_is, expected_sp_2_is])
        expected_sg = attr.evolve(sg, incarceration_sentences=[expected_inc_s])
        expected_sg_2 = attr.evolve(sg_2, supervision_sentences=[expected_ss])

        expected_person = attr.evolve(state_person, sentence_groups=[expected_sg, expected_sg_2])

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_contacts_onto_supervision_periods_for_person(input_people, _STATE_CODE)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_moveContactsOntoSupervisionPeriodsForPerson_contactNoLongerOverlaps(self) -> None:
        # Act
        sc_ss = StateSupervisionContact.new_with_defaults(
            supervision_contact_id=_ID, state_code=_STATE_CODE, contact_date=_DATE_4)

        # This supervision period, which has already been written to the DB, has presumably been updated so that the
        # date range no longer overlaps with the attached violation (or the violation has been updated).
        sp_ss = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_ID,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            start_date=_DATE_1,
            termination_date=_DATE_3,
            supervision_contacts=[sc_ss])
        ss = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=_ID,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            supervision_periods=[sp_ss])

        sg = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID, sentence_group_id=_ID, state_code=_STATE_CODE, supervision_sentences=[ss])

        state_person = StatePerson.new_with_defaults(state_code=_STATE_CODE, person_id=_ID, sentence_groups=[sg])

        expected_sp_ss = attr.evolve(sp_ss, supervision_contacts=[])
        expected_ss = attr.evolve(ss, supervision_periods=[expected_sp_ss])
        expected_sg = attr.evolve(sg, supervision_sentences=[expected_ss])

        expected_sc_ss = attr.evolve(sc_ss)
        expected_placeholder_sp = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            supervision_contacts=[expected_sc_ss])
        expected_placeholder_ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_periods=[expected_placeholder_sp])
        expected_placeholder_sg = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[expected_placeholder_ss])

        expected_person = attr.evolve(state_person, sentence_groups=[expected_sg, expected_placeholder_sg])

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_contacts_onto_supervision_periods_for_person(input_people, _STATE_CODE)

        # Assert
        self.assert_people_match([expected_person], input_people)
