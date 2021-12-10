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
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationSentence,
    StatePerson,
    StateSentenceGroup,
    StateSupervisionPeriod,
    StateSupervisionSentence,
)
from recidiviz.persistence.entity_matching.state.state_date_based_matching_utils import (
    _is_sentence_ended_by_status,
    move_periods_onto_sentences_by_date,
)
from recidiviz.tests.persistence.entity_matching.state.base_state_entity_matcher_test_classes import (
    BaseStateMatchingUtilsTest,
)

_DATE_1 = datetime.date(year=2019, month=1, day=1)
_DATE_2 = datetime.date(year=2019, month=2, day=1)
_DATE_3 = datetime.date(year=2019, month=3, day=1)
_DATE_4 = datetime.date(year=2019, month=4, day=1)
_DATE_5 = datetime.date(year=2019, month=5, day=1)
_DATE_6 = datetime.date(year=2019, month=6, day=1)
_DATE_7 = datetime.date(year=2019, month=7, day=1)
_DATE_8 = datetime.date(year=2019, month=8, day=1)
_EXTERNAL_ID = "EXTERNAL_ID-1"
_EXTERNAL_ID_2 = "EXTERNAL_ID-2"
_EXTERNAL_ID_3 = "EXTERNAL_ID-3"
_EXTERNAL_ID_4 = "EXTERNAL_ID-4"
_ID = 1
_ID_2 = 2
_ID_3 = 3
_STATE_CODE = "US_XX"
_ID_TYPE = "ID_TYPE"

# TODO(#9567): Delete this once StateSupervisionPeriod is on the StatePerson


# pylint: disable=protected-access
class TestStateDateBasedMatchingUtils(BaseStateMatchingUtilsTest):
    """Tests for state date based matching utils"""

    def test_associatedPeriodsWithSentences_periodStartsBeforeAndEndsAfterSentence(
        self,
    ) -> None:
        sp = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_1,
            termination_date=_DATE_4,
        )
        placeholder_ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            supervision_periods=[sp],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_2,
            completion_date=_DATE_3,
            status=StateSentenceStatus.SERVING,
        )

        sg = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[ss, placeholder_ss],
        )

        state_person = StatePerson.new_with_defaults(
            state_code=_STATE_CODE, sentence_groups=[sg]
        )

        expected_sp = attr.evolve(sp)

        expected_placeholder_ss = attr.evolve(placeholder_ss, supervision_periods=[])
        expected_ss = attr.evolve(ss, supervision_periods=[expected_sp])
        expected_sg = attr.evolve(
            sg, supervision_sentences=[expected_ss, expected_placeholder_ss]
        )
        expected_person = attr.evolve(state_person, sentence_groups=[expected_sg])

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_periods_onto_sentences_by_date(input_people, field_index=self.field_index)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_associatedPeriodsWithSentences_oneDayPeriodOverlapsWithStartOfSentence(
        self,
    ) -> None:
        sp = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_2,
            termination_date=_DATE_2,
        )
        placeholder_ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            supervision_periods=[sp],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_2,
            completion_date=_DATE_3,
            status=StateSentenceStatus.SERVING,
        )

        sg = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[ss, placeholder_ss],
        )

        state_person = StatePerson.new_with_defaults(
            state_code=_STATE_CODE, sentence_groups=[sg]
        )

        expected_sp = attr.evolve(sp)

        expected_placeholder_ss = attr.evolve(placeholder_ss, supervision_periods=[])
        expected_ss = attr.evolve(ss, supervision_periods=[expected_sp])
        expected_sg = attr.evolve(
            sg, supervision_sentences=[expected_ss, expected_placeholder_ss]
        )
        expected_person = attr.evolve(state_person, sentence_groups=[expected_sg])

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_periods_onto_sentences_by_date(input_people, field_index=self.field_index)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_associatedPeriodsWithSentences(self) -> None:
        # Arrange
        sp_no_match = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_1,
            termination_date=_DATE_3,
        )

        sp_1 = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_4,
            termination_date=_DATE_6,
        )

        sp_2 = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2,
            start_date=_DATE_6,
            termination_date=None,
        )

        placeholder_ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            supervision_periods=[sp_no_match, sp_1, sp_2],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_4,
            completion_date=None,
            status=StateSentenceStatus.SERVING,
        )
        inc_s = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_3,
            completion_date=_DATE_5,
            status=StateSentenceStatus.COMPLETED,
        )

        sg = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[inc_s],
            supervision_sentences=[ss, placeholder_ss],
        )

        state_person = StatePerson.new_with_defaults(
            state_code=_STATE_CODE, sentence_groups=[sg]
        )

        expected_sp_no_match = attr.evolve(sp_no_match)
        expected_sp_1 = attr.evolve(sp_1)
        expected_sp_2 = attr.evolve(sp_2)

        expected_placeholder_ss = attr.evolve(
            placeholder_ss,
            supervision_periods=[expected_sp_no_match],
        )
        expected_inc_s = attr.evolve(inc_s, supervision_periods=[expected_sp_1])
        expected_ss = attr.evolve(
            ss,
            supervision_periods=[expected_sp_1, expected_sp_2],
        )
        expected_sg = attr.evolve(
            sg,
            supervision_sentences=[expected_ss, expected_placeholder_ss],
            incarceration_sentences=[expected_inc_s],
        )
        expected_person = attr.evolve(state_person, sentence_groups=[expected_sg])

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_periods_onto_sentences_by_date(input_people, field_index=self.field_index)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_associatedPeriodsWithSentences_justSupervisionPeriods(self) -> None:
        # Arrange
        sp_1 = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_6,
            termination_date=None,
        )

        placeholder_ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            supervision_periods=[sp_1],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_4,
            completion_date=None,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        inc_s = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_4,
            completion_date=None,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        sg = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[inc_s],
            supervision_sentences=[ss, placeholder_ss],
        )

        state_person = StatePerson.new_with_defaults(
            state_code=_STATE_CODE, sentence_groups=[sg]
        )

        expected_sp_1 = attr.evolve(sp_1)

        expected_placeholder_ss = attr.evolve(
            placeholder_ss,
            supervision_periods=[],
        )
        expected_inc_s = attr.evolve(inc_s, supervision_periods=[expected_sp_1])
        expected_ss = attr.evolve(ss, supervision_periods=[expected_sp_1])
        expected_sg = attr.evolve(
            sg,
            supervision_sentences=[expected_ss, expected_placeholder_ss],
            incarceration_sentences=[expected_inc_s],
        )
        expected_person = attr.evolve(state_person, sentence_groups=[expected_sg])

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_periods_onto_sentences_by_date(
            input_people,
            field_index=self.field_index,
        )

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_associatedPeriodsWithSentences_doNotAssociateToClosedButUnterminatedSentences(
        self,
    ) -> None:
        # Arrange
        sp_1 = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_6,
            termination_date=None,
        )

        placeholder_ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            supervision_periods=[sp_1],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.COMPLETED,
            start_date=_DATE_4,
            completion_date=None,
        )
        inc_s = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.COMPLETED,
            start_date=_DATE_4,
            completion_date=None,
        )

        sg = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[inc_s],
            supervision_sentences=[ss, placeholder_ss],
        )

        state_person = StatePerson.new_with_defaults(
            state_code=_STATE_CODE, sentence_groups=[sg]
        )

        expected_sp_1 = attr.evolve(sp_1)

        expected_placeholder_ss = attr.evolve(
            placeholder_ss,
            supervision_periods=[expected_sp_1],
        )
        expected_inc_s = attr.evolve(inc_s)
        expected_ss = attr.evolve(ss)
        expected_sg = attr.evolve(
            sg,
            supervision_sentences=[expected_ss, expected_placeholder_ss],
            incarceration_sentences=[expected_inc_s],
        )
        expected_person = attr.evolve(state_person, sentence_groups=[expected_sg])

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_periods_onto_sentences_by_date(input_people, field_index=self.field_index)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_associatePeriodsWithSentence_doNotMatchSentenceWithNoStart(self) -> None:
        # Arrange
        placeholder_sp = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
        )

        sp = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_3,
            start_date=_DATE_2,
            termination_date=_DATE_3,
        )

        inc_s_no_dates = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            supervision_periods=[placeholder_sp],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        placeholder_inc_s = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE,
            supervision_periods=[sp],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        sg = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[inc_s_no_dates, placeholder_inc_s],
        )

        state_person = StatePerson.new_with_defaults(
            state_code=_STATE_CODE, sentence_groups=[sg]
        )

        # Should remain unchanged - the non-placeholder period should not get moved onto sentence with an id
        # but no start date
        expected_person = attr.evolve(state_person)

        # Act
        input_people = converter.convert_entity_people_to_schema_people([state_person])
        move_periods_onto_sentences_by_date(input_people, field_index=self.field_index)

        # Assert
        self.assert_people_match([expected_person], input_people)

    def test_isSentenceEndedByStatus(self) -> None:
        sentence = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE, status=StateSentenceStatus.PRESENT_WITHOUT_INFO
        )
        for status in StateSentenceStatus:
            sentence.status = status
            db_entity = converter.convert_entity_to_schema_object(sentence)
            if not isinstance(db_entity, schema.StateSupervisionSentence):
                self.fail(f"Unexpected type for db_entity: {[db_entity]}.")
            _is_sentence_ended_by_status(db_entity)
