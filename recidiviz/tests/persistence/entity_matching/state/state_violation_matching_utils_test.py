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
"""Tests for state_violation_matching_utils.py"""
import datetime

import attr
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodStatus
from recidiviz.common.constants.state.state_supervision_violation_response import \
    StateSupervisionViolationResponseRevocationType
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state.entities import StatePerson, StateSentenceGroup, \
    StateSupervisionPeriod, StateSupervisionSentence, StateIncarcerationSentence, \
    StateSupervisionViolation, StateSupervisionViolationResponse
from recidiviz.persistence.database.schema_entity_converter import schema_entity_converter as converter
from recidiviz.persistence.entity_matching.state.state_violation_matching_utils import \
    move_violations_onto_supervision_periods_for_sentence, move_violations_onto_supervision_periods_for_person, \
    revoked_to_prison
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
_STATE_CODE = 'NC'


# pylint: disable=protected-access
class TestStateMatchingUtils(BaseStateMatchingUtilsTest):
    """Tests for state violation matching utils"""

    def test_completeEnumSet_revokedToPrison(self):
        svr = schema.StateSupervisionViolationResponse()
        for revocation_type in StateSupervisionViolationResponseRevocationType:
            svr.revocation_type = revocation_type.value
            revoked_to_prison(svr)

    def test_moveViolationsOntoSupervisionPeriodsForSentence(self):
        # Arrange
        sv_1 = StateSupervisionViolation.new_with_defaults(violation_date=_DATE_2)
        svr = StateSupervisionViolationResponse.new_with_defaults(response_date=_DATE_3)
        sv_2 = StateSupervisionViolation.new_with_defaults(supervision_violation_responses=[svr])
        sv_3 = StateSupervisionViolation.new_with_defaults(violation_date=_DATE_8)

        placeholder_sp_ss = StateSupervisionPeriod.new_with_defaults(supervision_violation_entries=[sv_1, sv_3])
        sp_ss = StateSupervisionPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID, start_date=_DATE_1, termination_date=_DATE_3)
        sp_2_ss = StateSupervisionPeriod.new_with_defaults(external_id=_EXTERNAL_ID_2, start_date=_DATE_7)
        ss = StateSupervisionSentence.new_with_defaults(
            external_id=_EXTERNAL_ID, supervision_periods=[sp_ss, sp_2_ss, placeholder_sp_ss])

        placeholder_sp_is = StateSupervisionPeriod.new_with_defaults(supervision_violation_entries=[sv_2, sv_3])
        sp_is = StateSupervisionPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_3, start_date=_DATE_2, termination_date=_DATE_3)
        sp_2_is = StateSupervisionPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_4, start_date=_DATE_3, termination_date=_DATE_6)
        inc_s = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID, supervision_periods=[sp_is, sp_2_is, placeholder_sp_is])

        sg = StateSentenceGroup.new_with_defaults(incarceration_sentences=[inc_s], supervision_sentences=[ss])

        state_person = StatePerson.new_with_defaults(sentence_groups=[sg])

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

    def test_moveViolationsOntoSupervisionPeriodsForSentence_violationNoLongerOverlaps(self):
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

        state_person = StatePerson.new_with_defaults(person_id=_ID, sentence_groups=[sg])

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

    def test_moveViolationsOntoSupervisionPeriodsForPerson(self):
        # Arrange
        sv_1 = StateSupervisionViolation.new_with_defaults(violation_date=_DATE_2)
        svr = StateSupervisionViolationResponse.new_with_defaults(response_date=_DATE_3)
        sv_2 = StateSupervisionViolation.new_with_defaults(supervision_violation_responses=[svr])
        sv_3 = StateSupervisionViolation.new_with_defaults(violation_date=_DATE_8)

        placeholder_sp_ss = StateSupervisionPeriod.new_with_defaults(supervision_violation_entries=[sv_1, sv_3])
        sp_ss = StateSupervisionPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID, start_date=_DATE_1, termination_date=_DATE_3)
        sp_2_ss = StateSupervisionPeriod.new_with_defaults(external_id=_EXTERNAL_ID_2, start_date=_DATE_7)
        ss = StateSupervisionSentence.new_with_defaults(
            external_id=_EXTERNAL_ID, supervision_periods=[sp_ss, sp_2_ss, placeholder_sp_ss])

        placeholder_sp_is = StateSupervisionPeriod.new_with_defaults(supervision_violation_entries=[sv_2, sv_3])
        sp_is = StateSupervisionPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_3, start_date=_DATE_2, termination_date=_DATE_3)
        sp_2_is = StateSupervisionPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_4, start_date=_DATE_3, termination_date=_DATE_6)
        inc_s = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID, supervision_periods=[sp_is, sp_2_is, placeholder_sp_is])

        sg = StateSentenceGroup.new_with_defaults(incarceration_sentences=[inc_s])
        sg_2 = StateSentenceGroup.new_with_defaults(supervision_sentences=[ss])

        state_person = StatePerson.new_with_defaults(sentence_groups=[sg, sg_2])

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

    def test_moveViolationsOntoSupervisionPeriodsForPerson_violationNoLongerOverlaps(self):
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

        state_person = StatePerson.new_with_defaults(person_id=_ID, sentence_groups=[sg])

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
