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
"""Tests for us_mo_state_matching_utils.py"""
import datetime
from typing import List
from unittest import TestCase

import attr
import pytest

from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter)
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state.entities import \
    StateSupervisionViolation, StateSupervisionPeriod, \
    StateSupervisionSentence, StateSentenceGroup, StatePerson, \
    StateSupervisionViolationResponse, StateIncarcerationSentence
from recidiviz.persistence.entity_matching.state.us_mo.us_mo_matching_utils \
    import remove_suffix_from_violation_ids, \
    move_violations_onto_supervision_periods_by_date
from recidiviz.persistence.errors import EntityMatchingError

_DATE = datetime.date(year=2001, month=7, day=20)
_DATE_2 = datetime.date(year=2002, month=7, day=20)
_DATE_3 = datetime.date(year=2003, month=7, day=20)
_DATE_4 = datetime.date(year=2004, month=7, day=20)
_DATE_5 = datetime.date(year=2005, month=7, day=20)
_DATE_6 = datetime.date(year=2006, month=7, day=20)
_DATE_7 = datetime.date(year=2007, month=7, day=20)
_DATE_8 = datetime.date(year=2008, month=7, day=20)
_EXTERNAL_ID = 'EXTERNAL_ID'
_EXTERNAL_ID_2 = 'EXTERNAL_ID_2'
_EXTERNAL_ID_3 = 'EXTERNAL_ID_3'
_EXTERNAL_ID_4 = 'EXTERNAL_ID_4'
_ID = 1


class TestUsMoMatchingUtils(TestCase):
    """Test class for US_MO specific matching utils."""

    def to_entity(self, schema_obj):
        return converter.convert_schema_object_to_entity(
            schema_obj, populate_back_edges=False)

    def assert_people_match(self,
                            expected_people: List[StatePerson],
                            matched_people: List[schema.StatePerson]):
        converted_matched = \
            converter.convert_schema_objects_to_entity(matched_people)
        db_expected_with_backedges = \
            converter.convert_entity_people_to_schema_people(expected_people)
        expected_with_backedges = \
            converter.convert_schema_objects_to_entity(
                db_expected_with_backedges)
        self.assertCountEqual(expected_with_backedges, converted_matched)

    def test_removeSeosFromViolationIds(self):
        svr = schema.StateSupervisionViolationResponse(
            external_id='DOC-CYC-VSN1-SEO-FSO')
        sv = schema.StateSupervisionViolation(
            external_id='DOC-CYC-VSN1-SEO-FSO',
            supervision_violation_responses=[svr])
        svr_2 = schema.StateSupervisionViolationResponse(
            external_id='DOC-CYC-VSN1-SEO-FSO')
        sv_2 = schema.StateSupervisionViolation(
            external_id='DOC-CYC-VSN1-SEO-FSO',
            supervision_violation_responses=[svr_2])
        sp = schema.StateSupervisionPeriod(
            supervision_violation_entries=[sv, sv_2])
        ss = schema.StateSupervisionSentence(supervision_periods=[sp])
        sg = schema.StateSentenceGroup(supervision_sentences=[ss])
        p = schema.StatePerson(sentence_groups=[sg])

        expected_svr = StateSupervisionViolationResponse.new_with_defaults(
            external_id='DOC-CYC-VSN1')
        expected_sv = StateSupervisionViolation.new_with_defaults(
            external_id='DOC-CYC-VSN1',
            supervision_violation_responses=[expected_svr])
        expected_svr_2 = attr.evolve(expected_svr)
        expected_sv_2 = attr.evolve(
            expected_sv,
            supervision_violation_responses=[expected_svr_2])
        expected_sp = StateSupervisionPeriod.new_with_defaults(
            supervision_violation_entries=[expected_sv, expected_sv_2])
        expected_ss = StateSupervisionSentence.new_with_defaults(
            supervision_periods=[expected_sp])
        expected_sg = StateSentenceGroup.new_with_defaults(
            supervision_sentences=[expected_ss])
        expected_p = StatePerson.new_with_defaults(
            sentence_groups=[expected_sg])

        remove_suffix_from_violation_ids([p])
        self.assertEqual(expected_p, self.to_entity(p))

    def test_removeSeosFromViolationIds_unexpectedFormat(self):
        with pytest.raises(EntityMatchingError):
            sv = schema.StateSupervisionViolation(external_id='bad_id')
            sp = schema.StateSupervisionPeriod(
                supervision_violation_entries=[sv])
            ss = schema.StateSupervisionSentence(supervision_periods=[sp])
            sg = schema.StateSentenceGroup(supervision_sentences=[ss])
            p = schema.StatePerson(sentence_groups=[sg])
            remove_suffix_from_violation_ids([p])

    def test_associateViolationsWithSupervisionPeriods(self):
        sv_ss = StateSupervisionViolation.new_with_defaults(
            violation_date=_DATE_2)

        sv_is_ss = StateSupervisionViolation.new_with_defaults(
            violation_date=_DATE_8)
        svr_is = StateSupervisionViolationResponse.new_with_defaults(
            response_date=_DATE_3)
        sv_is = StateSupervisionViolation.new_with_defaults(
            supervision_violation_responses=[svr_is])

        placeholder_sp_ss = StateSupervisionPeriod.new_with_defaults(
            supervision_violation_entries=[sv_ss, sv_is_ss])
        sp_ss = StateSupervisionPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID,
            start_date=_DATE,
            termination_date=_DATE_3)
        sp_2_ss = StateSupervisionPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            start_date=_DATE_7)
        ss = StateSupervisionSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            supervision_periods=[sp_ss, sp_2_ss, placeholder_sp_ss])

        placeholder_sp_is = StateSupervisionPeriod.new_with_defaults(
            supervision_violation_entries=[sv_is, sv_is_ss])
        sp_is = StateSupervisionPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_3,
            start_date=_DATE_2,
            termination_date=_DATE_3)
        sp_2_is = StateSupervisionPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_4,
            start_date=_DATE_3,
            termination_date=_DATE_6)
        inc_s = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            supervision_periods=[sp_is, sp_2_is, placeholder_sp_is])

        sg = StateSentenceGroup.new_with_defaults(
            incarceration_sentences=[inc_s],
            supervision_sentences=[ss])

        state_person = StatePerson.new_with_defaults(
            sentence_groups=[sg])

        expected_sv_ss = attr.evolve(sv_ss)
        expected_sv_is_ss = attr.evolve(sv_is_ss)
        expected_svr_is = attr.evolve(svr_is)
        expected_sv_is = attr.evolve(
            sv_is,
            supervision_violation_responses=[expected_svr_is])

        expected_placeholder_sp_ss = attr.evolve(
            placeholder_sp_ss,
            supervision_violation_entries=[])
        expected_sp_ss = attr.evolve(
            sp_ss,
            supervision_violation_entries=[expected_sv_ss])
        expected_sp_2_ss = attr.evolve(
            sp_2_ss,
            supervision_violation_entries=[expected_sv_is_ss])
        expected_ss = attr.evolve(
            ss,
            supervision_periods=[
                expected_placeholder_sp_ss, expected_sp_ss, expected_sp_2_ss])

        expected_placeholder_sp_is = attr.evolve(
            placeholder_sp_is,
            supervision_violation_entries=[expected_sv_is_ss])
        expected_sp_is = attr.evolve(
            sp_is, supervision_violation_entries=[expected_sv_is])
        expected_sp_2_is = attr.evolve(
            sp_2_is, supervision_violation_entries=[expected_sv_is])
        expected_inc_s = attr.evolve(
            inc_s, supervision_periods=[
                expected_placeholder_sp_is,
                expected_sp_is,
                expected_sp_2_is])
        expected_sg = attr.evolve(
            sg,
            supervision_sentences=[expected_ss],
            incarceration_sentences=[expected_inc_s])
        expected_person = attr.evolve(
            state_person, sentence_groups=[expected_sg])

        # Act
        input_people = \
            converter.convert_entity_people_to_schema_people(
                [state_person])
        move_violations_onto_supervision_periods_by_date(input_people)

        # Assert
        self.assert_people_match([expected_person], input_people)
