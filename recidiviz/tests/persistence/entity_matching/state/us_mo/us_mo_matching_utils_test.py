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

import attr
import pytest

from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodStatus
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state.entities import \
    StateSupervisionViolation, StateSupervisionPeriod, \
    StateSupervisionSentence, StateSentenceGroup, StatePerson, \
    StateSupervisionViolationResponse
from recidiviz.persistence.entity_matching.state.us_mo.us_mo_matching_utils \
    import remove_suffix_from_violation_ids, \
    set_current_supervising_officer_from_supervision_periods
from recidiviz.tests.persistence.database.schema.state.schema_test_utils import \
    generate_sentence_group, generate_agent, generate_person, \
    generate_external_id, generate_supervision_period, \
    generate_supervision_sentence
from recidiviz.tests.persistence.entity_matching.state.base_state_entity_matcher_test_classes import \
    BaseStateMatchingUtilsTest

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
_ID_2 = 2
_ID_3 = 3
_ID_TYPE = 'ID_TYPE'
_STATE_CODE = 'US_MO'


class TestUsMoMatchingUtils(BaseStateMatchingUtilsTest):
    """Test class for US_MO specific matching utils."""

    def test_removeSeosFromViolationIds(self) -> None:
        svr = schema.StateSupervisionViolationResponse(
            state_code=_STATE_CODE, external_id='DOC-CYC-VSN1-SEO-FSO')
        sv = schema.StateSupervisionViolation(
            state_code=_STATE_CODE,
            external_id='DOC-CYC-VSN1-SEO-FSO',
            supervision_violation_responses=[svr])
        svr_2 = schema.StateSupervisionViolationResponse(
            state_code=_STATE_CODE, external_id='DOC-CYC-VSN1-SEO-FSO')
        sv_2 = schema.StateSupervisionViolation(
            state_code=_STATE_CODE,
            external_id='DOC-CYC-VSN1-SEO-FSO',
            supervision_violation_responses=[svr_2])
        sp = schema.StateSupervisionPeriod(
            state_code=_STATE_CODE,
            supervision_violation_entries=[sv, sv_2],
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO)
        ss = schema.StateSupervisionSentence(
            state_code=_STATE_CODE, supervision_periods=[sp], status=StateSentenceStatus.PRESENT_WITHOUT_INFO)
        sg = schema.StateSentenceGroup(state_code=_STATE_CODE,
                                       status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                                       supervision_sentences=[ss])
        p = schema.StatePerson(state_code=_STATE_CODE, sentence_groups=[sg])

        expected_svr = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE, external_id='DOC-CYC-VSN1')
        expected_sv = StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE,
            external_id='DOC-CYC-VSN1',
            supervision_violation_responses=[expected_svr])
        expected_svr_2 = attr.evolve(expected_svr)
        expected_sv_2 = attr.evolve(
            expected_sv,
            supervision_violation_responses=[expected_svr_2])
        expected_sp = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE, supervision_violation_entries=[expected_sv, expected_sv_2],
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO)
        expected_ss = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE, supervision_periods=[expected_sp],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO)
        expected_sg = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[expected_ss])
        expected_p = StatePerson.new_with_defaults(
            state_code=_STATE_CODE, sentence_groups=[expected_sg])

        remove_suffix_from_violation_ids([p])
        self.assertEqual(expected_p, self.to_entity(p))

    def test_removeSeosFromViolationIds_unexpectedFormat(self) -> None:
        with pytest.raises(ValueError) as e:
            sv = schema.StateSupervisionViolation(external_id='bad_id')
            sp = schema.StateSupervisionPeriod(
                supervision_violation_entries=[sv],
                status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO)
            ss = schema.StateSupervisionSentence(supervision_periods=[sp])
            sg = schema.StateSentenceGroup(supervision_sentences=[ss])
            p = schema.StatePerson(sentence_groups=[sg])
            remove_suffix_from_violation_ids([p])
        self.assertEqual(str(e.value),
                         'Unexpected id format [bad_id] for [StateSupervisionViolation(external_id=bad_id)]')

    def test_setCurrentSupervisingOfficerFromSupervision_periods(self) -> None:
        # Arrange
        person = generate_person(person_id=_ID)
        external_id = generate_external_id(
            person_external_id_id=_ID, external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            id_type=_ID_TYPE)

        supervising_officer = generate_agent(
            agent_id=_ID, external_id=_EXTERNAL_ID, state_code=_STATE_CODE)
        supervising_officer_2 = generate_agent(
            agent_id=_ID_2, external_id=_EXTERNAL_ID_2, state_code=_STATE_CODE)

        open_supervision_period = generate_supervision_period(
            person=person,
            supervision_period_id=_ID,
            external_id=_EXTERNAL_ID,
            start_date=_DATE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE,
            supervising_officer=supervising_officer_2)
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
            state_code=_STATE_CODE,
            supervising_officer=supervising_officer)
        supervision_sentence = generate_supervision_sentence(
            person=person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            supervision_sentence_id=_ID,
            supervision_periods=[open_supervision_period,
                                 placeholder_supervision_period,
                                 closed_supervision_period])
        sentence_group = generate_sentence_group(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            sentence_group_id=_ID,
            supervision_sentences=[supervision_sentence])
        person.external_ids = [external_id]
        person.sentence_groups = [sentence_group]

        # Act
        set_current_supervising_officer_from_supervision_periods([person])

        # Assert
        self.assertEqual(
            closed_supervision_period.supervising_officer, supervising_officer)
        self.assertEqual(
            open_supervision_period.supervising_officer, supervising_officer_2)
        self.assertIsNone(placeholder_supervision_period.supervising_officer)
        self.assertEqual(
            person.supervising_officer, supervising_officer_2)
