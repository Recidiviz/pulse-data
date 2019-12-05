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
from unittest import TestCase

import attr
import pytest

from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter)
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseType, \
    StateSupervisionViolationResponseDecision
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state.entities import \
    StateSupervisionViolation, StateSupervisionPeriod, \
    StateSupervisionSentence, StateSentenceGroup, StatePerson
from recidiviz.persistence.entity_matching.entity_matching_types import \
    EntityTree
from recidiviz.persistence.entity_matching.state.us_mo.us_mo_matching_utils \
    import is_supervision_violation_response_match, \
    remove_suffix_from_violation_ids
from recidiviz.persistence.errors import EntityMatchingError

_ID = 1
_DATE = datetime.date(year=2000, month=7, day=20)
_DATE_2 = datetime.date(year=2001, month=7, day=20)


class TestUsMoMatchingUtils(TestCase):
    """Test class for US_MO specific matching utils."""

    def to_entity(self, schema_obj):
        return converter.convert_schema_object_to_entity(
            schema_obj, populate_back_edges=False)

    def test_isSupervisionViolationResponse_match(self):
        ssvr = schema.StateSupervisionViolationResponse(
            response_type=StateSupervisionViolationResponseType.CITATION,
            response_date=_DATE)

        ssvr_2 = schema.StateSupervisionViolationResponse(
            response_type=StateSupervisionViolationResponseType.CITATION,
            response_date=_DATE,
            decision=StateSupervisionViolationResponseDecision.REVOCATION)
        tree = EntityTree(entity=ssvr, ancestor_chain=[])
        tree_2 = EntityTree(entity=ssvr_2, ancestor_chain=[])
        self.assertTrue(is_supervision_violation_response_match(tree, tree_2))
        ssvr_2.response_date = _DATE_2
        self.assertFalse(is_supervision_violation_response_match(tree, tree_2))

    def test_removeSeosFromViolationIds(self):
        sv = schema.StateSupervisionViolation(
            external_id='DOC-CYC-VSN1-SEO-FSO')
        sv_2 = schema.StateSupervisionViolation(
            external_id='DOC-CYC-VSN1-SEO-FSO')
        sp = schema.StateSupervisionPeriod(
            supervision_violation_entries=[sv, sv_2])
        ss = schema.StateSupervisionSentence(supervision_periods=[sp])
        sg = schema.StateSentenceGroup(supervision_sentences=[ss])
        p = schema.StatePerson(sentence_groups=[sg])

        expected_sv = StateSupervisionViolation.new_with_defaults(
            external_id='DOC-CYC-VSN1')
        expected_sv_2 = attr.evolve(expected_sv)
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
