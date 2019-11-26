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
"""US_MO specific entity matching tests."""

import attr
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.entity.state.entities import \
    StatePersonExternalId, StatePerson, \
    StateSupervisionSentence, StateSupervisionViolation, \
    StateSupervisionPeriod, StateSentenceGroup
from recidiviz.persistence.entity_matching import entity_matching
from recidiviz.tests.persistence.database.schema.state.schema_test_utils \
    import generate_person, generate_external_id, \
    generate_incarceration_sentence, \
    generate_sentence_group, \
    generate_supervision_period, \
    generate_supervision_violation
from recidiviz.tests.persistence.entity_matching.state. \
    base_state_entity_matcher_test import BaseStateEntityMatcherTest

_EXTERNAL_ID = 'EXTERNAL_ID'
_EXTERNAL_ID_WITH_SEO = 'EXTERNAL_ID-SEO'
_FULL_NAME = 'FULL_NAME'
_ID = 1
_ID_TYPE = 'ID_TYPE'
_US_MO = 'US_MO'


class TestMoEntityMatching(BaseStateEntityMatcherTest):
    """Test class for US_MO specific entity matching logic."""

    # TODO(2658): Update test to expect one merged SupervisionViolation after
    # SupervisionPeriod <-> SupervisionViolation mapping is many-to-many.
    def test_supervisionViolationsWithDifferentParents_persistsDuplicates(self):
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME)
        db_supervision_violation = generate_supervision_violation(
            person=db_person,
            state_code=_US_MO,
            supervision_violation_id=_ID,
            external_id=_EXTERNAL_ID)
        db_placeholder_supervision_period = generate_supervision_period(
            person=db_person,
            state_code=_US_MO,
            supervision_period_id=_ID,
            supervision_violations=[db_supervision_violation])
        db_incarceration_sentence = generate_incarceration_sentence(
            person=db_person,
            state_code=_US_MO,
            incarceration_sentence_id=_ID,
            external_id=_EXTERNAL_ID,
            supervision_periods=[db_placeholder_supervision_period])
        db_sentence_group = generate_sentence_group(
            person=db_person,
            state_code=_US_MO,
            sentence_group_id=_ID,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence])
        db_external_id = generate_external_id(
            person=db_person,
            person_external_id_id=_ID,
            state_code=_US_MO,
            external_id=_EXTERNAL_ID)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        supervision_violation = attr.evolve(
            self.to_entity(db_supervision_violation),
            supervision_violation_id=None,
            external_id=_EXTERNAL_ID_WITH_SEO)
        placeholder_supervision_period = attr.evolve(
            self.to_entity(db_placeholder_supervision_period),
            supervision_period_id=None,
            supervision_violations=[supervision_violation])
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_US_MO,
            external_id=_EXTERNAL_ID,
            supervision_periods=[placeholder_supervision_period])
        sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            sentence_group_id=None,
            incarceration_sentences=[],
            supervision_sentences=[supervision_sentence])
        external_id = attr.evolve(
            self.to_entity(db_external_id),
            person_external_id_id=None)
        person = attr.evolve(
            self.to_entity(db_person),
            person_id=None,
            sentence_groups=[sentence_group],
            external_ids=[external_id])

        expected_supervision_violation = attr.evolve(
            self.to_entity(db_supervision_violation))
        expected_supervision_violation_dup = attr.evolve(
            expected_supervision_violation)
        expected_placeholder_supervision_period_is = attr.evolve(
            placeholder_supervision_period,
            supervision_violations=[expected_supervision_violation])
        expected_placeholder_supervision_period_ss = attr.evolve(
            self.to_entity(db_placeholder_supervision_period),
            supervision_violations=[expected_supervision_violation_dup])
        expected_supervision_sentence = attr.evolve(
            supervision_sentence,
            supervision_periods=[expected_placeholder_supervision_period_ss])
        expected_incarceration_sentence = attr.evolve(
            self.to_entity(db_incarceration_sentence),
            supervision_periods=[expected_placeholder_supervision_period_is])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            incarceration_sentences=[expected_incarceration_sentence],
            supervision_sentences=[expected_supervision_sentence])
        expected_external_id = attr.evolve(self.to_entity(db_external_id))
        expected_person = attr.evolve(
            self.to_entity(db_person),
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])
        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_MO, ingested_people=[person])

        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_removeSeosFromSupervisionViolation(self):
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            state_code=_US_MO,
            external_id=_EXTERNAL_ID_WITH_SEO)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=_US_MO,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_violations=[supervision_violation])
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_US_MO,
            external_id=_EXTERNAL_ID,
            supervision_periods=[supervision_period])
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_MO,
            external_id=_EXTERNAL_ID_WITH_SEO,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[],
            supervision_sentences=[supervision_sentence])
        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_US_MO,
            id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID)
        person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group],
            external_ids=[external_id])

        updated_external_id = _EXTERNAL_ID
        expected_supervision_violation = attr.evolve(
            supervision_violation, external_id=updated_external_id)
        expected_supervision_period = attr.evolve(
            supervision_period,
            supervision_violations=[expected_supervision_violation])
        expected_supervision_sentence = attr.evolve(
            supervision_sentence,
            supervision_periods=[expected_supervision_period])
        expected_sentence_group = attr.evolve(
            sentence_group,
            supervision_sentences=[expected_supervision_sentence])
        expected_external_id = attr.evolve(external_id)
        expected_person = attr.evolve(
            person,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_MO, ingested_people=[person])

        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)
