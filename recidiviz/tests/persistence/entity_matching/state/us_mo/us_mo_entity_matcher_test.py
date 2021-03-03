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
import datetime

import attr

from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodStatus,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecidingBodyType,
    StateSupervisionViolationResponseType,
    StateSupervisionViolationResponseRevocationType,
)
from recidiviz.persistence.entity.state.entities import (
    StatePersonExternalId,
    StatePerson,
    StateSupervisionSentence,
    StateSupervisionViolation,
    StateSupervisionPeriod,
    StateSentenceGroup,
    StateSupervisionViolationResponse,
    StateAgent,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
)
from recidiviz.persistence.entity_matching import entity_matching
from recidiviz.tests.persistence.database.schema.state.schema_test_utils import (
    generate_person,
    generate_external_id,
    generate_incarceration_sentence,
    generate_sentence_group,
    generate_supervision_period,
    generate_supervision_violation,
    generate_agent,
    generate_supervision_sentence,
    generate_supervision_violation_response,
    generate_incarceration_period,
)
from recidiviz.tests.persistence.entity_matching.state.base_state_entity_matcher_test_classes import (
    BaseStateEntityMatcherTest,
)

_EXTERNAL_ID = "EXTERNAL_ID"
_EXTERNAL_ID_2 = "EXTERNAL_ID_2"
_EXTERNAL_ID_3 = "EXTERNAL_ID_3"
_EXTERNAL_ID_WITH_SUFFIX = "EXTERNAL_ID-SEO-FSO"
_FULL_NAME = "FULL_NAME"
_ID_TYPE = "ID_TYPE"
_US_MO = "US_MO"
_DATE_1 = datetime.date(year=2019, month=1, day=1)
_DATE_2 = datetime.date(year=2019, month=2, day=1)
_DATE_3 = datetime.date(year=2019, month=3, day=1)
_DATE_4 = datetime.date(year=2019, month=4, day=1)


class TestMoEntityMatching(BaseStateEntityMatcherTest):
    """Test class for US_MO specific entity matching logic."""

    def test_supervisionViolationsWithDifferentParents_mergesViolations(self) -> None:
        db_person = generate_person(full_name=_FULL_NAME, state_code=_US_MO)
        db_supervision_violation = generate_supervision_violation(
            person=db_person, state_code=_US_MO, external_id=_EXTERNAL_ID
        )
        db_placeholder_supervision_period = generate_supervision_period(
            person=db_person,
            state_code=_US_MO,
            supervision_violation_entries=[db_supervision_violation],
        )
        db_incarceration_sentence = generate_incarceration_sentence(
            person=db_person,
            state_code=_US_MO,
            external_id=_EXTERNAL_ID,
            supervision_periods=[db_placeholder_supervision_period],
        )
        db_sentence_group = generate_sentence_group(
            person=db_person,
            state_code=_US_MO,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence],
        )
        db_external_id = generate_external_id(
            person=db_person, state_code=_US_MO, external_id=_EXTERNAL_ID
        )
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        supervision_violation = attr.evolve(
            self.to_entity(db_supervision_violation),
            supervision_violation_id=None,
            external_id=_EXTERNAL_ID_WITH_SUFFIX,
        )
        placeholder_supervision_period = attr.evolve(
            self.to_entity(db_placeholder_supervision_period),
            supervision_period_id=None,
            supervision_violation_entries=[supervision_violation],
        )
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_US_MO,
            external_id=_EXTERNAL_ID,
            supervision_periods=[placeholder_supervision_period],
        )
        sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            sentence_group_id=None,
            incarceration_sentences=[],
            supervision_sentences=[supervision_sentence],
        )
        external_id = attr.evolve(
            self.to_entity(db_external_id), person_external_id_id=None
        )
        person = attr.evolve(
            self.to_entity(db_person),
            person_id=None,
            sentence_groups=[sentence_group],
            external_ids=[external_id],
        )

        expected_supervision_violation = attr.evolve(
            self.to_entity(db_supervision_violation)
        )
        expected_placeholder_supervision_period_is = attr.evolve(
            placeholder_supervision_period,
            supervision_violation_entries=[expected_supervision_violation],
        )
        expected_placeholder_supervision_period_ss = attr.evolve(
            self.to_entity(db_placeholder_supervision_period),
            supervision_violation_entries=[expected_supervision_violation],
        )
        expected_supervision_sentence = attr.evolve(
            supervision_sentence,
            supervision_periods=[expected_placeholder_supervision_period_ss],
        )
        expected_incarceration_sentence = attr.evolve(
            self.to_entity(db_incarceration_sentence),
            supervision_periods=[expected_placeholder_supervision_period_is],
        )
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            incarceration_sentences=[expected_incarceration_sentence],
            supervision_sentences=[expected_supervision_sentence],
        )
        expected_external_id = attr.evolve(self.to_entity(db_external_id))
        expected_person = attr.evolve(
            self.to_entity(db_person),
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group],
        )
        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_MO, ingested_people=[person]
        )

        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_removeSeosFromSupervisionViolation(self) -> None:
        supervision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_US_MO, external_id=_EXTERNAL_ID_WITH_SUFFIX
            )
        )
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            state_code=_US_MO,
            external_id=_EXTERNAL_ID_WITH_SUFFIX,
            supervision_violation_responses=[supervision_violation_response],
        )
        placeholder_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=_US_MO,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            supervision_violation_entries=[supervision_violation],
        )
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_US_MO,
            external_id=_EXTERNAL_ID,
            supervision_periods=[placeholder_supervision_period],
        )
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_MO,
            external_id=_EXTERNAL_ID_WITH_SUFFIX,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[],
            supervision_sentences=[supervision_sentence],
        )
        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_US_MO, id_type=_ID_TYPE, external_id=_EXTERNAL_ID
        )
        person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group],
            external_ids=[external_id],
            state_code=_US_MO,
        )

        updated_external_id = _EXTERNAL_ID
        expected_supervision_violation_response = attr.evolve(
            supervision_violation_response, external_id=updated_external_id
        )
        expected_supervision_violation = attr.evolve(
            supervision_violation,
            external_id=updated_external_id,
            supervision_violation_responses=[expected_supervision_violation_response],
        )
        expected_placeholder_supervision_period = attr.evolve(
            placeholder_supervision_period,
            supervision_violation_entries=[expected_supervision_violation],
        )
        expected_supervision_sentence = attr.evolve(
            supervision_sentence,
            supervision_periods=[expected_placeholder_supervision_period],
        )
        expected_sentence_group = attr.evolve(
            sentence_group, supervision_sentences=[expected_supervision_sentence]
        )
        expected_external_id = attr.evolve(external_id)
        expected_person = attr.evolve(
            person,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group],
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_MO, ingested_people=[person]
        )

        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_runMatch_supervisingOfficerNotMovedFromPersonOntoOpenSupervisionPeriods(
        self,
    ) -> None:
        db_supervising_officer = generate_agent(
            external_id=_EXTERNAL_ID, state_code=_US_MO
        )

        db_person = generate_person(
            supervising_officer=db_supervising_officer, state_code=_US_MO
        )
        db_external_id = generate_external_id(
            external_id=_EXTERNAL_ID, state_code=_US_MO, id_type=_ID_TYPE
        )
        db_supervision_period = generate_supervision_period(
            person=db_person,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_1,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_US_MO,
            supervising_officer=db_supervising_officer,
        )
        db_supervision_period_another = generate_supervision_period(
            person=db_person,
            external_id=_EXTERNAL_ID_2,
            start_date=_DATE_2,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_US_MO,
            supervising_officer=db_supervising_officer,
        )
        db_closed_supervision_period = generate_supervision_period(
            person=db_person,
            external_id=_EXTERNAL_ID_3,
            start_date=_DATE_3,
            termination_date=_DATE_4,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_US_MO,
            supervising_officer=db_supervising_officer,
        )
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_1,
            supervision_periods=[
                db_supervision_period,
                db_supervision_period_another,
                db_closed_supervision_period,
            ],
        )
        db_sentence_group = generate_sentence_group(
            external_id=_EXTERNAL_ID, supervision_sentences=[db_supervision_sentence]
        )
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]
        self._commit_to_db(db_person)

        external_id = attr.evolve(
            self.to_entity(db_external_id), person_external_id_id=None
        )
        new_supervising_officer = StateAgent.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            state_code=_US_MO,
            agent_type=StateAgentType.SUPERVISION_OFFICER,
        )
        person = StatePerson.new_with_defaults(
            external_ids=[external_id],
            supervising_officer=new_supervising_officer,
            state_code=_US_MO,
        )

        expected_person = attr.evolve(self.to_entity(db_person))

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_MO, ingested_people=[person]
        )

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_runMatch_supervisingOfficerMovedFromSupervisionPeriodToPerson(
        self,
    ) -> None:
        # Arrange
        db_supervising_officer = generate_agent(
            external_id=_EXTERNAL_ID,
            state_code=_US_MO,
            agent_type=StateAgentType.SUPERVISION_OFFICER.value,
        )
        db_person = generate_person(
            supervising_officer=db_supervising_officer, state_code=_US_MO
        )
        db_external_id = generate_external_id(
            external_id=_EXTERNAL_ID, state_code=_US_MO, id_type=_ID_TYPE
        )
        db_supervision_period = generate_supervision_period(
            person=db_person,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_1,
            termination_date=_DATE_2,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_US_MO,
            supervising_officer=db_supervising_officer,
        )
        db_supervision_period_open = generate_supervision_period(
            person=db_person,
            external_id=_EXTERNAL_ID_2,
            start_date=_DATE_2,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_US_MO,
            supervising_officer=db_supervising_officer,
        )
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            external_id=_EXTERNAL_ID,
            state_code=_US_MO,
            start_date=_DATE_1,
            supervision_periods=[db_supervision_period, db_supervision_period_open],
        )
        db_sentence_group = generate_sentence_group(
            external_id=_EXTERNAL_ID,
            state_code=_US_MO,
            supervision_sentences=[db_supervision_sentence],
        )
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]
        self._commit_to_db(db_person)

        new_supervising_officer = StateAgent.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            state_code=_US_MO,
            agent_type=StateAgentType.SUPERVISION_OFFICER,
        )

        new_supervision_period = StateSupervisionPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_3,
            state_code=_US_MO,
            start_date=_DATE_3,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            supervising_officer=new_supervising_officer,
        )
        supervision_period_update = StateSupervisionPeriod.new_with_defaults(
            external_id=db_supervision_period_open.external_id,
            state_code=_US_MO,
            termination_date=_DATE_3,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            external_id=db_supervision_sentence.external_id,
            state_code=_US_MO,
            supervision_periods=[supervision_period_update, new_supervision_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=db_sentence_group.external_id,
            state_code=_US_MO,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence],
        )

        external_id = attr.evolve(
            self.to_entity(db_external_id), person_external_id_id=None
        )
        person = StatePerson.new_with_defaults(
            external_ids=[external_id],
            sentence_groups=[sentence_group],
            state_code=_US_MO,
        )

        expected_person = attr.evolve(self.to_entity(db_person))
        expected_person.supervising_officer = new_supervising_officer
        expected_supervision_sentence = expected_person.sentence_groups[
            0
        ].supervision_sentences[0]

        expected_unchanged_supervision_period = attr.evolve(
            self.to_entity(db_supervision_period)
        )
        expected_updated_supervision_period = attr.evolve(
            self.to_entity(db_supervision_period_open),
            termination_date=supervision_period_update.termination_date,
            supervising_officer=expected_unchanged_supervision_period.supervising_officer,
        )
        expected_supervision_sentence.supervision_periods = [
            expected_unchanged_supervision_period,
            expected_updated_supervision_period,
            new_supervision_period,
        ]

        # Act
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_MO, ingested_people=[person]
        )

        # Assert
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_runMatch_supervisionPeriodDateChangesSoItDoesNotMatchSentenceOrViolations(
        self,
    ) -> None:
        # Arrange
        db_supervising_officer = generate_agent(
            external_id=_EXTERNAL_ID,
            state_code=_US_MO,
            agent_type=StateAgentType.SUPERVISION_OFFICER.value,
        )
        db_person = generate_person(
            supervising_officer=db_supervising_officer, state_code=_US_MO
        )
        db_external_id = generate_external_id(
            external_id=_EXTERNAL_ID, state_code=_US_MO, id_type=_ID_TYPE
        )

        # Violation has been date matched to the open supervision period
        db_supervision_violation = generate_supervision_violation(
            person=db_person,
            state_code=_US_MO,
            external_id=_EXTERNAL_ID,
            violation_date=_DATE_4,
        )

        db_supervision_period_open = generate_supervision_period(
            person=db_person,
            external_id=_EXTERNAL_ID_2,
            start_date=_DATE_2,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_US_MO,
            supervising_officer=db_supervising_officer,
            supervision_violation_entries=[db_supervision_violation],
        )
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            external_id=_EXTERNAL_ID,
            state_code=_US_MO,
            start_date=_DATE_1,
            supervision_periods=[db_supervision_period_open],
        )
        db_sentence_group = generate_sentence_group(
            external_id=_EXTERNAL_ID,
            state_code=_US_MO,
            supervision_sentences=[db_supervision_sentence],
        )
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]
        self._commit_to_db(db_person)

        supervsion_period_updated = StateSupervisionPeriod.new_with_defaults(
            state_code=_US_MO,
            external_id=db_supervision_period_open.external_id,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            start_date=_DATE_2,
            termination_date=_DATE_3,
        )

        placeholder_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code=_US_MO,
            supervision_periods=[supervsion_period_updated],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=db_sentence_group.external_id,
            state_code=_US_MO,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[placeholder_supervision_sentence],
        )
        external_id = StatePersonExternalId.new_with_defaults(
            external_id=db_external_id.external_id,
            state_code=_US_MO,
            id_type=db_external_id.id_type,
        )
        person = StatePerson.new_with_defaults(
            external_ids=[external_id],
            sentence_groups=[sentence_group],
            state_code=_US_MO,
        )

        expected_person = attr.evolve(self.to_entity(db_person))
        expected_sentence = expected_person.sentence_groups[0].supervision_sentences[0]
        expected_original_supervision_period = expected_sentence.supervision_periods[0]

        # Violation is moved off of the supervision period (it no longer matches) and the termination date is updated
        expected_original_supervision_period.supervision_violation_entries = []
        expected_original_supervision_period.termination_date = _DATE_3

        # A placeholder periods is created to hold the existing supervision violation
        expected_new_placeholder_supervision_period = (
            StateSupervisionPeriod.new_with_defaults(
                state_code=_US_MO,
                status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
                supervision_violation_entries=[
                    self.to_entity(db_supervision_violation)
                ],
            )
        )
        expected_sentence.supervision_periods.append(
            expected_new_placeholder_supervision_period
        )

        # Act
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_MO, ingested_people=[person]
        )

        # Assert
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_ssvrFlatFieldMatchingWithSomeNullValues(self) -> None:
        db_person = generate_person(state_code=_US_MO)
        db_supervision_violation_response = generate_supervision_violation_response(
            person=db_person,
            state_code=_US_MO,
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION.value,
            response_type_raw_text=StateSupervisionViolationResponseType.PERMANENT_DECISION.value,
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD.value,
            deciding_body_type_raw_text=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD.value,
        )
        db_incarceration_period = generate_incarceration_period(
            person=db_person,
            state_code=_US_MO,
            external_id=_EXTERNAL_ID,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION.value,
            source_supervision_violation_response=db_supervision_violation_response,
        )
        db_incarceration_sentence = generate_incarceration_sentence(
            person=db_person,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_US_MO,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[db_incarceration_period],
        )
        db_sentence_group = generate_sentence_group(
            person=db_person,
            state_code=_US_MO,
            external_id=_EXTERNAL_ID_WITH_SUFFIX,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            incarceration_sentences=[db_incarceration_sentence],
            supervision_sentences=[],
        )
        db_external_id = generate_external_id(
            person=db_person,
            state_code=_US_MO,
            id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID,
        )
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]

        self._commit_to_db(db_person)

        # Even though this violation response doesn't have a deciding_body_type set, it will not clear the values in
        # db_supervision_violation_response.
        supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_US_MO,
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text=StateSupervisionViolationResponseType.PERMANENT_DECISION.value,
        )
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_US_MO,
            external_id=_EXTERNAL_ID,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            source_supervision_violation_response=supervision_violation_response,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_US_MO,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[incarceration_period],
        )
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_MO,
            external_id=_EXTERNAL_ID_WITH_SUFFIX,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[incarceration_sentence],
            supervision_sentences=[],
        )
        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_US_MO, id_type=_ID_TYPE, external_id=_EXTERNAL_ID
        )
        person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group],
            external_ids=[external_id],
            state_code=_US_MO,
        )

        expected_person = self.to_entity(db_person)

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_MO, ingested_people=[person]
        )

        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_ssvrFlatFieldMatchingRevocationTypeChanges(self) -> None:
        db_person = generate_person(state_code=_US_MO)
        db_supervision_violation_response = generate_supervision_violation_response(
            person=db_person,
            state_code=_US_MO,
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION.value,
            response_type_raw_text=StateSupervisionViolationResponseType.PERMANENT_DECISION.value,
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD.value,
            deciding_body_type_raw_text=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD.value,
            revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION.value,
            revocation_type_raw_text="S",
        )
        db_incarceration_period = generate_incarceration_period(
            person=db_person,
            state_code=_US_MO,
            external_id=_EXTERNAL_ID,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION.value,
            source_supervision_violation_response=db_supervision_violation_response,
        )
        db_incarceration_sentence = generate_incarceration_sentence(
            person=db_person,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_US_MO,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[db_incarceration_period],
        )
        db_sentence_group = generate_sentence_group(
            person=db_person,
            state_code=_US_MO,
            external_id=_EXTERNAL_ID_WITH_SUFFIX,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            incarceration_sentences=[db_incarceration_sentence],
            supervision_sentences=[],
        )
        db_external_id = generate_external_id(
            person=db_person,
            state_code=_US_MO,
            id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID,
        )
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]

        self._commit_to_db(db_person)

        # Even though the revocation type has changed, we still allow a match
        supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_US_MO,
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text=StateSupervisionViolationResponseType.PERMANENT_DECISION.value,
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD.value,
            revocation_type=StateSupervisionViolationResponseRevocationType.TREATMENT_IN_PRISON,
            revocation_type_raw_text="L",
        )
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_US_MO,
            external_id=_EXTERNAL_ID,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            source_supervision_violation_response=supervision_violation_response,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_US_MO,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[incarceration_period],
        )
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_MO,
            external_id=_EXTERNAL_ID_WITH_SUFFIX,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[incarceration_sentence],
            supervision_sentences=[],
        )
        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_US_MO, id_type=_ID_TYPE, external_id=_EXTERNAL_ID
        )
        person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group],
            external_ids=[external_id],
            state_code=_US_MO,
        )

        expected_person = self.to_entity(db_person)
        expected_ip = (
            expected_person.sentence_groups[0]
            .incarceration_sentences[0]
            .incarceration_periods[0]
        )
        expected_ssvr = expected_ip.source_supervision_violation_response
        expected_ssvr.revocation_type = (
            StateSupervisionViolationResponseRevocationType.TREATMENT_IN_PRISON
        )
        expected_ssvr.revocation_type_raw_text = "L"

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_MO, ingested_people=[person]
        )

        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)
