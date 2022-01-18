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
"""Tests for state_entity_matcher.py."""
import datetime
import unittest
from typing import Any, List, Tuple

import attr
from mock import patch
from more_itertools import one

from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.person_characteristics import Ethnicity, Gender, Race
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
)
from recidiviz.common.ingest_metadata import IngestMetadata, SystemLevel
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.entities import EntityPersonType
from recidiviz.persistence.entity.state.entities import (
    StateAgent,
    StateAssessment,
    StateCharge,
    StateCourtCase,
    StateIncarcerationIncident,
    StateIncarcerationIncidentOutcome,
    StateIncarcerationSentence,
    StatePerson,
    StatePersonAlias,
    StatePersonEthnicity,
    StatePersonExternalId,
    StatePersonRace,
    StateSupervisionCaseTypeEntry,
    StateSupervisionSentence,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
)
from recidiviz.persistence.entity_matching import entity_matching
from recidiviz.persistence.entity_matching.entity_matching_types import MatchedEntities
from recidiviz.persistence.entity_matching.state import state_entity_matcher
from recidiviz.persistence.entity_matching.state.base_state_matching_delegate import (
    BaseStateMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.state_entity_matcher import (
    MAX_NUM_TREES_TO_SEARCH_FOR_NON_PLACEHOLDER_TYPES,
    StateEntityMatcher,
)
from recidiviz.persistence.errors import EntityMatchingError
from recidiviz.tests.persistence.database.schema.state.schema_test_utils import (
    generate_agent,
    generate_alias,
    generate_assessment,
    generate_charge,
    generate_court_case,
    generate_ethnicity,
    generate_external_id,
    generate_incarceration_incident,
    generate_incarceration_period,
    generate_incarceration_sentence,
    generate_person,
    generate_race,
    generate_supervision_case_type_entry,
    generate_supervision_period,
    generate_supervision_sentence,
    generate_supervision_violated_condition_entry,
    generate_supervision_violation,
    generate_supervision_violation_response,
    generate_supervision_violation_response_decision_entry,
    generate_supervision_violation_type_entry,
)
from recidiviz.tests.persistence.entity_matching.state.base_state_entity_matcher_test_classes import (
    BaseStateEntityMatcherTest,
)

_EXTERNAL_ID = "EXTERNAL_ID-1"
_EXTERNAL_ID_2 = "EXTERNAL_ID-2"
_EXTERNAL_ID_3 = "EXTERNAL_ID-3"
_EXTERNAL_ID_4 = "EXTERNAL_ID-4"
_EXTERNAL_ID_5 = "EXTERNAL_ID-5"
_EXTERNAL_ID_6 = "EXTERNAL_ID-6"
_ID_TYPE = "ID_TYPE"
_ID_TYPE_ANOTHER = "ID_TYPE_ANOTHER"
_FULL_NAME = "FULL_NAME"
_FULL_NAME_ANOTHER = "FULL_NAME_ANOTHER"
_COUNTY_CODE = "Iredell"
_STATE_CODE = "US_XX"
_DATE_1 = datetime.date(year=2019, month=1, day=1)
_DATE_2 = datetime.date(year=2019, month=2, day=1)
_DATE_3 = datetime.date(year=2019, month=3, day=1)
DEFAULT_METADATA = IngestMetadata(
    region="us_xx",
    system_level=SystemLevel.STATE,
    ingest_time=datetime.datetime(year=1000, month=1, day=1),
    database_key=SQLAlchemyDatabaseKey.canonical_for_schema(
        schema_type=SchemaType.STATE
    ),
)


class TestStateEntityMatching(BaseStateEntityMatcherTest):
    """Tests for default state entity matching logic."""

    def setUp(self) -> None:
        super().setUp()
        self.matching_delegate_patcher = patch(
            "recidiviz.persistence.entity_matching.state."
            "state_matching_delegate_factory.StateMatchingDelegateFactory."
            "build",
            new=self._get_base_delegate,
        )
        self.matching_delegate_patcher.start()
        self.addCleanup(self.matching_delegate_patcher.stop)

    def _get_base_delegate(self, **_kwargs: Any) -> BaseStateMatchingDelegate:
        return BaseStateMatchingDelegate(_STATE_CODE, DEFAULT_METADATA)

    @staticmethod
    def _match(
        session: Session, ingested_people: List[EntityPersonType]
    ) -> MatchedEntities:
        return entity_matching.match(
            session, _STATE_CODE, ingested_people, DEFAULT_METADATA
        )

    def test_match_newPerson(self) -> None:
        # Arrange 1 - Match
        db_person = schema.StatePerson(full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_external_id = schema.StatePersonExternalId(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE,
        )
        db_person.external_ids = [db_external_id]
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        external_id_2 = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2, id_type=_ID_TYPE
        )
        person_2 = StatePerson.new_with_defaults(
            full_name=_FULL_NAME, external_ids=[external_id_2], state_code=_STATE_CODE
        )

        expected_db_person = entity_person
        expected_person_2 = attr.evolve(person_2)

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, [person_2])

        self.assert_people_match_pre_and_post_commit(
            [expected_person_2],
            matched_entities.people,
            session,
            expected_unmatched_db_people=[expected_db_person],
        )
        self.assertEqual(1, matched_entities.total_root_entities)
        self.assert_no_errors(matched_entities)

    def test_match_overwriteAgent(self) -> None:
        # Arrange 1 - Match
        db_agent = generate_agent(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            agent_type=StateAgentType.SUPERVISION_OFFICER.value,
        )
        db_external_id = generate_external_id(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE,
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person = generate_person(
            full_name=_FULL_NAME,
            external_ids=[db_external_id],
            supervising_officer=db_agent,
            state_code=_STATE_CODE,
        )
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        external_id = attr.evolve(entity_external_id, person_external_id_id=None)
        agent = StateAgent.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE,
            agent_type=StateAgentType.SUPERVISION_OFFICER,
        )
        person = attr.evolve(
            entity_person,
            person_id=None,
            external_ids=[external_id],
            supervising_officer=agent,
        )

        expected_external_id = attr.evolve(
            external_id,
        )
        expected_agent = attr.evolve(agent)
        expected_person = attr.evolve(
            person,
            external_ids=[expected_external_id],
            supervising_officer=expected_agent,
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, [person])

        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )

    def test_match_twoMatchingIngestedPersons(self) -> None:
        # Arrange
        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, id_type=_ID_TYPE
        )
        external_id_2 = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, id_type=_ID_TYPE_ANOTHER
        )
        external_id_dup = attr.evolve(external_id)
        external_id_3 = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2, id_type=_ID_TYPE_ANOTHER
        )
        person = StatePerson.new_with_defaults(
            full_name=_FULL_NAME,
            external_ids=[external_id, external_id_2],
            state_code=_STATE_CODE,
        )
        person_2 = StatePerson.new_with_defaults(
            full_name=_FULL_NAME,
            external_ids=[external_id_dup, external_id_3],
            state_code=_STATE_CODE,
        )

        expected_external_id_1 = attr.evolve(external_id)
        expected_external_id_2 = attr.evolve(external_id_2)
        expected_external_id_3 = attr.evolve(external_id_3)
        expected_person = attr.evolve(
            person,
            external_ids=[
                expected_external_id_1,
                expected_external_id_2,
                expected_external_id_3,
            ],
        )

        # Act
        session = self._session()
        matched_entities = self._match(session, [person, person_2])

        # Assert
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_match_twoMatchingIngestedPersons_with_children(self) -> None:
        # Arrange
        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, id_type=_ID_TYPE
        )
        external_id_dup = attr.evolve(external_id)

        assessment = StateAssessment.new_with_defaults(
            state_code=_STATE_CODE, external_id="1"
        )
        assessment_dup = attr.evolve(assessment)
        assessment2 = StateAssessment.new_with_defaults(
            state_code=_STATE_CODE, external_id="2"
        )
        assessment3 = StateAssessment.new_with_defaults(
            state_code=_STATE_CODE, external_id="3"
        )

        person = StatePerson.new_with_defaults(
            full_name=_FULL_NAME,
            external_ids=[external_id],
            assessments=[assessment, assessment2],
            state_code=_STATE_CODE,
        )

        person_2 = StatePerson.new_with_defaults(
            full_name=_FULL_NAME,
            external_ids=[external_id_dup],
            assessments=[assessment_dup, assessment3],
            state_code=_STATE_CODE,
        )

        expected_external_id_1 = attr.evolve(external_id)
        expected_assessment_1 = attr.evolve(assessment)
        expected_assessment_2 = attr.evolve(assessment2)
        expected_assessment_3 = attr.evolve(assessment3)
        expected_person = attr.evolve(
            person,
            external_ids=[
                expected_external_id_1,
            ],
            assessments=[
                expected_assessment_1,
                expected_assessment_2,
                expected_assessment_3,
            ],
        )
        self.maxDiff = None
        # Act
        session = self._session()
        matched_entities = self._match(session, [person, person_2])

        # Assert
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_match_MatchingIngestedPersons_with_children_perf(self) -> None:
        # Arrange
        people = []
        incidents = []
        for i in range(1000):
            external_id = StatePersonExternalId.new_with_defaults(
                state_code=_STATE_CODE, external_id=_EXTERNAL_ID, id_type=_ID_TYPE
            )

            incident = StateIncarcerationIncident.new_with_defaults(
                state_code=_STATE_CODE,
                external_id=f"{i}",
                incarceration_incident_outcomes=[
                    StateIncarcerationIncidentOutcome.new_with_defaults(
                        state_code=_STATE_CODE,
                        external_id=f"{i}",
                    )
                ],
            )
            incidents.append(incident)

            people.append(
                StatePerson.new_with_defaults(
                    full_name=_FULL_NAME,
                    external_ids=[external_id],
                    incarceration_incidents=[incident],
                    state_code=_STATE_CODE,
                )
            )

        expected_person = StatePerson.new_with_defaults(
            full_name=_FULL_NAME,
            external_ids=[
                StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE, external_id=_EXTERNAL_ID, id_type=_ID_TYPE
                )
            ],
            incarceration_incidents=incidents,
            state_code=_STATE_CODE,
        )

        # Act
        session = self._session()
        start = datetime.datetime.now()
        matched_entities = self._match(session, people)
        end = datetime.datetime.now()
        duration_seconds = (end - start).total_seconds()
        self.assertTrue(
            duration_seconds < 10, f"Runtime [{duration_seconds}] not below threshold"
        )

        # Assert
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_match_noPlaceholders_simple(self) -> None:
        # Arrange 1 - Match
        db_person = schema.StatePerson(full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_external_id = schema.StatePersonExternalId(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE,
        )
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, id_type=_ID_TYPE
        )
        external_id_2 = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2, id_type=_ID_TYPE
        )
        person = StatePerson.new_with_defaults(
            full_name=_FULL_NAME,
            external_ids=[external_id, external_id_2],
            state_code=_STATE_CODE,
        )

        expected_person = attr.evolve(
            person,
            external_ids=[
                attr.evolve(
                    external_id,
                ),
                external_id_2,
            ],
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, [person])

        # Assert 1 - Match
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )

    def test_match_noPlaceholders_success(self) -> None:
        # Arrange 1 - Match
        db_person = schema.StatePerson(full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_incarceration_sentence = schema.StateIncarcerationSentence(
            person=db_person,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            county_code="county_code",
        )
        db_external_id = schema.StatePersonExternalId(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE,
        )

        db_person.incarceration_sentences = [db_incarceration_sentence]
        db_person.external_ids = [db_external_id]

        db_external_id_another = schema.StatePersonExternalId(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2,
            id_type=_ID_TYPE,
        )
        db_person_another = schema.StatePerson(
            full_name=_FULL_NAME,
            external_ids=[db_external_id_another],
            state_code=_STATE_CODE,
        )
        self._commit_to_db(db_person, db_person_another)

        new_charge = StateCharge.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
        )
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            external_id=_EXTERNAL_ID,
            county_code="county_code-updated",
            charges=[new_charge],
        )
        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, id_type=_ID_TYPE
        )
        person = StatePerson.new_with_defaults(
            full_name=_FULL_NAME,
            external_ids=[external_id],
            incarceration_sentences=[incarceration_sentence],
            state_code=_STATE_CODE,
        )

        external_id_another = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2, id_type=_ID_TYPE
        )
        person_another = StatePerson.new_with_defaults(
            full_name=_FULL_NAME,
            external_ids=[external_id_another],
            state_code=_STATE_CODE,
        )

        expected_person = attr.evolve(person, external_ids=[])
        expected_charge = attr.evolve(new_charge)
        expected_incarceration_sentence = attr.evolve(
            incarceration_sentence,
            charges=[expected_charge],
        )
        expected_external_id = attr.evolve(
            external_id,
        )
        expected_person.incarceration_sentences = [expected_incarceration_sentence]
        expected_person.external_ids = [expected_external_id]

        expected_person_another = attr.evolve(person_another)
        expected_external_id = attr.evolve(external_id_another)
        expected_person_another.external_ids = [expected_external_id]

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, [person, person_another])

        # Assert 1 - Match
        self.assert_no_errors(matched_entities)
        self.assertEqual(2, matched_entities.total_root_entities)
        self.assert_people_match_pre_and_post_commit(
            [expected_person, expected_person_another], matched_entities.people, session
        )

    def test_match_ErrorMergingIngestedEntities(self) -> None:
        # Arrange 1 - Match
        db_external_id = generate_external_id(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE,
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person = generate_person(
            external_ids=[db_external_id], state_code=_STATE_CODE
        )
        entity_person = self.to_entity(db_person)

        db_external_id_2 = generate_external_id(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2,
            id_type=_ID_TYPE,
        )
        entity_external_id_2 = self.to_entity(db_external_id_2)
        db_incarceration_sentence = generate_incarceration_sentence(
            person=db_person, external_id=_EXTERNAL_ID
        )
        entity_incarceration_sentence = self.to_entity(db_incarceration_sentence)
        db_person_2 = generate_person(
            incarceration_sentences=[db_incarceration_sentence],
            external_ids=[db_external_id_2],
            state_code=_STATE_CODE,
        )
        entity_person_2 = self.to_entity(db_person_2)

        self._commit_to_db(db_person, db_person_2)

        external_id = attr.evolve(entity_external_id, person_external_id_id=None)
        person = attr.evolve(entity_person, person_id=None, external_ids=[external_id])

        incarceration_sentence = attr.evolve(
            entity_incarceration_sentence, incarceration_sentence_id=None
        )
        incarceration_sentence_dup = attr.evolve(
            incarceration_sentence, county_code=_COUNTY_CODE
        )
        external_id_2 = attr.evolve(entity_external_id_2, person_external_id_id=None)
        person_2 = attr.evolve(
            entity_person_2,
            person_id=None,
            external_ids=[external_id_2],
            incarceration_sentences=[
                incarceration_sentence,
                incarceration_sentence_dup,
            ],
        )

        # Act 1 - Match
        session = self._session()
        with self.assertRaisesRegex(
            EntityMatchingError,
            r"Found multiple different ingested entities of type \[StateIncarcerationSentence\] "
            r"with conflicting information",
        ):
            _ = self._match(session, [person, person_2])

    def test_matchPersons_multipleIngestedPeopleMatchOneDbPerson(self) -> None:
        db_external_id = generate_external_id(
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE
        )
        entity_external_id = self.to_entity(db_external_id)
        db_external_id_2 = generate_external_id(
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE_ANOTHER,
        )
        entity_external_id_2 = self.to_entity(db_external_id_2)
        db_person = generate_person(
            full_name=_FULL_NAME,
            external_ids=[db_external_id, db_external_id_2],
            state_code=_STATE_CODE,
        )
        entity_person = self.to_entity(db_person)
        self._commit_to_db(db_person)
        external_id = attr.evolve(entity_external_id, person_external_id_id=None)
        race_1 = StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE, race=Race.WHITE
        )
        person = attr.evolve(
            entity_person,
            person_id=None,
            races=[race_1],
            external_ids=[external_id],
        )
        race_2 = StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE, race=Race.OTHER
        )
        external_id_2 = attr.evolve(entity_external_id_2, person_external_id_id=None)
        person_dup = attr.evolve(
            person, races=[race_2], external_ids=[attr.evolve(external_id_2)]
        )

        expected_external_id = attr.evolve(
            external_id,
        )
        expected_external_id_2 = attr.evolve(external_id_2)
        expected_race_1 = attr.evolve(race_1)
        expected_race_2 = attr.evolve(race_2)
        expected_person = attr.evolve(
            person,
            races=[expected_race_1, expected_race_2],
            external_ids=[expected_external_id, expected_external_id_2],
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, [person, person_dup])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assertEqual(2, matched_entities.total_root_entities)
        self.assert_no_errors(matched_entities)

    def test_matchPersons_conflictingExternalIds_error(self) -> None:
        # Arrange 1 - Match
        db_person = generate_person(state_code=_STATE_CODE)
        db_court_case = generate_court_case(
            person=db_person,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code="county_code",
        )
        entity_court_case = self.to_entity(db_court_case)
        db_charge = generate_charge(
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            description="charge_1",
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            court_case=db_court_case,
        )
        entity_charge = self.to_entity(db_charge)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            county_code="county_code",
            charges=[db_charge],
        )
        entity_supervision_sentence = self.to_entity(db_supervision_sentence)
        db_incarceration_sentence = generate_incarceration_sentence(
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code="county_code",
        )
        db_external_id = generate_external_id(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE,
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person.external_ids = [db_external_id]
        db_person.incarceration_sentences = [db_incarceration_sentence]
        db_person.supervision_sentences = [db_supervision_sentence]
        entity_person = self.to_entity(db_person)
        self._commit_to_db(db_person)

        conflicting_court_case = attr.evolve(
            entity_court_case,
            court_case_id=None,
            external_id=_EXTERNAL_ID_2,
        )
        charge_1 = attr.evolve(
            entity_charge, charge_id=None, court_case=conflicting_court_case
        )
        supervision_sentence = attr.evolve(
            entity_supervision_sentence,
            supervision_sentence_id=None,
            charges=[charge_1],
        )
        external_id = attr.evolve(entity_external_id, person_external_id_id=None)
        person = attr.evolve(
            entity_person,
            person_id=None,
            external_ids=[external_id],
            supervision_sentences=[supervision_sentence],
        )
        expected_unmatched_person = attr.evolve(entity_person)

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [],
            matched_entities.people,
            session,
            expected_unmatched_db_people=[expected_unmatched_person],
        )
        self.assertEqual(1, matched_entities.total_root_entities)
        self.assertEqual(1, matched_entities.error_count)
        self.assertEqual(0, matched_entities.database_cleanup_error_count)

    # TODO(#3194): Uncomment asserts once the bug is fixed.
    def test_matchPersons_replaceSingularChildFromMatchedParent(self) -> None:
        """Tests that if we have a singular placeholder child (ex StateCharge.court_case) in the DB, we properly update
        that child entity to no longer be a placeholder in the case that we have match an ingested parent with a
        non-placeholder child.
        """
        # When our DB is in a strange state and has an entity has a singluar placeholder child, we throw an error
        # when that child entity is updated. This test has logic that needs to be uncommented once this bug is resolved.
        # Arrange 1 - Match
        db_person = generate_person(state_code=_STATE_CODE)
        db_placeholder_court_case = generate_court_case(
            person=db_person, state_code=_STATE_CODE
        )
        entity_placeholder_court_case = self.to_entity(db_placeholder_court_case)
        db_charge = generate_charge(
            person=db_person,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            court_case=db_placeholder_court_case,
        )
        entity_charge = self.to_entity(db_charge)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            charges=[db_charge],
            state_code=_STATE_CODE,
        )
        entity_supervision_sentence = self.to_entity(db_supervision_sentence)
        db_external_id = generate_external_id(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE,
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person.supervision_sentences = [db_supervision_sentence]
        db_person.external_ids = [db_external_id]
        entity_person = self.to_entity(db_person)
        self._commit_to_db(db_person)

        # New court case should overwrite the existing placeholder.
        court_case = attr.evolve(
            entity_placeholder_court_case,
            court_case_id=None,
            external_id=_EXTERNAL_ID,
        )
        charge = attr.evolve(
            entity_charge,
            charge_id=None,
            external_id=_EXTERNAL_ID,
            court_case=court_case,
        )
        sentence = attr.evolve(
            entity_supervision_sentence,
            supervision_sentence_id=None,
            charges=[charge],
        )
        external_id = attr.evolve(entity_external_id, person_external_id_id=None)
        person = attr.evolve(
            entity_person,
            person_id=None,
            external_ids=[external_id],
            supervision_sentences=[sentence],
        )

        expected_updated_court_case = attr.evolve(
            court_case, court_case_id=entity_placeholder_court_case.court_case_id
        )
        expected_charge = attr.evolve(
            entity_charge, court_case=expected_updated_court_case
        )
        expected_supervision_sentence = attr.evolve(
            entity_supervision_sentence, charges=[expected_charge]
        )
        expected_external_id = attr.evolve(entity_external_id)
        _expected_person = attr.evolve(
            entity_person,
            external_ids=[expected_external_id],
            supervision_sentences=[expected_supervision_sentence],
        )

        # Act 1 - Match
        session = self._session()
        _matched_entities = self._match(session, ingested_people=[person])

        # Assert 1 - Match
        # TODO(#3194): Uncomment once resolved
        # self.assert_people_match_pre_and_post_commit([expected_person], matched_entities.people, session)
        # self.assertEqual(1, matched_entities.total_root_entities)
        # self.assertEqual(0, matched_entities.error_count)
        # self.assertEqual(0, matched_entities.database_cleanup_error_count)

    def test_matchPersons_partialUpdateBeforeError(self) -> None:
        """Tests that if a DB person tree is partially updated before an error occurs, that we properly set
        backedges on all entities of the person tree, even though an error occurred. This is important when the partial
        merge added a new entity to the DB tree.
        """
        # Arrange 1 - Match
        db_person = generate_person(state_code=_STATE_CODE)
        db_placeholder_charge = generate_charge(
            person=db_person,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
        )
        entity_placeholder_charge = self.to_entity(db_placeholder_charge)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            charges=[db_placeholder_charge],
            state_code=_STATE_CODE,
        )
        entity_supervision_sentence = self.to_entity(db_supervision_sentence)

        db_court_case_2 = generate_court_case(
            external_id=_EXTERNAL_ID_2, person=db_person, state_code=_STATE_CODE
        )
        entity_court_case_2 = self.to_entity(db_court_case_2)
        db_charge_2 = generate_charge(
            external_id=_EXTERNAL_ID_2,
            person=db_person,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            court_case=db_court_case_2,
        )
        entity_charge_2 = self.to_entity(db_charge_2)
        db_supervision_sentence_2 = generate_supervision_sentence(
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE,
            charges=[db_charge_2],
        )
        entity_supervision_sentence_2 = self.to_entity(db_supervision_sentence_2)
        db_external_id = generate_external_id(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, id_type=_ID_TYPE
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person.supervision_sentences = [
            db_supervision_sentence,
            db_supervision_sentence_2,
        ]
        db_person.external_ids = [db_external_id]
        entity_person = self.to_entity(db_person)
        self._commit_to_db(db_person)

        charge_1 = attr.evolve(
            entity_placeholder_charge,
            charge_id=None,
            external_id=_EXTERNAL_ID,
        )
        sentence_1 = attr.evolve(
            entity_supervision_sentence,
            supervision_sentence_id=None,
            charges=[charge_1],
        )

        # court case conflicts with existing court case in DB, throws error.
        conflicting_court_case = attr.evolve(
            entity_court_case_2, external_id=_EXTERNAL_ID_3
        )
        charge_2 = StateCharge.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            charge_id=None,
            court_case=conflicting_court_case,
        )
        sentence_2 = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            external_id=_EXTERNAL_ID_2,
            charges=[charge_2],
        )
        external_id = attr.evolve(entity_external_id, person_external_id_id=None)
        person = attr.evolve(
            entity_person,
            person_id=None,
            external_ids=[external_id],
            supervision_sentences=[sentence_1, sentence_2],
        )

        expected_placeholder_charge = attr.evolve(entity_placeholder_charge)
        expected_charge_1 = attr.evolve(charge_1)
        expected_supervision_sentence = attr.evolve(
            entity_supervision_sentence,
            charges=[expected_placeholder_charge, expected_charge_1],
        )
        expected_unchanged_court_case = attr.evolve(entity_court_case_2)
        expected_charge_2 = attr.evolve(
            entity_charge_2, court_case=expected_unchanged_court_case
        )
        expected_supervision_sentence_2 = attr.evolve(
            entity_supervision_sentence_2, charges=[expected_charge_2]
        )
        expected_external_id = attr.evolve(entity_external_id)
        expected_person = attr.evolve(
            entity_person,
            external_ids=[expected_external_id],
            supervision_sentences=[
                expected_supervision_sentence,
                expected_supervision_sentence_2,
            ],
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [],
            matched_entities.people,
            session,
            expected_unmatched_db_people=[expected_person],
        )
        self.assertEqual(1, matched_entities.total_root_entities)
        self.assertEqual(1, matched_entities.error_count)
        self.assertEqual(0, matched_entities.database_cleanup_error_count)

    def test_ingestedTreeHasDuplicateEntitiesInParentsAndChildrenEntities(self) -> None:
        """This tests an edge case in the interaction of `merge_multiparent_entities` and
        `_merge_new_parent_child_links`. Here we ingest duplicate supervision sentences with duplicate charge children.
        In entity matching, the charge children get merged by `merge_multiparent_entities`, and the
        supervision sentences get merged as a part of `merge_new_parent_child_links`. This test ensures that in
        this second step, we do NOT accidentally make the charge children placeholder objects.
        """
        # Arrange 1 - Match
        external_id = StatePersonExternalId.new_with_defaults(
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE, state_code=_STATE_CODE
        )
        charge = StateCharge.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=ChargeStatus.DROPPED,
        )
        charge_dup = attr.evolve(charge)
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge],
        )
        supervision_sentence_dup = attr.evolve(
            supervision_sentence, charges=[charge_dup]
        )
        person = StatePerson.new_with_defaults(
            full_name=_FULL_NAME,
            external_ids=[external_id],
            supervision_sentences=[supervision_sentence, supervision_sentence_dup],
            state_code=_STATE_CODE,
        )

        expected_charge = attr.evolve(charge)
        expected_supervision_sentence = attr.evolve(
            supervision_sentence, charges=[expected_charge]
        )
        expected_external_id = attr.evolve(external_id)
        expected_person = attr.evolve(
            person,
            supervision_sentences=[expected_supervision_sentence],
            external_ids=[expected_external_id],
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, [person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )

    def test_matchPersons_matchesTwoDbPeople_mergeDbPeopleMoveChildren(self) -> None:
        """Tests that our system correctly handles the situation where we have
        2 distinct people in our DB, but we learn the two DB people should be
        merged into 1 person based on a new ingested person. Here the two DB
        people to merge have distinct DB children.
        """
        # Arrange 1 - Match
        db_external_id = generate_external_id(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE,
        )
        entity_external_id = self.to_entity(db_external_id)
        db_external_id_2 = generate_external_id(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2,
            id_type=_ID_TYPE_ANOTHER,
        )
        entity_external_id_2 = self.to_entity(db_external_id_2)
        db_person = generate_person(
            full_name=_FULL_NAME,
            external_ids=[db_external_id],
            state_code=_STATE_CODE,
        )
        entity_person = self.to_entity(db_person)

        db_person_2 = generate_person(
            full_name=_FULL_NAME,
            external_ids=[db_external_id_2],
            state_code=_STATE_CODE,
        )
        db_incarceration_sentence = generate_incarceration_sentence(
            person=db_person_2, external_id=_EXTERNAL_ID
        )
        db_person_2.incarceration_sentences = [db_incarceration_sentence]
        entity_incarceration_sentence = self.to_entity(db_incarceration_sentence)
        entity_person_2 = self.to_entity(db_person_2)

        self._commit_to_db(db_person, db_person_2)

        race = StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE, race=Race.BLACK
        )
        alias = StatePersonAlias.new_with_defaults(
            state_code=_STATE_CODE, full_name=_FULL_NAME_ANOTHER
        )
        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code=_STATE_CODE, ethnicity=Ethnicity.NOT_HISPANIC
        )
        external_id = attr.evolve(entity_external_id, person_external_id_id=None)
        external_id_2 = attr.evolve(entity_external_id_2, person_external_id_id=None)
        person = attr.evolve(
            entity_person,
            person_id=None,
            external_ids=[external_id, external_id_2],
            races=[race],
            aliases=[alias],
            ethnicities=[ethnicity],
        )

        expected_race = attr.evolve(race)
        expected_ethnicity = attr.evolve(ethnicity)
        expected_alias = attr.evolve(alias)
        expected_person = attr.evolve(
            entity_person,
            external_ids=[
                entity_external_id,
                entity_external_id_2,
            ],
            races=[expected_race],
            ethnicities=[expected_ethnicity],
            aliases=[expected_alias],
            incarceration_sentences=[entity_incarceration_sentence],
        )
        expected_placeholder_person = attr.evolve(
            entity_person_2,
            full_name=None,
            incarceration_sentences=[],
            external_ids=[],
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

        # Assert 1 - Match
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)
        self.assert_people_match_pre_and_post_commit(
            [expected_person, expected_placeholder_person],
            matched_entities.people,
            session,
        )

    def test_matchPersons_matchesTwoDbPeople_mergeAndMoveChildren(self) -> None:
        """Tests that our system correctly handles the situation where we have
        2 distinct people in our DB, but we learn the two DB people should be
        merged into 1 person based on a new ingested person. Here the two DB
        people to merge have children which match with each other, and therefore
        need to be merged properly themselves.
        """
        # Arrange 1 - Match
        db_person = generate_person(full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person, external_id=_EXTERNAL_ID
        )
        entity_supervision_sentence = self.to_entity(db_supervision_sentence)
        db_external_id = generate_external_id(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE,
        )
        entity_external_id = self.to_entity(db_external_id)
        db_agent = generate_agent(
            external_id=_EXTERNAL_ID,
            agent_type=StateAgentType.SUPERVISION_OFFICER.value,
            state_code=_STATE_CODE,
        )
        entity_agent = self.to_entity(db_agent)
        db_person.external_ids = [db_external_id]
        db_person.supervising_officer = db_agent
        db_person.supervision_sentences = [db_supervision_sentence]
        entity_person = self.to_entity(db_person)

        db_person_dup = generate_person(full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_agent_dup = generate_agent(
            external_id=_EXTERNAL_ID,
            agent_type=StateAgentType.SUPERVISION_OFFICER.value,
            state_code=_STATE_CODE,
        )
        entity_agent_dup = self.to_entity(db_agent_dup)
        db_supervision_period = generate_supervision_period(
            person=db_person_dup,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_1,
            termination_date=_DATE_2,
        )
        entity_supervision_period = self.to_entity(db_supervision_period)
        db_supervision_sentence_dup = generate_supervision_sentence(
            person=db_person_dup,
            external_id=_EXTERNAL_ID_2,
            max_length_days=10,
        )
        entity_supervision_sentence_dup = self.to_entity(db_supervision_sentence_dup)
        db_external_id_2 = generate_external_id(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2,
            id_type=_ID_TYPE_ANOTHER,
        )
        entity_external_id_2 = self.to_entity(db_external_id_2)
        db_person_dup.external_ids = [db_external_id_2]
        db_person_dup.supervision_sentences = [db_supervision_sentence_dup]
        db_person_dup.supervising_officer = db_agent_dup
        db_person_dup.supervision_periods = [db_supervision_period]
        entity_person_dup = self.to_entity(db_person_dup)

        external_id = attr.evolve(entity_external_id, person_external_id_id=None)
        external_id_2 = attr.evolve(entity_external_id_2, person_external_id_id=None)
        person = attr.evolve(
            entity_person,
            person_id=None,
            external_ids=[external_id, external_id_2],
        )

        self._commit_to_db(db_person, db_person_dup)

        expected_supervision_period = attr.evolve(entity_supervision_period)
        expected_supervision_sentence = attr.evolve(
            entity_supervision_sentence,
        )
        expected_supervision_sentence_dup = attr.evolve(entity_supervision_sentence_dup)
        expected_agent = attr.evolve(entity_agent)
        expected_person = attr.evolve(
            entity_person,
            external_ids=[
                entity_external_id,
                entity_external_id_2,
            ],
            supervising_officer=expected_agent,
            supervision_sentences=[
                expected_supervision_sentence,
                expected_supervision_sentence_dup,
            ],
            supervision_periods=[expected_supervision_period],
        )
        expected_placeholder_agent = attr.evolve(
            entity_agent_dup,
            external_id=None,
            agent_type=StateAgentType.PRESENT_WITHOUT_INFO,
        )
        expected_placeholder_person = attr.evolve(
            entity_person_dup,
            full_name=None,
            supervising_officer=expected_placeholder_agent,
            external_ids=[],
            supervision_periods=[],
            supervision_sentences=[],
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])
        self.maxDiff = None
        # Assert 1 - Match
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)
        self.assert_people_match_pre_and_post_commit(
            [expected_person, expected_placeholder_person],
            matched_entities.people,
            session,
        )

    def test_matchPersons_noPlaceholders_newPerson(self) -> None:
        # Arrange 1 - Match
        alias = StatePersonAlias.new_with_defaults(
            state_code=_STATE_CODE, full_name=_FULL_NAME
        )
        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, id_type=_ID_TYPE, external_id=_EXTERNAL_ID
        )
        race = StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE, race=Race.WHITE
        )
        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code=_STATE_CODE, ethnicity=Ethnicity.NOT_HISPANIC
        )
        person = StatePerson.new_with_defaults(
            gender=Gender.MALE,
            aliases=[alias],
            external_ids=[external_id],
            races=[race],
            ethnicities=[ethnicity],
            state_code=_STATE_CODE,
        )

        expected_person = attr.evolve(person)

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_noPlaceholders_updatePersonAttributes(self) -> None:
        # Arrange 1 - Match
        db_race = generate_race(state_code=_STATE_CODE, race=Race.WHITE.value)
        entity_race = self.to_entity(db_race)
        db_alias = generate_alias(state_code=_STATE_CODE, full_name=_FULL_NAME)
        entity_alias = self.to_entity(db_alias)
        db_ethnicity = generate_ethnicity(
            state_code=_STATE_CODE,
            ethnicity=Ethnicity.HISPANIC.value,
        )
        entity_ethnicity = self.to_entity(db_ethnicity)
        db_external_id = generate_external_id(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE,
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person = generate_person(
            full_name=_FULL_NAME,
            external_ids=[db_external_id],
            races=[db_race],
            aliases=[db_alias],
            ethnicities=[db_ethnicity],
            state_code=_STATE_CODE,
        )
        entity_person = self.to_entity(db_person)
        self._commit_to_db(db_person)

        race = StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE, race=Race.BLACK
        )
        alias = StatePersonAlias.new_with_defaults(
            state_code=_STATE_CODE, full_name=_FULL_NAME_ANOTHER
        )
        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code=_STATE_CODE, ethnicity=Ethnicity.NOT_HISPANIC
        )
        external_id = attr.evolve(entity_external_id, person_external_id_id=None)
        person = attr.evolve(
            entity_person,
            person_id=None,
            external_ids=[external_id],
            races=[race],
            aliases=[alias],
            ethnicities=[ethnicity],
        )

        expected_person = attr.evolve(
            entity_person,
            races=[entity_race, race],
            ethnicities=[entity_ethnicity, ethnicity],
            aliases=[entity_alias, alias],
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_noPlaceholders_partialTreeIngested(self) -> None:
        # Arrange 1 - Match
        db_person = generate_person(full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_court_case = generate_court_case(
            person=db_person,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code="county_code",
        )
        entity_court_case = self.to_entity(db_court_case)
        db_charge_1 = generate_charge(
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            description="charge_1",
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
        )
        entity_charge_1 = self.to_entity(db_charge_1)
        db_charge_2 = generate_charge(
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2,
            description="charge_2",
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            court_case=db_court_case,
        )
        entity_charge_2 = self.to_entity(db_charge_2)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            county_code="county_code",
            charges=[db_charge_1, db_charge_2],
        )
        entity_supervision_sentence = self.to_entity(db_supervision_sentence)
        db_incarceration_sentence = generate_incarceration_sentence(
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code="county_code",
        )
        entity_incarceration_sentence = self.to_entity(db_incarceration_sentence)
        db_external_id = generate_external_id(
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE,
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person.external_ids = [db_external_id]
        db_person.supervision_sentences = [db_supervision_sentence]
        db_person.incarceration_sentences = [db_incarceration_sentence]
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        new_court_case = StateCourtCase.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE,
            county_code="county_code",
        )
        charge_1 = attr.evolve(
            entity_charge_1,
            charge_id=None,
            description="charge_1-updated",
            court_case=new_court_case,
        )
        charge_2 = attr.evolve(
            entity_charge_2,
            charge_id=None,
            description="charge_2-updated",
            court_case=None,
        )
        supervision_sentence = attr.evolve(
            entity_supervision_sentence,
            supervision_sentence_id=None,
            county_code="county-updated",
            charges=[charge_1, charge_2],
        )
        external_id = attr.evolve(
            entity_external_id, person_external_id_id=None, id_type=_ID_TYPE
        )
        person = attr.evolve(
            entity_person,
            person_id=None,
            external_ids=[external_id],
            supervision_sentences=[supervision_sentence],
        )

        expected_unchanged_court_case = attr.evolve(entity_court_case)
        expected_new_court_case = attr.evolve(new_court_case)
        expected_charge1 = attr.evolve(
            charge_1,
            court_case=expected_new_court_case,
        )
        expected_charge2 = attr.evolve(
            charge_2, court_case=expected_unchanged_court_case
        )
        expected_supervision_sentence = attr.evolve(
            supervision_sentence,
            charges=[expected_charge1, expected_charge2],
        )
        expected_unchanged_incarceration_sentence = attr.evolve(
            entity_incarceration_sentence
        )
        expected_external_id = attr.evolve(
            external_id,
        )
        expected_person = attr.evolve(
            person,
            external_ids=[expected_external_id],
            supervision_sentences=[expected_supervision_sentence],
            incarceration_sentences=[expected_unchanged_incarceration_sentence],
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_noPlaceholders_completeTreeUpdate(self) -> None:
        # Arrange 1 - Match
        db_person = generate_person(full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_court_case = generate_court_case(
            person=db_person,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code="county_code",
        )
        entity_court_case = self.to_entity(db_court_case)
        db_charge_1 = generate_charge(
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2,
            description="charge_1",
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            court_case=db_court_case,
        )
        entity_charge_1 = self.to_entity(db_charge_1)
        db_charge_2 = generate_charge(
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_3,
            description="charge_2",
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
        )
        entity_charge_2 = self.to_entity(db_charge_2)
        db_assessment = generate_assessment(
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            assessment_metadata="metadata",
        )
        entity_assessment = self.to_entity(db_assessment)
        db_assessment_2 = generate_assessment(
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2,
            assessment_metadata="metadata_2",
        )
        entity_assessment_2 = self.to_entity(db_assessment_2)
        db_agent = generate_agent(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, full_name="full_name"
        )
        entity_agent = self.to_entity(db_agent)
        db_agent_po = generate_agent(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_5, full_name="full_name_po"
        )
        entity_agent_po = self.to_entity(db_agent_po)
        db_agent_term = generate_agent(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_6,
            full_name="full_name_term",
        )
        entity_agent_term = self.to_entity(db_agent_term)
        db_incarceration_incident = generate_incarceration_incident(
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            incident_details="details",
            responding_officer=db_agent,
        )
        entity_incarceration_incident = self.to_entity(db_incarceration_incident)
        db_supervision_violation_response_decision = (
            generate_supervision_violation_response_decision_entry(
                person=db_person,
                decision=StateSupervisionViolationResponseDecision.REVOCATION.value,
            )
        )
        entity_supervision_violation_response_decision = self.to_entity(
            db_supervision_violation_response_decision
        )
        db_supervision_violation_response = generate_supervision_violation_response(
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            supervision_violation_response_decisions=[
                db_supervision_violation_response_decision
            ],
            decision_agents=[db_agent_term],
        )
        entity_supervision_violation_response = self.to_entity(
            db_supervision_violation_response
        )
        db_supervision_violation_type = generate_supervision_violation_type_entry(
            person=db_person,
            violation_type=StateSupervisionViolationType.ABSCONDED.value,
        )
        entity_supervision_violation_type = self.to_entity(
            db_supervision_violation_type
        )
        db_supervision_violated_condition = (
            generate_supervision_violated_condition_entry(
                person=db_person, condition="COND"
            )
        )
        entity_supervision_violated_condition = self.to_entity(
            db_supervision_violated_condition
        )
        db_supervision_violation = generate_supervision_violation(
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            is_violent=True,
            supervision_violation_types=[db_supervision_violation_type],
            supervision_violated_conditions=[db_supervision_violated_condition],
            supervision_violation_responses=[db_supervision_violation_response],
        )
        entity_supervision_violation = self.to_entity(db_supervision_violation)
        db_incarceration_period = generate_incarceration_period(
            person=db_person,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            facility="facility",
        )
        entity_incarceration_period = self.to_entity(db_incarceration_period)
        db_incarceration_sentence = generate_incarceration_sentence(
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code="county_code",
            charges=[db_charge_1, db_charge_2],
        )
        entity_incarceration_sentence = self.to_entity(db_incarceration_sentence)
        db_case_type_dv = generate_supervision_case_type_entry(
            person=db_person,
            state_code=_STATE_CODE,
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE.value,
            case_type_raw_text="DV",
        )
        entity_case_type_dv = self.to_entity(db_case_type_dv)
        db_supervision_period = generate_supervision_period(
            person=db_person,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code="county_code",
            case_type_entries=[db_case_type_dv],
            supervising_officer=db_agent_po,
        )
        entity_supervision_period = self.to_entity(db_supervision_period)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0,
        )
        entity_supervision_sentence = self.to_entity(db_supervision_sentence)
        db_external_id = generate_external_id(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person.external_ids = [db_external_id]
        db_person.supervision_sentences = [db_supervision_sentence]
        db_person.incarceration_sentences = [db_incarceration_sentence]
        db_person.incarceration_periods = [db_incarceration_period]
        db_person.supervision_periods = [db_supervision_period]
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        court_case = attr.evolve(
            entity_court_case,
            court_case_id=None,
            county_code="county_code-updated",
        )
        charge_1 = attr.evolve(
            entity_charge_1,
            charge_id=None,
            description="charge_1-updated",
            court_case=court_case,
        )
        charge_2 = attr.evolve(
            entity_charge_2, charge_id=None, description="charge_2-updated"
        )
        assessment = attr.evolve(
            entity_assessment,
            assessment_id=None,
            assessment_metadata="metadata_updated",
        )
        assessment_2 = attr.evolve(
            entity_assessment_2,
            assessment_id=None,
            assessment_metadata="metadata_2-updated",
        )
        agent = attr.evolve(entity_agent, agent_id=None, full_name="full_name-updated")
        agent_po = attr.evolve(
            entity_agent_po, agent_id=None, full_name="full_name_po-updated"
        )
        agent_term = attr.evolve(
            entity_agent_term,
            agent_id=None,
            full_name="full_name_term-updated",
        )
        incarceration_incident = attr.evolve(
            entity_incarceration_incident,
            incarceration_incident_id=None,
            incident_details="details-updated",
            responding_officer=agent,
        )
        incarceration_period = attr.evolve(
            entity_incarceration_period,
            incarceration_period_id=None,
            facility="facility-updated",
        )
        incarceration_sentence = attr.evolve(
            entity_incarceration_sentence,
            incarceration_sentence_id=None,
            county_code="county_code-updated",
            charges=[charge_1, charge_2],
        )
        supervision_violation_response_decision = attr.evolve(
            entity_supervision_violation_response_decision,
            supervision_violation_response_decision_entry_id=None,
        )
        supervision_violation_response = attr.evolve(
            entity_supervision_violation_response,
            supervision_violation_response_id=None,
            decision_agents=[agent_term],
            supervision_violation_response_decisions=[
                supervision_violation_response_decision
            ],
        )
        supervision_violation_type = attr.evolve(
            entity_supervision_violation_type,
            supervision_violation_type_entry_id=None,
        )
        supervision_violated_condition = attr.evolve(
            entity_supervision_violated_condition,
            supervision_violated_condition_entry_id=None,
        )
        supervision_violation = attr.evolve(
            entity_supervision_violation,
            supervision_violation_id=None,
            is_violent=False,
            supervision_violation_responses=[supervision_violation_response],
            supervision_violated_conditions=[supervision_violated_condition],
            supervision_violation_types=[supervision_violation_type],
        )
        case_type_dv = attr.evolve(
            entity_case_type_dv, supervision_case_type_entry_id=None
        )
        case_type_so = StateSupervisionCaseTypeEntry.new_with_defaults(
            state_code=_STATE_CODE,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            case_type_raw_text="SO",
        )
        supervision_period = attr.evolve(
            entity_supervision_period,
            supervision_period_id=None,
            county_code="county_code-updated",
            case_type_entries=[case_type_dv, case_type_so],
            supervising_officer=agent_po,
        )
        supervision_sentence = attr.evolve(
            entity_supervision_sentence,
            supervision_sentence_id=None,
            min_length_days=1,
        )
        external_id = attr.evolve(entity_external_id, person_external_id_id=None)
        person = attr.evolve(
            entity_person,
            person_id=None,
            external_ids=[external_id],
            assessments=[assessment, assessment_2],
            incarceration_incidents=[incarceration_incident],
            supervision_violations=[supervision_violation],
            supervision_sentences=[supervision_sentence],
            incarceration_sentences=[incarceration_sentence],
            incarceration_periods=[incarceration_period],
            supervision_periods=[supervision_period],
        )

        expected_court_case = attr.evolve(court_case)
        expected_charge_1 = attr.evolve(
            charge_1,
            court_case=expected_court_case,
        )
        expected_charge_2 = attr.evolve(charge_2)
        expected_assessment = attr.evolve(assessment)
        expected_assessment_2 = attr.evolve(assessment_2)
        expected_agent = attr.evolve(agent)
        expected_agent_po = attr.evolve(agent_po)
        expected_agent_term = attr.evolve(agent_term)
        expected_incarceration_incident = attr.evolve(
            incarceration_incident,
            responding_officer=expected_agent,
        )
        expected_incarceration_period = attr.evolve(
            incarceration_period,
        )
        expected_incarceration_sentence = attr.evolve(
            incarceration_sentence,
            charges=[expected_charge_1, expected_charge_2],
        )
        expected_supervision_violation_response_decision = attr.evolve(
            supervision_violation_response_decision,
        )
        expected_supervision_violation_response = attr.evolve(
            supervision_violation_response,
            decision_agents=[expected_agent_term],
            supervision_violation_response_decisions=[
                expected_supervision_violation_response_decision
            ],
        )
        expected_supervision_violation_type = attr.evolve(supervision_violation_type)
        expected_supervision_violated_condition = attr.evolve(
            supervision_violated_condition
        )
        expected_supervision_violation = attr.evolve(
            supervision_violation,
            supervision_violation_responses=[expected_supervision_violation_response],
            supervision_violated_conditions=[expected_supervision_violated_condition],
            supervision_violation_types=[expected_supervision_violation_type],
        )
        expected_case_type_dv = attr.evolve(case_type_dv)
        expected_case_type_so = attr.evolve(case_type_so)
        expected_supervision_period = attr.evolve(
            supervision_period,
            case_type_entries=[expected_case_type_dv, expected_case_type_so],
            supervising_officer=expected_agent_po,
        )
        expected_supervision_sentence = attr.evolve(
            supervision_sentence,
        )
        expected_external_id = attr.evolve(
            external_id,
        )
        expected_person = attr.evolve(
            person,
            external_ids=[expected_external_id],
            assessments=[expected_assessment, expected_assessment_2],
            incarceration_incidents=[expected_incarceration_incident],
            supervision_violations=[expected_supervision_violation],
            supervision_sentences=[expected_supervision_sentence],
            incarceration_sentences=[expected_incarceration_sentence],
            incarceration_periods=[expected_incarceration_period],
            supervision_periods=[expected_supervision_period],
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

        # Assert 1 - Match
        self.assert_no_errors(matched_entities)
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_ingestedPersonWithNewExternalId(self) -> None:
        db_external_id = generate_external_id(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE,
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person = generate_person(
            full_name=_FULL_NAME,
            external_ids=[db_external_id],
            state_code=_STATE_CODE,
        )
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        external_id = attr.evolve(entity_external_id, person_external_id_id=None)
        external_id_another = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2, id_type=_ID_TYPE_ANOTHER
        )
        person = StatePerson.new_with_defaults(
            external_ids=[external_id, external_id_another], state_code=_STATE_CODE
        )

        expected_external_id = attr.evolve(
            external_id,
        )
        expected_external_id_another = attr.evolve(external_id_another)
        expected_person = attr.evolve(
            entity_person,
            external_ids=[expected_external_id, expected_external_id_another],
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

        # Assert 1 - Match
        self.assert_no_errors(matched_entities)
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_holeInDbGraph(self) -> None:
        db_external_id = generate_external_id(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE,
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person = generate_person(
            full_name=_FULL_NAME,
            external_ids=[db_external_id],
            state_code=_STATE_CODE,
        )

        db_charge = generate_charge(
            person=db_person, external_id=_EXTERNAL_ID, statute="1234"
        )
        entity_charge = self.to_entity(db_charge)

        db_charge_unchanged = generate_charge(
            person=db_person, external_id=_EXTERNAL_ID_2, statute="4567"
        )
        entity_charge_unchanged = self.to_entity(db_charge_unchanged)

        db_placeholder_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE,
            charges=[db_charge, db_charge_unchanged],
        )
        entity_placeholder_supervision_sentence = self.to_entity(
            db_placeholder_supervision_sentence
        )

        db_person.supervision_sentences = [
            db_placeholder_supervision_sentence,
        ]
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        charge_unchanged = attr.evolve(
            entity_charge_unchanged,
        )
        charge = attr.evolve(
            entity_charge,
            statute="9999",
        )
        new_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.SERVING,
            charges=[charge, charge_unchanged],
        )
        external_id = attr.evolve(entity_external_id)
        person = attr.evolve(
            entity_person,
            external_ids=[external_id],
            supervision_sentences=[
                new_supervision_sentence,
            ],
        )

        expected_supervision_sentence = attr.evolve(new_supervision_sentence)
        expected_placeholder_supervision_sentence = attr.evolve(
            entity_placeholder_supervision_sentence, charges=[]
        )

        expected_external_id = attr.evolve(
            external_id,
        )
        expected_person = attr.evolve(
            person,
            external_ids=[expected_external_id],
            supervision_sentences=[
                expected_supervision_sentence,
                expected_placeholder_supervision_sentence,
            ],
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_holeInIngestedGraph(self) -> None:
        db_person = generate_person(full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_charge = generate_charge(person=db_person, external_id=_EXTERNAL_ID)
        entity_charge = self.to_entity(db_charge)

        db_charge_another = generate_charge(
            person=db_person, external_id=_EXTERNAL_ID_2
        )
        entity_charge_another = self.to_entity(db_charge_another)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0,
            charges=[db_charge, db_charge_another],
        )
        entity_supervision_sentence = self.to_entity(db_supervision_sentence)

        db_external_id = generate_external_id(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person.external_ids = [db_external_id]
        db_person.supervision_sentences = [db_supervision_sentence]
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        charge_updated = attr.evolve(
            entity_charge, charge_id=None, offense_date=datetime.date(2020, 1, 1)
        )
        sentence_placeholder = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge_updated],
        )
        external_id = attr.evolve(entity_external_id, person_external_id_id=None)
        person = attr.evolve(
            entity_person,
            person_id=None,
            external_ids=[external_id],
            supervision_sentences=[sentence_placeholder],
        )

        expected_supervision_sentence = attr.evolve(
            entity_supervision_sentence,
            charges=[charge_updated, entity_charge_another],
        )

        expected_external_id = attr.evolve(
            external_id,
        )
        expected_person = attr.evolve(
            person,
            external_ids=[expected_external_id],
            supervision_sentences=[
                expected_supervision_sentence,
            ],
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_dbPlaceholderSplits(self) -> None:
        db_external_id = generate_external_id(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID
        )
        db_person = generate_person(
            full_name=_FULL_NAME, state_code=_STATE_CODE, external_ids=[db_external_id]
        )

        db_charge = generate_charge(
            person=db_person, external_id=_EXTERNAL_ID, statute="1234"
        )
        entity_charge = self.to_entity(db_charge)

        db_charge_another = generate_charge(
            person=db_person, external_id=_EXTERNAL_ID_2, statute="4567"
        )
        entity_charge_another = self.to_entity(db_charge_another)

        db_placeholder_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE,
            charges=[db_charge, db_charge_another],
        )
        entity_placeholder_supervision_sentence = self.to_entity(
            db_placeholder_supervision_sentence
        )

        db_person.supervision_sentences = [
            db_placeholder_supervision_sentence,
        ]
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        charge_updated = attr.evolve(entity_charge, statute="888")
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.SERVING,
            charges=[charge_updated],
        )

        charge_another_updated = attr.evolve(entity_charge_another, statute="999")
        supervision_sentence_another = StateSupervisionSentence.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.SUSPENDED,
            charges=[charge_another_updated],
        )

        person = attr.evolve(
            entity_person,
            supervision_sentences=[supervision_sentence, supervision_sentence_another],
        )

        expected_charge = attr.evolve(charge_updated)
        expected_charge_another = attr.evolve(charge_another_updated)
        expected_supervision_sentence = attr.evolve(
            supervision_sentence, charges=[expected_charge]
        )
        expected_supervision_sentence_another = attr.evolve(
            supervision_sentence_another, charges=[expected_charge_another]
        )
        expected_placeholder_supervision_sentence = attr.evolve(
            entity_placeholder_supervision_sentence, charges=[]
        )
        expected_person = attr.evolve(
            person,
            supervision_sentences=[
                expected_supervision_sentence,
                expected_supervision_sentence_another,
                expected_placeholder_supervision_sentence,
            ],
        )

        expected_people = [expected_person]

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            expected_people, matched_entities.people, session
        )
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_ingestedPlaceholderSplits(self) -> None:
        db_person = generate_person(full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_charge = generate_charge(
            person=db_person,
            offense_date=datetime.date(2000, 1, 1),
            external_id=_EXTERNAL_ID,
        )
        entity_charge = self.to_entity(db_charge)
        db_charge_another = generate_charge(
            person=db_person,
            offense_date=datetime.date(2001, 12, 3),
            external_id=_EXTERNAL_ID_2,
        )
        entity_charge_another = self.to_entity(db_charge_another)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0,
            charges=[db_charge, db_charge_another],
        )
        entity_supervision_sentence = self.to_entity(db_supervision_sentence)
        db_external_id = generate_external_id(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person.external_ids = [db_external_id]
        db_person.supervision_sentences = [db_supervision_sentence]
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        charge_updated = attr.evolve(
            entity_charge, charge_id=None, offense_date=datetime.date(1999, 9, 9)
        )
        charge_another_updated = attr.evolve(
            entity_charge_another,
            charge_id=None,
            offense_date=datetime.date(1989, 10, 3),
        )
        placeholder_sentence = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge_updated, charge_another_updated],
        )
        external_id = attr.evolve(entity_external_id, person_external_id_id=None)
        person = attr.evolve(
            entity_person,
            person_id=None,
            external_ids=[external_id],
            supervision_sentences=[placeholder_sentence],
        )

        expected_charge = attr.evolve(charge_updated)
        expected_charge_another = attr.evolve(charge_another_updated)
        expected_sentence = attr.evolve(
            entity_supervision_sentence,
            charges=[expected_charge, expected_charge_another],
        )
        expected_external_id = attr.evolve(
            external_id,
        )
        expected_person = attr.evolve(
            person,
            external_ids=[expected_external_id],
            supervision_sentences=[expected_sentence],
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_multipleHolesInIngestedGraph(self) -> None:
        # Arrange 1 - Match
        db_person = generate_person(full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_court_case = generate_court_case(
            person=db_person,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code="county_code",
        )
        entity_court_case = self.to_entity(db_court_case)
        db_charge = generate_charge(
            person=db_person,
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            court_case=db_court_case,
        )
        entity_charge = self.to_entity(db_charge)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person, external_id=_EXTERNAL_ID, charges=[db_charge]
        )
        entity_supervision_sentence = self.to_entity(db_supervision_sentence)
        db_external_id = generate_external_id(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person.external_ids = [db_external_id]
        db_person.supervision_sentences = [db_supervision_sentence]
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        court_case_updated = attr.evolve(
            entity_court_case, county_code="county_code_updated"
        )
        placeholder_charge = StateCharge.new_with_defaults(
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            court_case=court_case_updated,
        )
        placeholder_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[placeholder_charge],
        )
        person = StatePerson.new_with_defaults(
            state_code=_STATE_CODE,
            external_ids=[attr.evolve(entity_external_id)],
            supervision_sentences=[placeholder_supervision_sentence],
        )

        expected_court_case = attr.evolve(court_case_updated)
        expected_charge = attr.evolve(entity_charge, court_case=expected_court_case)
        expected_supervision_sentence = attr.evolve(
            entity_supervision_sentence,
            charges=[expected_charge],
        )
        expected_external_id = attr.evolve(entity_external_id)
        expected_person = attr.evolve(
            entity_person,
            external_ids=[expected_external_id],
            supervision_sentences=[expected_supervision_sentence],
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_multipleHolesInDbGraph(self) -> None:
        # Arrange 1 - Match
        db_external_id = generate_external_id(
            state_code=_STATE_CODE,
            id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID,
        )
        db_person = generate_person(
            external_ids=[db_external_id],
            state_code=_STATE_CODE,
            full_name=_FULL_NAME,
        )
        db_court_case = generate_court_case(
            person=db_person,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code="county_code",
        )
        entity_court_case = self.to_entity(db_court_case)
        db_placeholder_charge = generate_charge(
            person=db_person,
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE,
            court_case=db_court_case,
        )
        entity_placeholder_charge = self.to_entity(db_placeholder_charge)
        db_placeholder_supervision_sentence = generate_supervision_sentence(
            person=db_person, charges=[db_placeholder_charge]
        )
        entity_placeholder_supervision_sentence = self.to_entity(
            db_placeholder_supervision_sentence
        )
        db_person.supervision_sentences = [db_placeholder_supervision_sentence]
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        court_case_updated = attr.evolve(
            entity_court_case, county_code="county_code_updated"
        )
        charge = StateCharge.new_with_defaults(
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            court_case=court_case_updated,
        )
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge],
        )
        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, id_type=_ID_TYPE
        )
        person = attr.evolve(
            entity_person,
            external_ids=[external_id],
            supervision_sentences=[supervision_sentence],
        )

        expected_court_case_updated = attr.evolve(court_case_updated)
        expected_charge = attr.evolve(charge, court_case=expected_court_case_updated)
        expected_supervision_sentence = attr.evolve(
            supervision_sentence,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[expected_charge],
        )
        expected_placeholder_charge = attr.evolve(
            entity_placeholder_charge, court_case=None
        )
        expected_placeholder_supervision_sentence = attr.evolve(
            entity_placeholder_supervision_sentence,
            charges=[expected_placeholder_charge],
        )
        expected_external_id = attr.evolve(external_id)
        expected_person = attr.evolve(
            person,
            external_ids=[expected_external_id],
            supervision_sentences=[
                expected_supervision_sentence,
                expected_placeholder_supervision_sentence,
            ],
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session, debug=True
        )
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_ingestedPersonPlaceholder_throws(self) -> None:
        sentence_new = StateSupervisionSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        placeholder_person = StatePerson.new_with_defaults(
            supervision_sentences=[sentence_new],
            state_code=_STATE_CODE,
        )

        session = self._session()
        with self.assertRaisesRegex(
            ValueError,
            "Ingested StatePerson objects must have one or more assigned external ids.",
        ):
            _ = self._match(session, ingested_people=[placeholder_person])

    def test_matchPersons_matchAfterManyIngestedPlaceholders(self) -> None:
        # Arrange 1 - Match
        db_external_id = generate_external_id(
            state_code=_STATE_CODE,
            id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID,
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person = generate_person(
            external_ids=[db_external_id],
            state_code=_STATE_CODE,
        )
        db_incarceration_sentence = generate_incarceration_sentence(
            person=db_person, external_id=_EXTERNAL_ID
        )
        db_person.incarceration_sentences = [db_incarceration_sentence]
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        incarceration_incident = StateIncarcerationIncident.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID
        )
        external_id = attr.evolve(entity_external_id, person_external_id_id=None)
        person = StatePerson.new_with_defaults(
            incarceration_incidents=[incarceration_incident],
            external_ids=[external_id],
            state_code=_STATE_CODE,
        )

        expected_incarceration_incident = attr.evolve(incarceration_incident)

        expected_external_id = attr.evolve(entity_external_id)
        expected_person = attr.evolve(
            entity_person,
            external_ids=[expected_external_id],
            incarceration_incidents=[expected_incarceration_incident],
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_runMatch_multipleExternalIdsOnRootEntity(self) -> None:
        db_external_id = generate_external_id(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            id_type=_ID_TYPE,
        )
        entity_external_id = self.to_entity(db_external_id)
        db_external_id_2 = generate_external_id(
            external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE,
            id_type=_ID_TYPE_ANOTHER,
        )
        entity_external_id_2 = self.to_entity(db_external_id_2)
        db_person = generate_person(
            external_ids=[db_external_id, db_external_id_2],
            state_code=_STATE_CODE,
        )
        entity_person = self.to_entity(db_person)
        self._commit_to_db(db_person)

        external_id = attr.evolve(entity_external_id, person_external_id_id=None)
        external_id_2 = attr.evolve(entity_external_id_2, person_external_id_id=None)
        race = StatePersonRace.new_with_defaults(
            race=Race.WHITE, state_code=_STATE_CODE
        )
        person = StatePerson.new_with_defaults(
            external_ids=[external_id, external_id_2],
            races=[race],
            state_code=_STATE_CODE,
        )

        expected_race = attr.evolve(race)
        expected_person = attr.evolve(entity_person, races=[expected_race])

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_mergeMultiParentEntities(self) -> None:
        # Arrange 1 - Match
        charge_merged = StateCharge.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
        )
        charge_unmerged = attr.evolve(charge_merged, charge_id=None)
        charge_duplicate_unmerged = attr.evolve(charge_unmerged)
        sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge_unmerged],
        )
        sentence_2 = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge_duplicate_unmerged],
        )
        sentence_3 = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID_3,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge_merged],
        )
        external_id = StatePersonExternalId.new_with_defaults(
            external_id=_EXTERNAL_ID, state_code=_STATE_CODE, id_type=_ID_TYPE
        )
        person = StatePerson.new_with_defaults(
            external_ids=[external_id],
            incarceration_sentences=[sentence, sentence_2, sentence_3],
            state_code=_STATE_CODE,
        )

        expected_charge = attr.evolve(charge_merged)
        expected_sentence = attr.evolve(sentence, charges=[expected_charge])
        expected_sentence_2 = attr.evolve(sentence_2, charges=[expected_charge])
        expected_sentence_3 = attr.evolve(sentence_3, charges=[expected_charge])
        expected_external_id = attr.evolve(external_id)
        expected_person = attr.evolve(
            person,
            external_ids=[expected_external_id],
            incarceration_sentences=[
                expected_sentence,
                expected_sentence_2,
                expected_sentence_3,
            ],
        )

        # Arrange 1 - Match
        session = self._session()
        matched_entities = self._match(session, [person])

        # Assert 1 - Match
        found_person = matched_entities.people[0]
        found_charge = found_person.incarceration_sentences[0].charges[0]
        found_charge_2 = found_person.incarceration_sentences[1].charges[0]
        found_charge_3 = found_person.incarceration_sentences[2].charges[0]
        self.assertEqual(id(found_charge), id(found_charge_2), id(found_charge_3))
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )

    def test_mergeMultiParentEntities_mergeChargesAndCourtCases(self) -> None:
        # Arrange 1 - Match
        court_case_merged = StateCourtCase.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID
        )
        court_case_unmerged = attr.evolve(court_case_merged, court_case_id=None)
        charge_merged = StateCharge.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            court_case=court_case_merged,
        )
        charge_unmerged = attr.evolve(
            charge_merged, charge_id=None, court_case=attr.evolve(court_case_unmerged)
        )
        charge_unmerged_2 = attr.evolve(
            charge_merged, charge_id=None, court_case=attr.evolve(court_case_unmerged)
        )
        charge_2 = StateCharge.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            court_case=attr.evolve(court_case_unmerged),
        )
        sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge_unmerged],
        )
        sentence_2 = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge_unmerged_2],
        )
        sentence_3 = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID_3,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge_2],
        )
        external_id = StatePersonExternalId.new_with_defaults(
            external_id=_EXTERNAL_ID, state_code=_STATE_CODE, id_type=_ID_TYPE
        )
        person = StatePerson.new_with_defaults(
            external_ids=[external_id],
            incarceration_sentences=[sentence, sentence_2, sentence_3],
            state_code=_STATE_CODE,
        )

        expected_court_case = attr.evolve(court_case_merged)
        expected_charge = attr.evolve(charge_merged, court_case=expected_court_case)
        expected_charge_2 = attr.evolve(charge_2, court_case=expected_court_case)
        expected_sentence = attr.evolve(sentence, charges=[expected_charge])
        expected_sentence_2 = attr.evolve(sentence_2, charges=[expected_charge])
        expected_sentence_3 = attr.evolve(sentence_3, charges=[expected_charge_2])
        expected_external_id = attr.evolve(external_id)
        expected_person = attr.evolve(
            person,
            external_ids=[expected_external_id],
            incarceration_sentences=[
                expected_sentence,
                expected_sentence_2,
                expected_sentence_3,
            ],
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, [person])

        # Assert 1 - Match
        found_person = matched_entities.people[0]
        found_charge = found_person.incarceration_sentences[0].charges[0]
        found_charge_2 = found_person.incarceration_sentences[1].charges[0]
        self.assertEqual(id(found_charge), id(found_charge_2))

        found_court_case = found_person.incarceration_sentences[0].charges[0].court_case
        found_court_case_2 = (
            found_person.incarceration_sentences[1].charges[0].court_case
        )
        found_court_case_3 = (
            found_person.incarceration_sentences[2].charges[0].court_case
        )
        self.assertEqual(
            id(found_court_case), id(found_court_case_2), id(found_court_case_3)
        )
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )

    def test_mergeMultiParentEntities_mergeCharges_errorInMerge_keepsOne(self) -> None:
        # Arrange 1 - Match
        court_case = StateCourtCase.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID
        )
        different_court_case = StateCourtCase.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2
        )
        charge = StateCharge.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            court_case=court_case,
        )
        charge_matching = attr.evolve(charge, court_case=different_court_case)
        sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge_matching],
        )
        sentence_2 = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge],
        )
        external_id = StatePersonExternalId.new_with_defaults(
            external_id=_EXTERNAL_ID, state_code=_STATE_CODE, id_type=_ID_TYPE
        )
        person = StatePerson.new_with_defaults(
            external_ids=[external_id],
            incarceration_sentences=[sentence, sentence_2],
            state_code=_STATE_CODE,
        )

        # `different_court_case` was not successfully merged. An error is logged
        # but otherwise, the failure is silent, as intended (so we do not halt
        # ingestion because of a failure here).
        expected_court_case = attr.evolve(different_court_case)
        expected_charge = attr.evolve(charge, court_case=expected_court_case)
        expected_sentence = attr.evolve(sentence, charges=[expected_charge])
        expected_sentence_2 = attr.evolve(sentence_2, charges=[expected_charge])
        expected_external_id = attr.evolve(external_id)
        expected_person = attr.evolve(
            person,
            external_ids=[expected_external_id],
            incarceration_sentences=[expected_sentence, expected_sentence_2],
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, [person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )

    def test_mergeMultiParentEntities_mergeCourtCaseWithJudge(self) -> None:
        # Arrange 1 - Match
        db_person = generate_person(state_code=_STATE_CODE)
        db_court_case = generate_court_case(
            person=db_person,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code="county_code",
        )
        db_charge = generate_charge(
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            description="charge_1",
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            court_case=db_court_case,
        )
        db_incarceration_sentence = generate_incarceration_sentence(
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code="county_code",
            charges=[db_charge],
        )
        db_external_id = generate_external_id(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE,
        )
        db_person.external_ids = [db_external_id]
        db_person.incarceration_sentences = [db_incarceration_sentence]
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        new_agent = StateAgent.new_with_defaults(
            agent_type=StateAgentType.JUDGE, state_code=_STATE_CODE
        )

        person = StatePerson.new_with_defaults(
            state_code=_STATE_CODE,
            external_ids=[
                StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE,
                    external_id=_EXTERNAL_ID,
                    id_type=_ID_TYPE,
                )
            ],
            incarceration_sentences=[
                StateIncarcerationSentence.new_with_defaults(
                    state_code=_STATE_CODE,
                    external_id=_EXTERNAL_ID,
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                    charges=[
                        StateCharge.new_with_defaults(
                            state_code=_STATE_CODE,
                            external_id=_EXTERNAL_ID,
                            status=ChargeStatus.PRESENT_WITHOUT_INFO,
                            court_case=StateCourtCase.new_with_defaults(
                                state_code=_STATE_CODE,
                                external_id=_EXTERNAL_ID,
                                judge=new_agent,
                            ),
                        ),
                    ],
                ),
            ],
        )

        expected_person = attr.evolve(entity_person)
        expected_incarceration_sentence = expected_person.incarceration_sentences[0]
        expected_incarceration_sentence.charges[0].court_case.judge = attr.evolve(
            new_agent
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, [person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )

    def test_mergeMultiParentEntities_mergeCourtCaseWithJudge2(self) -> None:
        # Arrange 1 - Match
        db_person = generate_person(state_code=_STATE_CODE)
        db_court_case = generate_court_case(
            person=db_person,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code="county_code",
        )
        db_charge = generate_charge(
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            description="charge_1",
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            court_case=db_court_case,
        )
        db_incarceration_sentence = generate_incarceration_sentence(
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code="county_code",
            charges=[db_charge],
        )
        db_external_id = generate_external_id(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE,
        )
        db_person.external_ids = [db_external_id]
        db_person.incarceration_sentences = [db_incarceration_sentence]
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        new_agent = StateAgent.new_with_defaults(
            agent_type=StateAgentType.JUDGE, state_code=_STATE_CODE
        )

        person = StatePerson.new_with_defaults(
            state_code=_STATE_CODE,
            external_ids=[
                StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE,
                    external_id=_EXTERNAL_ID,
                    id_type=_ID_TYPE,
                )
            ],
            incarceration_sentences=[
                StateIncarcerationSentence.new_with_defaults(
                    state_code=_STATE_CODE,
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                    charges=[
                        StateCharge.new_with_defaults(
                            state_code=_STATE_CODE,
                            status=ChargeStatus.PRESENT_WITHOUT_INFO,
                            court_case=StateCourtCase.new_with_defaults(
                                state_code=_STATE_CODE,
                                external_id=_EXTERNAL_ID,
                                judge=new_agent,
                            ),
                        ),
                    ],
                ),
            ],
        )

        expected_person = attr.evolve(entity_person)
        expected_incarceration_sentence = expected_person.incarceration_sentences[0]
        expected_incarceration_sentence.charges[0].court_case.judge = attr.evolve(
            new_agent
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, [person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )

    def get_mismatched_tree_shape_input_and_expected(
        self,
    ) -> Tuple[List[StatePerson], List[StatePerson]]:
        """Returns a tuple of input_people, expected_matched_people where the input
        people have mismatched tree shapes.
        """
        charge_merged = StateCharge.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
        )
        charge_unmerged = attr.evolve(charge_merged, charge_id=None)
        charge_duplicate_unmerged = attr.evolve(charge_unmerged)
        sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge_unmerged],
        )
        sentence_2 = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge_duplicate_unmerged],
        )
        sentence_3 = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID_3,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge_merged],
        )
        external_id = StatePersonExternalId.new_with_defaults(
            external_id=_EXTERNAL_ID, state_code=_STATE_CODE, id_type=_ID_TYPE
        )
        person = StatePerson.new_with_defaults(
            external_ids=[external_id],
            incarceration_sentences=[sentence, sentence_2, sentence_3],
            state_code=_STATE_CODE,
        )

        other_person = StatePerson.new_with_defaults(
            state_code=_STATE_CODE,
            external_ids=[
                StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2, id_type=_ID_TYPE
                )
            ],
        )

        expected_charge = attr.evolve(charge_merged)
        expected_sentence = attr.evolve(sentence, charges=[expected_charge])
        expected_sentence_2 = attr.evolve(sentence_2, charges=[expected_charge])
        expected_sentence_3 = attr.evolve(sentence_3, charges=[expected_charge])
        expected_external_id = attr.evolve(external_id)
        expected_person = attr.evolve(
            person,
            external_ids=[expected_external_id],
            incarceration_sentences=[
                expected_sentence,
                expected_sentence_2,
                expected_sentence_3,
            ],
        )
        expected_other_person = attr.evolve(other_person)

        return [other_person, person], [expected_other_person, expected_person]

    def test_mergeMultiParentEntitiesMismatchedTreeShape(self) -> None:
        # Arrange 1 - Match
        (
            input_people,
            expected_people,
        ) = self.get_mismatched_tree_shape_input_and_expected()

        # Arrange 1 - Match
        session = self._session()
        matched_entities = self._match(session, input_people)

        # Assert 1 - Match
        found_person = one(
            p
            for p in matched_entities.people
            if p.external_ids[0].external_id == _EXTERNAL_ID
        )
        found_charge = found_person.incarceration_sentences[0].charges[0]
        found_charge_2 = found_person.incarceration_sentences[1].charges[0]
        found_charge_3 = found_person.incarceration_sentences[2].charges[0]
        self.assertEqual(id(found_charge), id(found_charge_2), id(found_charge_3))
        self.assert_people_match_pre_and_post_commit(
            expected_people,
            matched_entities.people,
            session,
        )

    @patch(
        f"{state_entity_matcher.__name__}.NUM_TREES_TO_SEARCH_FOR_NON_PLACEHOLDER_TYPES",
        1,
    )
    @unittest.skip(
        "TODO(#7908): Re-enable once we go back to throwing errors on mismatched shapes"
    )
    def test_mergeMultiParentEntitiesMismatchedTreeShapeSmallerSearch(self) -> None:
        # Arrange 1 - Match
        (
            input_people,
            _,
        ) = self.get_mismatched_tree_shape_input_and_expected()

        # Arrange 1 - Match
        session = self._session()
        with self.assertRaisesRegex(
            ValueError,
            "^Failed to identify one of the non-placeholder ingest types: "
            r"\[StateSentenceGroup\]\.",
        ):
            _ = self._match(session, input_people)

    @patch(
        f"{state_entity_matcher.__name__}.MAX_NUM_TREES_TO_SEARCH_FOR_NON_PLACEHOLDER_TYPES",
        1,
    )
    def test_mismatchedTreeShapeEnumTypesOnly(self) -> None:
        # Arrange 1 - Match
        db_external_id = generate_external_id(
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE
        )
        entity_external_id = self.to_entity(db_external_id)
        db_external_id_2 = generate_external_id(
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE_ANOTHER,
        )
        entity_external_id_2 = self.to_entity(db_external_id_2)
        db_person = generate_person(
            full_name=_FULL_NAME,
            external_ids=[db_external_id, db_external_id_2],
            state_code=_STATE_CODE,
        )
        entity_person = self.to_entity(db_person)
        self._commit_to_db(db_person)
        external_id = attr.evolve(entity_external_id, person_external_id_id=None)
        person = attr.evolve(
            entity_person,
            person_id=None,
            external_ids=[external_id],
        )
        race = StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE, race=Race.OTHER
        )
        external_id_2 = attr.evolve(entity_external_id_2, person_external_id_id=None)
        person_dup = attr.evolve(
            person, races=[race], external_ids=[attr.evolve(external_id_2)]
        )

        expected_external_id = attr.evolve(
            external_id,
        )
        expected_external_id_2 = attr.evolve(external_id_2)
        expected_race = attr.evolve(race)
        expected_person = attr.evolve(
            person,
            races=[expected_race],
            external_ids=[expected_external_id, expected_external_id_2],
        )

        # Arrange 1 - Match
        session = self._session()
        matched_entities = self._match(session, [person, person_dup])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person],
            matched_entities.people,
            session,
        )

    def test_databaseCleanup_dontMergeMultiParentEntities(self) -> None:
        db_person = generate_person(full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_court_case = generate_court_case(person=db_person, external_id=_EXTERNAL_ID)
        db_placeholder_charge = generate_charge(
            person=db_person, court_case=db_court_case
        )
        db_placeholder_incarceration_sentence = generate_incarceration_sentence(
            person=db_person,
            charges=[db_placeholder_charge],
        )
        db_external_id = generate_external_id(
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE,
        )
        db_person.external_ids = [db_external_id]
        db_person.incarceration_sentences = [db_placeholder_incarceration_sentence]

        db_person_2 = generate_person(full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_court_case_2 = generate_court_case(
            person=db_person_2, external_id=_EXTERNAL_ID
        )
        db_placeholder_charge_2 = generate_charge(
            person=db_person_2, court_case=db_court_case_2
        )
        db_placeholder_incarceration_sentence_2 = generate_incarceration_sentence(
            person=db_person_2,
            charges=[db_placeholder_charge_2],
        )
        db_external_id_2 = generate_external_id(
            person=db_person_2,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2,
            id_type=_ID_TYPE,
        )
        db_person_2.external_ids = [db_external_id_2]
        db_person_2.incarceration_sentences = [db_placeholder_incarceration_sentence_2]

        court_case = attr.evolve(self.to_entity(db_court_case), court_case_id=None)
        placeholder_charge = attr.evolve(
            self.to_entity(db_placeholder_charge), charge_id=None, court_case=court_case
        )
        placeholder_incarceration_sentence = attr.evolve(
            self.to_entity(db_placeholder_incarceration_sentence),
            incarceration_sentence_id=None,
            charges=[placeholder_charge],
        )
        external_id = attr.evolve(
            self.to_entity(db_external_id), person_external_id_id=None
        )
        person = attr.evolve(
            self.to_entity(db_person),
            person_id=None,
            external_ids=[external_id],
            incarceration_sentences=[placeholder_incarceration_sentence],
            current_address="address",
        )

        expected_person = attr.evolve(
            self.to_entity(db_person), current_address="address"
        )
        expected_person_2 = attr.evolve(self.to_entity(db_person_2))

        self._commit_to_db(db_person, db_person_2)

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person],
            matched_entities.people,
            session,
            expected_unmatched_db_people=[expected_person_2],
        )
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_mergeMultiParentEntityParentAndChild_multipleSentenceParents(self) -> None:
        """Tests merging multi-parent entities, but the two types of entities
        that must be merged are directly connected (themselves parent/child).
        In this tests case they are StateSupervisionViolation and
        StateSupervisionViolationResponse.
        """
        # Arrange
        db_person = generate_person(state_code=_STATE_CODE)
        db_external_id = generate_external_id(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE,
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person.external_ids = [db_external_id]
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        supervision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                external_id=_EXTERNAL_ID, state_code=_STATE_CODE
            )
        )
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            supervision_violation_responses=[supervision_violation_response],
        )

        supervision_violation_response_dup = attr.evolve(supervision_violation_response)
        supervision_violation_dup = attr.evolve(
            supervision_violation,
            supervision_violation_responses=[supervision_violation_response_dup],
        )

        external_id = attr.evolve(entity_external_id, person_external_id_id=None)
        person = attr.evolve(
            entity_person,
            person_id=None,
            external_ids=[external_id],
            # These supervision violations should get merged
            supervision_violations=[supervision_violation, supervision_violation_dup],
        )

        # Only one expected violation response and violation, as they've been
        # merged
        expected_supervision_violation_response = attr.evolve(
            supervision_violation_response,
        )
        expected_supervision_violation = attr.evolve(
            supervision_violation,
            supervision_violation_responses=[expected_supervision_violation_response],
        )

        expected_person = attr.evolve(
            entity_person, supervision_violations=[expected_supervision_violation]
        )

        # Act
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

        # Assert
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_mergePersonIntoPersonWithPlaceholderChains(self) -> None:
        # Arrange
        db_person_1 = generate_person(state_code=_STATE_CODE)
        db_external_id_1 = generate_external_id(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE,
        )
        db_person_1.external_ids = [db_external_id_1]

        self._commit_to_db(db_person_1)

        db_person_2 = generate_person(state_code=_STATE_CODE)
        db_external_id_2 = generate_external_id(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2,
            id_type=_ID_TYPE_ANOTHER,
        )
        db_person_2.external_ids = [db_external_id_2]

        db_placeholder_supervision_sentence_2a = generate_supervision_sentence(
            person=db_person_2,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE,
        )
        db_person_2.supervision_sentences = [db_placeholder_supervision_sentence_2a]

        db_supervision_period_2a_1 = generate_supervision_period(
            person=db_person_2,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_1,
            termination_date=_DATE_2,
        )
        db_supervision_period_2a_b = generate_supervision_period(
            person=db_person_2,
            external_id=_EXTERNAL_ID_2,
            start_date=_DATE_2,
            termination_date=_DATE_3,
        )

        # Dangling placeholder "chain" #1
        db_placeholder_supervision_period_2a = generate_supervision_period(
            person=db_person_2,
        )
        db_placeholder_supervision_sentence_2a.supervision_periods = [
            db_supervision_period_2a_1,
            db_supervision_period_2a_b,
            db_placeholder_supervision_period_2a,
        ]

        # Start dangling placeholder chain 2
        db_placeholder_supervision_sentence_2b = generate_supervision_sentence(
            person=db_person_2,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE,
        )

        db_person_2.supervision_sentences = [db_placeholder_supervision_sentence_2b]

        db_placeholder_supervision_period_2b = generate_supervision_period(
            person=db_person_2,
        )

        db_placeholder_supervision_sentence_2b.supervision_periods = [
            db_placeholder_supervision_period_2b
        ]
        # End dangling placeholder chain 2

        self._commit_to_db(db_person_2)

        person = StatePerson.new_with_defaults(
            state_code=_STATE_CODE,
            external_ids=[
                StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE,
                    external_id=_EXTERNAL_ID,
                    id_type=_ID_TYPE,
                ),
                StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE,
                    external_id=_EXTERNAL_ID_2,
                    id_type=_ID_TYPE_ANOTHER,
                ),
            ],
        )

        # Act
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_atomicIpMerge(self) -> None:
        # Arrange 1 - Match
        db_external_id = generate_external_id(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID
        )
        db_person = generate_person(
            full_name=_FULL_NAME,
            state_code=_STATE_CODE,
            external_ids=[db_external_id],
        )
        db_incarceration_period = generate_incarceration_period(
            db_person,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            facility="facility",
            admission_date=datetime.date(2021, 4, 14),
            release_date=datetime.date(2022, 1, 1),
        )
        db_person.incarceration_periods = [db_incarceration_period]

        entity_external_id = self.to_entity(db_external_id)
        entity_incarceration_period = self.to_entity(db_incarceration_period)
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        incarceration_period = attr.evolve(
            entity_incarceration_period,
            admission_date=datetime.date(2021, 5, 15),
            release_date=None,
        )
        external_id = attr.evolve(entity_external_id)
        person = attr.evolve(
            entity_person,
            external_ids=[external_id],
            incarceration_periods=[incarceration_period],
        )
        expected_incarceration_period = attr.evolve(incarceration_period)
        expected_external_id = attr.evolve(external_id)
        expected_person = attr.evolve(
            person,
            external_ids=[expected_external_id],
            incarceration_periods=[expected_incarceration_period],
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

        # Assert 1 - Match
        self.assert_no_errors(matched_entities)
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_get_non_placeholder_ingest_types_indices_to_search(self) -> None:
        for i in range(0, 2500 + 1):
            indices = (
                StateEntityMatcher.get_non_placeholder_ingest_types_indices_to_search(i)
            )
            if i < MAX_NUM_TREES_TO_SEARCH_FOR_NON_PLACEHOLDER_TYPES:
                self.assertEqual(i, len(indices))
            else:
                self.assertEqual(
                    MAX_NUM_TREES_TO_SEARCH_FOR_NON_PLACEHOLDER_TYPES, len(indices)
                )

            # Make sure all indices are in bounds
            self.assertFalse(any(index > i - 1 for index in indices))
