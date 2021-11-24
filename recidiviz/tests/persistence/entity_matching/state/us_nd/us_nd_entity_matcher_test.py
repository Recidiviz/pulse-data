# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""US_ND specific entity matching tests."""
import datetime
from typing import List

import attr

from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.states import StateCode
from recidiviz.common.ingest_metadata import IngestMetadata, SystemLevel
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity.entities import EntityPersonType
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StatePerson,
    StatePersonExternalId,
    StateSentenceGroup,
    StateSupervisionCaseTypeEntry,
)
from recidiviz.persistence.entity_matching import entity_matching
from recidiviz.persistence.entity_matching.entity_matching_types import MatchedEntities
from recidiviz.tests.persistence.database.schema.state.schema_test_utils import (
    generate_external_id,
    generate_incarceration_period,
    generate_incarceration_sentence,
    generate_person,
    generate_sentence_group,
    generate_supervision_case_type_entry,
    generate_supervision_period,
    generate_supervision_sentence,
)
from recidiviz.tests.persistence.entity_matching.state.base_state_entity_matcher_test_classes import (
    BaseStateEntityMatcherTest,
)

_EXTERNAL_ID = "EXTERNAL_ID-1"
_EXTERNAL_ID_2 = "EXTERNAL_ID-2"
_EXTERNAL_ID_3 = "EXTERNAL_ID-3"
_EXTERNAL_ID_4 = "EXTERNAL_ID-4"
_ID_TYPE = "ID_TYPE"
_FULL_NAME = "FULL_NAME"
_COUNTY_CODE = "Iredell"
_US_ND = "US_ND"
_OTHER_STATE_CODE = "US_XX"
_FACILITY = "FACILITY"
_FACILITY_2 = "FACILITY_2"
_DATE_1 = datetime.date(year=2019, month=1, day=1)
_DATE_2 = datetime.date(year=2019, month=2, day=1)
_DATE_3 = datetime.date(year=2019, month=3, day=1)
_DATE_4 = datetime.date(year=2019, month=4, day=1)
_DATE_5 = datetime.date(year=2019, month=5, day=1)
_DATE_6 = datetime.date(year=2019, month=6, day=1)
DEFAULT_METADATA = IngestMetadata(
    region=_US_ND,
    system_level=SystemLevel.STATE,
    ingest_time=datetime.datetime(year=1000, month=1, day=1),
    # TODO(#10152): Change this to the following once
    #  elite_externalmovements_incarceration_periods has shipped to prod:
    #      database_key=SQLAlchemyDatabaseKey.canonical_for_schema(
    #         schema_type=SchemaType.STATE
    #     ),
    database_key=DirectIngestInstance.PRIMARY.database_key_for_state(StateCode.US_ND),
)


class TestNdEntityMatching(BaseStateEntityMatcherTest):
    """Test class for US_ND specific entity matching logic."""

    @staticmethod
    def _match(
        session: Session,
        ingested_people: List[EntityPersonType],
    ) -> MatchedEntities:
        return entity_matching.match(session, _US_ND, ingested_people, DEFAULT_METADATA)

    def test_runMatch_checkPlaceholdersWhenNoRootEntityMatch(self) -> None:
        """Tests that ingested people are matched with DB placeholder people
        when a root entity match doesn't exist. Specific to US_ND.
        """
        db_placeholder_person = generate_person(state_code=_US_ND)
        db_sg = generate_sentence_group(state_code=_US_ND, external_id=_EXTERNAL_ID)
        entity_sg = self.to_entity(db_sg)
        db_placeholder_person.sentence_groups = [db_sg]
        entity_placeholder_person = self.to_entity(db_placeholder_person)
        db_placeholder_person_other_state = generate_person(state_code=_US_ND)
        db_sg_other_state = generate_sentence_group(
            state_code=_OTHER_STATE_CODE, external_id=_EXTERNAL_ID
        )
        db_placeholder_person_other_state.sentence_groups = [db_sg_other_state]
        entity_placeholder_person_other_state = self.to_entity(
            db_placeholder_person_other_state
        )

        self._commit_to_db(db_placeholder_person, db_placeholder_person_other_state)

        sg = attr.evolve(entity_sg, sentence_group_id=None)
        external_id = StatePersonExternalId.new_with_defaults(
            external_id=_EXTERNAL_ID, state_code=_US_ND, id_type=_ID_TYPE
        )
        person = attr.evolve(
            entity_placeholder_person,
            person_id=None,
            full_name=_FULL_NAME,
            external_ids=[external_id],
            sentence_groups=[sg],
        )

        expected_sg = attr.evolve(entity_sg)
        expected_external_id = attr.evolve(external_id)
        expected_person = attr.evolve(
            person, external_ids=[expected_external_id], sentence_groups=[expected_sg]
        )
        expected_placeholder_person = attr.evolve(
            entity_placeholder_person, sentence_groups=[]
        )
        expected_placeholder_person_other_state = entity_placeholder_person_other_state

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [
                expected_person,
                expected_placeholder_person,
                expected_placeholder_person_other_state,
            ],
            matched_entities.people,
            session,
        )
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_runMatch_sameEntities_noDuplicates(self) -> None:
        db_placeholder_person = generate_person(state_code=_US_ND)
        db_is = generate_incarceration_sentence(
            person=db_placeholder_person, state_code=_US_ND, date_imposed=_DATE_1
        )
        entity_is = self.to_entity(db_is)
        db_sg = generate_sentence_group(
            external_id=_EXTERNAL_ID, state_code=_US_ND, incarceration_sentences=[db_is]
        )
        entity_sg = self.to_entity(db_sg)
        db_placeholder_person.sentence_groups = [db_sg]
        entity_placeholder_person = self.to_entity(db_placeholder_person)

        self._commit_to_db(db_placeholder_person)

        inc_s = attr.evolve(
            entity_is, state_code=_US_ND, incarceration_sentence_id=None
        )
        sg = attr.evolve(
            entity_sg,
            sentence_group_id=None,
            state_code=_US_ND,
            incarceration_sentences=[inc_s],
        )
        placeholder_person = attr.evolve(
            entity_placeholder_person, person_id=None, sentence_groups=[sg]
        )

        expected_is = attr.evolve(entity_is)
        expected_sg = attr.evolve(entity_sg, incarceration_sentences=[expected_is])
        expected_placeholder_person = attr.evolve(
            entity_placeholder_person, sentence_groups=[expected_sg]
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_placeholder_person], matched_entities.people, session
        )
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    # TODO(#10152): Delete once elite_externalmovements_incarceration_periods has
    #  shipped to prod
    def test_match_mergeIncomingIncarcerationPeriods(self) -> None:
        # Arrange 1 - Match
        db_person = schema.StatePerson(full_name=_FULL_NAME, state_code=_US_ND)
        db_incarceration_sentence = schema.StateIncarcerationSentence(
            state_code=_US_ND,
            status=StateIncarcerationPeriodStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            person=db_person,
        )
        entity_incarceration_sentence = self.to_entity(db_incarceration_sentence)
        db_sentence_group = schema.StateSentenceGroup(
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence],
            person=db_person,
        )
        db_external_id = schema.StatePersonExternalId(
            state_code=_US_ND,
            id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID,
            person=db_person,
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            facility=_FACILITY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=datetime.date(year=2019, month=1, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID_2,
            facility=_FACILITY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            release_date=datetime.date(year=2019, month=1, day=2),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )
        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID_3,
            facility=_FACILITY_2,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=datetime.date(year=2019, month=1, day=2),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
        )
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[
                incarceration_period,
                incarceration_period_2,
                incarceration_period_3,
            ],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[incarceration_sentence],
        )
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group], state_code=_US_ND
        )

        expected_person = StatePerson.new_with_defaults(
            full_name=_FULL_NAME, state_code=_US_ND
        )
        expected_complete_incarceration_period = (
            StateIncarcerationPeriod.new_with_defaults(
                external_id=_EXTERNAL_ID + "|" + _EXTERNAL_ID_2,
                state_code=_US_ND,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                admission_date=datetime.date(year=2019, month=1, day=1),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                release_date=datetime.date(year=2019, month=1, day=2),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            )
        )
        expected_incomplete_incarceration_period = attr.evolve(incarceration_period_3)
        expected_incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=entity_incarceration_sentence.incarceration_sentence_id,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[
                expected_complete_incarceration_period,
                expected_incomplete_incarceration_period,
            ],
        )
        expected_sentence_group = StateSentenceGroup.new_with_defaults(
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[expected_incarceration_sentence],
        )
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=entity_external_id.person_external_id_id,
            state_code=_US_ND,
            id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID,
        )
        expected_person.external_ids = [expected_external_id]
        expected_person.sentence_groups = [expected_sentence_group]

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    # TODO(#10152): Delete once elite_externalmovements_incarceration_periods has
    #  shipped to prod
    def test_matchPersons_mergeIncompleteIncarcerationPeriodOntoComplete(self) -> None:
        """Tests correct matching behavior when an incomplete period is ingested
        and a matching complete period is in the db.
        """
        # Arrange 1 - Match
        db_person = generate_person(full_name=_FULL_NAME, state_code=_US_ND)
        db_complete_incarceration_period = generate_incarceration_period(
            person=db_person,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID + "|" + _EXTERNAL_ID_2,
            facility=_FACILITY,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY.value,
            admission_date=datetime.date(year=2018, month=1, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value,
            release_date=datetime.date(year=2018, month=1, day=2),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED.value,
        )
        entity_complete_incarceration_period = self.to_entity(
            db_complete_incarceration_period
        )

        db_incarceration_sentence = generate_incarceration_sentence(
            person=db_person,
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[db_complete_incarceration_period],
        )
        entity_incarceration_sentence = self.to_entity(db_incarceration_sentence)
        db_sentence_group = generate_sentence_group(
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence],
        )
        entity_sentence_group = self.to_entity(db_sentence_group)
        db_external_id = generate_external_id(
            state_code=_US_ND, id_type=_ID_TYPE, external_id=_EXTERNAL_ID
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        incomplete_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID_2,
            release_date=datetime.date(year=2018, month=1, day=2),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            county_code=_COUNTY_CODE,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[incomplete_incarceration_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_ND,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[incarceration_sentence],
        )
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group], state_code=_US_ND
        )

        expected_complete_incarceration_period = attr.evolve(
            entity_complete_incarceration_period, county_code=_COUNTY_CODE
        )

        expected_incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=entity_incarceration_sentence.incarceration_sentence_id,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[expected_complete_incarceration_period],
        )
        expected_sentence_group = attr.evolve(
            entity_sentence_group,
            incarceration_sentences=[expected_incarceration_sentence],
        )
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=entity_external_id.person_external_id_id,
            state_code=_US_ND,
            id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID,
        )
        expected_person = StatePerson.new_with_defaults(
            person_id=entity_person.person_id,
            full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group],
            state_code=_US_ND,
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    # TODO(#10152): Delete once elite_externalmovements_incarceration_periods has
    #  shipped to prod
    def test_matchPersons_mergeCompleteIncarcerationPeriodOntoIncomplete(self) -> None:
        """Tests correct matching behavior when a complete period is ingested
        and a matching incomplete period is in the db.
        """
        # Arrange 1 - Match
        db_person = generate_person(full_name=_FULL_NAME, state_code=_US_ND)
        db_incomplete_incarceration_period = generate_incarceration_period(
            person=db_person,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            facility=_FACILITY,
            incarceration_type=StateIncarcerationType.STATE_PRISON.value,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY.value,
            admission_date=datetime.date(year=2019, month=1, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value,
        )
        entity_incomplete_incarceration_period = self.to_entity(
            db_incomplete_incarceration_period
        )

        db_incarceration_sentence = generate_incarceration_sentence(
            person=db_person,
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[db_incomplete_incarceration_period],
        )
        entity_incarceration_sentence = self.to_entity(db_incarceration_sentence)
        db_sentence_group = generate_sentence_group(
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence],
        )
        entity_sentence_group = self.to_entity(db_sentence_group)
        db_external_id = generate_external_id(
            state_code=_US_ND, id_type=_ID_TYPE, external_id=_EXTERNAL_ID
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        complete_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID + "|" + _EXTERNAL_ID_2,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            facility=_FACILITY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=datetime.date(year=2019, month=1, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=datetime.date(year=2019, month=1, day=2),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[complete_incarceration_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            incarceration_sentences=[incarceration_sentence],
        )
        placeholder_person = StatePerson.new_with_defaults(
            state_code=_US_ND, sentence_groups=[sentence_group]
        )

        expected_complete_incarceration_period = attr.evolve(
            complete_incarceration_period,
            incarceration_period_id=entity_incomplete_incarceration_period.incarceration_period_id,
        )

        expected_incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=entity_incarceration_sentence.incarceration_sentence_id,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[expected_complete_incarceration_period],
        )
        expected_sentence_group = attr.evolve(
            entity_sentence_group,
            incarceration_sentences=[expected_incarceration_sentence],
        )
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=entity_external_id.person_external_id_id,
            state_code=_US_ND,
            id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID,
        )
        expected_person = StatePerson.new_with_defaults(
            person_id=entity_person.person_id,
            full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group],
            state_code=_US_ND,
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    # TODO(#10152): Delete once elite_externalmovements_incarceration_periods has
    #  shipped to prod
    def test_matchPersons_mergeCompleteIncarcerationPeriods(self) -> None:
        """Tests correct matching behavior when a complete period is ingested
        and a matching complete period is in the db.
        """
        # Arrange 1 - Match
        db_person = generate_person(full_name=_FULL_NAME, state_code=_US_ND)
        db_complete_incarceration_period = generate_incarceration_period(
            person=db_person,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID + "|" + _EXTERNAL_ID_2,
            facility=_FACILITY,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY.value,
            admission_date=datetime.date(year=2018, month=1, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value,
            release_date=datetime.date(year=2018, month=1, day=2),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED.value,
        )
        entity_complete_incarceration_period = self.to_entity(
            db_complete_incarceration_period
        )

        db_incarceration_sentence = generate_incarceration_sentence(
            person=db_person,
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[db_complete_incarceration_period],
        )
        entity_incarceration_sentence = self.to_entity(db_incarceration_sentence)
        db_sentence_group = generate_sentence_group(
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence],
        )
        entity_sentence_group = self.to_entity(db_sentence_group)
        db_external_id = generate_external_id(
            state_code=_US_ND, id_type=_ID_TYPE, external_id=_EXTERNAL_ID
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        updated_incarceration_period = attr.evolve(
            entity_complete_incarceration_period,
            incarceration_period_id=None,
            county_code=_COUNTY_CODE,
        )
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            incarceration_periods=[updated_incarceration_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_ND,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[incarceration_sentence],
        )
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group], state_code=_US_ND
        )

        expected_complete_incarceration_period = attr.evolve(
            updated_incarceration_period,
            incarceration_period_id=entity_complete_incarceration_period.incarceration_period_id,
        )

        expected_incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=entity_incarceration_sentence.incarceration_sentence_id,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[expected_complete_incarceration_period],
        )
        expected_sentence_group = attr.evolve(
            entity_sentence_group,
            incarceration_sentences=[expected_incarceration_sentence],
        )
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=entity_external_id.person_external_id_id,
            state_code=_US_ND,
            id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID,
        )
        expected_person = StatePerson.new_with_defaults(
            person_id=entity_person.person_id,
            full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group],
            state_code=_US_ND,
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    # TODO(#10152): Delete once elite_externalmovements_incarceration_periods has
    #  shipped to prod
    def test_matchPersons_mergeIncompleteIncarcerationPeriods(self) -> None:
        """Tests correct matching behavior when an incomplete period is ingested
        and a matching incomplete period is in the db.
        """
        # Arrange 1 - Match
        db_person = generate_person(full_name=_FULL_NAME, state_code=_US_ND)
        db_incomplete_incarceration_period = generate_incarceration_period(
            person=db_person,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID_3,
            facility=_FACILITY,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY.value,
            admission_date=datetime.date(year=2019, month=1, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value,
        )
        entity_incomplete_incarceration_period = self.to_entity(
            db_incomplete_incarceration_period
        )
        db_complete_incarceration_period = generate_incarceration_period(
            person=db_person,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID + "|" + _EXTERNAL_ID_2,
            facility=_FACILITY,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY.value,
            admission_date=datetime.date(year=2018, month=1, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value,
            release_date=datetime.date(year=2018, month=1, day=2),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED.value,
        )
        entity_complete_incarceration_period = self.to_entity(
            db_complete_incarceration_period
        )

        db_incarceration_sentence = generate_incarceration_sentence(
            person=db_person,
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[
                db_complete_incarceration_period,
                db_incomplete_incarceration_period,
            ],
        )
        entity_incarceration_sentence = self.to_entity(db_incarceration_sentence)
        db_sentence_group = generate_sentence_group(
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence],
        )
        entity_sentence_group = self.to_entity(db_sentence_group)
        db_external_id = generate_external_id(
            state_code=_US_ND, id_type=_ID_TYPE, external_id=_EXTERNAL_ID
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID_4,
            facility=_FACILITY,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            release_date=datetime.date(year=2019, month=1, day=2),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            incarceration_periods=[incarceration_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_ND,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[incarceration_sentence],
        )
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group], state_code=_US_ND
        )

        expected_complete_incarceration_period = attr.evolve(
            entity_complete_incarceration_period
        )
        expected_new_complete_incarceration_period = attr.evolve(
            entity_incomplete_incarceration_period,
            external_id=_EXTERNAL_ID_3 + "|" + _EXTERNAL_ID_4,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            release_date=datetime.date(year=2019, month=1, day=2),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        expected_incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=entity_incarceration_sentence.incarceration_sentence_id,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[
                expected_new_complete_incarceration_period,
                expected_complete_incarceration_period,
            ],
        )
        expected_sentence_group = attr.evolve(
            entity_sentence_group,
            incarceration_sentences=[expected_incarceration_sentence],
        )
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=entity_external_id.person_external_id_id,
            state_code=_US_ND,
            id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID,
        )
        expected_person = StatePerson.new_with_defaults(
            person_id=entity_person.person_id,
            full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group],
            state_code=_US_ND,
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    # TODO(#10152): Delete once elite_externalmovements_incarceration_periods has
    #  shipped to prod
    def test_matchPersons_dontMergePeriodsFromDifferentStates(self) -> None:
        """Tests that incarceration periods don't match when either doesn't have
        US_ND as its state code.
        """
        # Arrange 1 - Match
        db_person = generate_person(full_name=_FULL_NAME, state_code=_US_ND)
        db_incomplete_incarceration_period = generate_incarceration_period(
            person=db_person,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID_3,
            facility=_FACILITY,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY.value,
            admission_date=datetime.date(year=2019, month=1, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value,
        )
        entity_incomplete_incarceration_period = self.to_entity(
            db_incomplete_incarceration_period
        )

        db_incarceration_sentence = generate_incarceration_sentence(
            person=db_person,
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[db_incomplete_incarceration_period],
        )
        entity_incarceration_sentence = self.to_entity(db_incarceration_sentence)
        db_sentence_group = generate_sentence_group(
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence],
        )
        entity_sentence_group = self.to_entity(db_sentence_group)
        db_external_id = generate_external_id(
            state_code=_US_ND, id_type=_ID_TYPE, external_id=_EXTERNAL_ID
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        incarceration_period_different_state = (
            StateIncarcerationPeriod.new_with_defaults(
                state_code=_OTHER_STATE_CODE,
                external_id=_EXTERNAL_ID_4,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                release_date=datetime.date(year=2019, month=1, day=2),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            )
        )
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            incarceration_periods=[incarceration_period_different_state],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_ND,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[incarceration_sentence],
        )
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group], state_code=_US_ND
        )

        expected_incarceration_period = attr.evolve(
            entity_incomplete_incarceration_period
        )
        expected_incarceration_period_different_state = attr.evolve(
            incarceration_period_different_state
        )

        expected_incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=entity_incarceration_sentence.incarceration_sentence_id,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[
                expected_incarceration_period,
                expected_incarceration_period_different_state,
            ],
        )
        expected_sentence_group = attr.evolve(
            entity_sentence_group,
            incarceration_sentences=[expected_incarceration_sentence],
        )
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=entity_external_id.person_external_id_id,
            state_code=_US_ND,
            id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID,
        )
        expected_person = StatePerson.new_with_defaults(
            person_id=entity_person.person_id,
            full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group],
            state_code=_US_ND,
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    # TODO(#10152): Delete once elite_externalmovements_incarceration_periods has
    #  shipped to prod
    def test_matchPersons_temporaryCustodyPeriods(self) -> None:
        # Arrange 1 - Match
        db_person = generate_person(full_name=_FULL_NAME, state_code=_US_ND)
        db_incomplete_temporary_custody = generate_incarceration_period(
            person=db_person,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            facility=_FACILITY,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY.value,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL.value,
            admission_date=datetime.date(year=2018, month=1, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value,
            admission_reason_raw_text="ADMN",
        )
        entity_incomplete_temporary_custody = self.to_entity(
            db_incomplete_temporary_custody
        )

        db_incarceration_sentence = generate_incarceration_sentence(
            person=db_person,
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[db_incomplete_temporary_custody],
        )
        entity_incarceration_sentence = self.to_entity(db_incarceration_sentence)
        db_sentence_group = generate_sentence_group(
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence],
        )
        entity_sentence_group = self.to_entity(db_sentence_group)
        db_external_id = generate_external_id(
            state_code=_US_ND, id_type=_ID_TYPE, external_id=_EXTERNAL_ID
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        new_temporary_custody_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID_2,
            facility=_FACILITY,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            release_date=datetime.date(year=2018, month=1, day=2),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        new_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID_3,
            facility=_FACILITY_2,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=datetime.date(year=2018, month=1, day=3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
        )
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            incarceration_periods=[
                new_temporary_custody_period,
                new_incarceration_period,
            ],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_ND,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[incarceration_sentence],
        )
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group], state_code=_US_ND
        )

        expected_complete_temporary_custody = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=entity_incomplete_temporary_custody.incarceration_period_id,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID + "|" + _EXTERNAL_ID_2,
            facility=_FACILITY,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            admission_date=datetime.date(year=2018, month=1, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="ADMN",
            release_date=datetime.date(year=2018, month=1, day=2),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )
        expected_new_period = attr.evolve(
            new_incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        expected_incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=entity_incarceration_sentence.incarceration_sentence_id,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[
                expected_complete_temporary_custody,
                expected_new_period,
            ],
        )
        expected_sentence_group = attr.evolve(
            entity_sentence_group,
            incarceration_sentences=[expected_incarceration_sentence],
        )
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=entity_external_id.person_external_id_id,
            state_code=_US_ND,
            id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID,
        )
        expected_person = StatePerson.new_with_defaults(
            person_id=entity_person.person_id,
            full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group],
            state_code=_US_ND,
        )

        # Act 1 - Match
        session = self._session()
        # This is testing code that is only running on PRIMARY right now
        ingest_metadata = IngestMetadata(
            region=_US_ND,
            system_level=SystemLevel.STATE,
            ingest_time=datetime.datetime(year=1000, month=1, day=1),
            database_key=DirectIngestInstance.PRIMARY.database_key_for_state(
                StateCode.US_ND
            ),
        )

        matched_entities = entity_matching.match(
            session,
            _US_ND,
            ingested_people=[placeholder_person],
            ingest_metadata=ingest_metadata,
        )

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    # TODO(#10152): Delete once elite_externalmovements_incarceration_periods has
    #  shipped to prod
    def test_matchPersons_mergeIngestedAndDbIncarcerationPeriods_reverse(self) -> None:
        """Tests that periods are correctly merged when the release period is
        ingested before the admission period."""
        # Arrange 1 - Match
        db_person = generate_person(full_name=_FULL_NAME, state_code=_US_ND)
        db_incomplete_incarceration_period = generate_incarceration_period(
            person=db_person,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID_4,
            facility=_FACILITY,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY.value,
            release_date=datetime.date(year=2019, month=1, day=2),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED.value,
        )
        entity_incomplete_incarceration_period = self.to_entity(
            db_incomplete_incarceration_period
        )
        db_complete_incarceration_period = generate_incarceration_period(
            person=db_person,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID + "|" + _EXTERNAL_ID_2,
            facility=_FACILITY,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY.value,
            admission_date=datetime.date(year=2018, month=1, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value,
            release_date=datetime.date(year=2018, month=1, day=2),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED.value,
        )
        entity_complete_incarceration_period = self.to_entity(
            db_complete_incarceration_period
        )

        db_incarceration_sentence = generate_incarceration_sentence(
            person=db_person,
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[
                db_complete_incarceration_period,
                db_incomplete_incarceration_period,
            ],
        )
        entity_incarceration_sentence = self.to_entity(db_incarceration_sentence)
        db_sentence_group = generate_sentence_group(
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence],
        )
        entity_sentence_group = self.to_entity(db_sentence_group)
        db_external_id = generate_external_id(
            state_code=_US_ND, id_type=_ID_TYPE, external_id=_EXTERNAL_ID
        )
        entity_external_id = self.to_entity(db_external_id)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]
        entity_person = self.to_entity(db_person)

        self._commit_to_db(db_person)

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID_3,
            facility=_FACILITY,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=datetime.date(year=2019, month=1, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_periods=[incarceration_period],
        )
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[incarceration_sentence],
        )
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group], state_code=_US_ND
        )

        expected_complete_incarceration_period = attr.evolve(
            entity_complete_incarceration_period
        )
        expected_new_complete_incarceration_period = attr.evolve(
            entity_incomplete_incarceration_period,
            external_id=_EXTERNAL_ID_3 + "|" + _EXTERNAL_ID_4,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2019, month=1, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        expected_incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=entity_incarceration_sentence.incarceration_sentence_id,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[
                expected_new_complete_incarceration_period,
                expected_complete_incarceration_period,
            ],
        )
        expected_sentence_group = attr.evolve(
            entity_sentence_group,
            incarceration_sentences=[expected_incarceration_sentence],
        )
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=entity_external_id.person_external_id_id,
            state_code=_US_ND,
            id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID,
        )
        expected_person = StatePerson.new_with_defaults(
            person_id=entity_person.person_id,
            full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group],
            state_code=_US_ND,
        )

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    # TODO(#10152): Delete once elite_externalmovements_incarceration_periods has
    #  shipped to prod
    def test_matchPersons_mergeIngestedAndDbSupervisionCaseTypeEntries(self) -> None:
        db_person = generate_person(state_code=_US_ND)
        db_external_id = generate_external_id(
            external_id=_EXTERNAL_ID, state_code=_US_ND, id_type=_ID_TYPE
        )
        entity_external_id = self.to_entity(db_external_id)
        db_supervision_case_type_entry = generate_supervision_case_type_entry(
            person=db_person,
            case_type=StateSupervisionCaseType.GENERAL.value,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
        )
        db_supervision_period = generate_supervision_period(
            person=db_person,
            state_code=_US_ND,
            case_type_entries=[db_supervision_case_type_entry],
        )
        entity_supervision_period = self.to_entity(db_supervision_period)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person, supervision_periods=[db_supervision_period]
        )
        entity_supervision_sentence = self.to_entity(db_supervision_sentence)
        db_sentence_group = generate_sentence_group(
            person=db_person, supervision_sentences=[db_supervision_sentence]
        )
        entity_sentence_group = self.to_entity(db_sentence_group)
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]
        entity_person = self.to_entity(db_person)
        self._commit_to_db(db_person)

        external_id = attr.evolve(entity_external_id, person_external_id_id=None)
        person = StatePerson.new_with_defaults(
            external_ids=[external_id], state_code=_US_ND
        )
        new_case_type = StateSupervisionCaseTypeEntry.new_with_defaults(
            state_code=_US_ND,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            external_id=_EXTERNAL_ID,
        )
        supervision_period = attr.evolve(
            entity_supervision_period,
            case_type_entries=[new_case_type],
            supervision_period_id=None,
        )
        supervision_sentence = attr.evolve(
            entity_supervision_sentence,
            supervision_periods=[supervision_period],
            supervision_sentence_id=None,
        )
        sentence_group = attr.evolve(
            entity_sentence_group,
            supervision_sentences=[supervision_sentence],
            sentence_group_id=None,
        )
        person.sentence_groups = [sentence_group]

        expected_supervision_case_type = attr.evolve(new_case_type)
        expected_supervision_period = attr.evolve(
            entity_supervision_period,
            case_type_entries=[expected_supervision_case_type],
        )
        expected_supervision_sentence = attr.evolve(
            entity_supervision_sentence,
            supervision_periods=[expected_supervision_period],
        )
        expected_sentence_group = attr.evolve(
            entity_sentence_group,
            supervision_sentences=[expected_supervision_sentence],
        )
        expected_external_id = entity_external_id
        expected_person = attr.evolve(
            entity_person,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group],
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
