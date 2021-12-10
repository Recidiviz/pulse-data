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

from recidiviz.common.constants.states import StateCode
from recidiviz.common.ingest_metadata import IngestMetadata, SystemLevel
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity.entities import EntityPersonType
from recidiviz.persistence.entity.state.entities import StatePersonExternalId
from recidiviz.persistence.entity_matching import entity_matching
from recidiviz.persistence.entity_matching.entity_matching_types import MatchedEntities
from recidiviz.tests.persistence.database.schema.state.schema_test_utils import (
    generate_incarceration_sentence,
    generate_person,
    generate_sentence_group,
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
    database_key=DirectIngestInstance.SECONDARY.database_key_for_state(StateCode.US_ND),
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
