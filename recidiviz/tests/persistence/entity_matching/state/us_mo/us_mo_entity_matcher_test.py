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
"""US_MO specific entity matching tests."""
import datetime
from typing import List

import attr

from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.ingest_metadata import IngestMetadata, SystemLevel
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.entities import EntityPersonType
from recidiviz.persistence.entity.state.entities import (
    StateAgent,
    StatePerson,
    StateSupervisionPeriod,
)
from recidiviz.persistence.entity_matching import entity_matching
from recidiviz.persistence.entity_matching.entity_matching_types import MatchedEntities
from recidiviz.tests.persistence.database.schema.state.schema_test_utils import (
    generate_agent,
    generate_external_id,
    generate_person,
    generate_supervision_period,
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
DEFAULT_METADATA = IngestMetadata(
    region=_US_MO,
    system_level=SystemLevel.STATE,
    ingest_time=datetime.datetime(year=1000, month=1, day=1),
    database_key=SQLAlchemyDatabaseKey.canonical_for_schema(
        schema_type=SchemaType.STATE
    ),
)


class TestMoEntityMatching(BaseStateEntityMatcherTest):
    """Test class for US_MO specific entity matching logic."""

    @staticmethod
    def _match(
        session: Session, ingested_people: List[EntityPersonType]
    ) -> MatchedEntities:
        return entity_matching.match(session, _US_MO, ingested_people, DEFAULT_METADATA)

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
        entity_external_id = self.to_entity(db_external_id)
        db_supervision_period = generate_supervision_period(
            person=db_person,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_1,
            state_code=_US_MO,
            supervising_officer=db_supervising_officer,
        )
        db_supervision_period_another = generate_supervision_period(
            person=db_person,
            external_id=_EXTERNAL_ID_2,
            start_date=_DATE_2,
            state_code=_US_MO,
            supervising_officer=db_supervising_officer,
        )
        db_closed_supervision_period = generate_supervision_period(
            person=db_person,
            external_id=_EXTERNAL_ID_3,
            start_date=_DATE_3,
            termination_date=_DATE_4,
            state_code=_US_MO,
            supervising_officer=db_supervising_officer,
        )

        db_person.external_ids = [db_external_id]
        db_person.supervision_periods = [
            db_supervision_period,
            db_supervision_period_another,
            db_closed_supervision_period,
        ]
        entity_person = self.to_entity(db_person)
        self._commit_to_db(db_person)

        external_id = attr.evolve(entity_external_id, person_external_id_id=None)
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

        expected_person = attr.evolve(entity_person)

        # Act 1 - Match
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

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
        entity_external_id = self.to_entity(db_external_id)
        db_supervision_period = generate_supervision_period(
            person=db_person,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_1,
            termination_date=_DATE_2,
            state_code=_US_MO,
            supervising_officer=db_supervising_officer,
        )
        entity_supervision_period = self.to_entity(db_supervision_period)
        db_supervision_period_open = generate_supervision_period(
            person=db_person,
            external_id=_EXTERNAL_ID_2,
            start_date=_DATE_2,
            state_code=_US_MO,
            supervising_officer=db_supervising_officer,
        )
        entity_supervision_period_open = self.to_entity(db_supervision_period_open)

        db_person.external_ids = [db_external_id]
        db_person.supervision_periods = [
            db_supervision_period,
            db_supervision_period_open,
        ]
        entity_person = self.to_entity(db_person)
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
            supervising_officer=new_supervising_officer,
        )
        supervision_period_update = StateSupervisionPeriod.new_with_defaults(
            external_id=entity_supervision_period_open.external_id,
            state_code=_US_MO,
            start_date=_DATE_2,
            termination_date=_DATE_3,
        )

        external_id = attr.evolve(entity_external_id, person_external_id_id=None)
        person = StatePerson.new_with_defaults(
            external_ids=[external_id],
            supervision_periods=[supervision_period_update, new_supervision_period],
            state_code=_US_MO,
        )

        expected_person = attr.evolve(entity_person)
        expected_person.supervising_officer = new_supervising_officer

        expected_unchanged_supervision_period = attr.evolve(entity_supervision_period)
        expected_updated_supervision_period = attr.evolve(
            entity_supervision_period_open,
            termination_date=supervision_period_update.termination_date,
            supervising_officer=expected_unchanged_supervision_period.supervising_officer,
        )
        expected_person.supervision_periods = [
            expected_unchanged_supervision_period,
            expected_updated_supervision_period,
            new_supervision_period,
        ]

        # Act
        session = self._session()
        matched_entities = self._match(session, ingested_people=[person])

        # Assert
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session
        )
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)
