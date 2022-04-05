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
"""State tests for persistence.py."""

from datetime import datetime
from unittest import TestCase

from mock import patch, Mock

from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.ingest_metadata import IngestMetadata, SystemLevel
from recidiviz.ingest.models.ingest_info_pb2 import IngestInfo
from recidiviz.persistence import persistence
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.schema.state import schema, dao
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter,
)
from recidiviz.persistence.entity.state.entities import StatePerson, \
    StatePersonExternalId, StateSentenceGroup
from recidiviz.tests.utils import fakes

EXTERNAL_ID = 'EXTERNAL_ID'
EXTERNAL_ID_2 = 'EXTERNAL_ID_2'
FULL_NAME_1 = 'TEST_FULL_NAME_1'
REGION_CODE = 'US_ND'
COUNTY_CODE = 'COUNTY'
DEFAULT_METADATA = IngestMetadata.new_with_defaults(
    region='US_ND',
    jurisdiction_id='12345678',
    system_level=SystemLevel.STATE,
    ingest_time=datetime(year=1000, month=1, day=1))
ID_TYPE = 'ID_TYPE'
ID = 1
ID_2 = 2
ID_3 = 3
ID_4 = 4
SENTENCE_GROUP_ID = 'SG1'
SENTENCE_GROUP_ID_2 = 'SG2'
SENTENCE_GROUP_ID_3 = 'SG3'


@patch('os.getenv', Mock(return_value='production'))
@patch.dict('os.environ', {'PERSIST_LOCALLY': 'false'})
class TestStatePersistence(TestCase):
    """Test that the persistence layer correctly writes to the SQL database for
    the state schema specifically."""

    def setUp(self) -> None:
        fakes.use_in_memory_sqlite_database(StateBase)

    def to_entity(self, schema_obj):
        return converter.convert_schema_object_to_entity(
            schema_obj, populate_back_edges=True)

    def test_state_threeSentenceGroups_dontPersistAboveThreshold(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.state_people.add(
            state_person_id='1_GENERATE',
            state_sentence_group_ids=[
                SENTENCE_GROUP_ID, SENTENCE_GROUP_ID_2])
        ingest_info.state_sentence_groups.add(
            state_sentence_group_id=SENTENCE_GROUP_ID,
            county_code=COUNTY_CODE)
        ingest_info.state_sentence_groups.add(
            state_sentence_group_id=SENTENCE_GROUP_ID_2,
            county_code=COUNTY_CODE)

        db_person = schema.StatePerson(
            person_id=ID, full_name=FULL_NAME_1)
        db_sentence_group = schema.StateSentenceGroup(
            sentence_group_id=ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=SENTENCE_GROUP_ID,
            state_code=REGION_CODE)
        db_sentence_group_2 = schema.StateSentenceGroup(
            sentence_group_id=ID_2,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=SENTENCE_GROUP_ID_2,
            state_code=REGION_CODE)
        db_external_id = schema.StatePersonExternalId(
            person_external_id_id=ID, state_code=REGION_CODE,
            external_id=EXTERNAL_ID, id_type=ID_TYPE)
        db_person.sentence_groups = [
            db_sentence_group, db_sentence_group_2]
        db_person.external_ids = [db_external_id]

        db_person_2 = schema.StatePerson(person_id=ID_2, full_name=FULL_NAME_1)
        db_sentence_group_2_dup = schema.StateSentenceGroup(
            sentence_group_id=ID_3,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=SENTENCE_GROUP_ID_2,
            state_code=REGION_CODE)
        db_external_id_2 = schema.StatePersonExternalId(
            person_external_id_id=ID_2, state_code=REGION_CODE,
            external_id=EXTERNAL_ID_2, id_type=ID_TYPE)
        db_person_2.sentence_groups = [db_sentence_group_2_dup]
        db_person_2.external_ids = [db_external_id_2]

        # No updates
        expected_person = self.to_entity(db_person)
        expected_person_2 = self.to_entity(db_person_2)

        session = SessionFactory.for_schema_base(StateBase)
        session.add(db_person)
        session.add(db_person_2)
        session.commit()

        # Act
        persistence.write(ingest_info, DEFAULT_METADATA)
        session = SessionFactory.for_schema_base(StateBase)
        persons = dao.read_people(session)

        # Assert
        self.assertEqual([expected_person, expected_person_2],
                         converter.convert_schema_objects_to_entity(persons))

    def test_state_threeSentenceGroups_persistsTwoBelowThreshold(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.state_people.add(
            state_person_id='1_GENERATE',
            state_sentence_group_ids=[
                SENTENCE_GROUP_ID, SENTENCE_GROUP_ID_2, SENTENCE_GROUP_ID_3])
        ingest_info.state_sentence_groups.add(
            state_sentence_group_id=SENTENCE_GROUP_ID,
            county_code=COUNTY_CODE)
        ingest_info.state_sentence_groups.add(
            state_sentence_group_id=SENTENCE_GROUP_ID_2,
            county_code=COUNTY_CODE)
        ingest_info.state_sentence_groups.add(
            state_sentence_group_id=SENTENCE_GROUP_ID_3,
            county_code=COUNTY_CODE)

        db_person = schema.StatePerson(
            person_id=ID, full_name=FULL_NAME_1)
        db_sentence_group = schema.StateSentenceGroup(
            sentence_group_id=ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=SENTENCE_GROUP_ID,
            state_code=REGION_CODE)
        db_sentence_group_2 = schema.StateSentenceGroup(
            sentence_group_id=ID_2,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=SENTENCE_GROUP_ID_2,
            state_code=REGION_CODE)
        db_sentence_group_3 = schema.StateSentenceGroup(
            sentence_group_id=ID_3,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=SENTENCE_GROUP_ID_3,
            state_code=REGION_CODE)
        db_external_id = schema.StatePersonExternalId(
            person_external_id_id=ID, state_code=REGION_CODE,
            external_id=EXTERNAL_ID, id_type=ID_TYPE)
        db_person.sentence_groups = [
            db_sentence_group, db_sentence_group_2, db_sentence_group_3]
        db_person.external_ids = [db_external_id]

        db_person_2 = schema.StatePerson(person_id=ID_2, full_name=FULL_NAME_1)
        db_sentence_group_3_dup = schema.StateSentenceGroup(
            sentence_group_id=ID_4,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=SENTENCE_GROUP_ID_3,
            state_code=REGION_CODE)
        db_external_id_2 = schema.StatePersonExternalId(
            person_external_id_id=ID_2, state_code=REGION_CODE,
            external_id=EXTERNAL_ID_2, id_type=ID_TYPE)
        db_person_2.sentence_groups = [db_sentence_group_3_dup]
        db_person_2.external_ids = [db_external_id_2]

        expected_person = StatePerson.new_with_defaults(
            person_id=ID, full_name=FULL_NAME_1, external_ids=[],
            sentence_groups=[])
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=ID, state_code=REGION_CODE,
            external_id=EXTERNAL_ID,
            id_type=ID_TYPE, person=expected_person)
        expected_sentence_group = StateSentenceGroup.new_with_defaults(
            sentence_group_id=ID, status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            external_id=SENTENCE_GROUP_ID, state_code=REGION_CODE,
            county_code=COUNTY_CODE,
            person=expected_person)
        expected_sentence_group_2 = StateSentenceGroup.new_with_defaults(
            sentence_group_id=ID_2, status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            external_id=SENTENCE_GROUP_ID_2, state_code=REGION_CODE,
            county_code=COUNTY_CODE,
            person=expected_person)
        # No county code because errors during match
        expected_sentence_group_3 = StateSentenceGroup.new_with_defaults(
            sentence_group_id=ID_3, status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            external_id=SENTENCE_GROUP_ID_3, state_code=REGION_CODE,
            person=expected_person)
        expected_person.external_ids = [expected_external_id]
        expected_person.sentence_groups = [expected_sentence_group,
                                           expected_sentence_group_2,
                                           expected_sentence_group_3]

        expected_person_2 = StatePerson.new_with_defaults(
            person_id=ID_2, full_name=FULL_NAME_1)
        expected_external_id_2 = StatePersonExternalId.new_with_defaults(
            person_external_id_id=ID_2, state_code=REGION_CODE,
            external_id=EXTERNAL_ID_2,
            id_type=ID_TYPE, person=expected_person_2)
        # No county code because unmatched
        expected_sentence_group_3_dup = StateSentenceGroup.new_with_defaults(
            sentence_group_id=ID_4, status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            external_id=SENTENCE_GROUP_ID_3, state_code=REGION_CODE,
            person=expected_person_2)
        expected_person_2.sentence_groups = [expected_sentence_group_3_dup]
        expected_person_2.external_ids = [expected_external_id_2]

        session = SessionFactory.for_schema_base(StateBase)
        session.add(db_person)
        session.add(db_person_2)
        session.commit()

        # Act
        persistence.write(ingest_info, DEFAULT_METADATA)
        session = SessionFactory.for_schema_base(StateBase)
        persons = dao.read_people(session)

        # Assert
        self.assertEqual([expected_person, expected_person_2],
                         converter.convert_schema_objects_to_entity(persons))
