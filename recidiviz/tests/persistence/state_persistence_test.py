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
from typing import Dict, List, Optional, Type
from unittest import TestCase

import pytest
from mock import Mock, patch

from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.ingest_metadata import IngestMetadata, SystemLevel
from recidiviz.persistence import persistence
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import dao, schema
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StatePersonExternalId,
    StateSentenceGroup,
)
from recidiviz.persistence.entity_matching.entity_matching_types import (
    EntityTree,
    IndividualMatchResult,
)
from recidiviz.persistence.entity_matching.state.state_entity_matcher import (
    StateEntityMatcher,
)
from recidiviz.persistence.entity_matching.state.state_matching_delegate_factory import (
    StateMatchingDelegateFactory,
)
from recidiviz.persistence.errors import EntityMatchingError
from recidiviz.persistence.ingest_info_converter.base_converter import (
    EntityDeserializationResult,
)
from recidiviz.persistence.persistence import (
    DATABASE_INVARIANT_THRESHOLD,
    ENTITY_MATCHING_THRESHOLD,
    ENUM_THRESHOLD,
    OVERALL_THRESHOLD,
)
from recidiviz.tools.postgres import local_postgres_helpers

EXTERNAL_ID = "EXTERNAL_ID"
EXTERNAL_ID_2 = "EXTERNAL_ID_2"
FULL_NAME_1 = "TEST_FULL_NAME_1"
STATE_CODE = "US_ND"
COUNTY_CODE = "COUNTY"
DEFAULT_METADATA = IngestMetadata(
    region="us_nd",
    system_level=SystemLevel.STATE,
    ingest_time=datetime(year=1000, month=1, day=1),
    database_key=SQLAlchemyDatabaseKey.canonical_for_schema(
        schema_type=SchemaType.STATE
    ),
)
ID_TYPE = "ID_TYPE"
ID = 1
ID_2 = 2
ID_3 = 3
ID_4 = 4
SENTENCE_GROUP_ID = "SG1"
SENTENCE_GROUP_ID_2 = "SG2"
SENTENCE_GROUP_ID_3 = "SG3"
SENTENCE_GROUP_ID_4 = "SG4"

STATE_ERROR_THRESHOLDS_WITH_FORTY_PERCENT_RATIOS = {
    SystemLevel.STATE: {
        OVERALL_THRESHOLD: 0.4,
        ENUM_THRESHOLD: 0.4,
        ENTITY_MATCHING_THRESHOLD: 0.4,
        DATABASE_INVARIANT_THRESHOLD: 0,
    }
}

FAKE_PROJECT_ID = "fake-project"

STATE_CODE_TO_ENTITY_MATCHING_THRESHOLD_OVERRIDE_FAKE_PROJECT: Dict[
    str, Dict[str, float]
] = {
    FAKE_PROJECT_ID: {
        "US_XY": 0.4,
    }
}

STATE_CODE_TO_ENTITY_MATCHING_THRESHOLD_FORTY_PERCENT = {
    FAKE_PROJECT_ID: {
        "US_ND": 0.4,
    }
}


class _PatchedStateEntityMatcher(StateEntityMatcher):
    """Subclass of StateEntityMatcher which will throw entity matching errors for certain objects."""

    def __init__(
        self, region_code: str, erroring_class: Type, erroring_external_ids: List[str]
    ):
        state_matching_delegate = StateMatchingDelegateFactory.build(
            region_code=region_code,
            ingest_metadata=DEFAULT_METADATA,
        )
        super().__init__(state_matching_delegate)
        self.erroring_external_ids = erroring_external_ids
        self.erroring_class = erroring_class

    def _match_entity_tree(
        self,
        *,
        ingested_entity_tree: EntityTree,
        db_entity_trees: List[EntityTree],
        matched_entities_by_db_ids: Dict[int, List[DatabaseEntity]],
        root_entity_cls: Type
    ) -> IndividualMatchResult:
        if (
            isinstance(ingested_entity_tree.entity, self.erroring_class)
            and ingested_entity_tree.entity.get_external_id()
            in self.erroring_external_ids
        ):
            raise EntityMatchingError(
                "error!", ingested_entity_tree.entity.get_entity_name()
            )

        return super()._match_entity_tree(
            ingested_entity_tree=ingested_entity_tree,
            db_entity_trees=db_entity_trees,
            matched_entities_by_db_ids=matched_entities_by_db_ids,
            root_entity_cls=root_entity_cls,
        )


@pytest.mark.uses_db
@patch("recidiviz.utils.environment.in_gcp", Mock(return_value=True))
@patch("recidiviz.utils.metadata.project_id", Mock(return_value=FAKE_PROJECT_ID))
@patch.dict("os.environ", {"PERSIST_LOCALLY": "false"})
class TestStatePersistence(TestCase):
    """Test that the persistence layer correctly writes to the SQL database for
    the state schema specifically."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.canonical_for_schema(SchemaType.STATE)
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)

        # State persistence ends up having to instantiate the us_nd_controller to
        # get enum overrides, and the controller goes on to create bigquery,
        # storage, and tasks clients.
        self.bq_client_patcher = patch("google.cloud.bigquery.Client")
        self.storage_client_patcher = patch("google.cloud.storage.Client")
        self.task_client_patcher = patch("google.cloud.tasks_v2.CloudTasksClient")
        self.bq_client_patcher.start()
        self.storage_client_patcher.start()
        self.task_client_patcher.start()

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

        self.bq_client_patcher.stop()
        self.storage_client_patcher.stop()
        self.task_client_patcher.stop()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def to_entity(self, schema_obj):
        return converter.convert_schema_object_to_entity(
            schema_obj, populate_back_edges=True
        )

    @patch(
        "recidiviz.persistence.persistence.STATE_CODE_TO_ENTITY_MATCHING_THRESHOLD_OVERRIDE",
        STATE_CODE_TO_ENTITY_MATCHING_THRESHOLD_OVERRIDE_FAKE_PROJECT,
    )
    @patch(
        "recidiviz.persistence.persistence.SYSTEM_TYPE_TO_ERROR_THRESHOLD",
        STATE_ERROR_THRESHOLDS_WITH_FORTY_PERCENT_RATIOS,
    )
    @patch("recidiviz.persistence.entity_matching.entity_matching._get_matcher")
    def test_state_threeSentenceGroups_dontPersistAboveThreshold(
        self, mock_get_matcher
    ):
        # Arrange
        mock_get_matcher.return_value = _PatchedStateEntityMatcher(
            region_code=STATE_CODE,
            erroring_class=schema.StateSentenceGroup,
            erroring_external_ids=[SENTENCE_GROUP_ID, SENTENCE_GROUP_ID_4],
        )

        # Arrange
        person_1 = entities.StatePerson.new_with_defaults(
            state_code=STATE_CODE,
            sentence_groups=[
                entities.StateSentenceGroup.new_with_defaults(
                    state_code=STATE_CODE,
                    external_id=SENTENCE_GROUP_ID,
                    county_code=COUNTY_CODE,
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                ),
                entities.StateSentenceGroup.new_with_defaults(
                    state_code=STATE_CODE,
                    external_id=SENTENCE_GROUP_ID_2,
                    county_code=COUNTY_CODE,
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                ),
            ],
        )
        person_2 = entities.StatePerson.new_with_defaults(
            state_code=STATE_CODE,
            sentence_groups=[
                entities.StateSentenceGroup.new_with_defaults(
                    state_code=STATE_CODE,
                    external_id=SENTENCE_GROUP_ID_3,
                    county_code=COUNTY_CODE,
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                ),
                entities.StateSentenceGroup(
                    state_code=STATE_CODE,
                    external_id=SENTENCE_GROUP_ID_4,
                    county_code=COUNTY_CODE,
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                ),
            ],
        )

        db_person = schema.StatePerson(
            person_id=ID, full_name=FULL_NAME_1, state_code=STATE_CODE
        )
        db_sentence_group = schema.StateSentenceGroup(
            sentence_group_id=ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=SENTENCE_GROUP_ID,
            state_code=STATE_CODE,
        )
        db_sentence_group_2 = schema.StateSentenceGroup(
            sentence_group_id=ID_2,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=SENTENCE_GROUP_ID_2,
            state_code=STATE_CODE,
        )
        db_external_id = schema.StatePersonExternalId(
            person_external_id_id=ID,
            state_code=STATE_CODE,
            external_id=EXTERNAL_ID,
            id_type=ID_TYPE,
        )
        db_person.sentence_groups = [db_sentence_group, db_sentence_group_2]
        db_person.external_ids = [db_external_id]

        db_person_2 = schema.StatePerson(
            person_id=ID_2, full_name=FULL_NAME_1, state_code=STATE_CODE
        )
        db_sentence_group_3 = schema.StateSentenceGroup(
            sentence_group_id=ID_3,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=SENTENCE_GROUP_ID_3,
            state_code=STATE_CODE,
        )
        db_external_id_2 = schema.StatePersonExternalId(
            person_external_id_id=ID_2,
            state_code=STATE_CODE,
            external_id=EXTERNAL_ID_2,
            id_type=ID_TYPE,
        )
        db_person_2.external_ids = [db_external_id_2]
        db_person_2.sentence_groups = [db_sentence_group_3]

        # No updates
        expected_person = self.to_entity(db_person)
        expected_person_2 = self.to_entity(db_person_2)

        with SessionFactory.using_database(self.database_key) as session:
            session.add(db_person)
            session.add(db_person_2)

        parsed_entities = [person_1, person_2]
        # Act
        persistence.write_entities(
            conversion_result=EntityDeserializationResult(
                people=parsed_entities,
                enum_parsing_errors=0,
                general_parsing_errors=0,
                protected_class_errors=0,
            ),
            ingest_metadata=DEFAULT_METADATA,
            total_people=len(parsed_entities),
        )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            # Assert
            persons = dao.read_people(session)

            self.assertEqual(
                [expected_person, expected_person_2],
                converter.convert_schema_objects_to_entity(persons),
            )

    @patch(
        "recidiviz.persistence.persistence.SYSTEM_TYPE_TO_ERROR_THRESHOLD",
        STATE_ERROR_THRESHOLDS_WITH_FORTY_PERCENT_RATIOS,
    )
    @patch(
        "recidiviz.persistence.persistence.STATE_CODE_TO_ENTITY_MATCHING_THRESHOLD_OVERRIDE",
        STATE_CODE_TO_ENTITY_MATCHING_THRESHOLD_FORTY_PERCENT,
    )
    @patch("recidiviz.persistence.entity_matching.entity_matching._get_matcher")
    def test_state_threeSentenceGroups_persistsTwoBelowThreshold(
        self, mock_get_matcher
    ):
        """Ensure that the number of errors is below the ND specific threshold"""
        mock_get_matcher.return_value = _PatchedStateEntityMatcher(
            region_code=STATE_CODE,
            erroring_class=schema.StateSentenceGroup,
            erroring_external_ids=[SENTENCE_GROUP_ID],
        )

        # Set the ENTITY_MATCHING_THRESHOLD to 0, such that we can verify that the forty percent threshold for
        # ENTITY_MATCHING_THRESHOLD is dictated by the state-specific override in
        # STATE_CODE_TO_ENTITY_MATCHING_THRESHOLD_FORTY_PERCENT.
        STATE_ERROR_THRESHOLDS_WITH_FORTY_PERCENT_RATIOS[ENTITY_MATCHING_THRESHOLD] = 0

        # Arrange
        person = entities.StatePerson.new_with_defaults(
            state_code=STATE_CODE,
            sentence_groups=[
                entities.StateSentenceGroup(
                    state_code=STATE_CODE,
                    external_id=SENTENCE_GROUP_ID,
                    county_code=COUNTY_CODE,
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                ),
                entities.StateSentenceGroup(
                    state_code=STATE_CODE,
                    external_id=SENTENCE_GROUP_ID_2,
                    county_code=COUNTY_CODE,
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                ),
                entities.StateSentenceGroup(
                    state_code=STATE_CODE,
                    external_id=SENTENCE_GROUP_ID_3,
                    county_code=COUNTY_CODE,
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                ),
            ],
        )

        db_person = schema.StatePerson(
            person_id=ID, full_name=FULL_NAME_1, state_code=STATE_CODE
        )
        db_sentence_group = schema.StateSentenceGroup(
            sentence_group_id=ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=SENTENCE_GROUP_ID,
            state_code=STATE_CODE,
        )
        db_sentence_group_2 = schema.StateSentenceGroup(
            sentence_group_id=ID_2,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=SENTENCE_GROUP_ID_2,
            state_code=STATE_CODE,
        )
        db_external_id = schema.StatePersonExternalId(
            person_external_id_id=ID,
            state_code=STATE_CODE,
            external_id=EXTERNAL_ID,
            id_type=ID_TYPE,
        )
        db_person.sentence_groups = [db_sentence_group, db_sentence_group_2]
        db_person.external_ids = [db_external_id]

        db_person_2 = schema.StatePerson(
            person_id=ID_2, full_name=FULL_NAME_1, state_code=STATE_CODE
        )
        db_sentence_group_3 = schema.StateSentenceGroup(
            sentence_group_id=ID_3,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=SENTENCE_GROUP_ID_3,
            state_code=STATE_CODE,
        )
        db_external_id_2 = schema.StatePersonExternalId(
            person_external_id_id=ID_2,
            state_code=STATE_CODE,
            external_id=EXTERNAL_ID_2,
            id_type=ID_TYPE,
        )
        db_person_2.external_ids = [db_external_id_2]
        db_person_2.sentence_groups = [db_sentence_group_3]

        with SessionFactory.using_database(self.database_key) as session:
            session.add(db_person)
            session.add(db_person_2)

        expected_person = StatePerson.new_with_defaults(
            person_id=ID,
            full_name=FULL_NAME_1,
            external_ids=[],
            sentence_groups=[],
            state_code=STATE_CODE,
        )
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=ID,
            state_code=STATE_CODE,
            external_id=EXTERNAL_ID,
            id_type=ID_TYPE,
            person=expected_person,
        )
        # No county code because errors during match
        expected_sentence_group = StateSentenceGroup.new_with_defaults(
            sentence_group_id=ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            external_id=SENTENCE_GROUP_ID,
            state_code=STATE_CODE,
            person=expected_person,
        )
        expected_sentence_group_2 = StateSentenceGroup.new_with_defaults(
            sentence_group_id=ID_2,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            external_id=SENTENCE_GROUP_ID_2,
            state_code=STATE_CODE,
            county_code=COUNTY_CODE,
            person=expected_person,
        )
        expected_person.external_ids = [expected_external_id]
        expected_person.sentence_groups = [
            expected_sentence_group,
            expected_sentence_group_2,
        ]

        expected_person_2 = StatePerson.new_with_defaults(
            person_id=ID_2, full_name=FULL_NAME_1, state_code=STATE_CODE
        )
        expected_external_id_2 = StatePersonExternalId.new_with_defaults(
            person_external_id_id=ID_2,
            state_code=STATE_CODE,
            external_id=EXTERNAL_ID_2,
            id_type=ID_TYPE,
            person=expected_person_2,
        )
        expected_sentence_group_3 = StateSentenceGroup.new_with_defaults(
            sentence_group_id=ID_3,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            external_id=SENTENCE_GROUP_ID_3,
            state_code=STATE_CODE,
            county_code=COUNTY_CODE,
            person=expected_person_2,
        )
        expected_person_2.sentence_groups = [expected_sentence_group_3]
        expected_person_2.external_ids = [expected_external_id_2]

        parsed_entities = [person]
        # Act
        persistence.write_entities(
            conversion_result=EntityDeserializationResult(
                people=parsed_entities,
                enum_parsing_errors=0,
                general_parsing_errors=0,
                protected_class_errors=0,
            ),
            ingest_metadata=DEFAULT_METADATA,
            total_people=len(parsed_entities),
        )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            # Assert
            persons = dao.read_people(session)

            self.assertEqual(
                [expected_person, expected_person_2],
                converter.convert_schema_objects_to_entity(persons),
            )
