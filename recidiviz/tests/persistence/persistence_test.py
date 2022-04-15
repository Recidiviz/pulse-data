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
"""Tests for persistence.py."""

import abc
import logging
import threading
import time
from copy import deepcopy
from datetime import date, datetime
from typing import Callable, Dict, List, Optional
from unittest import TestCase

import pytest
import sqlalchemy
from mock import Mock, patch
from opencensus.stats.measurement_map import MeasurementMap

from recidiviz.common.constants.state.state_assessment import StateAssessmentClass
from recidiviz.common.constants.state.state_person import StateGender, StateRace
from recidiviz.common.constants.state.state_person_alias import StatePersonAliasType
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.states import StateCode
from recidiviz.common.ingest_metadata import IngestMetadata, SystemLevel
from recidiviz.persistence import persistence
from recidiviz.persistence.database.schema.state import dao as state_dao
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StateIncarcerationSentence,
    StatePerson,
    StatePersonAlias,
    StatePersonExternalId,
    StatePersonRace,
    StateProgramAssignment,
)
from recidiviz.persistence.entity_matching.state.base_state_matching_delegate import (
    BaseStateMatchingDelegate,
)
from recidiviz.persistence.ingest_info_converter.base_converter import (
    EntityDeserializationResult,
)
from recidiviz.tools.postgres import local_postgres_helpers

DATE = date(year=2019, day=1, month=2)
DATETIME = datetime(DATE.year, DATE.month, DATE.day)
DATE_2 = date(year=2020, day=1, month=2)
DATETIME_2 = datetime(DATE_2.year, DATE_2.month, DATE_2.day)

STATE_CODE = StateCode.US_XX.value
STATE_CODE_2 = StateCode.US_WW.value
FAKE_PROJECT_ID = "test-project"
FAKE_ID_TYPE = "US_XX_ID_TYPE"
FAKE_ID_TYPE_2 = "US_WW_ID_TYPE_2"
STATE_CODE_TO_ENTITY_MATCHING_THRESHOLD_OVERRIDE_FAKE_PROJECT: Dict[
    str, Dict[str, float]
] = {
    FAKE_PROJECT_ID: {
        "US_XY": 0.4,
    }
}

PERSON_1_FULL_NAME = '{"given_names": "JON", "surname": "HOPKINS"}'

PERSON_STATE_1_ENTITY = StatePerson(
    state_code=STATE_CODE,
    full_name=PERSON_1_FULL_NAME,
    birthdate=datetime(1979, 8, 15),
    gender=StateGender.MALE,
    external_ids=[
        StatePersonExternalId(
            state_code=STATE_CODE, external_id="39768", id_type=FAKE_ID_TYPE
        )
    ],
    races=[
        StatePersonRace(
            state_code=STATE_CODE, race=StateRace.WHITE, race_raw_text="CAUCASIAN"
        )
    ],
    aliases=[
        StatePersonAlias(
            state_code=STATE_CODE,
            full_name=PERSON_1_FULL_NAME,
            alias_type=StatePersonAliasType.GIVEN_NAME,
            alias_type_raw_text="GIVEN_NAME",
        )
    ],
    incarceration_sentences=[
        StateIncarcerationSentence(
            state_code=STATE_CODE,
            external_id="123",
            status=StateSentenceStatus.SERVING,
            status_raw_text="SERVING",
        )
    ],
)

PERSON_2_FULL_NAME = '{"given_names": "SOLANGE", "surname": "KNOWLES"}'
PERSON_STATE_2_ENTITY = StatePerson(
    state_code=STATE_CODE_2,
    full_name=PERSON_2_FULL_NAME,
    birthdate=datetime(1986, 6, 24),
    gender=StateGender.FEMALE,
    external_ids=[
        StatePersonExternalId(
            state_code=STATE_CODE_2, external_id="52163", id_type=FAKE_ID_TYPE_2
        )
    ],
    races=[
        StatePersonRace(
            state_code=STATE_CODE_2, race=StateRace.BLACK, race_raw_text="BLACK"
        )
    ],
    aliases=[
        StatePersonAlias(
            state_code=STATE_CODE_2,
            full_name=PERSON_2_FULL_NAME,
            alias_type=StatePersonAliasType.GIVEN_NAME,
            alias_type_raw_text="GIVEN_NAME",
        )
    ],
    incarceration_sentences=[
        StateIncarcerationSentence(
            state_code=STATE_CODE_2,
            external_id="123",
            status=StateSentenceStatus.SERVING,
            status_raw_text="SERVING",
        )
    ],
)
INGEST_METADATA_STATE_1_INSERT = IngestMetadata(
    region=STATE_CODE,
    ingest_time=DATETIME,
    system_level=SystemLevel.STATE,
    database_key=SQLAlchemyDatabaseKey.canonical_for_schema(
        schema_type=SchemaType.STATE
    ),
)
INGEST_METADATA_STATE_1_UPDATE = IngestMetadata(
    region=STATE_CODE,
    ingest_time=DATETIME_2,
    system_level=SystemLevel.STATE,
    database_key=SQLAlchemyDatabaseKey.canonical_for_schema(
        schema_type=SchemaType.STATE
    ),
)
INGEST_METADATA_STATE_2_INSERT = IngestMetadata(
    region=STATE_CODE_2,
    ingest_time=DATETIME,
    system_level=SystemLevel.STATE,
    database_key=SQLAlchemyDatabaseKey.canonical_for_schema(
        schema_type=SchemaType.STATE
    ),
)
INGEST_METADATA_STATE_2_UPDATE = IngestMetadata(
    region=STATE_CODE_2,
    ingest_time=DATETIME_2,
    system_level=SystemLevel.STATE,
    database_key=SQLAlchemyDatabaseKey.canonical_for_schema(
        schema_type=SchemaType.STATE
    ),
)


def mock_build_matching_delegate(
    *, region_code: str, ingest_metadata: IngestMetadata
) -> BaseStateMatchingDelegate:
    return BaseStateMatchingDelegate(region_code, ingest_metadata)


class MultipleStateTestMixin:
    """Defines the test cases for running multiple state transactions simultaneously.

    To be used by a concrete test class that defines *how* to run them simultaneously.
    """

    @abc.abstractmethod
    def run_transactions(
        self,
        state_1_entities: List[state_entities.StatePerson],
        state_2_entities: List[state_entities.StatePerson],
    ):
        """Writes the given ingest infos in separate transactions"""

    def write_entities(
        self, entities: List[StatePerson], metadata: IngestMetadata
    ) -> None:
        persistence.write_entities(
            conversion_result=EntityDeserializationResult(
                people=entities,
                enum_parsing_errors=0,
                general_parsing_errors=0,
                protected_class_errors=0,
            ),
            ingest_metadata=metadata,
            total_people=len(entities),
        )

    def test_insertRootEntities_succeeds(self):
        # Arrange
        # Write initial placeholder person to database
        placeholder_person = state_schema.StatePerson(
            person_id=0, state_code=STATE_CODE
        )
        with SessionFactory.using_database(self.database_key) as session:
            session.add(placeholder_person)

        # Act
        self.run_transactions(
            [deepcopy(PERSON_STATE_1_ENTITY)], [deepcopy(PERSON_STATE_2_ENTITY)]
        )

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            result = state_dao.read_people(session)

        assert len(result) == 3
        names = {person.full_name for person in result}
        assert names == {
            None,
            '{"given_names": "JON", "surname": "HOPKINS"}',
            '{"given_names": "SOLANGE", "surname": "KNOWLES"}',
        }

    def test_insertOverlappingTypes_succeeds(self):
        # Arrange
        # Write initial placeholder person to database
        with SessionFactory.using_database(self.database_key) as session:
            placeholder_person = state_schema.StatePerson(
                person_id=0, state_code=STATE_CODE
            )
            session.add(placeholder_person)

        # Write persons to be updated
        self.write_entities(
            [deepcopy(PERSON_STATE_1_ENTITY)], metadata=INGEST_METADATA_STATE_1_INSERT
        )
        self.write_entities(
            [deepcopy(PERSON_STATE_2_ENTITY)], metadata=INGEST_METADATA_STATE_2_INSERT
        )

        # Act
        # Add risk assessment to both persons
        person_state_1 = deepcopy(PERSON_STATE_1_ENTITY)
        person_state_1.assessments.append(
            StateAssessment.new_with_defaults(
                state_code=STATE_CODE,
                assessment_class=StateAssessmentClass.RISK,
                assessment_class_raw_text="RISK",
            )
        )

        person_state_2 = deepcopy(PERSON_STATE_2_ENTITY)
        person_state_2.assessments.append(
            StateAssessment.new_with_defaults(
                state_code=STATE_CODE_2,
                assessment_class=StateAssessmentClass.RISK,
                assessment_class_raw_text="RISK",
            )
        )

        self.run_transactions([person_state_1], [person_state_2])

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            result = state_dao.read_people(session)

        result = sorted(result, key=lambda p: p.person_id)
        assert len(result) == 3
        assert result[0].full_name is None
        assert result[1].full_name == '{"given_names": "JON", "surname": "HOPKINS"}'
        assert len(result[1].assessments) == 1
        assert result[2].full_name == '{"given_names": "SOLANGE", "surname": "KNOWLES"}'
        assert len(result[2].assessments) == 1

    def test_insertNonOverlappingTypes_succeeds(self):
        # Arrange
        # Write initial placeholder person to database
        with SessionFactory.using_database(self.database_key) as session:
            placeholder_person = state_schema.StatePerson(state_code=STATE_CODE)
            session.add(placeholder_person)

        # Write persons to be updated
        self.write_entities(
            [deepcopy(PERSON_STATE_1_ENTITY)], metadata=INGEST_METADATA_STATE_1_INSERT
        )
        self.write_entities(
            [deepcopy(PERSON_STATE_2_ENTITY)], metadata=INGEST_METADATA_STATE_2_INSERT
        )

        # Act
        # Add risk assessment to person 1 and program assignment to person 2
        person_state_1 = deepcopy(PERSON_STATE_1_ENTITY)
        person_state_1.assessments.append(
            StateAssessment.new_with_defaults(
                state_code=STATE_CODE,
                assessment_class=StateAssessmentClass.RISK,
                assessment_class_raw_text="RISK",
            )
        )

        person_state_2 = deepcopy(PERSON_STATE_2_ENTITY)
        person_state_2.program_assignments.append(
            StateProgramAssignment.new_with_defaults(
                state_code=STATE_CODE_2,
                external_id="1234",
                participation_status=StateProgramAssignmentParticipationStatus.PRESENT_WITHOUT_INFO,
            )
        )

        self.run_transactions([person_state_1], [person_state_2])

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            result = state_dao.read_people(session)

        result = sorted(result, key=lambda p: p.person_id)
        assert len(result) == 3
        assert result[0].full_name is None
        assert result[1].full_name == '{"given_names": "JON", "surname": "HOPKINS"}'
        assert len(result[1].assessments) == 1
        assert result[2].full_name == '{"given_names": "SOLANGE", "surname": "KNOWLES"}'
        assert len(result[2].program_assignments) == 1

    def test_updateOverlappingTypes_succeeds(self):
        # Arrange
        # Write initial placeholder person to database
        with SessionFactory.using_database(self.database_key) as session:
            placeholder_person = state_schema.StatePerson(
                person_id=0, state_code=STATE_CODE
            )
            session.add(placeholder_person)

        # Write persons to be updated
        self.write_entities(
            [deepcopy(PERSON_STATE_1_ENTITY)], INGEST_METADATA_STATE_1_INSERT
        )
        self.write_entities(
            [deepcopy(PERSON_STATE_2_ENTITY)], INGEST_METADATA_STATE_2_INSERT
        )

        # Act
        # Update existing sentence on both persons
        person_state_1 = deepcopy(PERSON_STATE_1_ENTITY)
        person_state_1.incarceration_sentences[0].status = StateSentenceStatus.COMPLETED

        person_state_2 = deepcopy(PERSON_STATE_2_ENTITY)
        person_state_2.incarceration_sentences[0].status = StateSentenceStatus.COMPLETED

        self.run_transactions([person_state_1], [person_state_2])

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            result = state_dao.read_people(session)

        result = sorted(result, key=lambda p: p.person_id)
        assert len(result) == 3
        assert result[0].full_name is None
        assert result[1].full_name == '{"given_names": "JON", "surname": "HOPKINS"}'
        assert len(result[1].incarceration_sentences) == 1
        assert result[1].incarceration_sentences[0].status == "COMPLETED"
        assert result[2].full_name == '{"given_names": "SOLANGE", "surname": "KNOWLES"}'
        assert len(result[2].incarceration_sentences) == 1

        assert result[2].incarceration_sentences[0].status == "COMPLETED"

    def test_updateNonOverlappingTypes_succeeds(self):
        # Arrange
        # Write initial placeholder person to database
        with SessionFactory.using_database(self.database_key) as session:
            placeholder_person = state_schema.StatePerson(
                person_id=0, state_code=STATE_CODE
            )
            session.add(placeholder_person)

        # Write persons to be updated
        self.write_entities(
            [deepcopy(PERSON_STATE_1_ENTITY)], INGEST_METADATA_STATE_1_INSERT
        )
        self.write_entities(
            [deepcopy(PERSON_STATE_2_ENTITY)], INGEST_METADATA_STATE_2_INSERT
        )

        # Act
        # Update race on person 1 and sentence on person 2
        person_state_1 = deepcopy(PERSON_STATE_1_ENTITY)
        person_state_1.races[0].race = StateRace.WHITE

        person_state_2 = deepcopy(PERSON_STATE_2_ENTITY)
        person_state_2.incarceration_sentences[0].status = StateSentenceStatus.COMPLETED

        self.run_transactions([person_state_1], [person_state_2])

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            result = state_dao.read_people(session)

        result = sorted(result, key=lambda p: p.person_id)
        assert len(result) == 3
        assert result[0].full_name is None
        assert result[1].full_name == '{"given_names": "JON", "surname": "HOPKINS"}'
        assert len(result[1].races) == 1
        assert result[1].races[0].race == "WHITE"
        assert result[2].full_name == '{"given_names": "SOLANGE", "surname": "KNOWLES"}'
        assert len(result[2].incarceration_sentences) == 1
        assert result[2].incarceration_sentences[0].status == "COMPLETED"


@pytest.mark.uses_db
@patch("recidiviz.utils.metadata.project_id", Mock(return_value="test-project"))
@patch("os.getenv", Mock(return_value="production"))
@patch(
    "recidiviz.persistence.persistence.STATE_CODE_TO_ENTITY_MATCHING_THRESHOLD_OVERRIDE",
    STATE_CODE_TO_ENTITY_MATCHING_THRESHOLD_OVERRIDE_FAKE_PROJECT,
)
class TestPersistenceMultipleThreadsOverlapping(TestCase, MultipleStateTestMixin):
    """Test that the persistence layer writes to Postgres from multiple threads in an overlapping fashion

    This forces the transactions to commit in the opposite order that they were started to guarantee that they overlap.
    """

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.bq_client_patcher = patch("google.cloud.bigquery.Client")
        self.storage_client_patcher = patch("google.cloud.storage.Client")
        self.task_client_patcher = patch("google.cloud.tasks_v2.CloudTasksClient")
        self.bq_client_patcher.start()
        self.storage_client_patcher.start()
        self.task_client_patcher.start()

        self.isolation_level_patcher = patch.object(
            SQLAlchemyDatabaseKey,
            "isolation_level",
            # Note: This test fails if switched to 'SERIALIZABLE'. We aren't sure why
            # Postgres thinks the two transactions have conflicting locks.
            "REPEATABLE READ",
        )
        self.isolation_level_patcher.start()
        self.database_key = SQLAlchemyDatabaseKey.canonical_for_schema(SchemaType.STATE)
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)

        self.matching_delegate_patcher = patch(
            "recidiviz.persistence.entity_matching.state."
            "state_matching_delegate_factory.StateMatchingDelegateFactory.build",
            mock_build_matching_delegate,
        )
        self.mock_matching_delegate = self.matching_delegate_patcher.start()

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)
        self.isolation_level_patcher.stop()

        self.bq_client_patcher.stop()
        self.storage_client_patcher.stop()
        self.task_client_patcher.stop()
        self.matching_delegate_patcher.stop()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def run_transactions(
        self,
        state_1_entities: List[state_entities.StatePerson],
        state_2_entities: List[state_entities.StatePerson],
    ):
        return _run_transactions_overlapping(state_1_entities, state_2_entities)


def _run_transactions_overlapping(
    state_1_entities: List[state_entities.StatePerson],
    state_2_entities: List[state_entities.StatePerson],
):
    """This coordinates two transactions such that they overlap

    - Runs T1
    - Runs T2
    - Commits T2
    - Commits T1
    """

    def transaction1(
        precommit_event: threading.Event, other_committed_event: threading.Event
    ):
        persistence.write_entities(
            conversion_result=EntityDeserializationResult(
                people=state_1_entities,
                enum_parsing_errors=0,
                general_parsing_errors=0,
                protected_class_errors=0,
            ),
            ingest_metadata=INGEST_METADATA_STATE_1_UPDATE,
            total_people=len(state_1_entities),
            run_txn_fn=_get_run_transaction_block_commit_fn(
                precommit_event, other_committed_event
            ),
        )

    def transaction2(other_precommit_event, committed_event):
        persistence.write_entities(
            conversion_result=EntityDeserializationResult(
                people=state_2_entities,
                enum_parsing_errors=0,
                general_parsing_errors=0,
                protected_class_errors=0,
            ),
            ingest_metadata=INGEST_METADATA_STATE_2_UPDATE,
            total_people=len(state_2_entities),
            run_txn_fn=_get_run_transaction_after_other_fn(
                other_precommit_event, committed_event
            ),
        )

    transaction1_precommit = threading.Event()
    transaction2_committed = threading.Event()

    thread1 = threading.Thread(
        target=transaction1, args=(transaction1_precommit, transaction2_committed)
    )
    thread2 = threading.Thread(
        target=transaction2, args=(transaction1_precommit, transaction2_committed)
    )

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()


def _get_run_transaction_block_commit_fn(
    precommit_event: threading.Event, other_committed_event: threading.Event
):
    def run_transaction_block_commit(
        session: Session,
        _m: MeasurementMap,
        txn_body: Callable[[Session], bool],
        _r: Optional[int],
    ) -> bool:
        try:
            logging.info("Starting Transaction 1")
            # Run our transaction but don't commit.
            _should_continue = txn_body(session)
        finally:
            # Notify the other transaction to run
            logging.info("Notifying Transaction 2")
            precommit_event.set()

        try:
            # Wait for it to finish completely, then commit.
            other_committed_event.wait()
            logging.info("Committing Transaction 1")
            session.commit()
            logging.info("Successfully Committed Transaction 1")
        finally:
            session.close()

        return True

    return run_transaction_block_commit


def _get_run_transaction_after_other_fn(
    other_precommit_event: threading.Event, committed_event: threading.Event
):
    def run_transaction_after_other(
        session: Session,
        _m: MeasurementMap,
        txn_body: Callable[[Session], bool],
        _r: Optional[int],
    ) -> bool:
        try:
            # Wait for the other transaction to have run but not committed before we start.
            other_precommit_event.wait()

            # Run all the way through.
            logging.info("Starting Transaction 2")
            _should_continue = txn_body(session)
            logging.info("Committing Transaction 2")
            session.commit()
            logging.info("Successfully Committed Transaction 2")
        finally:
            # Notify the other transaction to continue.
            logging.info("Notifying Transaction 1")
            committed_event.set()

            session.close()

        return True

    return run_transaction_after_other


@pytest.mark.uses_db
@patch("recidiviz.utils.metadata.project_id", Mock(return_value=FAKE_PROJECT_ID))
@patch("os.getenv", Mock(return_value="production"))
@patch(
    "recidiviz.persistence.persistence.STATE_CODE_TO_ENTITY_MATCHING_THRESHOLD_OVERRIDE",
    STATE_CODE_TO_ENTITY_MATCHING_THRESHOLD_OVERRIDE_FAKE_PROJECT,
)
class TestPersistenceMultipleThreadsInterleaved(TestCase, MultipleStateTestMixin):
    """Test that the persistence layer writes to Postgres from multiple threads in an interleaved fashion

    This offsets the transactions and inserts delay between each operation such that they are fully interleaved.
    """

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.bq_client_patcher = patch("google.cloud.bigquery.Client")
        self.storage_client_patcher = patch("google.cloud.storage.Client")
        self.task_client_patcher = patch("google.cloud.tasks_v2.CloudTasksClient")
        self.bq_client_patcher.start()
        self.storage_client_patcher.start()
        self.task_client_patcher.start()

        self.isolation_level_patcher = patch.object(
            SQLAlchemyDatabaseKey,
            "isolation_level",
            # Note: This test fails if switched to 'SERIALIZABLE'. We aren't sure why
            # Postgres thinks the two transactions have conflicting locks.
            "REPEATABLE READ",
        )
        self.isolation_level_patcher.start()
        self.database_key = SQLAlchemyDatabaseKey.canonical_for_schema(SchemaType.STATE)
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)

        self.matching_delegate_patcher = patch(
            "recidiviz.persistence.entity_matching.state."
            "state_matching_delegate_factory.StateMatchingDelegateFactory.build",
            mock_build_matching_delegate,
        )
        self.mock_matching_delegate = self.matching_delegate_patcher.start()

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)
        self.isolation_level_patcher.stop()

        self.bq_client_patcher.stop()
        self.storage_client_patcher.stop()
        self.task_client_patcher.stop()
        self.matching_delegate_patcher.stop()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def run_transactions(
        self,
        state_1_entities: List[state_entities.StatePerson],
        state_2_entities: List[state_entities.StatePerson],
    ):
        return _run_transactions_interleaved(state_1_entities, state_2_entities)


DELAY = 0.1


def _run_transactions_interleaved(
    state_1_entities: List[state_entities.StatePerson],
    state_2_entities: List[state_entities.StatePerson],
):
    """Offset transactions and delay writes slightly so that transactions are interleaved

    Example execution timeline:

    T1            | T2
    ------------- | --------------
    read person   | ...delay
    ...delay      | read person
    write person  | ...delay
    commit        | write person
                  | commit
    """
    orig_flush = sqlalchemy.orm.session.Session.flush

    def delayed_flush(self) -> None:
        time.sleep(DELAY)
        logging.info("Flushing")
        orig_flush(self)

    with patch.object(sqlalchemy.orm.session.Session, "flush", delayed_flush):

        def transaction1() -> None:
            persistence.write_entities(
                conversion_result=EntityDeserializationResult(
                    people=state_1_entities,
                    enum_parsing_errors=0,
                    general_parsing_errors=0,
                    protected_class_errors=0,
                ),
                ingest_metadata=INGEST_METADATA_STATE_1_UPDATE,
                total_people=len(state_1_entities),
                run_txn_fn=_get_run_transaction_fn(1),
            )

        def transaction2() -> None:
            # Delay start of T2
            time.sleep(DELAY)
            persistence.write_entities(
                conversion_result=EntityDeserializationResult(
                    people=state_2_entities,
                    enum_parsing_errors=0,
                    general_parsing_errors=0,
                    protected_class_errors=0,
                ),
                ingest_metadata=INGEST_METADATA_STATE_2_UPDATE,
                total_people=len(state_1_entities),
                run_txn_fn=_get_run_transaction_fn(2),
            )

        thread1 = threading.Thread(target=transaction1)
        thread2 = threading.Thread(target=transaction2)

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()


def _get_run_transaction_fn(transaction_id: int):
    def run_transaction(
        session: Session,
        _m: MeasurementMap,
        txn_body: Callable[[Session], bool],
        _r: Optional[int],
    ) -> bool:
        try:
            logging.info("Starting Transaction %d", transaction_id)
            _should_continue = txn_body(session)
            logging.info("Committing Transaction %d", transaction_id)
            session.commit()
            logging.info("Successfully Committed Transaction %d", transaction_id)
        finally:
            session.close()

        return True

    return run_transaction
