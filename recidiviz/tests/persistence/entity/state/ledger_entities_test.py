# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""
This module tests aspects of LedgerEntityMixin entities. Like:
    - Are the ledger_datetimes correct?
    - Are the partition_keys correct?
    - Are projected date validations correct?
    - When collections of invalid ledgers are merged, do we catch them?

We accomplish this by providing a base TestCase to be used for each
entity class, and a check to ensure all LedgerEntityMixin entities
have a specific TestCase.
"""
import datetime
import inspect
import sys
import unittest
from typing import List, Type
from unittest.mock import MagicMock, patch

from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
)
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.state_entity_mixins import LedgerEntityMixin
from recidiviz.pipelines.ingest.state.validator import validate_root_entity

ALL_LEDGER_ENTITIES = [
    cls
    for cls in get_all_entity_classes_in_module(entities)
    if issubclass(cls, LedgerEntityMixin)
]


class LedgerEntityTestCaseProtocol:
    """This protocol ensures the associated TestCase checks everything we
    want to check about a LedgerEntityMixin entity. Including:

    - correctly hydrating the ledger datetime
    - correctly hydrating a unique partition key
    - correct error handling post root-entity-merge
    - ensuring ledger_entity_check is called from validate_root_entity
    """

    the_past = datetime.datetime.now() - datetime.timedelta(days=7)
    the_future = datetime.datetime.now() + datetime.timedelta(days=7)
    ledger_time = datetime.datetime(2023, 1, 1)
    state_code = "US_XX"
    # For testing projected date pairings
    before = datetime.date(1999, 1, 1)
    after = datetime.date(2022, 1, 1)

    @classmethod
    def ledger_entity(cls) -> Type[LedgerEntityMixin]:
        raise NotImplementedError

    def new_state_person(self) -> entities.StatePerson:
        return entities.StatePerson(
            state_code=self.state_code,
            person_id=1,
            external_ids=[
                entities.StatePersonExternalId(
                    external_id="1",
                    state_code="US_XX",
                    id_type="US_XX_TEST_PERSON",
                ),
            ],
        )

    def new_state_sentence(self) -> entities.StateSentence:
        return entities.StateSentence(
            state_code=self.state_code,
            external_id="EXTERNAL SENTENCE",
            sentence_type=StateSentenceType.STATE_PRISON,
            imposed_date=datetime.date(2022, 1, 1),
        )

    def test_ledger_datetime_is_not_future(self) -> None:
        """
        This tests that the ledger_datetime_field is set correctly
        and raises a ValueError if the value is in the future.
        """
        raise NotImplementedError

    def test_ledger_partition_key(self) -> None:
        """Tests the ledger_entity has a well-defined partition key."""
        raise NotImplementedError

    @patch("recidiviz.pipelines.ingest.state.validator.ledger_entity_checks")
    def test_ledger_entity_checks_is_called(
        self, mock_ledger_entity_checks: MagicMock
    ) -> None:
        """Tests that we call ledger_entity_checks when the ledger entity is in the tree."""
        raise NotImplementedError


class SequenceNumTest(unittest.TestCase):
    """Tests various configurations of sequence_num for LedgerEntityMixin."""

    state_code = "US_XX"

    def new_state_person(self) -> entities.StatePerson:
        return entities.StatePerson(
            state_code=self.state_code,
            person_id=1,
            external_ids=[
                entities.StatePersonExternalId(
                    external_id="1",
                    state_code="US_XX",
                    id_type="US_XX_TEST_PERSON",
                ),
            ],
        )

    def test_sequence_num_all_none_valid(self) -> None:
        """Checks that partition_key is unique when sequence_num is not hydrated."""
        person = self.new_state_person()
        deadline_1 = entities.StateTaskDeadline(
            person=person,
            state_code=self.state_code,
            task_type=StateTaskType.INTERNAL_UNKNOWN,
            update_datetime=datetime.datetime(2022, 1, 1),
            sequence_num=None,
        )
        deadline_2 = entities.StateTaskDeadline(
            person=person,
            state_code=self.state_code,
            task_type=StateTaskType.INTERNAL_UNKNOWN,
            update_datetime=datetime.datetime(2023, 1, 1),
            sequence_num=None,
        )
        person.task_deadlines = [deadline_1, deadline_2]
        errors = validate_root_entity(person)
        self.assertEqual(errors, [])

    def test_sequence_num_all_none_invalid(self) -> None:
        """Checks that partition_key is unique when sequence_num is not hydrated."""
        person = self.new_state_person()
        deadline_1 = entities.StateTaskDeadline(
            person=person,
            state_code=self.state_code,
            task_type=StateTaskType.INTERNAL_UNKNOWN,
            update_datetime=datetime.datetime(2022, 1, 1),
            sequence_num=None,
        )
        deadline_2 = entities.StateTaskDeadline(
            person=person,
            state_code=self.state_code,
            task_type=StateTaskType.INTERNAL_UNKNOWN,
            update_datetime=datetime.datetime(2022, 1, 1),
            sequence_num=None,
        )
        person.task_deadlines = [deadline_1, deadline_2]
        errors = validate_root_entity(person)
        # One error for unique constraint,
        # another error for ledger entity check
        self.assertEqual(len(errors), 2)
        self.assertIn(
            "If sequence_num is None, then the ledger's partition_key must be unique across hydrated entities.",
            errors[1],
        )

    def test_sequence_num_sometimes_none_invalid(self) -> None:
        """Checks that partition_key is either ALL None or all not-None."""
        person = self.new_state_person()
        deadline_1 = entities.StateTaskDeadline(
            person=person,
            state_code=self.state_code,
            task_type=StateTaskType.INTERNAL_UNKNOWN,
            update_datetime=datetime.datetime(2022, 1, 1),
            sequence_num=None,
        )
        deadline_2 = entities.StateTaskDeadline(
            person=person,
            state_code=self.state_code,
            task_type=StateTaskType.INTERNAL_UNKNOWN,
            update_datetime=datetime.datetime(2023, 1, 1),
            sequence_num=1,
        )
        person.task_deadlines = [deadline_1, deadline_2]
        errors = validate_root_entity(person)
        self.assertIn(
            "sequence_num should be None for ALL hydrated entities or NO hydrated",
            errors[0],
        )

    def test_sequence_num_not_unique_invalid(self) -> None:
        """Checks that we fail when partition_key is not unique (and not-None)."""
        person = self.new_state_person()
        deadline_1 = entities.StateTaskDeadline(
            person=person,
            state_code=self.state_code,
            task_type=StateTaskType.INTERNAL_UNKNOWN,
            update_datetime=datetime.datetime(2022, 1, 1),
            sequence_num=1,
        )
        deadline_2 = entities.StateTaskDeadline(
            person=person,
            state_code=self.state_code,
            task_type=StateTaskType.INTERNAL_UNKNOWN,
            update_datetime=datetime.datetime(2023, 1, 1),
            sequence_num=1,
        )
        person.task_deadlines = [deadline_1, deadline_2]
        errors = validate_root_entity(person)
        self.assertIn("DUPLICATE sequence_num hydration.", errors[0])


class StateTaskDeadlineTest(unittest.TestCase, LedgerEntityTestCaseProtocol):
    """Ensures StateTaskDeadline is tested against the LedgerEntityTestCaseProtocol"""

    @classmethod
    def ledger_entity(cls) -> Type[LedgerEntityMixin]:
        return entities.StateTaskDeadline

    def test_ledger_datetime_is_not_future(self) -> None:
        ok = entities.StateTaskDeadline(
            state_code=self.state_code,
            update_datetime=self.the_past,
            task_type=StateTaskType.DRUG_SCREEN,
        )
        self.assertEqual(ok.ledger_datetime_field, self.the_past)
        with self.assertRaisesRegex(ValueError, "Datetime field with value"):
            _ = entities.StateTaskDeadline(
                state_code=self.state_code,
                update_datetime=self.the_future,
                task_type=StateTaskType.DRUG_SCREEN,
            )

    def test_ledger_partition_key(self) -> None:
        ledger = entities.StateTaskDeadline(
            update_datetime=self.ledger_time,
            task_type=StateTaskType.DISCHARGE_FROM_SUPERVISION,
            state_code=self.state_code,
            sequence_num=None,
        )
        self.assertEqual(
            ledger.partition_key,
            "2023-01-01T00:00:00-None-StateTaskType.DISCHARGE_FROM_SUPERVISION-None-None",
        )
        ledger = entities.StateTaskDeadline(
            update_datetime=self.ledger_time,
            task_type=StateTaskType.DISCHARGE_FROM_SUPERVISION,
            state_code=self.state_code,
            sequence_num=1,
            task_subtype="SUB",
            task_metadata="META",
        )
        self.assertEqual(
            ledger.partition_key,
            "2023-01-01T00:00:00-001-StateTaskType.DISCHARGE_FROM_SUPERVISION-SUB-META",
        )
        first = entities.StateTaskDeadline(
            update_datetime=self.ledger_time,
            task_type=StateTaskType.DISCHARGE_FROM_SUPERVISION,
            state_code=self.state_code,
            sequence_num=9,
            task_subtype="SUB",
            task_metadata="META",
        )
        second = entities.StateTaskDeadline(
            update_datetime=self.ledger_time,
            task_type=StateTaskType.DISCHARGE_FROM_SUPERVISION,
            state_code=self.state_code,
            sequence_num=10,
            task_subtype="SUB",
            task_metadata="META",
        )
        self.assertEqual(
            sorted([second, first], key=lambda x: x.partition_key),
            [first, second],
        )

    @patch("recidiviz.pipelines.ingest.state.validator.ledger_entity_checks")
    def test_ledger_entity_checks_is_called(
        self, mock_ledger_entity_checks: MagicMock
    ) -> None:
        """Tests that we call ledger_entity_checks when the ledger entity is in the tree."""
        person = self.new_state_person()
        deadline_1 = entities.StateTaskDeadline(
            person=person,
            state_code=self.state_code,
            task_type=StateTaskType.INTERNAL_UNKNOWN,
            update_datetime=datetime.datetime(2022, 1, 1),
            sequence_num=1,
        )
        person.task_deadlines = [deadline_1]
        _ = validate_root_entity(person)
        mock_ledger_entity_checks.assert_called()


class StateSentenceStatusSnapshotTest(unittest.TestCase, LedgerEntityTestCaseProtocol):
    """Ensures StateSentenceStatusSnapshot is tested against the LedgerEntityTestCaseProtocol"""

    @classmethod
    def ledger_entity(cls) -> Type[LedgerEntityMixin]:
        return entities.StateSentenceStatusSnapshot

    def test_ledger_datetime_is_not_future(self) -> None:
        ok = entities.StateSentenceStatusSnapshot(
            state_code=self.state_code,
            status_update_datetime=self.the_past,
            status=StateSentenceStatus.SERVING,
        )
        self.assertEqual(ok.ledger_datetime_field, self.the_past)
        with self.assertRaisesRegex(
            ValueError,
            r"Found \[status_update_datetime\] value on class \[StateSentenceStatusSnapshot\]",
        ):
            _ = entities.StateSentenceStatusSnapshot(
                state_code=self.state_code,
                status_update_datetime=self.the_future,
                status=StateSentenceStatus.COMPLETED,
            )

    def test_ledger_partition_key(self) -> None:
        ledger = entities.StateSentenceStatusSnapshot(
            status_update_datetime=self.ledger_time,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=self.state_code,
            sequence_num=0,
        )
        self.assertEqual(ledger.partition_key, "2023-01-01T00:00:00-000-")

    @patch("recidiviz.pipelines.ingest.state.validator.ledger_entity_checks")
    def test_ledger_entity_checks_is_called(
        self, mock_ledger_entity_checks: MagicMock
    ) -> None:
        """Tests that we call ledger_entity_checks when the ledger entity is in the tree."""
        person = self.new_state_person()
        sentence = self.new_state_sentence()
        snapshot = entities.StateSentenceStatusSnapshot(
            sentence=sentence,
            state_code=self.state_code,
            status=StateSentenceStatus.SERVING,
            status_update_datetime=datetime.datetime(2022, 1, 1),
            sequence_num=1,
        )
        sentence.sentence_status_snapshots = [snapshot]
        person.sentences = [sentence]
        _ = validate_root_entity(person)
        mock_ledger_entity_checks.assert_called()


class StateSentenceLengthTest(unittest.TestCase, LedgerEntityTestCaseProtocol):
    """Ensures StateSentenceLength is tested against the LedgerEntityTestCaseProtocol"""

    @classmethod
    def ledger_entity(cls) -> Type[LedgerEntityMixin]:
        return entities.StateSentenceLength

    def test_ledger_datetime_is_not_future(self) -> None:
        ok = entities.StateSentenceLength(
            state_code=self.state_code,
            length_update_datetime=self.the_past,
            sentence_length_days_min=99,
        )
        self.assertEqual(ok.ledger_datetime_field, self.the_past)
        with self.assertRaisesRegex(ValueError, "Datetime field with value"):
            _ = entities.StateSentenceLength(
                state_code=self.state_code,
                length_update_datetime=self.the_future,
                sentence_length_days_min=99,
            )

    def test_ledger_partition_key(self) -> None:
        ledger = entities.StateSentenceLength(
            length_update_datetime=self.ledger_time,
            state_code=self.state_code,
            sequence_num=None,
        )
        self.assertEqual(ledger.partition_key, "2023-01-01T00:00:00-None-")

    @patch("recidiviz.pipelines.ingest.state.validator.ledger_entity_checks")
    def test_ledger_entity_checks_is_called(
        self, mock_ledger_entity_checks: MagicMock
    ) -> None:
        """Tests that we call ledger_entity_checks when the ledger entity is in the tree."""
        person = self.new_state_person()
        sentence = self.new_state_sentence()
        length = entities.StateSentenceLength(
            sentence=sentence,
            state_code=self.state_code,
            length_update_datetime=datetime.datetime(2022, 1, 1),
            sequence_num=None,
        )
        sentence.sentence_lengths = [length]
        person.sentences = [sentence]
        _ = validate_root_entity(person)
        mock_ledger_entity_checks.assert_called()

    def test_enforced_datetime_pairs(self) -> None:
        # "projected_parole_release_date_external" before "projected_completion_date_min_external"
        _ = entities.StateSentenceLength(
            projected_parole_release_date_external=self.before,
            projected_completion_date_min_external=self.after,
            length_update_datetime=self.ledger_time,
            state_code=self.state_code,
            sequence_num=None,
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Found StateSentenceLength\(sentence_length_id=None\) with projected parole release datetime 2022-01-01 after projected minimum completion datetime 1999-01-01.",
        ):
            _ = entities.StateSentenceLength(
                projected_parole_release_date_external=self.after,
                projected_completion_date_min_external=self.before,
                length_update_datetime=self.ledger_time,
                state_code=self.state_code,
                sequence_num=None,
            )
        # "projected_parole_release_date_external" before "projected_completion_date_max_external"
        _ = entities.StateSentenceLength(
            projected_parole_release_date_external=self.before,
            projected_completion_date_max_external=self.after,
            length_update_datetime=self.ledger_time,
            state_code=self.state_code,
            sequence_num=None,
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Found StateSentenceLength\(sentence_length_id=None\) with projected parole release datetime 2022-01-01 after projected maximum completion datetime 1999-01-01.",
        ):
            _ = entities.StateSentenceLength(
                projected_parole_release_date_external=self.after,
                projected_completion_date_max_external=self.before,
                length_update_datetime=self.ledger_time,
                state_code=self.state_code,
                sequence_num=None,
            )


class StateSentenceGroupLengthTest(unittest.TestCase, LedgerEntityTestCaseProtocol):
    """Ensures StateSentenceGroupLength is tested against the LedgerEntityTestCaseProtocol"""

    @classmethod
    def ledger_entity(cls) -> Type[LedgerEntityMixin]:
        return entities.StateSentenceGroupLength

    def test_ledger_datetime_is_not_future(self) -> None:
        ok = entities.StateSentenceGroupLength(
            state_code=self.state_code,
            group_update_datetime=self.the_past,
        )
        self.assertEqual(ok.ledger_datetime_field, self.the_past)
        with self.assertRaisesRegex(
            ValueError,
            r"Found \[group_update_datetime\] value on class "
            r"\[StateSentenceGroupLength\] with value \[.+\] which is greater than or "
            r"equal to \[.+\], the \(exclusive\) max allowed date\.$",
        ):
            _ = entities.StateSentenceGroupLength(
                state_code=self.state_code,
                group_update_datetime=self.the_future,
            )

    def test_ledger_partition_key(self) -> None:
        ledger = entities.StateSentenceGroupLength(
            group_update_datetime=self.ledger_time,
            state_code=self.state_code,
            sequence_num=None,
        )
        self.assertEqual(ledger.partition_key, "2023-01-01T00:00:00-None-")

    @patch("recidiviz.pipelines.ingest.state.validator.ledger_entity_checks")
    def test_ledger_entity_checks_is_called(
        self, mock_ledger_entity_checks: MagicMock
    ) -> None:
        """Tests that we call ledger_entity_checks when the ledger entity is in the tree."""
        person = self.new_state_person()
        group_length_1 = entities.StateSentenceGroupLength(
            state_code=self.state_code,
            group_update_datetime=datetime.datetime(2022, 1, 1),
            sequence_num=None,
        )
        group_length_2 = entities.StateSentenceGroupLength(
            state_code=self.state_code,
            group_update_datetime=datetime.datetime(2023, 1, 1),
            sequence_num=None,
        )
        group = entities.StateSentenceGroup(
            state_code=self.state_code,
            external_id="TEST-SG",
            sentence_group_lengths=[group_length_1, group_length_2],
        )
        person.sentence_groups.append(group)
        _ = validate_root_entity(person)
        mock_ledger_entity_checks.assert_called()

    def test_enforced_datetime_pairs(self) -> None:
        # "projected_parole_release_date_external" before "projected_full_term_release_date_min_external"
        _ = entities.StateSentenceGroupLength(
            projected_parole_release_date_external=self.before,
            projected_full_term_release_date_min_external=self.after,
            group_update_datetime=self.ledger_time,
            state_code=self.state_code,
            sequence_num=None,
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Found StateSentenceGroupLength\(sentence_group_length_id=None\) with projected parole release datetime 2022-01-01 after projected minimum full term release datetime 1999-01-01.",
        ):
            _ = entities.StateSentenceGroupLength(
                projected_parole_release_date_external=self.after,
                projected_full_term_release_date_min_external=self.before,
                group_update_datetime=self.ledger_time,
                state_code=self.state_code,
                sequence_num=None,
            )
        # "projected_parole_release_date_external" before "projected_full_term_release_date_max_external"
        _ = entities.StateSentenceGroupLength(
            projected_parole_release_date_external=self.before,
            projected_full_term_release_date_max_external=self.after,
            group_update_datetime=self.ledger_time,
            state_code=self.state_code,
            sequence_num=None,
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Found StateSentenceGroupLength\(sentence_group_length_id=None\) with projected parole release datetime 2022-01-01 after projected maximum full term release datetime 1999-01-01.",
        ):
            _ = entities.StateSentenceGroupLength(
                projected_parole_release_date_external=self.after,
                projected_full_term_release_date_max_external=self.before,
                group_update_datetime=self.ledger_time,
                state_code=self.state_code,
                sequence_num=None,
            )


def test_all_ledger_entities_have_a_test_case() -> None:
    """Ensures every entity with a LedgerEntityMixin has a test implementing LedgerEntityTestCaseProtocol"""
    tested_entities: List[LedgerEntityMixin] = []
    for _, test_case in inspect.getmembers(
        sys.modules[__name__],
        lambda mod: (
            inspect.isclass(mod)
            and issubclass(mod, LedgerEntityTestCaseProtocol)
            and mod != LedgerEntityTestCaseProtocol
        ),
    ):
        tested_entities.append(test_case.ledger_entity())
    assert sorted(tested_entities, key=str) == sorted(ALL_LEDGER_ENTITIES, key=str)
