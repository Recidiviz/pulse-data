# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for EntryPartitionCheck, exercised against the ER extractor collection
synthesized from the fake collection's `assignment` entity group.

Its wiring into the validator is covered in
llm_extraction_result_validator_test.py.
"""

from typing import Any
from unittest import TestCase

from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    ENTITIES_FIELD_NAME,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.documents.extraction.validation.entry_partition_check import (
    EntryPartitionCheck,
)
from recidiviz.documents.extraction.validation.llm_document_validation_result import (
    ValidationCheckType,
    ValidationIssue,
)
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    fake_entity_resolution_extractor_config,
    patch_fake_entity_resolution_model_config_name,
)
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    build_fake_entity_resolution_entity_result_json,
    build_fake_entity_resolution_result_content,
)

_ASSIGNMENT_GROUP_NAME = "assignment"


def _entity(entity_id: int, *, entry_nums: list[int]) -> dict[str, Any]:
    """Returns one resolved-entity JSON object claiming |entry_nums|."""
    return build_fake_entity_resolution_entity_result_json(
        entity_id,
        entry_nums=entry_nums,
        assignment_name=f"Assignment {entity_id}",
        assignment_type="internal",
    )


class EntryPartitionCheckTest(TestCase):
    """Tests for EntryPartitionCheck."""

    def setUp(self) -> None:
        self.enterContext(patch_fake_entity_resolution_model_config_name())
        self.output_schema = fake_entity_resolution_extractor_config(
            _ASSIGNMENT_GROUP_NAME
        ).extractor_collection.output_schema

    def _issues(
        self, entities: list[dict[str, Any]], *, expected_entry_nums: set[int]
    ) -> list[ValidationIssue]:
        return EntryPartitionCheck.issues(
            output=LLMRequestOutputValues(
                output_schema=self.output_schema,
                output_json=build_fake_entity_resolution_result_content(entities),
            ),
            expected_entry_nums=expected_entry_nums,
        )

    def _assert_single_issue(
        self,
        entities: list[dict[str, Any]],
        *,
        expected_entry_nums: set[int],
        expected_detail_substring: str,
    ) -> None:
        """Asserts the check raises exactly one retrying finding against the
        `entities` field whose detail carries |expected_detail_substring|."""
        issues = self._issues(entities, expected_entry_nums=expected_entry_nums)
        self.assertEqual(1, len(issues))
        issue = issues[0]
        self.assertEqual(
            ValidationCheckType.ENTRY_PARTITION_VIOLATION, issue.check_type
        )
        self.assertEqual(ENTITIES_FIELD_NAME, issue.field_name)
        self.assertTrue(issue.will_retry)
        self.assertIn(expected_detail_substring, issue.detail)

    def test_exact_partition_passes(self) -> None:
        # Assignment order and order within an entity's entry_nums don't matter;
        # only the multiset of assigned entries does.
        self.assertEqual(
            [],
            self._issues(
                [_entity(1, entry_nums=[4, 1]), _entity(2, entry_nums=[3, 2, 5])],
                expected_entry_nums={1, 2, 3, 4, 5},
            ),
        )

    def test_single_entity_claiming_every_entry_passes(self) -> None:
        self.assertEqual(
            [],
            self._issues(
                [_entity(1, entry_nums=[1, 2, 3])], expected_entry_nums={1, 2, 3}
            ),
        )

    def test_dropped_entries_flagged(self) -> None:
        self._assert_single_issue(
            [_entity(1, entry_nums=[1, 4])],
            expected_entry_nums={1, 2, 3, 4},
            expected_detail_substring=(
                "Entries [2, 3] of the composite document are assigned to no entity"
            ),
        )

    def test_invented_entries_flagged(self) -> None:
        self._assert_single_issue(
            [_entity(1, entry_nums=[1, 2]), _entity(2, entry_nums=[7])],
            expected_entry_nums={1, 2},
            expected_detail_substring=(
                "Entities claim entry numbers [7], which match no entry"
            ),
        )

    def test_entry_assigned_to_two_entities_flagged(self) -> None:
        self._assert_single_issue(
            [_entity(1, entry_nums=[1, 2]), _entity(2, entry_nums=[2, 3])],
            expected_entry_nums={1, 2, 3},
            expected_detail_substring=("Entries [2] are each assigned more than once"),
        )

    def test_entry_repeated_within_one_entity_flagged(self) -> None:
        # A duplicate inside a single entity's entry_nums still double-counts
        # the mention, the same as a duplicate across entities.
        self._assert_single_issue(
            [_entity(1, entry_nums=[1, 2, 2])],
            expected_entry_nums={1, 2},
            expected_detail_substring=("Entries [2] are each assigned more than once"),
        )

    def test_every_failure_mode_lands_in_one_audit(self) -> None:
        # Entry 3 dropped, entry 9 invented, entry 1 duplicated: one finding per
        # failure mode, so the audit records everything wrong with the partition.
        issues = self._issues(
            [_entity(1, entry_nums=[1, 2]), _entity(2, entry_nums=[1, 9])],
            expected_entry_nums={1, 2, 3},
        )
        self.assertEqual(3, len(issues))
        details = [issue.detail for issue in issues]
        self.assertIn(
            "Entries [3] of the composite document are assigned to no entity; "
            "every entry must be assigned to exactly one.",
            details,
        )
        self.assertIn(
            "Entities claim entry numbers [9], which match no entry of the "
            "composite document.",
            details,
        )
        self.assertIn(
            "Entries [1] are each assigned more than once across the emitted "
            "entities; every entry must be assigned to exactly one.",
            details,
        )
