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
"""Tests for DuplicateEntityCheck, exercised against the ER extractor collection
synthesized from the fake collection's `assignment` entity group, whose entity
fields are `assignment_name` (required) and `assignment_type` (optional, so a
canonical value can be an explicit null).

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
from recidiviz.documents.extraction.validation.duplicate_entity_check import (
    DuplicateEntityCheck,
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


class DuplicateEntityCheckTest(TestCase):
    """Tests for DuplicateEntityCheck."""

    def setUp(self) -> None:
        self.enterContext(patch_fake_entity_resolution_model_config_name())
        self.output_schema = fake_entity_resolution_extractor_config(
            _ASSIGNMENT_GROUP_NAME
        ).extractor_collection.output_schema

    @staticmethod
    def _entity(
        entity_id: int, *, assignment_name: str, assignment_type: str | None
    ) -> dict[str, Any]:
        """Returns one resolved-entity JSON object. Every entity claims its own
        entry number, so entities never collide on the framework fields — only
        their canonical entity-field values decide duplication."""
        return build_fake_entity_resolution_entity_result_json(
            entity_id,
            entry_nums=[entity_id],
            assignment_name=assignment_name,
            assignment_type=assignment_type,
        )

    def _issues(self, entities: list[dict[str, Any]]) -> list[ValidationIssue]:
        return DuplicateEntityCheck.issues(
            output=LLMRequestOutputValues(
                output_schema=self.output_schema,
                output_json=build_fake_entity_resolution_result_content(entities),
            )
        )

    def _assert_single_issue(
        self,
        entities: list[dict[str, Any]],
        *,
        expected_detail_substring: str,
    ) -> ValidationIssue:
        """Asserts the check raises exactly one retrying finding against the
        `entities` field whose detail carries |expected_detail_substring|, and
        returns it."""
        [issue] = self._issues(entities)
        self.assertEqual(ValidationCheckType.DUPLICATE_ENTITY, issue.check_type)
        self.assertEqual(ENTITIES_FIELD_NAME, issue.field_name)
        self.assertTrue(issue.will_retry)
        self.assertIn(expected_detail_substring, issue.detail)
        return issue

    def test_distinct_entities_pass(self) -> None:
        self.assertEqual(
            [],
            self._issues(
                [
                    self._entity(
                        1, assignment_name="Kitchen", assignment_type="internal"
                    ),
                    self._entity(
                        2, assignment_name="Laundry", assignment_type="internal"
                    ),
                ]
            ),
        )

    def test_single_entity_passes(self) -> None:
        self.assertEqual(
            [],
            self._issues(
                [self._entity(1, assignment_name="Kitchen", assignment_type="internal")]
            ),
        )

    def test_identical_entities_flagged(self) -> None:
        # entity_id and entry_nums differ, but those are framework fields, not
        # part of an entity's canonical identity.
        issue = self._assert_single_issue(
            [
                self._entity(1, assignment_name="Kitchen", assignment_type="internal"),
                self._entity(2, assignment_name="Laundry", assignment_type="internal"),
                self._entity(3, assignment_name="Kitchen", assignment_type="internal"),
            ],
            expected_detail_substring="entities[0], entities[2]",
        )
        # The finding names the shared canonical values so an audit reader can
        # see what the model failed to merge.
        self.assertIn("'assignment_name': 'Kitchen'", issue.detail)
        self.assertIn("'assignment_type': 'internal'", issue.detail)

    def test_entities_differing_in_one_field_pass(self) -> None:
        # Same name at a different assignment type is a distinct canonical
        # identity — the values must be identical across every entity field to
        # count as duplicates.
        self.assertEqual(
            [],
            self._issues(
                [
                    self._entity(
                        1, assignment_name="Kitchen", assignment_type="internal"
                    ),
                    self._entity(
                        2, assignment_name="Kitchen", assignment_type="external"
                    ),
                ]
            ),
        )

    def test_identical_entities_with_null_field_flagged(self) -> None:
        # An explicit null is a canonical value like any other: two entities
        # that match on the filled fields and are both null on the rest are
        # identical.
        self._assert_single_issue(
            [
                self._entity(1, assignment_name="Kitchen", assignment_type=None),
                self._entity(2, assignment_name="Kitchen", assignment_type=None),
            ],
            expected_detail_substring="entities[0], entities[1]",
        )

    def test_three_way_duplicate_lands_in_one_finding(self) -> None:
        self._assert_single_issue(
            [
                self._entity(1, assignment_name="Kitchen", assignment_type="internal"),
                self._entity(2, assignment_name="Kitchen", assignment_type="internal"),
                self._entity(3, assignment_name="Kitchen", assignment_type="internal"),
            ],
            expected_detail_substring="entities[0], entities[1], entities[2]",
        )

    def test_two_duplicate_groups_each_flagged(self) -> None:
        issues = self._issues(
            [
                self._entity(1, assignment_name="Kitchen", assignment_type="internal"),
                self._entity(2, assignment_name="Laundry", assignment_type="external"),
                self._entity(3, assignment_name="Kitchen", assignment_type="internal"),
                self._entity(4, assignment_name="Laundry", assignment_type="external"),
            ]
        )
        self.assertEqual(2, len(issues))
        details = "\n".join(issue.detail for issue in issues)
        self.assertIn("'assignment_name': 'Kitchen'", details)
        self.assertIn("'assignment_name': 'Laundry'", details)
