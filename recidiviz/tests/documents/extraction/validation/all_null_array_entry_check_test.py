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
"""Tests for AllNullArrayEntryCheck.

The first-order cases run against the fake extractor collection's `assignments`
array. The check reads values through the schema without running structural
conformance first, so an entry can hold a null for the required
`assignment_name` here even though the validator would reject it earlier.

The entity-resolution cases run against the ER collections synthesized from the
fake collection's `location` group, whose single entity field is optional, and
its `assignment` group, which has several entity fields.

Its wiring into the validator is covered in
llm_extraction_result_validator_test.py.
"""

from typing import Any
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.documents.extraction.validation.all_null_array_entry_check import (
    AllNullArrayEntryCheck,
)
from recidiviz.documents.extraction.validation.llm_document_validation_result import (
    ValidationCheckType,
    ValidationIssue,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    fake_entity_resolution_extractor_config,
    patch_fake_entity_resolution_model_config_name,
)
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    build_fake_entity_resolution_entity_result_json,
    build_fake_entity_resolution_result_content,
    build_fake_extractor_assignment_result_json,
    build_fake_extractor_result_content,
    build_inferred_field_result_json,
    build_null_inferred_field_result_json,
    wrap_in_result_key,
)

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
_LOCATION_GROUP_NAME = "location"
_ASSIGNMENT_GROUP_NAME = "assignment"
_ASSIGNMENT_SUB_FIELD_NAMES = [
    "assignment_name",
    "assignment_type",
    "rate_amount",
    "rate_period",
]


def _assignment_entry(**values: Any) -> dict[str, Any]:
    """Returns one `assignments` element holding |values| on their value branch
    and every other sub-field on its null branch.
    """
    return {
        name: (
            build_inferred_field_result_json(values[name])
            if name in values
            else build_null_inferred_field_result_json()
        )
        for name in _ASSIGNMENT_SUB_FIELD_NAMES
    }


def _location_entity(entity_id: int, *, location: str | None) -> dict[str, Any]:
    """Returns one resolved-entity JSON object for the location group."""
    return build_fake_entity_resolution_entity_result_json(
        entity_id, entry_nums=[entity_id], location=location
    )


class AllNullArrayEntryCheckFirstOrderTest(TestCase):
    """Tests AllNullArrayEntryCheck against the fake first-order collection."""

    def setUp(self) -> None:
        self.output_schema = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        ).extractor_collection.output_schema

    def _issues(self, assignments: list[dict[str, Any]]) -> list[ValidationIssue]:
        return AllNullArrayEntryCheck.issues(
            output=LLMRequestOutputValues(
                output_schema=self.output_schema,
                output_json=wrap_in_result_key(
                    build_fake_extractor_result_content(
                        primary_status="active",
                        status_note="Working nights",
                        location="Kitchen",
                        assignments=assignments,
                    )
                ),
            )
        )

    def test_populated_entries_pass(self) -> None:
        self.assertEqual(
            [],
            self._issues(
                [
                    build_fake_extractor_assignment_result_json(
                        "Dish duty", "internal", 15.5, "hourly"
                    ),
                    build_fake_extractor_assignment_result_json(
                        "Laundry", "external", 1200.0, "monthly"
                    ),
                ]
            ),
        )

    def test_empty_array_passes(self) -> None:
        self.assertEqual([], self._issues([]))

    def test_all_null_entry_flagged_at_its_element(self) -> None:
        issues = self._issues(
            [
                build_fake_extractor_assignment_result_json(
                    "Dish duty", "internal", 15.5, "hourly"
                ),
                _assignment_entry(),
            ]
        )
        [issue] = issues
        self.assertEqual(ValidationCheckType.ALL_NULL_ARRAY_ENTRY, issue.check_type)
        self.assertEqual("assignments[1]", issue.field_name)
        self.assertTrue(issue.will_retry)
        self.assertIn("null for every field", issue.detail)
        for name in _ASSIGNMENT_SUB_FIELD_NAMES:
            self.assertIn(f"'{name}'", issue.detail)

    def test_every_all_null_entry_flagged(self) -> None:
        issues = self._issues(
            [
                _assignment_entry(),
                build_fake_extractor_assignment_result_json(
                    "Dish duty", "internal", 15.5, "hourly"
                ),
                _assignment_entry(),
            ]
        )
        self.assertEqual(
            ["assignments[0]", "assignments[2]"],
            [issue.field_name for issue in issues],
        )

    def test_entry_with_only_rate_fields_passes(self) -> None:
        # A pay rate with no name, type, or period still reports a value.
        self.assertEqual(
            [],
            self._issues([_assignment_entry(rate_amount=15.5)]),
        )

    def test_entry_with_only_a_primary_key_passes(self) -> None:
        self.assertEqual(
            [],
            self._issues([_assignment_entry(assignment_name="Dish duty")]),
        )


class AllNullArrayEntryCheckEntityResolutionTest(TestCase):
    """Tests AllNullArrayEntryCheck against the synthesized ER collections."""

    def setUp(self) -> None:
        self.enterContext(patch_fake_entity_resolution_model_config_name())

    def _issues(
        self, group_name: str, entities: list[dict[str, Any]]
    ) -> list[ValidationIssue]:
        output_schema = fake_entity_resolution_extractor_config(
            group_name
        ).extractor_collection.output_schema
        return AllNullArrayEntryCheck.issues(
            output=LLMRequestOutputValues(
                output_schema=output_schema,
                output_json=build_fake_entity_resolution_result_content(entities),
            )
        )

    def test_populated_entities_pass(self) -> None:
        self.assertEqual(
            [],
            self._issues(
                _LOCATION_GROUP_NAME,
                [
                    _location_entity(1, location="Kitchen"),
                    _location_entity(2, location="Laundry"),
                ],
            ),
        )

    def test_all_null_entity_flagged_at_its_element(self) -> None:
        # entity_id and entry_nums are non-null on every entity, but they are
        # framework fields; only the entity fields identify the entity.
        issues = self._issues(
            _LOCATION_GROUP_NAME,
            [
                _location_entity(1, location="Kitchen"),
                _location_entity(2, location=None),
            ],
        )
        [issue] = issues
        self.assertEqual(ValidationCheckType.ALL_NULL_ARRAY_ENTRY, issue.check_type)
        self.assertEqual("entities[1]", issue.field_name)
        self.assertTrue(issue.will_retry)
        self.assertIn("null for every field", issue.detail)
        self.assertIn("'location'", issue.detail)
        self.assertNotIn("entity_id", issue.detail)
        self.assertNotIn("entry_nums", issue.detail)

    def test_every_all_null_entity_flagged(self) -> None:
        issues = self._issues(
            _LOCATION_GROUP_NAME,
            [
                _location_entity(1, location=None),
                _location_entity(2, location="Kitchen"),
                _location_entity(3, location=None),
            ],
        )
        self.assertEqual(
            ["entities[0]", "entities[2]"], [issue.field_name for issue in issues]
        )

    def test_entity_with_some_null_fields_passes(self) -> None:
        # One non-null entity field is enough to identify the entity.
        self.assertEqual(
            [],
            self._issues(
                _ASSIGNMENT_GROUP_NAME,
                [
                    build_fake_entity_resolution_entity_result_json(
                        1,
                        entry_nums=[1],
                        assignment_name="Kitchen",
                        assignment_type=None,
                    )
                ],
            ),
        )
