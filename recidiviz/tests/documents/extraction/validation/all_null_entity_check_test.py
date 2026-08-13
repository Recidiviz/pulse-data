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
"""Tests for AllNullEntityCheck.

Exercised mainly against the ER extractor collection synthesized from the fake
collection's `location` entity group, whose single entity field is optional —
the only kind of group whose entities can be all-null and still structurally
conformant. The some-fields-null case uses the `assignment` group, which has
two entity fields.

Its wiring into the validator is covered in
llm_extraction_result_validator_test.py.
"""

from typing import Any
from unittest import TestCase

from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.documents.extraction.validation.all_null_entity_check import (
    AllNullEntityCheck,
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

_LOCATION_GROUP_NAME = "location"
_ASSIGNMENT_GROUP_NAME = "assignment"


def _location_entity(entity_id: int, *, location: str | None) -> dict[str, Any]:
    """Returns one resolved-entity JSON object for the location group."""
    return build_fake_entity_resolution_entity_result_json(
        entity_id, entry_nums=[entity_id], location=location
    )


class AllNullEntityCheckTest(TestCase):
    """Tests for AllNullEntityCheck."""

    def setUp(self) -> None:
        self.enterContext(patch_fake_entity_resolution_model_config_name())

    def _issues(
        self, group_name: str, entities: list[dict[str, Any]]
    ) -> list[ValidationIssue]:
        output_schema = fake_entity_resolution_extractor_config(
            group_name
        ).extractor_collection.output_schema
        return AllNullEntityCheck.issues(
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
        # framework fields — only the entity fields identify the entity.
        issues = self._issues(
            _LOCATION_GROUP_NAME,
            [
                _location_entity(1, location="Kitchen"),
                _location_entity(2, location=None),
            ],
        )
        [issue] = issues
        self.assertEqual(ValidationCheckType.ALL_NULL_ENTITY, issue.check_type)
        self.assertEqual("entities[1]", issue.field_name)
        self.assertTrue(issue.will_retry)
        self.assertIn("null for every entity field", issue.detail)
        self.assertIn("'location'", issue.detail)

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
