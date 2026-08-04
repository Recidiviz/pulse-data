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
"""Tests that the fake extractor result JSON fixtures conform to the JSON Schema
the extractor sends the model, so the tests that consume them can't silently drift
from the result shapes the real pipeline stores.
"""
import unittest
from typing import Any

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.entity_resolution.entity_resolution_output_schema_builder import (
    build_entity_resolution_output_schema,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_json_schema_generator import (
    LLMJsonSchemaGenerator,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    RESULT_KEY,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    fake_first_order_collection,
    get_entity_group_by_name,
)
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    build_fake_entity_resolution_entity_result_json,
    build_fake_entity_resolution_result_content,
    build_fake_extractor_assignment_result_json,
    build_fake_extractor_irrelevant_result_content,
    build_fake_extractor_result_content,
    fake_all_fields_result_json,
    fake_irrelevant_result_json,
    fake_minimal_relevant_result_json,
)
from recidiviz.utils.validate_json_schema import iter_leaf_validation_errors

_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"


class FakeExtractorResultJsonSchemaConformanceTest(unittest.TestCase):
    """Validates the fixture `result_json` builders against the JSON Schema
    generated for the fake extractor collection — the same schema the model's
    structured output is constrained by.
    """

    def setUp(self) -> None:
        config = get_first_order_llm_extractor_config(
            StateCode.US_XX, _COLLECTION_NAME, config_module=fake_config
        )
        self.json_schema = LLMJsonSchemaGenerator.generate(
            config.extractor_collection.output_schema
        )

    def _assert_wrapped_result_conforms(self, wrapped_result: dict[str, Any]) -> None:
        # The API-level schema nests the result object under the `result` key.
        violations = [
            f"[{error.json_path}]: {error.message}"
            for error in iter_leaf_validation_errors(wrapped_result, self.json_schema)
        ]
        self.assertEqual([], violations)

    def _assert_content_conforms(self, result_content: dict[str, Any]) -> None:
        # The pipeline stores the unwrapped object as `result_json`; wrap it back up
        # to validate against the API-level schema.
        self._assert_wrapped_result_conforms({RESULT_KEY: result_content})

    def test_relevant_result_content_conforms(self) -> None:
        self._assert_content_conforms(
            build_fake_extractor_result_content(
                primary_status="active",
                status_note="Currently active",
                location="HQ",
                assignments=[
                    build_fake_extractor_assignment_result_json(
                        "Kitchen", "internal", 15.5, "hourly"
                    ),
                    build_fake_extractor_assignment_result_json(
                        "Laundry", "external", 20.0, "monthly"
                    ),
                ],
            )
        )

    def test_null_branch_result_content_conforms(self) -> None:
        self._assert_content_conforms(
            build_fake_extractor_result_content(
                primary_status="other",
                status_note="No location found",
                location=None,
                assignments=[],
            )
        )

    def test_irrelevant_result_content_conforms(self) -> None:
        self._assert_content_conforms(build_fake_extractor_irrelevant_result_content())

    def test_wrapped_result_builders_conform(self) -> None:
        # The pre-wrapped `{"result": ...}` builders the validator/processor tests
        # consume must conform directly.
        self._assert_wrapped_result_conforms(fake_minimal_relevant_result_json())
        self._assert_wrapped_result_conforms(fake_all_fields_result_json())
        self._assert_wrapped_result_conforms(fake_irrelevant_result_json())


class FakeEntityResolutionResultJsonSchemaConformanceTest(unittest.TestCase):
    """Validates the entity-resolution fixture result_json builders against the JSON
    Schema synthesized for each of the fake collection's entity groups, the same schema
    that constrains the ER model's structured output.

    The ER schema is generated from the entity group rather than authored, so without this
    the fixtures could drift from what the model can actually emit.
    """

    def _assert_conforms(
        self, *, entity_group_name: str, result_content: dict[str, Any]
    ) -> None:
        entity_group = get_entity_group_by_name(
            fake_first_order_collection(), entity_group_name
        )
        json_schema = LLMJsonSchemaGenerator.generate(
            build_entity_resolution_output_schema(entity_group=entity_group)
        )
        # The ER schema has no "result" wrapper, unlike a first-order schema. With no
        # is_relevant field there is no relevance branch to discriminate, so the entities
        # field sits at the root, which is also the shape stored in result_json.
        violations = [
            f"[{error.json_path}]: {error.message}"
            for error in iter_leaf_validation_errors(result_content, json_schema)
        ]
        self.assertEqual([], violations)

    def test_top_level_group_entities_conform(self) -> None:
        self._assert_conforms(
            entity_group_name="location",
            result_content=build_fake_entity_resolution_result_content(
                [
                    build_fake_entity_resolution_entity_result_json(
                        1, entry_nums=[1, 3], location="HQ"
                    ),
                    build_fake_entity_resolution_entity_result_json(
                        2, entry_nums=[2], location="Annex"
                    ),
                ]
            ),
        )

    def test_array_group_entities_conform(self) -> None:
        self._assert_conforms(
            entity_group_name="assignment",
            result_content=build_fake_entity_resolution_result_content(
                [
                    build_fake_entity_resolution_entity_result_json(
                        1,
                        entry_nums=[1, 2],
                        assignment_name="Kitchen",
                        assignment_type="internal",
                    )
                ]
            ),
        )

    def test_float_entity_field_conforms(self) -> None:
        self._assert_conforms(
            entity_group_name="pay_rate",
            result_content=build_fake_entity_resolution_result_content(
                [
                    build_fake_entity_resolution_entity_result_json(
                        1, entry_nums=[1], rate_amount=15.5
                    )
                ]
            ),
        )

    def test_nullable_entity_field_conforms(self) -> None:
        # assignment_type is optional in the first-order collection, so the synthesized
        # ER schema makes its value nullable; an entity no mention filled it for returns
        # an explicit null rather than an invented value.
        self._assert_conforms(
            entity_group_name="assignment",
            result_content=build_fake_entity_resolution_result_content(
                [
                    build_fake_entity_resolution_entity_result_json(
                        1,
                        entry_nums=[1],
                        assignment_name="Kitchen",
                        assignment_type=None,
                    )
                ]
            ),
        )
