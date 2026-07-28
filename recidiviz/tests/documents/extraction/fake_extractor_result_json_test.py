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
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
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
