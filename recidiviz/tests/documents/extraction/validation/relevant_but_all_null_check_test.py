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
"""Tests for RelevantButAllNullCheck, exercised against the fake extractor
collection, whose user-defined fields cover every shape a value can be carried
in: INFERRED scalars (`primary_status`, `location`), a bare STRUCTURAL scalar
(`status_note`), and an array (`assignments`).

Its wiring into the validator is covered in
llm_extraction_result_validator_test.py.
"""

from typing import Any
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    IS_RELEVANT_FIELD_NAME,
    RESULT_KEY,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.documents.extraction.validation.llm_document_validation_result import (
    ValidationCheckType,
    ValidationIssue,
)
from recidiviz.documents.extraction.validation.relevant_but_all_null_check import (
    RelevantButAllNullCheck,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    build_fake_extractor_assignment_result_json,
    build_inferred_field_result_json,
    build_null_inferred_field_result_json,
    fake_all_fields_result_json,
    fake_irrelevant_result_json,
    fake_minimal_relevant_result_json,
    wrap_in_result_key,
)

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"


def _all_null_content(**overrides: Any) -> dict[str, Any]:
    """Returns relevant result content whose every user-defined field is null,
    with |overrides| written over specific fields by name to populate one. The
    `assignments` array is omitted entirely — emitting it, even empty, would be
    an affirmative statement rather than a null.
    """
    content: dict[str, Any] = {
        IS_RELEVANT_FIELD_NAME: True,
        "primary_status": build_null_inferred_field_result_json(),
        "status_note": None,
        "location": build_null_inferred_field_result_json(),
    }
    content.update(overrides)
    return content


class RelevantButAllNullCheckTest(TestCase):
    """Tests for RelevantButAllNullCheck."""

    def setUp(self) -> None:
        self.output_schema = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        ).extractor_collection.output_schema

    def _issues(self, result_json: dict[str, Any]) -> list[ValidationIssue]:
        return RelevantButAllNullCheck.issues(
            output=LLMRequestOutputValues(
                output_schema=self.output_schema, output_json=result_json
            )
        )

    def _assert_flagged(self, result_json: dict[str, Any]) -> None:
        """Asserts |result_json| draws the single document-level finding."""
        issues = self._issues(result_json)
        self.assertEqual(1, len(issues))
        [issue] = issues
        self.assertEqual(ValidationCheckType.RELEVANT_BUT_ALL_NULL, issue.check_type)
        # The finding is about the document as a whole, so it names no field.
        self.assertIsNone(issue.field_name)
        self.assertIn("every extracted field is null", issue.detail)
        # It names the fields it looked at, so an audit reader can see what the
        # model was asked for and left empty.
        self.assertIn("'primary_status'", issue.detail)

    def test_all_fields_populated_passes(self) -> None:
        self.assertEqual([], self._issues(fake_all_fields_result_json()))

    def test_minimal_result_passes(self) -> None:
        self.assertEqual([], self._issues(fake_minimal_relevant_result_json()))

    def test_irrelevant_result_passes(self) -> None:
        # An irrelevant document is expected to carry nothing — that is what the
        # short irrelevant branch of the schema is for.
        self.assertEqual([], self._issues(fake_irrelevant_result_json()))

    def test_relevant_with_every_field_null_flagged(self) -> None:
        self._assert_flagged(wrap_in_result_key(_all_null_content()))

    def test_relevant_with_no_fields_at_all_flagged(self) -> None:
        # Nothing but the relevance determination, in the shape an older
        # extractor version's output would take: every field reads as null.
        self._assert_flagged(wrap_in_result_key({IS_RELEVANT_FIELD_NAME: True}))

    def test_inferred_field_alone_passes(self) -> None:
        self.assertEqual(
            [],
            self._issues(
                wrap_in_result_key(
                    _all_null_content(
                        location=build_inferred_field_result_json("Kitchen")
                    )
                )
            ),
        )

    def test_structural_field_alone_passes(self) -> None:
        # A bare STRUCTURAL value is still something the model extracted, so it
        # keeps the document out of the all-null case.
        self.assertEqual(
            [],
            self._issues(
                wrap_in_result_key(_all_null_content(status_note="Currently active."))
            ),
        )

    def test_empty_string_structural_field_alone_passes(self) -> None:
        # Emptiness is not nullness for a scalar: the model returned a value, and
        # this check does not judge its quality.
        self.assertEqual(
            [], self._issues(wrap_in_result_key(_all_null_content(status_note="")))
        )

    def test_empty_array_alone_passes(self) -> None:
        # An empty array is not null: the extractor affirmatively said the
        # document lists no assignments, unlike an omitted key, which says
        # nothing about them.
        self.assertEqual(
            [], self._issues(wrap_in_result_key(_all_null_content(assignments=[])))
        )

    def test_populated_array_alone_passes(self) -> None:
        self.assertEqual(
            [],
            self._issues(
                wrap_in_result_key(
                    _all_null_content(
                        assignments=[
                            build_fake_extractor_assignment_result_json(
                                "Dish duty", "internal", 12.5, "hourly"
                            )
                        ]
                    )
                )
            ),
        )

    def test_array_of_all_null_elements_passes(self) -> None:
        # The model emitted an element, so it found something worth reporting even
        # though every sub-field of it came back null. Judging the element's
        # contents is a job for the ER-era completeness checks, not this guard.
        self.assertEqual(
            [],
            self._issues(
                wrap_in_result_key(
                    _all_null_content(
                        assignments=[
                            {
                                "assignment_name": (
                                    build_null_inferred_field_result_json()
                                ),
                                "assignment_type": (
                                    build_null_inferred_field_result_json()
                                ),
                                "rate_amount": build_null_inferred_field_result_json(),
                                "rate_period": build_null_inferred_field_result_json(),
                            }
                        ]
                    )
                )
            ),
        )

    def test_relevance_read_from_result_envelope(self) -> None:
        # Sanity check on the shape the check reads: is_relevant lives inside the
        # result envelope, so a false one there suppresses the finding even with
        # every other field null.
        self.assertEqual(
            [],
            self._issues({RESULT_KEY: {IS_RELEVANT_FIELD_NAME: False}}),
        )
