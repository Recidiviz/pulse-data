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
"""Tests for LLMRequestOutputValues."""
from typing import Any, Callable
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ArrayOfStructLLMRequestOutputSchemaField,
    LLMOutputFieldType,
    PrimitiveScalarLLMRequestOutputSchemaField,
    ScalarValuedLLMRequestOutputSchemaField,
    assert_scalar_valued_field,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    CONFIDENCE_LEVEL_FIELD_NAME,
    IS_RELEVANT_FIELD_NAME,
    RESULT_KEY,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMOutputParsingError,
    LLMRequestOutputValues,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    build_fake_extractor_assignment_result_json,
    build_fake_extractor_result_content,
    build_null_inferred_field_result_json,
    wrap_in_result_key,
)
from recidiviz.utils.types import assert_type

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"


def _schema_without_is_relevant() -> LLMRequestOutputSchema:
    """Returns a schema with no relevance criteria, whose result JSON therefore
    carries the extracted fields directly rather than inside a `result` envelope.
    """
    return LLMRequestOutputSchema(
        full_batch_description="Batch of parsed fake documents for testing.",
        result_level_description="One parsed fake document for testing.",
        relevance_criteria=None,
        user_defined_fields=[
            PrimitiveScalarLLMRequestOutputSchemaField(
                name="status_note",
                description="A bare note about the record's status.",
                required=True,
                inferred_field_config=None,
                scalar_type=LLMOutputFieldType.STRING,
            )
        ],
    )


def _scalar_field(
    schema: LLMRequestOutputSchema, field_name: str
) -> ScalarValuedLLMRequestOutputSchemaField:
    """Returns the scalar-valued field |schema| declares under |field_name|."""
    return assert_scalar_valued_field(schema.get_field(field_name))


class LLMRequestOutputValuesTest(TestCase):
    """Tests for LLMRequestOutputValues."""

    def setUp(self) -> None:
        self.output_schema = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        ).extractor_collection.output_schema
        self.assignments_field = assert_type(
            self.output_schema.get_field("assignments"),
            ArrayOfStructLLMRequestOutputSchemaField,
        )

    def _output_values(self, output_json: dict[str, Any]) -> LLMRequestOutputValues:
        return LLMRequestOutputValues(
            output_schema=self.output_schema, output_json=output_json
        )

    def _full_output_values(self, **overrides: Any) -> LLMRequestOutputValues:
        """Returns output values whose JSON populates every field, overriding the
        result content key by key.
        """
        kwargs: dict[str, Any] = {
            "primary_status": "active",
            "status_note": "Currently active.",
            "location": "Kitchen",
            "assignments": [
                build_fake_extractor_assignment_result_json(
                    "Dish duty", "internal", 12.5, "hourly"
                )
            ],
        }
        kwargs.update(overrides)
        return self._output_values(
            wrap_in_result_key(build_fake_extractor_result_content(**kwargs))
        )

    def _malformed_output_values(
        self, **field_overrides: Any
    ) -> LLMRequestOutputValues:
        """Returns full output values with |field_overrides| written over the
        result envelope, for shapes `build_fake_extractor_result_content` cannot
        produce.
        """
        output_json = assert_type(self._full_output_values().output_json, dict)
        output_json[RESULT_KEY].update(field_overrides)
        return self._output_values(output_json)

    def test_value_for_inferred_field_unwraps_value(self) -> None:
        output_values = self._full_output_values()
        self.assertEqual(
            "Kitchen",
            output_values.value_for_field(
                field=_scalar_field(self.output_schema, "location")
            ),
        )

    def test_value_for_inferred_field_null_branch_is_none(self) -> None:
        output_values = self._full_output_values(location=None)
        self.assertIsNone(
            output_values.value_for_field(
                field=_scalar_field(self.output_schema, "location")
            )
        )

    def test_value_for_structural_field_is_bare(self) -> None:
        output_values = self._full_output_values()
        self.assertEqual(
            "Currently active.",
            output_values.value_for_field(
                field=_scalar_field(self.output_schema, "status_note")
            ),
        )
        self.assertIs(
            True,
            output_values.value_for_field(
                field=assert_type(
                    self.output_schema.is_relevant_field,
                    PrimitiveScalarLLMRequestOutputSchemaField,
                )
            ),
        )

    def test_value_for_absent_field_is_none(self) -> None:
        output_values = self._output_values(
            wrap_in_result_key(
                {
                    IS_RELEVANT_FIELD_NAME: True,
                    "status_note": "Currently active.",
                }
            )
        )
        self.assertIsNone(
            output_values.value_for_field(
                field=_scalar_field(self.output_schema, "location")
            )
        )

    def test_output_json_is_required(self) -> None:
        # "Nothing usable extracted" is the absence of an LLMRequestOutputValues,
        # not one wrapping no JSON, so the JSON itself is never null.
        with self.assertRaises(TypeError):
            LLMRequestOutputValues(
                output_schema=self.output_schema,
                output_json=None,  # type: ignore[arg-type]
            )

    def test_schema_without_is_relevant_reads_unwrapped_json(self) -> None:
        schema = _schema_without_is_relevant()
        output_values = LLMRequestOutputValues(
            output_schema=schema, output_json={"status_note": "Currently active."}
        )
        self.assertEqual(
            "Currently active.",
            output_values.value_for_field(field=_scalar_field(schema, "status_note")),
        )
        self.assertTrue(output_values.has_field("status_note"))

    def test_is_relevant_reads_field_value(self) -> None:
        self.assertIs(True, self._full_output_values().is_relevant)
        self.assertIs(
            False,
            self._output_values(
                {RESULT_KEY: {IS_RELEVANT_FIELD_NAME: False}}
            ).is_relevant,
        )

    def test_is_relevant_defaults_true_when_schema_has_no_relevance_field(self) -> None:
        # A schema with no relevance field means every document is relevant by
        # construction, so relevance defaults to True.
        schema = _schema_without_is_relevant()
        self.assertIs(
            True,
            LLMRequestOutputValues(
                output_schema=schema, output_json={"status_note": "Currently active."}
            ).is_relevant,
        )

    def test_is_relevant_absent_from_schema_that_declares_it_raises(self) -> None:
        # A schema declaring is_relevant always requires it, so an output that
        # omits it is malformed rather than "not relevant" or "unknown".
        output_values = self._output_values(
            {RESULT_KEY: {"status_note": "Currently active."}}
        )
        with self.assertRaisesRegex(
            LLMOutputParsingError,
            r"^Output JSON for a schema that declares \[is_relevant\] does not "
            r"carry it\. Found keys: \['status_note'\]\.$",
        ):
            _ = output_values.is_relevant

    def test_is_relevant_non_bool_raises(self) -> None:
        output_values = self._output_values(
            {RESULT_KEY: {IS_RELEVANT_FIELD_NAME: "true"}}
        )
        with self.assertRaisesRegex(
            LLMOutputParsingError,
            r"^Expected \[is_relevant\] to hold a bool, found \[<class 'str'>\]\.$",
        ):
            _ = output_values.is_relevant

    def test_malformed_result_envelope_raises_from_every_accessor(self) -> None:
        # Every accessor reads the extracted fields through the `result`
        # envelope, so a malformed envelope fails loudly out of all of them
        # rather than reading as "nothing produced" from some.
        accessors: dict[str, Callable[[LLMRequestOutputValues], Any]] = {
            "value_for_field": lambda values: values.value_for_field(
                field=_scalar_field(self.output_schema, "status_note")
            ),
            "has_field": lambda values: values.has_field("status_note"),
            "is_relevant": lambda values: values.is_relevant,
            "array_elements": lambda values: values.array_elements(
                field=self.assignments_field
            ),
        }
        for case, output_json in (
            ("missing envelope", {IS_RELEVANT_FIELD_NAME: True}),
            ("non-dict envelope", {RESULT_KEY: "not a dict"}),
        ):
            output_values = self._output_values(output_json)
            for accessor_name, accessor in accessors.items():
                with self.subTest(case=case, accessor=accessor_name):
                    with self.assertRaises(LLMOutputParsingError):
                        accessor(output_values)

    def test_has_field(self) -> None:
        output_values = self._full_output_values()
        self.assertTrue(output_values.has_field("assignments"))
        self.assertTrue(output_values.has_field("location"))
        self.assertFalse(output_values.has_field("not_a_field"))

    def test_array_elements_unwraps_sub_field_values(self) -> None:
        element_json = build_fake_extractor_assignment_result_json(
            "Dish duty", "internal", 12.5, "hourly"
        )
        element_json["rate_period"] = build_null_inferred_field_result_json()
        output_values = self._full_output_values(assignments=[element_json])
        self.assertEqual(
            [
                {
                    "assignment_name": "Dish duty",
                    "assignment_type": "internal",
                    "rate_amount": 12.5,
                    "rate_period": None,
                }
            ],
            output_values.array_elements(field=self.assignments_field),
        )

    def test_array_elements_absent_field_is_none(self) -> None:
        # An omitted array field is distinguishable from one the extractor
        # returned empty.
        output_values = self._output_values(
            wrap_in_result_key(
                {
                    IS_RELEVANT_FIELD_NAME: True,
                    "status_note": "Currently active.",
                }
            )
        )
        self.assertIsNone(output_values.array_elements(field=self.assignments_field))

    def test_array_elements_empty_field_is_empty_list(self) -> None:
        output_values = self._full_output_values(assignments=[])
        self.assertEqual([], output_values.array_elements(field=self.assignments_field))

    def test_missing_result_envelope_raises(self) -> None:
        output_values = self._output_values(
            {IS_RELEVANT_FIELD_NAME: True, "status_note": "Currently active."}
        )
        with self.assertRaisesRegex(
            LLMOutputParsingError,
            r"^Output JSON for a schema with a result envelope has no \[result\] key\. "
            r"Found keys: \['is_relevant', 'status_note'\]\.$",
        ):
            output_values.value_for_field(
                field=_scalar_field(self.output_schema, "status_note")
            )

    def test_non_dict_result_envelope_raises(self) -> None:
        output_values = self._output_values({RESULT_KEY: "not a dict"})
        with self.assertRaisesRegex(
            LLMOutputParsingError,
            r"^Expected the \[result\] envelope to hold a dict, found \[<class 'str'>\]\.$",
        ):
            output_values.value_for_field(
                field=_scalar_field(self.output_schema, "status_note")
            )

    def test_non_dict_inferred_field_wrapper_raises(self) -> None:
        # `location` is INFERRED, so its value must be the value/null_reason
        # wrapper rather than a bare scalar.
        output_values = self._malformed_output_values(location="Kitchen")
        with self.assertRaisesRegex(
            LLMOutputParsingError,
            r"^Expected INFERRED field \[location\] to hold a dict, found "
            r"\[<class 'str'>\]\.$",
        ):
            output_values.value_for_field(
                field=_scalar_field(self.output_schema, "location")
            )

    def test_inferred_field_wrapper_with_neither_value_nor_null_reason_raises(
        self,
    ) -> None:
        output_values = self._malformed_output_values(
            location={CONFIDENCE_LEVEL_FIELD_NAME: "explicit"}
        )
        with self.assertRaisesRegex(
            LLMOutputParsingError,
            r"^INFERRED field \[location\] holds neither a \[value\] nor a "
            r"\[null_reason\]\. Found keys: \['confidence_level'\]\.$",
        ):
            output_values.value_for_field(
                field=_scalar_field(self.output_schema, "location")
            )

    def test_non_list_array_field_raises(self) -> None:
        output_values = self._malformed_output_values(assignments={"not": "a list"})
        with self.assertRaisesRegex(
            LLMOutputParsingError,
            r"^Expected ARRAY_OF_STRUCT field \[assignments\] to hold a list, found "
            r"\[<class 'dict'>\]\.$",
        ):
            output_values.array_elements(field=self.assignments_field)

    def test_non_dict_array_element_raises(self) -> None:
        output_values = self._full_output_values(assignments=["not a dict"])
        with self.assertRaisesRegex(
            LLMOutputParsingError,
            r"^Expected element \[0\] of ARRAY_OF_STRUCT field \[assignments\] to "
            r"hold a dict, found \[<class 'str'>\]\.$",
        ):
            output_values.array_elements(field=self.assignments_field)

    def test_null_array_element_reads_as_all_null(self) -> None:
        element_json = build_fake_extractor_assignment_result_json(
            "Dish duty", "internal", 12.5, "hourly"
        )
        output_values = self._full_output_values(assignments=[element_json, None])
        self.assertEqual(
            {
                "assignment_name": None,
                "assignment_type": None,
                "rate_amount": None,
                "rate_period": None,
            },
            assert_type(
                output_values.array_elements(field=self.assignments_field), list
            )[1],
        )
