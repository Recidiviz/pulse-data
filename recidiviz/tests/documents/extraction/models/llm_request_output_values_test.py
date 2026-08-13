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
import copy
from typing import Any, Callable
from unittest import TestCase

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.entity_resolution.entity_resolution_output_schema_builder import (
    build_entity_resolution_output_schema,
)
from recidiviz.documents.extraction.exceptions import LLMOutputParsingError
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_inferred_field_output import (
    InferredFieldOutput,
)
from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ArrayOfIntegerLLMRequestOutputSchemaField,
    ArrayOfStructLLMRequestOutputSchemaField,
    ConfidenceLevel,
    LLMOutputFieldType,
    PrimitiveScalarLLMRequestOutputSchemaField,
    ScalarValuedLLMRequestOutputSchemaField,
    assert_scalar_valued_field,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    CITATIONS_FIELD_NAME,
    CONFIDENCE_LEVEL_FIELD_NAME,
    ENTITIES_FIELD_NAME,
    ENTITY_ID_FIELD_NAME,
    ENTRY_NUMS_FIELD_NAME,
    IS_RELEVANT_FIELD_NAME,
    RESULT_KEY,
    VALUE_FIELD_NAME,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
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
    build_fake_extractor_result_content,
    build_inferred_field_result_json,
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


class _LLMRequestOutputValuesTestBase(TestCase):
    """Shared setup for the LLMRequestOutputValues tests: the fake collection's
    output schema, and builders for result JSON read against it."""

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


class LLMRequestOutputValuesTest(_LLMRequestOutputValuesTestBase):
    """Tests the value readers of LLMRequestOutputValues."""

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
            "values_dict": lambda values: values.values_dict,
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

    def test_values_dict_carries_every_schema_field(self) -> None:
        output_values = self._full_output_values()
        self.assertEqual(
            {
                IS_RELEVANT_FIELD_NAME: True,
                "primary_status": "active",
                "status_note": "Currently active.",
                "location": "Kitchen",
                "assignments": [
                    {
                        "assignment_name": "Dish duty",
                        "assignment_type": "internal",
                        "rate_amount": 12.5,
                        "rate_period": "hourly",
                    }
                ],
            },
            output_values.values_dict,
        )

    def test_values_dict_absent_fields_are_none(self) -> None:
        # Every field the schema declares appears in the dict, whether or not
        # the output carries it.
        output_values = self._output_values(
            wrap_in_result_key(
                {
                    IS_RELEVANT_FIELD_NAME: True,
                    "status_note": "Currently active.",
                }
            )
        )
        self.assertEqual(
            {
                IS_RELEVANT_FIELD_NAME: True,
                "primary_status": None,
                "status_note": "Currently active.",
                "location": None,
                "assignments": None,
            },
            output_values.values_dict,
        )

    def test_values_dict_for_schema_without_is_relevant(self) -> None:
        schema = _schema_without_is_relevant()
        output_values = LLMRequestOutputValues(
            output_schema=schema, output_json={"status_note": "Currently active."}
        )
        self.assertEqual(
            {"status_note": "Currently active."}, output_values.values_dict
        )

    def test_values_dict_reads_integer_array_field(self) -> None:
        # A top-level ARRAY_OF_INTEGER field reads as its bare integer list,
        # same as an ARRAY_OF_INTEGER sub-field of an ARRAY_OF_STRUCT element.
        schema = attr.evolve(
            _schema_without_is_relevant(),
            user_defined_fields=[
                ArrayOfIntegerLLMRequestOutputSchemaField(
                    name="entry_nums",
                    description="The entry numbers this entity was resolved from.",
                    required=True,
                    inferred_field_config=None,
                )
            ],
        )
        output_values = LLMRequestOutputValues(
            output_schema=schema, output_json={"entry_nums": [1, 2]}
        )
        self.assertEqual({"entry_nums": [1, 2]}, output_values.values_dict)

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


class LLMRequestOutputValuesPresenceTest(_LLMRequestOutputValuesTestBase):
    """Tests LLMRequestOutputValues.is_value_present, which reports whether a
    top-level field carries a value without reading into an array's elements."""

    def _is_present(
        self, output_values: LLMRequestOutputValues, field_name: str
    ) -> bool:
        return output_values.is_value_present(
            field=self.output_schema.get_field(field_name)
        )

    def test_populated_fields_are_present(self) -> None:
        output_values = self._full_output_values()
        for field_name in ("primary_status", "status_note", "location", "assignments"):
            with self.subTest(field_name=field_name):
                self.assertTrue(self._is_present(output_values, field_name))

    def test_empty_scalar_is_present(self) -> None:
        # Emptiness and falsiness are not nullness for anything but an array: an
        # empty string is a value the model returned.
        self.assertTrue(
            self._is_present(self._full_output_values(status_note=""), "status_note")
        )

    def test_null_branch_field_is_absent(self) -> None:
        self.assertFalse(
            self._is_present(self._full_output_values(location=None), "location")
        )

    def test_empty_array_is_present(self) -> None:
        # An empty array is the extractor's affirmative statement that there are
        # no values for the field — unlike an omitted key, which says nothing.
        self.assertTrue(
            self._is_present(self._full_output_values(assignments=[]), "assignments")
        )

    def test_field_omitted_from_json_is_absent(self) -> None:
        output_json = assert_type(self._full_output_values().output_json, dict)
        del output_json[RESULT_KEY]["location"]
        del output_json[RESULT_KEY]["assignments"]
        output_values = self._output_values(output_json)
        self.assertFalse(self._is_present(output_values, "location"))
        self.assertFalse(self._is_present(output_values, "assignments"))

    def test_array_of_all_null_elements_is_present(self) -> None:
        # Presence is about the array itself, not what its elements hold — the
        # accessor never reads into them, which is what makes it safe for arrays
        # whose sub-fields the scalar readers cannot unwrap.
        self.assertTrue(
            self._is_present(
                self._full_output_values(assignments=[None]), "assignments"
            )
        )

    def test_non_list_array_field_raises(self) -> None:
        output_values = self._malformed_output_values(assignments={"not": "a list"})
        with self.assertRaisesRegex(
            LLMOutputParsingError,
            r"^Expected array field \[assignments\] to hold a list, found "
            r"\[<class 'dict'>\]\.$",
        ):
            self._is_present(output_values, "assignments")


class LLMRequestOutputValuesInferredFieldOutputsTest(_LLMRequestOutputValuesTestBase):
    """Tests LLMRequestOutputValues.inferred_field_outputs, the walk over the
    companion-metadata wrappers the validation checks read."""

    def _display_names(self, output_values: LLMRequestOutputValues) -> list[str]:
        return [
            field_output.display_name
            for field_output in output_values.inferred_field_outputs()
        ]

    def test_walks_top_level_and_array_sub_fields(self) -> None:
        # `status_note` is STRUCTURAL and `assignments` is an ARRAY_OF_STRUCT
        # emitted as a bare array, so neither carries a wrapper of its own —
        # only the array's sub-fields do, named by element index.
        self.assertEqual(
            [
                "primary_status",
                "location",
                "assignments[0].assignment_name",
                "assignments[0].assignment_type",
                "assignments[0].rate_amount",
                "assignments[0].rate_period",
            ],
            self._display_names(self._full_output_values()),
        )

    def test_absent_fields_skipped(self) -> None:
        output_json = assert_type(self._full_output_values().output_json, dict)
        del output_json[RESULT_KEY]["location"]
        del output_json[RESULT_KEY]["assignments"]
        self.assertEqual(
            ["primary_status"], self._display_names(self._output_values(output_json))
        )

    def test_every_array_element_walked(self) -> None:
        output_values = self._full_output_values(
            assignments=[
                build_fake_extractor_assignment_result_json(
                    "Dish duty", "internal", 12.5, "hourly"
                ),
                build_fake_extractor_assignment_result_json(
                    "Laundry", "external", 20.0, "monthly"
                ),
            ]
        )
        self.assertEqual(
            [
                "assignments[0].assignment_name",
                "assignments[1].assignment_name",
            ],
            [
                display_name
                for display_name in self._display_names(output_values)
                if display_name.endswith("assignment_name")
            ],
        )

    def _field_output(
        self, output_values: LLMRequestOutputValues, display_name: str
    ) -> InferredFieldOutput:
        [field_output] = [
            candidate
            for candidate in output_values.inferred_field_outputs()
            if candidate.display_name == display_name
        ]
        return field_output

    def test_value_branch_output_read(self) -> None:
        field_output = self._field_output(self._full_output_values(), "location")
        self.assertTrue(field_output.has_value)
        self.assertEqual("Kitchen", field_output.value)
        self.assertEqual(ConfidenceLevel.INFERRED, field_output.confidence_level)
        self.assertIsNone(field_output.adversarial_interpretation)

    def test_null_branch_output_read(self) -> None:
        field_output = self._field_output(
            self._full_output_values(location=None), "location"
        )
        self.assertFalse(field_output.has_value)
        self.assertIsNone(field_output.value)
        self.assertEqual(ConfidenceLevel.EXPLICIT, field_output.confidence_level)
        self.assertIsNone(field_output.adversarial_interpretation)

    def test_null_out_value_writes_through_to_output_json(self) -> None:
        # The views read a field's value at `$.<field>.value`, so nulling that key
        # is what withholds the value downstream; the companion metadata beside it
        # is deliberately left in place.
        output_values = self._full_output_values()
        field_output = self._field_output(output_values, "location")
        field_output.null_out_value()

        location_json = assert_type(output_values.output_json, dict)[RESULT_KEY][
            "location"
        ]
        self.assertIsNone(location_json[VALUE_FIELD_NAME])
        self.assertEqual("inferred", location_json[CONFIDENCE_LEVEL_FIELD_NAME])
        self.assertTrue(location_json[CITATIONS_FIELD_NAME])

    def test_null_out_value_on_null_branch_raises(self) -> None:
        field_output = self._field_output(
            self._full_output_values(location=None), "location"
        )
        with self.assertRaisesRegex(
            ValueError,
            r"^Cannot null out field \[location\], which took its null branch",
        ):
            field_output.null_out_value()

    def test_structural_field_rejected(self) -> None:
        # `status_note` is STRUCTURAL, so it carries none of the companion
        # metadata this view exists to read — building one over it is a caller
        # bug, not a malformed result.
        with self.assertRaisesRegex(
            ValueError,
            r"^field \[status_note\] is STRUCTURAL, but this view reads the "
            r"companion metadata only an INFERRED field carries\.$",
        ):
            InferredFieldOutput(
                field=self.output_schema.get_field("status_note"),
                display_name="status_note",
                field_json={},
            )

    def test_wrapper_with_neither_branch_raises(self) -> None:
        # A wrapper carrying neither key matches neither branch of the field's
        # anyOf, so it must not read as the null branch — that would pass the
        # field off as "nothing to extract" when the extractor never said so.
        field_output = self._field_output(
            self._malformed_output_values(
                location={CONFIDENCE_LEVEL_FIELD_NAME: "explicit"}
            ),
            "location",
        )
        with self.assertRaisesRegex(
            LLMOutputParsingError,
            r"^INFERRED field \[location\] holds neither a \[value\] nor a "
            r"\[null_reason\]\. Found keys: \['confidence_level'\]\.$",
        ):
            _ = field_output.has_value

    def test_wrapper_missing_confidence_level_raises(self) -> None:
        wrapper = build_inferred_field_result_json("Kitchen")
        del wrapper[CONFIDENCE_LEVEL_FIELD_NAME]
        field_output = self._field_output(
            self._malformed_output_values(location=wrapper), "location"
        )
        with self.assertRaisesRegex(
            LLMOutputParsingError,
            r"^INFERRED field \[location\] holds no \[confidence_level\]\. Found "
            r"keys: \['adversarial_interpretation', 'citations', 'value'\]\.$",
        ):
            _ = field_output.confidence_level

    def test_wrapper_with_unrecognized_confidence_level_raises(self) -> None:
        field_output = self._field_output(
            self._malformed_output_values(
                location=build_inferred_field_result_json(
                    "Kitchen", confidence_level="pretty_sure"
                )
            ),
            "location",
        )
        with self.assertRaisesRegex(
            LLMOutputParsingError,
            r"^INFERRED field \[location\] holds an unrecognized "
            r"\[confidence_level\] of \[pretty_sure\]\.$",
        ):
            _ = field_output.confidence_level

    def test_non_dict_inferred_field_wrapper_raises(self) -> None:
        output_values = self._malformed_output_values(location="Kitchen")
        with self.assertRaisesRegex(
            LLMOutputParsingError,
            r"^Expected INFERRED field \[location\] to hold a dict, found "
            r"\[<class 'str'>\]\.$",
        ):
            output_values.inferred_field_outputs()

    def test_non_list_array_field_raises(self) -> None:
        output_values = self._malformed_output_values(assignments={"not": "a list"})
        with self.assertRaisesRegex(
            LLMOutputParsingError,
            r"^Expected ARRAY_OF_STRUCT field \[assignments\] to hold a list, found "
            r"\[<class 'dict'>\]\.$",
        ):
            output_values.inferred_field_outputs()

    def test_non_dict_array_element_raises(self) -> None:
        output_values = self._full_output_values(assignments=["not a dict"])
        with self.assertRaisesRegex(
            LLMOutputParsingError,
            r"^Expected element \[0\] of ARRAY_OF_STRUCT field \[assignments\] to "
            r"hold a dict, found \[<class 'str'>\]\.$",
        ):
            output_values.inferred_field_outputs()


class LLMRequestOutputValuesDeepCopyTest(_LLMRequestOutputValuesTestBase):
    """Tests LLMRequestOutputValues.deep_copy, which is what lets the validation
    quality filters edit a validated copy without touching the raw output."""

    def test_copy_equals_the_original(self) -> None:
        output_values = self._full_output_values()
        self.assertEqual(output_values, output_values.deep_copy())

    def test_editing_the_copy_leaves_the_original_alone(self) -> None:
        output_values = self._full_output_values()
        before = copy.deepcopy(output_values.output_json)
        copied = output_values.deep_copy()

        for field_output in copied.inferred_field_outputs():
            if field_output.has_value:
                field_output.null_out_value()

        self.assertEqual(before, output_values.output_json)
        self.assertNotEqual(before, copied.output_json)

    def test_copy_shares_the_output_schema(self) -> None:
        output_values = self._full_output_values()
        self.assertIs(
            output_values.output_schema, output_values.deep_copy().output_schema
        )


class LLMRequestOutputValuesEntityResolutionSchemaTest(TestCase):
    """Tests reads against a synthesized entity-resolution schema, whose
    `entities` elements carry an ARRAY_OF_INTEGER `entry_nums` sub-field that the
    scalar readers cannot unwrap. The ER schema declares no is_relevant, so its
    result JSON carries the extracted fields directly with no `result` envelope.
    """

    def setUp(self) -> None:
        self.output_schema = build_entity_resolution_output_schema(
            entity_group=get_entity_group_by_name(
                fake_first_order_collection(), "location"
            )
        )
        self.entities_field = assert_type(
            self.output_schema.get_field(ENTITIES_FIELD_NAME),
            ArrayOfStructLLMRequestOutputSchemaField,
        )

    def _output_values(self, entities: list[dict[str, Any]]) -> LLMRequestOutputValues:
        return LLMRequestOutputValues(
            output_schema=self.output_schema,
            output_json=build_fake_entity_resolution_result_content(entities),
        )

    def test_array_elements_reads_entry_nums_list(self) -> None:
        output_values = self._output_values(
            [
                build_fake_entity_resolution_entity_result_json(
                    1, entry_nums=[1, 3], location="HQ"
                ),
                build_fake_entity_resolution_entity_result_json(
                    2, entry_nums=[2], location="Annex"
                ),
            ]
        )
        self.assertEqual(
            [
                {
                    ENTITY_ID_FIELD_NAME: 1,
                    "location": "HQ",
                    ENTRY_NUMS_FIELD_NAME: [1, 3],
                },
                {
                    ENTITY_ID_FIELD_NAME: 2,
                    "location": "Annex",
                    ENTRY_NUMS_FIELD_NAME: [2],
                },
            ],
            output_values.array_elements(field=self.entities_field),
        )

    def test_omitted_entry_nums_is_none(self) -> None:
        # An element omitting the field (as an older extractor version's output
        # would) reads as None, same as an omitted scalar sub-field.
        output_values = self._output_values(
            [{ENTITY_ID_FIELD_NAME: 1, "location": "HQ"}]
        )
        [element] = assert_type(
            output_values.array_elements(field=self.entities_field), list
        )
        self.assertIsNone(element[ENTRY_NUMS_FIELD_NAME])

    def test_values_dict_carries_entities(self) -> None:
        output_values = self._output_values(
            [
                build_fake_entity_resolution_entity_result_json(
                    1, entry_nums=[1, 3], location="HQ"
                )
            ]
        )
        self.assertEqual(
            {
                ENTITIES_FIELD_NAME: [
                    {
                        ENTITY_ID_FIELD_NAME: 1,
                        "location": "HQ",
                        ENTRY_NUMS_FIELD_NAME: [1, 3],
                    }
                ]
            },
            output_values.values_dict,
        )

    def test_non_list_entry_nums_raises(self) -> None:
        output_values = self._output_values(
            [
                build_fake_entity_resolution_entity_result_json(
                    1, entry_nums=5, location="HQ"  # type: ignore[arg-type]
                )
            ]
        )
        with self.assertRaisesRegex(
            LLMOutputParsingError,
            r"^Expected ARRAY_OF_INTEGER field \[entry_nums\] to hold a list, "
            r"found \[<class 'int'>\]\.$",
        ):
            output_values.array_elements(field=self.entities_field)
