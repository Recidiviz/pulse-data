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
"""Tests for the annotatable field helpers."""
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
    ConfidenceLevel,
    InferredFieldConfig,
    LLMOutputFieldType,
    PrimitiveScalarLLMRequestOutputSchemaField,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    IS_RELEVANT_FIELD_NAME,
)
from recidiviz.llm_eval.document_extraction.document_extraction_annotatable_field_utils import (
    annotatable_field_description,
    annotatable_field_names,
    scalar_top_level_annotatable_field_names,
)
from recidiviz.tests.documents import fake_config
from recidiviz.utils.types import assert_type

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"


def _schema_with_structural_only_array() -> LLMRequestOutputSchema:
    """Returns a schema whose one array field has no INFERRED sub-field, so the array holds
    nothing an annotator can be asked about.
    """
    return LLMRequestOutputSchema(
        full_batch_description="Batch of parsed fake documents for testing.",
        result_level_description="One parsed fake document for testing.",
        relevance_criteria=None,
        user_defined_fields=[
            PrimitiveScalarLLMRequestOutputSchemaField(
                name="primary_status",
                description="The primary status described in the document.",
                required=True,
                inferred_field_config=InferredFieldConfig(
                    minimum_confidence_level=ConfidenceLevel.INFERRED,
                    semantic_consistency_constraints=[],
                ),
                scalar_type=LLMOutputFieldType.STRING,
            ),
            ArrayOfStructLLMRequestOutputSchemaField(
                name="model_notes",
                description="Notes the model composed about the document.",
                required=False,
                inferred_field_config=None,
                fields=[
                    PrimitiveScalarLLMRequestOutputSchemaField(
                        name="note_text",
                        description="A note the model composed.",
                        required=True,
                        inferred_field_config=None,
                        scalar_type=LLMOutputFieldType.STRING,
                    )
                ],
                primary_keys=["note_text"],
                min_items=None,
            ),
        ],
    )


class AnnotatableFieldNamesTest(TestCase):
    """Tests the set of field names an annotated value can be about."""

    def setUp(self) -> None:
        self.output_schema = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        ).extractor_collection.output_schema

    def test_names_from_schema(self) -> None:
        # status_note is absent because it is STRUCTURAL. assignments is present because a
        # document whose array came back empty gets a value annotated on the array field.
        self.assertEqual(
            {
                "primary_status",
                "location",
                "assignments",
                "assignment_name",
                "assignment_type",
                "rate_amount",
                "rate_period",
            },
            annotatable_field_names(self.output_schema),
        )

    def test_array_with_no_inferred_sub_fields_is_excluded(self) -> None:
        self.assertEqual(
            {"primary_status"},
            annotatable_field_names(_schema_with_structural_only_array()),
        )

    def test_is_relevant_is_not_annotatable(self) -> None:
        """The framework-injected is_relevant field is STRUCTURAL, so no task asks an
        annotator whether the extractor was right to judge the document relevant.
        """
        is_relevant_field = assert_type(
            self.output_schema.is_relevant_field,
            PrimitiveScalarLLMRequestOutputSchemaField,
        )
        self.assertFalse(is_relevant_field.is_inferred_field)
        self.assertNotIn(
            IS_RELEVANT_FIELD_NAME, annotatable_field_names(self.output_schema)
        )


class ScalarTopLevelAnnotatableFieldNamesTest(TestCase):
    """Tests the set of top-level scalar field names, which a document holds exactly one
    annotated value under.
    """

    def setUp(self) -> None:
        self.output_schema = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        ).extractor_collection.output_schema

    def test_names_from_schema(self) -> None:
        # assignments stands at the top level but is absent, because a document that
        # populated the array holds no value under the array's own name. Its sub-fields are
        # absent because they sit inside array elements.
        self.assertEqual(
            {"primary_status", "location"},
            scalar_top_level_annotatable_field_names(self.output_schema),
        )

    def test_array_with_no_inferred_sub_fields_is_excluded(self) -> None:
        self.assertEqual(
            {"primary_status"},
            scalar_top_level_annotatable_field_names(
                _schema_with_structural_only_array()
            ),
        )


class AnnotatableFieldDescriptionTest(TestCase):
    """Tests the description an annotator is shown for one field."""

    def setUp(self) -> None:
        self.output_schema = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        ).extractor_collection.output_schema

    def test_description_for_top_level_field(self) -> None:
        self.assertEqual(
            "The location associated with the record.",
            annotatable_field_description(
                self.output_schema, field_name="location", array_field_name=None
            ),
        )

    def test_enum_description_spells_out_every_allowed_value(self) -> None:
        self.assertEqual(
            "The primary status described in the document. Allowed values:\n"
            "- active: The record is currently active.\n"
            "- inactive: The record is currently inactive.\n"
            "- other: A status that is neither active nor inactive.",
            annotatable_field_description(
                self.output_schema, field_name="primary_status", array_field_name=None
            ),
        )

    def test_description_for_array_sub_field(self) -> None:
        self.assertEqual(
            "Name of the assignment.",
            annotatable_field_description(
                self.output_schema,
                field_name="assignment_name",
                array_field_name="assignments",
            ),
        )

    def test_description_for_the_array_field_itself(self) -> None:
        self.assertEqual(
            "Assignments mentioned in the document.",
            annotatable_field_description(
                self.output_schema, field_name="assignments", array_field_name=None
            ),
        )

    def test_non_array_parent_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Field \[location\] is not an ARRAY_OF_STRUCT field, so it has no "
            r"sub-field \[assignment_name\] to describe\.$",
        ):
            annotatable_field_description(
                self.output_schema,
                field_name="assignment_name",
                array_field_name="location",
            )
