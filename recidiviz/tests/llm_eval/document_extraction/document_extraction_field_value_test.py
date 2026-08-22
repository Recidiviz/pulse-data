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
"""Tests for DocumentExtractionFieldValue."""
from typing import Any
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ConfidenceLevel,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.llm_eval.document_extraction.document_extraction_field_value import (
    DocumentExtractionFieldValue,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    build_fake_extractor_assignment_result_json,
    build_fake_extractor_irrelevant_result_content,
    build_fake_extractor_result_content,
    build_inferred_field_result_json,
    build_null_inferred_field_result_json,
    wrap_in_result_key,
)

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
_DOCUMENT_ID = "doc_a"
_DOCUMENT_TEXT = "Client is on dish duty at $12.50 an hour."
_VERSION = "v_sampled"


def _field_value(
    *,
    field_name: str = "location",
    array_field_name: str | None = None,
    source_array_index: int | None = None,
    extracted_value: Any = "Kitchen",
    confidence_level: ConfidenceLevel | None = ConfidenceLevel.EXPLICIT,
    array_element_json: str | None = None,
    document_contents_id: str = _DOCUMENT_ID,
) -> DocumentExtractionFieldValue:
    """Returns a field value built directly, for tests about one of its own accessors."""
    return DocumentExtractionFieldValue(
        state_code=_STATE_CODE,
        document_contents_id=document_contents_id,
        document_text=_DOCUMENT_TEXT,
        extractor_version_id=_VERSION,
        field_name=field_name,
        array_field_name=array_field_name,
        source_array_index=source_array_index,
        extracted_value=extracted_value,
        confidence_level=confidence_level,
        array_element_json=array_element_json,
    )


class FromExtractionResultTest(TestCase):
    """Tests the field values read out of one document's extraction result."""

    def setUp(self) -> None:
        self.output_schema = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        ).extractor_collection.output_schema

    def _field_values(
        self, result_content: dict[str, Any]
    ) -> list[DocumentExtractionFieldValue]:
        return DocumentExtractionFieldValue.from_extraction_result(
            output_values=LLMRequestOutputValues(
                output_schema=self.output_schema,
                output_json=wrap_in_result_key(result_content),
            ),
            state_code=_STATE_CODE,
            document_contents_id=_DOCUMENT_ID,
            document_text=_DOCUMENT_TEXT,
            extractor_version_id=_VERSION,
        )

    @staticmethod
    def _coordinates(
        field_values: list[DocumentExtractionFieldValue],
    ) -> list[tuple[str, str | None, int | None, Any]]:
        return [
            (
                field_value.field_name,
                field_value.array_field_name,
                field_value.source_array_index,
                field_value.extracted_value,
            )
            for field_value in field_values
        ]

    def _result_with_assignments(
        self, assignments: list[dict[str, Any]]
    ) -> dict[str, Any]:
        return build_fake_extractor_result_content(
            primary_status="active",
            status_note="Currently on dish duty.",
            location="Kitchen",
            assignments=assignments,
        )

    def test_reads_top_level_and_array_element_values(self) -> None:
        field_values = self._field_values(
            self._result_with_assignments(
                [
                    build_fake_extractor_assignment_result_json(
                        "Dish duty", "internal", 12.5, "hourly"
                    )
                ]
            )
        )
        # status_note is STRUCTURAL, so it carries no companion metadata and is not
        # annotated. assignments itself is skipped because it came back populated.
        self.assertEqual(
            [
                ("primary_status", None, None, "active"),
                ("location", None, None, "Kitchen"),
                ("assignment_name", "assignments", 0, "Dish duty"),
                ("assignment_type", "assignments", 0, "internal"),
                ("rate_amount", "assignments", 0, 12.5),
                ("rate_period", "assignments", 0, "hourly"),
            ],
            self._coordinates(field_values),
        )

    def test_every_array_element_is_read(self) -> None:
        field_values = self._field_values(
            self._result_with_assignments(
                [
                    build_fake_extractor_assignment_result_json(
                        "Dish duty", "internal", 12.5, "hourly"
                    ),
                    build_fake_extractor_assignment_result_json(
                        "Laundry", "external", 20.0, "monthly"
                    ),
                ]
            )
        )
        self.assertEqual(
            [
                ("assignment_name", "assignments", 0, "Dish duty"),
                ("assignment_name", "assignments", 1, "Laundry"),
            ],
            [
                coordinates
                for coordinates in self._coordinates(field_values)
                if coordinates[0] == "assignment_name"
            ],
        )

    def test_empty_array_field_yields_one_value_on_the_array_field(self) -> None:
        [assignments_value] = [
            field_value
            for field_value in self._field_values(self._result_with_assignments([]))
            if field_value.field_name == "assignments"
        ]
        self.assertIsNone(assignments_value.extracted_value)
        self.assertTrue(assignments_value.is_null)
        # An ARRAY_OF_STRUCT field carries no companion-metadata wrapper of its own, so
        # there is no confidence to report and no element to show.
        self.assertIsNone(assignments_value.confidence_level)
        self.assertIsNone(assignments_value.array_element_json)
        self.assertIsNone(assignments_value.array_field_name)

    def test_irrelevant_result_yields_nothing(self) -> None:
        # An irrelevant result is exactly `{"is_relevant": false}`: it carries none
        # of the fields the schema declares, so it holds nothing to annotate.
        self.assertEqual(
            [],
            self._field_values(build_fake_extractor_irrelevant_result_content()),
        )

    def test_array_element_json_holds_every_sub_field_including_nulls(self) -> None:
        field_values = self._field_values(
            self._result_with_assignments(
                [
                    {
                        "assignment_name": build_inferred_field_result_json("Laundry"),
                        "assignment_type": build_null_inferred_field_result_json(),
                        "rate_amount": build_null_inferred_field_result_json(),
                        "rate_period": build_null_inferred_field_result_json(),
                    }
                ]
            )
        )
        [name_value] = [
            field_value
            for field_value in field_values
            if field_value.field_name == "assignment_name"
        ]
        self.assertEqual(
            '{"assignment_name": "Laundry", "assignment_type": null, '
            '"rate_amount": null, "rate_period": null}',
            name_value.array_element_json,
        )

    def test_array_element_json_is_none_for_a_top_level_field(self) -> None:
        [location_value] = [
            field_value
            for field_value in self._field_values(self._result_with_assignments([]))
            if field_value.field_name == "location"
        ]
        self.assertIsNone(location_value.array_element_json)

    def test_null_top_level_field_is_read_as_null(self) -> None:
        [location_value] = [
            field_value
            for field_value in self._field_values(
                build_fake_extractor_result_content(
                    primary_status="active",
                    status_note="No location given.",
                    location=None,
                    assignments=[],
                )
            )
            if field_value.field_name == "location"
        ]
        self.assertIsNone(location_value.extracted_value)
        self.assertTrue(location_value.is_null)
        self.assertEqual(ConfidenceLevel.EXPLICIT, location_value.confidence_level)

    def test_confidence_level_is_read_per_field(self) -> None:
        field_values = self._field_values(
            self._result_with_assignments(
                [
                    build_fake_extractor_assignment_result_json(
                        "Dish duty", "internal", 12.5, "hourly"
                    )
                ]
            )
        )
        self.assertEqual(
            {
                "primary_status": ConfidenceLevel.EXPLICIT,
                "location": ConfidenceLevel.INFERRED,
                "assignment_name": ConfidenceLevel.EXPLICIT,
                "assignment_type": ConfidenceLevel.EXPLICIT,
                "rate_amount": ConfidenceLevel.EXPLICIT,
                "rate_period": ConfidenceLevel.EXPLICIT,
            },
            {
                field_value.field_name: field_value.confidence_level
                for field_value in field_values
            },
        )


class DocumentExtractionFieldValueTest(TestCase):
    """Tests the accessors a field value derives from what it holds."""

    def test_paired_coordinates_required_together(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Field value \[job_title\] of document \[doc_a\] sets exactly one of "
            r"array_field_name \[employers\] and source_array_index \[None\]\.",
        ):
            _field_value(
                field_name="job_title",
                array_field_name="employers",
                source_array_index=None,
            )

    def test_index_without_array_field_name_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Field value \[job_title\] of document \[doc_a\] sets exactly one of "
            r"array_field_name \[None\] and source_array_index \[0\]\.",
        ):
            _field_value(
                field_name="job_title", array_field_name=None, source_array_index=0
            )

    def test_display_group_names_the_element(self) -> None:
        self.assertEqual(
            "assignments[1]",
            _field_value(
                array_field_name="assignments", source_array_index=1
            ).display_group,
        )

    def test_display_group_is_empty_for_a_top_level_field(self) -> None:
        self.assertEqual("", _field_value().display_group)

    def test_file_safe_field_id_for_an_array_element(self) -> None:
        self.assertEqual(
            "assignments_1__rate_amount",
            _field_value(
                field_name="rate_amount",
                array_field_name="assignments",
                source_array_index=1,
            ).file_safe_field_id,
        )

    def test_file_safe_field_id_for_a_top_level_field(self) -> None:
        self.assertEqual(
            "location", _field_value(field_name="location").file_safe_field_id
        )

    def test_array_element_key_shared_across_one_elements_sub_fields(self) -> None:
        first_sub_field = _field_value(
            field_name="assignment_name",
            array_field_name="assignments",
            source_array_index=0,
        )
        second_sub_field = _field_value(
            field_name="rate_amount",
            array_field_name="assignments",
            source_array_index=0,
        )
        other_element = _field_value(
            field_name="assignment_name",
            array_field_name="assignments",
            source_array_index=1,
        )
        self.assertEqual(("doc_a", "assignments", 0), first_sub_field.array_element_key)
        self.assertEqual(
            first_sub_field.array_element_key, second_sub_field.array_element_key
        )
        self.assertNotEqual(
            first_sub_field.array_element_key, other_element.array_element_key
        )

    def test_array_element_key_is_none_for_a_top_level_field(self) -> None:
        self.assertIsNone(_field_value().array_element_key)

    def test_extracted_value_str_renders_non_strings(self) -> None:
        self.assertEqual("12.5", _field_value(extracted_value=12.5).extracted_value_str)
        self.assertEqual("True", _field_value(extracted_value=True).extracted_value_str)
        self.assertIsNone(_field_value(extracted_value=None).extracted_value_str)

    def test_display_value_substitutes_the_sentinel_for_a_null(self) -> None:
        self.assertEqual(
            "(no value extracted)",
            _field_value(extracted_value=None).display_value(
                null_sentinel="(no value extracted)"
            ),
        )

    def test_display_value_passes_an_extracted_value_through(self) -> None:
        self.assertEqual(
            "Kitchen",
            _field_value(extracted_value="Kitchen").display_value(
                null_sentinel="(no value extracted)"
            ),
        )

    def test_has_inferred_confidence(self) -> None:
        self.assertTrue(
            _field_value(
                confidence_level=ConfidenceLevel.INFERRED
            ).has_inferred_confidence
        )
        self.assertFalse(
            _field_value(
                confidence_level=ConfidenceLevel.EXPLICIT
            ).has_inferred_confidence
        )
        self.assertFalse(_field_value(confidence_level=None).has_inferred_confidence)
