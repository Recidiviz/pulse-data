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
"""Tests for DocumentExtractionAnnotatableRecord."""
from typing import Any
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ConfidenceLevel,
)
from recidiviz.llm_eval.document_extraction.document_extraction_annotatable_record import (
    DocumentExtractionAnnotatableRecord,
)
from recidiviz.llm_eval.document_extraction.document_extraction_field_value import (
    DocumentExtractionFieldValue,
)

_STATE_CODE = StateCode.US_XX
_DOCUMENT_ID = "doc_a"
_VERSION = "v_sampled"


def _field_value(
    *,
    field_name: str,
    extracted_value: Any,
    array_field_name: str | None = None,
    source_array_index: int | None = None,
    confidence_level: ConfidenceLevel | None = ConfidenceLevel.EXPLICIT,
    document_contents_id: str = _DOCUMENT_ID,
) -> DocumentExtractionFieldValue:
    return DocumentExtractionFieldValue(
        state_code=_STATE_CODE,
        document_contents_id=document_contents_id,
        document_text="Client is on dish duty at $12.50 an hour.",
        extractor_version_id=_VERSION,
        field_name=field_name,
        array_field_name=array_field_name,
        source_array_index=source_array_index,
        extracted_value=extracted_value,
        confidence_level=confidence_level,
        array_element_json=(
            None if array_field_name is None else '{"assignment_name": "Dish duty"}'
        ),
    )


def _assignment_sub_field(
    *,
    field_name: str,
    extracted_value: Any,
    source_array_index: int,
    confidence_level: ConfidenceLevel | None = ConfidenceLevel.EXPLICIT,
) -> DocumentExtractionFieldValue:
    return _field_value(
        field_name=field_name,
        extracted_value=extracted_value,
        array_field_name="assignments",
        source_array_index=source_array_index,
        confidence_level=confidence_level,
    )


class FromFieldValuesTest(TestCase):
    """Tests how field values get grouped into records."""

    @staticmethod
    def _grouped_field_names(
        records: list[DocumentExtractionAnnotatableRecord],
    ) -> list[list[str]]:
        return [
            sorted(field_value.field_name for field_value in record.field_values)
            for record in records
        ]

    def test_groups_array_elements_and_leaves_top_level_fields_alone(self) -> None:
        records = DocumentExtractionAnnotatableRecord.from_field_values(
            [
                _field_value(field_name="primary_status", extracted_value="active"),
                _assignment_sub_field(
                    field_name="assignment_name",
                    extracted_value="Dish duty",
                    source_array_index=0,
                ),
                _assignment_sub_field(
                    field_name="rate_amount",
                    extracted_value=12.5,
                    source_array_index=0,
                ),
                _assignment_sub_field(
                    field_name="assignment_name",
                    extracted_value=None,
                    source_array_index=1,
                ),
                _assignment_sub_field(
                    field_name="rate_amount", extracted_value=None, source_array_index=1
                ),
            ]
        )
        self.assertEqual(
            [["primary_status"], ["assignment_name", "rate_amount"]],
            self._grouped_field_names(records),
        )
        self.assertEqual([False, True], [record.is_array_element for record in records])

    def test_element_with_no_populated_sub_fields_is_dropped(self) -> None:
        self.assertEqual(
            [],
            DocumentExtractionAnnotatableRecord.from_field_values(
                [
                    _assignment_sub_field(
                        field_name="assignment_name",
                        extracted_value=None,
                        source_array_index=0,
                    ),
                    _assignment_sub_field(
                        field_name="rate_amount",
                        extracted_value=None,
                        source_array_index=0,
                    ),
                ]
            ),
        )

    def test_element_with_one_populated_sub_field_is_still_an_array_element(
        self,
    ) -> None:
        """A record holding a single value can still have come from an array element, so
        how many values it holds says nothing about where it came from.
        """
        [record] = DocumentExtractionAnnotatableRecord.from_field_values(
            [
                _assignment_sub_field(
                    field_name="assignment_name",
                    extracted_value="Dish duty",
                    source_array_index=0,
                ),
                _assignment_sub_field(
                    field_name="rate_amount", extracted_value=None, source_array_index=0
                ),
            ]
        )
        self.assertEqual(["assignment_name"], self._grouped_field_names([record])[0])
        self.assertTrue(record.is_array_element)

    def test_elements_of_different_documents_are_separate_records(self) -> None:
        records = DocumentExtractionAnnotatableRecord.from_field_values(
            [
                _field_value(
                    field_name="assignment_name",
                    extracted_value="Dish duty",
                    array_field_name="assignments",
                    source_array_index=0,
                    document_contents_id="doc_a",
                ),
                _field_value(
                    field_name="assignment_name",
                    extracted_value="Laundry",
                    array_field_name="assignments",
                    source_array_index=0,
                    document_contents_id="doc_b",
                ),
            ]
        )
        self.assertEqual(
            ["doc_a", "doc_b"], [record.document_contents_id for record in records]
        )


class DocumentExtractionAnnotatableRecordTest(TestCase):
    """Tests a record's own invariants and accessors."""

    def test_empty_field_values_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Field \[field_values\] on \[DocumentExtractionAnnotatableRecord\] must "
            r"be a non-empty list",
        ):
            DocumentExtractionAnnotatableRecord(field_values=[])

    def test_mixing_two_elements_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Record mixes values from different array elements .*Each element is its "
            r"own record\.$",
        ):
            DocumentExtractionAnnotatableRecord(
                field_values=[
                    _assignment_sub_field(
                        field_name="assignment_name",
                        extracted_value="Dish duty",
                        source_array_index=0,
                    ),
                    _assignment_sub_field(
                        field_name="assignment_name",
                        extracted_value="Laundry",
                        source_array_index=1,
                    ),
                ]
            )

    def test_grouping_two_top_level_fields_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Record groups \['location', 'primary_status'\], which are top-level "
            r"fields\. Those are independently answerable, so each is its own record\.$",
        ):
            DocumentExtractionAnnotatableRecord(
                field_values=[
                    _field_value(field_name="primary_status", extracted_value="active"),
                    _field_value(field_name="location", extracted_value="Kitchen"),
                ]
            )

    def test_top_level_record_holding_a_null_is_null(self) -> None:
        record = DocumentExtractionAnnotatableRecord(
            field_values=[_field_value(field_name="location", extracted_value=None)]
        )
        self.assertTrue(record.is_null)

    def test_array_element_record_is_never_null(self) -> None:
        record = DocumentExtractionAnnotatableRecord(
            field_values=[
                _assignment_sub_field(
                    field_name="assignment_name",
                    extracted_value="Dish duty",
                    source_array_index=0,
                )
            ]
        )
        self.assertFalse(record.is_null)

    def test_has_inferred_confidence_when_any_value_is_inferred(self) -> None:
        record = DocumentExtractionAnnotatableRecord(
            field_values=[
                _assignment_sub_field(
                    field_name="assignment_name",
                    extracted_value="Dish duty",
                    source_array_index=0,
                    confidence_level=ConfidenceLevel.EXPLICIT,
                ),
                _assignment_sub_field(
                    field_name="rate_amount",
                    extracted_value=12.5,
                    source_array_index=0,
                    confidence_level=ConfidenceLevel.INFERRED,
                ),
            ]
        )
        self.assertTrue(record.has_inferred_confidence)

    def test_no_inferred_confidence_when_every_value_is_explicit(self) -> None:
        record = DocumentExtractionAnnotatableRecord(
            field_values=[
                _field_value(
                    field_name="location",
                    extracted_value="Kitchen",
                    confidence_level=ConfidenceLevel.EXPLICIT,
                )
            ]
        )
        self.assertFalse(record.has_inferred_confidence)
