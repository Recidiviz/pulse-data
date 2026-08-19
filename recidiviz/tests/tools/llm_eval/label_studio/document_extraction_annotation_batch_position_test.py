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
"""Tests for DocumentExtractionAnnotationBatchPosition."""
from unittest import TestCase

from recidiviz.tools.llm_eval.label_studio.document_extraction_annotation_batch_position import (
    DocumentExtractionAnnotationBatchPosition,
)


class DocumentExtractionAnnotationBatchPositionTest(TestCase):
    """Tests the positions assigned across a batch of annotation tasks."""

    def test_positions_for_non_adjacent_repeats(self) -> None:
        self.assertEqual(
            [
                DocumentExtractionAnnotationBatchPosition(
                    doc_index=1, field_index=1, total_fields=3, task_order=1
                ),
                DocumentExtractionAnnotationBatchPosition(
                    doc_index=1, field_index=2, total_fields=3, task_order=2
                ),
                DocumentExtractionAnnotationBatchPosition(
                    doc_index=2, field_index=1, total_fields=1, task_order=3
                ),
                DocumentExtractionAnnotationBatchPosition(
                    doc_index=1, field_index=3, total_fields=3, task_order=4
                ),
            ],
            DocumentExtractionAnnotationBatchPosition.for_document_id_sequence(
                ["doc_a", "doc_a", "doc_b", "doc_a"]
            ),
        )

    def test_empty_sequence_yields_no_positions(self) -> None:
        self.assertEqual(
            [], DocumentExtractionAnnotationBatchPosition.for_document_id_sequence([])
        )

    def test_field_index_past_total_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Task is field \[3\] of \[2\] for its document, which counts past the "
            r"total\.$",
        ):
            DocumentExtractionAnnotationBatchPosition(
                doc_index=1, field_index=3, total_fields=2, task_order=1
            )

    def test_zero_index_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Field \[doc_index\] on \[DocumentExtractionAnnotationBatchPosition\] "
            r"must be a positive integer\. Found value \[0\]$",
        ):
            DocumentExtractionAnnotationBatchPosition(
                doc_index=0, field_index=1, total_fields=1, task_order=1
            )
