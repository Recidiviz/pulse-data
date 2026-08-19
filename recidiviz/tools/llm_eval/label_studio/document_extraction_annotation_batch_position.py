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
"""Where one annotation task sits in the batch it was exported with."""
from collections import Counter

import attr

from recidiviz.common import attr_validators


@attr.define(frozen=True, kw_only=True)
class DocumentExtractionAnnotationBatchPosition:
    """Where one annotation task sits in the batch it was exported with, which an annotator
    reads as "document 12, field 3 of 7". It orients someone working through a queue and says
    nothing about what was extracted.

    Field names match the task.data keys they are written under.
    """

    doc_index: int = attr.ib(validator=attr_validators.is_positive_int)
    """1-indexed position of this task's document within the batch."""

    field_index: int = attr.ib(validator=attr_validators.is_positive_int)
    """1-indexed position of this task among those asked about the same document."""

    total_fields: int = attr.ib(validator=attr_validators.is_positive_int)
    """How many tasks the batch asks about this task's document."""

    task_order: int = attr.ib(validator=attr_validators.is_positive_int)
    """1-indexed position of this task within the whole batch."""

    def __attrs_post_init__(self) -> None:
        if self.field_index > self.total_fields:
            raise ValueError(
                f"Task is field [{self.field_index}] of [{self.total_fields}] for its "
                f"document, which counts past the total."
            )

    @classmethod
    def for_document_id_sequence(
        cls, document_ids: list[str]
    ) -> list["DocumentExtractionAnnotationBatchPosition"]:
        """Returns one position per given document id, in that same order. Pass the document
        each task in the batch is about, ordered as the batch will be written.

        For a batch covering doc_a twice, then doc_b, then doc_a once more:

            ["doc_a", "doc_a", "doc_b", "doc_a"]
            -> doc_index=1, field_index=1, total_fields=3, task_order=1
               doc_index=1, field_index=2, total_fields=3, task_order=2
               doc_index=2, field_index=1, total_fields=1, task_order=3
               doc_index=1, field_index=3, total_fields=3, task_order=4

        A document's index is fixed by where it first appears, and its field count spans
        every task about it whether or not those tasks are adjacent.
        """
        doc_index_by_document_id = {
            document_id: doc_index
            for doc_index, document_id in enumerate(
                dict.fromkeys(document_ids), start=1
            )
        }
        total_fields_by_document_id = Counter(document_ids)
        field_index_by_document_id: Counter[str] = Counter()

        positions = []
        for task_order, document_id in enumerate(document_ids, start=1):
            field_index_by_document_id[document_id] += 1
            positions.append(
                cls(
                    doc_index=doc_index_by_document_id[document_id],
                    field_index=field_index_by_document_id[document_id],
                    total_fields=total_fields_by_document_id[document_id],
                    task_order=task_order,
                )
            )
        return positions
