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
"""Groups the field values an extractor pulled out of one document into the records that
only make sense read together.

A top-level field stands alone, because "is primary_status really employed?" is answerable
by itself. An array element does not. Asking "is cashier the right job_title?" is
unanswerable without the employer_name saying which job, so an element's values form one
record, and anything selecting a subset of a document's values keeps or drops a record whole.

Say a document's employment result holds two top-level fields and three employers, where the
second names nothing at all and the third names only its employer. Grouping its field values
returns four records.

    field values                             records
    primary_status=employed                  [primary_status]
    search_status=NULL                       [search_status]
    employers[0].employer_name=Walmart       [employer_name, job_title]
    employers[0].job_title=cashier
    employers[0].self_employed=NULL
    employers[1].employer_name=NULL          (none)
    employers[1].job_title=NULL
    employers[2].employer_name=Ace Hardware  [employer_name]

An element contributes only its non-null values, and an element with none at all contributes
no record, since there is nothing to ask about a row the model never populated. The third
employer shows that an element can reduce to a single value, so the number of values a record
holds says nothing about where it came from. Read is_array_element for that.
"""
import attr

from recidiviz.common import attr_validators
from recidiviz.llm_eval.document_extraction.document_extraction_field_value import (
    DocumentExtractionFieldValue,
)


@attr.define(frozen=True, kw_only=True)
class DocumentExtractionAnnotatableRecord:
    """One record an extractor pulled out of a document, holding the field values that only
    make sense read together. A top-level field stands alone; an array element's values form
    one record, because a question about any one of them is unanswerable without the others.
    """

    field_values: list[DocumentExtractionFieldValue] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(DocumentExtractionFieldValue),
        ]
    )
    """This record's values. A top-level field record holds one, and an array element record
    holds that element's non-null values.
    """

    def __attrs_post_init__(self) -> None:
        array_element_keys = {
            field_value.array_element_key for field_value in self.field_values
        }
        if len(array_element_keys) > 1:
            raise ValueError(
                f"Record mixes values from different array elements "
                f"{sorted(str(key) for key in array_element_keys)}. Each element is its "
                f"own record."
            )
        if not self.is_array_element and len(self.field_values) > 1:
            raise ValueError(
                f"Record groups {sorted(v.field_name for v in self.field_values)}, which "
                f"are top-level fields. Those are independently answerable, so each is "
                f"its own record."
            )

    @classmethod
    def from_field_values(
        cls, field_values: list[DocumentExtractionFieldValue]
    ) -> list["DocumentExtractionAnnotatableRecord"]:
        """Returns the given field values grouped into records, one per top-level field and
        one per array element that populated at least one sub-field.

        Reading a note that names two employers, the second of which gave no details at all,
        groups five field values into three records.

            field values                             records
            primary_status=employed                  [primary_status]
            employers[0].employer_name=Walmart       [employer_name, job_title]
            employers[0].job_title=cashier
            employers[1].employer_name=NULL          (none)
            employers[1].job_title=NULL
        """
        top_level_records = []
        values_by_array_element: dict[
            tuple[str, str, int], list[DocumentExtractionFieldValue]
        ] = {}

        for field_value in field_values:
            if (array_element_key := field_value.array_element_key) is None:
                top_level_records.append(cls(field_values=[field_value]))
                continue
            element_values = values_by_array_element.setdefault(array_element_key, [])
            if not field_value.is_null:
                element_values.append(field_value)

        return top_level_records + [
            cls(field_values=element_values)
            for element_values in values_by_array_element.values()
            if element_values
        ]

    @property
    def is_array_element(self) -> bool:
        """Returns whether this record came from an array element rather than standing at the
        document's top level. Read this rather than counting values, since an element that
        populated only one sub-field is still an array element.
        """
        return self.field_values[0].array_element_key is not None

    @property
    def is_null(self) -> bool:
        """Returns whether this record carries no extracted value at all, so the only claim
        it puts to an annotator is an absence. Always False for an array element record,
        which holds only non-null values.
        """
        return all(field_value.is_null for field_value in self.field_values)

    @property
    def has_inferred_confidence(self) -> bool:
        """Returns whether the model reported only INFERRED confidence in any of this
        record's values. An inferred value is likelier to be wrong than one the document
        states outright, so the export over-samples these.
        """
        return any(
            field_value.has_inferred_confidence for field_value in self.field_values
        )

    @property
    def document_contents_id(self) -> str:
        """Returns the document this record was extracted from. Every value in the record
        shares it, because a top-level record holds a single value and an array element
        belongs to one document.
        """
        return self.field_values[0].document_contents_id
