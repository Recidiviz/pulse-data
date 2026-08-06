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
"""One labeled test document from a collection's golden eval set, plus the values
the extractor is expected to produce for it.
"""
from typing import Any

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.models.llm_document_extraction_golden_eval_config import (
    GoldenEvalTestType,
)
from recidiviz.utils.types import assert_type


@attr.define(frozen=True, kw_only=True)
class GoldenEvalDocument:
    """One labeled test document plus its golden expected values"""

    golden_document_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Identifier of the test document: a real `document_contents_id`, or a
    synthetic ID for a hand-authored document.
    """

    test_type: GoldenEvalTestType = attr.ib(
        validator=attr.validators.in_(GoldenEvalTestType)
    )
    """Whether this is a minimal, targeted `unit` document or a realistic full
    `sample` document.
    """

    test_case: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Scenario category within the test type (e.g. `base_case`,
    `missing_fields`), so eval reporting can check coverage across scenarios.
    """

    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))
    """The state this document belongs to."""

    document_text: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The full document text to extract from."""

    expected_values: dict[str, Any] = attr.ib(
        validator=attr_validators.is_dict_of(str, object)
    )
    """Expected value per output-schema field, keyed by field name — every field
    the schema declares, `is_relevant` included. A `None` value means the field is
    not expected to be present. An ARRAY_OF_STRUCT field's value is a list of
    per-element dicts, each mapping sub-field name to expected scalar value; the
    list's order carries no meaning, since elements pair on the field's
    `primary_keys`, and a sub-field absent from an element is not expected to be
    present.
    """

    def expected_scalar_value(self, field_name: str) -> Any:
        """Returns the expected value of scalar-valued field |field_name|;
        `None` means the field is not expected to be present.
        """
        return self.expected_values[field_name]

    def expects_field(self, field_name: str) -> bool:
        """Returns whether |field_name| is expected to be present at all."""
        return self.expected_values[field_name] is not None

    def expected_array_elements(self, field_name: str) -> list[dict[str, Any]]:
        """Returns the expected elements of ARRAY_OF_STRUCT field |field_name|.
        A field that is not expected to be present has no elements.
        """
        expected_value = self.expected_values[field_name]
        if expected_value is None:
            return []
        return assert_type(expected_value, list)
