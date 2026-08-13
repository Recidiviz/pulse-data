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
"""Tests for FieldCitation.

InferredFieldOutput itself is exercised in llm_request_output_values_test.py,
through LLMRequestOutputValues.inferred_field_outputs() — the entry point every
other test builds one from — whose fixtures this file reuses.
"""

from recidiviz.documents.extraction.exceptions import LLMOutputParsingError
from recidiviz.documents.extraction.models.llm_inferred_field_output import (
    FieldCitation,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    CITATION_END_FIELD_NAME,
    CITATION_START_FIELD_NAME,
    CITATION_TEXT_FIELD_NAME,
    CITATIONS_FIELD_NAME,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    build_inferred_field_result_json,
)
from recidiviz.tests.documents.extraction.models.llm_request_output_values_test import (
    _LLMRequestOutputValuesTestBase,
)


class FieldCitationTest(_LLMRequestOutputValuesTestBase):
    """Tests FieldCitation's accessors against a malformed citation JSON."""

    def _citation(
        self, output_values: LLMRequestOutputValues, display_name: str
    ) -> FieldCitation:
        [field_output] = [
            candidate
            for candidate in output_values.inferred_field_outputs()
            if candidate.display_name == display_name
        ]
        [citation] = field_output.citations
        return citation

    def test_well_formed_citation_read(self) -> None:
        citation = self._citation(self._full_output_values(), "location")
        self.assertEqual("citation for Kitchen", citation.text)
        self.assertEqual(0, citation.start)
        self.assertEqual(15, citation.end)

    def test_citation_missing_text_raises(self) -> None:
        wrapper = build_inferred_field_result_json("Kitchen")
        del wrapper[CITATIONS_FIELD_NAME][0][CITATION_TEXT_FIELD_NAME]
        citation = self._citation(
            self._malformed_output_values(location=wrapper), "location"
        )
        with self.assertRaisesRegex(
            LLMOutputParsingError,
            r"^Citation \[0\] on field \[location\] holds no \[text\]\. Found "
            r"keys: \['end', 'start'\]\.$",
        ):
            _ = citation.text

    def test_citation_missing_start_raises(self) -> None:
        wrapper = build_inferred_field_result_json("Kitchen")
        del wrapper[CITATIONS_FIELD_NAME][0][CITATION_START_FIELD_NAME]
        citation = self._citation(
            self._malformed_output_values(location=wrapper), "location"
        )
        with self.assertRaisesRegex(
            LLMOutputParsingError,
            r"^Citation \[0\] on field \[location\] holds no \[start\]\. Found "
            r"keys: \['end', 'text'\]\.$",
        ):
            _ = citation.start

    def test_citation_missing_end_raises(self) -> None:
        wrapper = build_inferred_field_result_json("Kitchen")
        del wrapper[CITATIONS_FIELD_NAME][0][CITATION_END_FIELD_NAME]
        citation = self._citation(
            self._malformed_output_values(location=wrapper), "location"
        )
        with self.assertRaisesRegex(
            LLMOutputParsingError,
            r"^Citation \[0\] on field \[location\] holds no \[end\]\. Found "
            r"keys: \['start', 'text'\]\.$",
        ):
            _ = citation.end

    def test_field_missing_citations_raises(self) -> None:
        wrapper = build_inferred_field_result_json("Kitchen")
        del wrapper[CITATIONS_FIELD_NAME]
        [field_output] = [
            candidate
            for candidate in self._malformed_output_values(
                location=wrapper
            ).inferred_field_outputs()
            if candidate.display_name == "location"
        ]
        with self.assertRaisesRegex(
            LLMOutputParsingError,
            r"^INFERRED field \[location\] holds no \[citations\]\. Found keys: "
            r"\['adversarial_interpretation', 'confidence_level', 'value'\]\.$",
        ):
            _ = field_output.citations
