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
"""Names of the framework-emitted fields in an extractor's JSON output.

These are the property names in the JSON Schema the model is constrained to (see
`llm_json_schema_generator`) and therefore the keys downstream validation and
view generation read the model output by. Defined once here so a rename can't
silently desync the generated schema from the code that consumes its output.
"""

RESULT_KEY = "result"
"""Top-level wrapper property holding a single extraction result."""

IS_RELEVANT_FIELD_NAME = "is_relevant"
"""Relevance field the framework injects into every first-order output schema.
Reserved — a collection's `output_schema` may not define it.
"""

# Companion-metadata properties emitted alongside each INFERRED field's value.
ADVERSARIAL_INTERPRETATION_FIELD_NAME = "adversarial_interpretation"
VALUE_FIELD_NAME = "value"
NULL_REASON_FIELD_NAME = "null_reason"
CONFIDENCE_LEVEL_FIELD_NAME = "confidence_level"
CITATIONS_FIELD_NAME = "citations"

# Sub-field names of a single citation object on a field's `citations` array.
CITATION_TEXT_FIELD_NAME = "text"
CITATION_START_FIELD_NAME = "start"
CITATION_END_FIELD_NAME = "end"
