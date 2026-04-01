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
"""Unified exclusion type enum for document extraction results.

Covers both LLM-level errors (extraction failed) and post-extraction
validation failures (extraction succeeded but result didn't pass checks).
Used in both the raw results table (error_type column, LLM errors only)
and the exclusions table (exclusion_type column, all types).
"""
import enum


class ExtractionExclusionType(enum.Enum):
    """Why a document was excluded from validated extraction output."""

    # TODO(#66175): Add an is_row_level property to distinguish row-level
    #  failures from field-level failures so that partial results
    #  can be kept in the validated output (e.g., if a field with no dependencies
    #  is low confidence).

    # --- LLM extraction errors (written to both raw table and exclusions table) ---
    DOCUMENT_NOT_FOUND = "DOCUMENT_NOT_FOUND"
    DOCUMENT_UNREADABLE = "DOCUMENT_UNREADABLE"
    DOCUMENT_TOO_LARGE = "DOCUMENT_TOO_LARGE"
    LLM_MALFORMED_RESPONSE = "LLM_MALFORMED_RESPONSE"
    LLM_EMPTY_RESPONSE = "LLM_EMPTY_RESPONSE"
    LLM_CONTENT_FILTERED = "LLM_CONTENT_FILTERED"
    LLM_INDIVIDUAL_TIMEOUT = "LLM_INDIVIDUAL_TIMEOUT"
    LLM_RATE_LIMITED = "LLM_RATE_LIMITED"
    SCHEMA_VALIDATION_FAILURE = "SCHEMA_VALIDATION_FAILURE"
    JOB_LEVEL_FAILURE = "JOB_LEVEL_FAILURE"
    LLM_UNKNOWN_ERROR = "LLM_UNKNOWN_ERROR"

    # --- Validation failures (written only to exclusions table) ---
    NOT_RELEVANT = "NOT_RELEVANT"
    LOW_CONFIDENCE = "LOW_CONFIDENCE"
    SEMANTIC_CONSISTENCY_FAILURE = "SEMANTIC_CONSISTENCY_FAILURE"

    @property
    def is_llm_error(self) -> bool:
        """Whether this is an LLM-level error (vs a post-extraction
        validation failure)."""
        return self not in (
            ExtractionExclusionType.NOT_RELEVANT,
            ExtractionExclusionType.LOW_CONFIDENCE,
            ExtractionExclusionType.SEMANTIC_CONSISTENCY_FAILURE,
        )
