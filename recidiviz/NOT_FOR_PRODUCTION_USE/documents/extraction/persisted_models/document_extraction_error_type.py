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
"""Per-document extraction error type enum."""
import enum


class DocumentExtractionErrorType(enum.Enum):
    DOCUMENT_NOT_FOUND = "DOCUMENT_NOT_FOUND"
    DOCUMENT_UNREADABLE = "DOCUMENT_UNREADABLE"
    DOCUMENT_TOO_LARGE = "DOCUMENT_TOO_LARGE"
    LLM_CONTENT_FILTERED = "LLM_CONTENT_FILTERED"
    LLM_MALFORMED_RESPONSE = "LLM_MALFORMED_RESPONSE"
    SCHEMA_VALIDATION_FAILURE = "SCHEMA_VALIDATION_FAILURE"
    LLM_EMPTY_RESPONSE = "LLM_EMPTY_RESPONSE"
    LLM_INDIVIDUAL_TIMEOUT = "LLM_INDIVIDUAL_TIMEOUT"
    LLM_RATE_LIMITED = "LLM_RATE_LIMITED"
    JOB_LEVEL_FAILURE = "JOB_LEVEL_FAILURE"
    UNKNOWN = "UNKNOWN"
