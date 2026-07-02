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
"""Helpers for generating datasets related to LLM documents."""

from recidiviz.common.constants.states import StateCode


def document_store_metadata_dataset_for_region(
    state_code: StateCode, sandbox_dataset_prefix: str | None = None
) -> str:
    """Returns the dataset containing document collection metadata tables for this region."""
    prefix = f"{sandbox_dataset_prefix}_" if sandbox_dataset_prefix else ""
    return f"{prefix}{state_code.value.lower()}_document_store_metadata"


def document_contents_dataset_for_region(
    state_code: StateCode, sandbox_dataset_prefix: str | None = None
) -> str:
    """Returns the dataset containing document contents tables for this region."""
    prefix = f"{sandbox_dataset_prefix}_" if sandbox_dataset_prefix else ""
    return f"{prefix}{state_code.value.lower()}_document_contents"


def document_store_temp_dataset_for_region(
    state_code: StateCode, sandbox_dataset_prefix: str | None = None
) -> str:
    """Returns the dataset containing temporary tables used during document store processing."""
    prefix = f"{sandbox_dataset_prefix}_" if sandbox_dataset_prefix else ""
    return f"{prefix}{state_code.value.lower()}_document_store_temp"


def document_extraction_raw_results_dataset_for_region(
    state_code: StateCode, sandbox_dataset_prefix: str | None = None
) -> str:
    """Returns the dataset containing raw LLM document-extraction result tables for this region."""
    prefix = f"{sandbox_dataset_prefix}_" if sandbox_dataset_prefix else ""
    return f"{prefix}{state_code.value.lower()}_document_extraction_results__raw"


def document_extraction_validated_results_dataset_for_region(
    state_code: StateCode, sandbox_dataset_prefix: str | None = None
) -> str:
    """Returns the dataset containing validated LLM document-extraction result tables for this region."""
    prefix = f"{sandbox_dataset_prefix}_" if sandbox_dataset_prefix else ""
    return f"{prefix}{state_code.value.lower()}_document_extraction_results__validated"


def document_extraction_validation_audit_dataset_for_region(
    state_code: StateCode, sandbox_dataset_prefix: str | None = None
) -> str:
    """Returns the dataset containing LLM document-extraction validation audit tables for this region."""
    prefix = f"{sandbox_dataset_prefix}_" if sandbox_dataset_prefix else ""
    return f"{prefix}{state_code.value.lower()}_document_extraction_validation_audit"
