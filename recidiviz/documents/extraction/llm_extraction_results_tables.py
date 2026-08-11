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
"""The three BigQuery tables the LLM document-extraction pipeline writes per
extractor collection: raw results, validated results, and the validation audit.

Each table owns its address, its schema, and the row it derives from a set of
primitive column values, so the columns a row supplies and the columns the schema
declares live in one place and can't drift apart.
"""

import datetime
import json
from typing import Any

from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.enums import SqlTypeNames

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.dataset_config import (
    document_extraction_raw_results_dataset_for_region,
    document_extraction_validated_results_dataset_for_region,
    document_extraction_validation_audit_dataset_for_region,
)
from recidiviz.documents.extraction.extraction_results_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    EXTRACTION_JOB_ID_COLUMN_NAME,
    EXTRACTOR_ID_COLUMN_NAME,
    EXTRACTOR_VERSION_ID_COLUMN_NAME,
    IS_RELEVANT_COLUMN_NAME,
    PASSED_VALIDATION_COLUMN_NAME,
    RESULT_DATETIME_UTC_COLUMN_NAME,
    RESULT_JSON_COLUMN_NAME,
    STATE_CODE_COLUMN_NAME,
    VALIDATION_CONFIG_VERSION_ID_COLUMN_NAME,
    VALIDATION_DATETIME_UTC_COLUMN_NAME,
    VALIDATION_ISSUES_JSON_COLUMN_NAME,
    WILL_RETRY_COLUMN_NAME,
)


class ExtractionRawResultsBQTable:
    """The raw results table: one row per successful model call, holding the raw
    JSON exactly as the model returned it, before any validation or quality
    filtering."""

    @staticmethod
    def table_id(collection_name: str) -> str:
        return collection_name.lower()

    @classmethod
    def address(
        cls,
        *,
        state_code: StateCode,
        collection_name: str,
        sandbox_prefix: str | None = None,
    ) -> BigQueryAddress:
        return BigQueryAddress(
            dataset_id=document_extraction_raw_results_dataset_for_region(
                state_code, sandbox_prefix
            ),
            table_id=cls.table_id(collection_name),
        )

    @staticmethod
    def description(*, collection_name: str, state_code: StateCode) -> str:
        return (
            f"Raw model output for the [{collection_name}] collection in "
            f"{StateCode.get_state(state_code)}. One row per successful model "
            "call, holding the raw JSON before any validation or quality "
            "filtering."
        )

    @staticmethod
    def schema() -> list[SchemaField]:
        return [
            SchemaField(
                name=STATE_CODE_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="The state code",
            ),
            SchemaField(
                name=EXTRACTION_JOB_ID_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="The extraction job that produced this result",
            ),
            SchemaField(
                name=EXTRACTOR_ID_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="The logical extractor",
            ),
            SchemaField(
                name=EXTRACTOR_VERSION_ID_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="The specific extractor version used",
            ),
            SchemaField(
                name=DOCUMENT_CONTENTS_ID_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="SHA256 hash identifying the document",
            ),
            SchemaField(
                name=RESULT_DATETIME_UTC_COLUMN_NAME,
                field_type=SqlTypeNames.TIMESTAMP.value,
                mode="REQUIRED",
                description="When this result was processed",
            ),
            SchemaField(
                name=RESULT_JSON_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="Raw LLM output JSON",
            ),
        ]

    @staticmethod
    def to_row(
        *,
        state_code_str: str,
        job_id: str,
        extractor_id: str,
        extractor_version_id: str,
        document_contents_id: str,
        result_datetime_utc: datetime.datetime,
        result_json: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            STATE_CODE_COLUMN_NAME: state_code_str,
            EXTRACTION_JOB_ID_COLUMN_NAME: job_id,
            EXTRACTOR_ID_COLUMN_NAME: extractor_id,
            EXTRACTOR_VERSION_ID_COLUMN_NAME: extractor_version_id,
            DOCUMENT_CONTENTS_ID_COLUMN_NAME: document_contents_id,
            RESULT_DATETIME_UTC_COLUMN_NAME: result_datetime_utc,
            RESULT_JSON_COLUMN_NAME: json.dumps(result_json),
        }


class ExtractionValidatedResultsBQTable:
    """The validated results table: model results that passed all
    extraction-error checks, with quality-filter corrections applied, one row per
    (document, extractor version)."""

    @staticmethod
    def table_id(collection_name: str) -> str:
        return collection_name.lower()

    @classmethod
    def address(
        cls,
        *,
        state_code: StateCode,
        collection_name: str,
        sandbox_prefix: str | None = None,
    ) -> BigQueryAddress:
        return BigQueryAddress(
            dataset_id=document_extraction_validated_results_dataset_for_region(
                state_code, sandbox_prefix
            ),
            table_id=cls.table_id(collection_name),
        )

    @staticmethod
    def description(*, collection_name: str, state_code: StateCode) -> str:
        return (
            f"Validated model output for the [{collection_name}] collection in "
            f"{StateCode.get_state(state_code)}. One row per (document, extractor "
            "version) that passed all extraction-error checks, with quality-filter "
            "corrections applied."
        )

    @staticmethod
    def schema() -> list[SchemaField]:
        return [
            SchemaField(
                name=STATE_CODE_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="State code",
            ),
            SchemaField(
                name=DOCUMENT_CONTENTS_ID_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="Document identifier",
            ),
            SchemaField(
                name=EXTRACTION_JOB_ID_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="Job that produced the raw result",
            ),
            SchemaField(
                name=EXTRACTOR_VERSION_ID_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="Extractor version of the raw result",
            ),
            SchemaField(
                name=VALIDATION_CONFIG_VERSION_ID_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="Threshold config used",
            ),
            SchemaField(
                name=VALIDATION_DATETIME_UTC_COLUMN_NAME,
                field_type=SqlTypeNames.TIMESTAMP.value,
                mode="REQUIRED",
                description="When validation was performed",
            ),
            SchemaField(
                name=IS_RELEVANT_COLUMN_NAME,
                field_type=SqlTypeNames.BOOLEAN.value,
                mode="REQUIRED",
                description="Model's relevance determination (top-level for efficient filtering)",
            ),
            SchemaField(
                name=RESULT_JSON_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description=(
                    "Quality-filtered JSON. For relevant: all fields. For irrelevant: "
                    '{"is_relevant": false}.'
                ),
            ),
        ]

    @staticmethod
    def to_row(
        *,
        state_code_str: str,
        document_contents_id: str,
        job_id: str,
        extractor_version_id: str,
        validation_config_version_id: str,
        validation_datetime_utc: datetime.datetime,
        is_relevant: bool,
        validated_output_json: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            STATE_CODE_COLUMN_NAME: state_code_str,
            DOCUMENT_CONTENTS_ID_COLUMN_NAME: document_contents_id,
            EXTRACTION_JOB_ID_COLUMN_NAME: job_id,
            EXTRACTOR_VERSION_ID_COLUMN_NAME: extractor_version_id,
            VALIDATION_CONFIG_VERSION_ID_COLUMN_NAME: validation_config_version_id,
            VALIDATION_DATETIME_UTC_COLUMN_NAME: validation_datetime_utc,
            IS_RELEVANT_COLUMN_NAME: is_relevant,
            RESULT_JSON_COLUMN_NAME: json.dumps(validated_output_json),
        }


class ExtractionValidationAuditBQTable:
    """The validation audit table: one row per document per validation run
    recording the outcome and the specific problems found, holding no extracted
    values."""

    @staticmethod
    def table_id(collection_name: str) -> str:
        return collection_name.lower()

    @classmethod
    def address(
        cls,
        *,
        state_code: StateCode,
        collection_name: str,
        sandbox_prefix: str | None = None,
    ) -> BigQueryAddress:
        return BigQueryAddress(
            dataset_id=document_extraction_validation_audit_dataset_for_region(
                state_code, sandbox_prefix
            ),
            table_id=cls.table_id(collection_name),
        )

    @staticmethod
    def description(*, collection_name: str, state_code: StateCode) -> str:
        return (
            f"Validation audit for the [{collection_name}] collection in "
            f"{StateCode.get_state(state_code)}. One row per document per "
            "validation run recording the outcome and any issues found."
        )

    @staticmethod
    def schema() -> list[SchemaField]:
        return [
            SchemaField(
                name=STATE_CODE_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="State code",
            ),
            SchemaField(
                name=DOCUMENT_CONTENTS_ID_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="Document identifier",
            ),
            SchemaField(
                name=EXTRACTION_JOB_ID_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="Job that produced the raw result",
            ),
            SchemaField(
                name=EXTRACTOR_VERSION_ID_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="Extractor version",
            ),
            SchemaField(
                name=VALIDATION_CONFIG_VERSION_ID_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="Threshold config used",
            ),
            SchemaField(
                name=VALIDATION_DATETIME_UTC_COLUMN_NAME,
                field_type=SqlTypeNames.TIMESTAMP.value,
                mode="REQUIRED",
                description="When validation was performed",
            ),
            SchemaField(
                name=PASSED_VALIDATION_COLUMN_NAME,
                field_type=SqlTypeNames.BOOLEAN.value,
                mode="REQUIRED",
                description="Whether the document passed all extraction error checks",
            ),
            SchemaField(
                name=WILL_RETRY_COLUMN_NAME,
                field_type=SqlTypeNames.BOOLEAN.value,
                mode="REQUIRED",
                description="Whether the document is queued for LLM retry",
            ),
            SchemaField(
                name=IS_RELEVANT_COLUMN_NAME,
                field_type=SqlTypeNames.BOOLEAN.value,
                mode="NULLABLE",
                description="Model's relevance determination (null when it could not be determined)",
            ),
            SchemaField(
                name=VALIDATION_ISSUES_JSON_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="NULLABLE",
                description="JSON array of issues found (null if none)",
            ),
        ]

    @staticmethod
    def to_row(
        *,
        state_code_str: str,
        document_contents_id: str,
        job_id: str,
        extractor_version_id: str,
        validation_config_version_id: str,
        validation_datetime_utc: datetime.datetime,
        passed_validation: bool,
        will_retry: bool,
        is_relevant: bool | None,
        audit_issues_json: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            STATE_CODE_COLUMN_NAME: state_code_str,
            DOCUMENT_CONTENTS_ID_COLUMN_NAME: document_contents_id,
            EXTRACTION_JOB_ID_COLUMN_NAME: job_id,
            EXTRACTOR_VERSION_ID_COLUMN_NAME: extractor_version_id,
            VALIDATION_CONFIG_VERSION_ID_COLUMN_NAME: validation_config_version_id,
            VALIDATION_DATETIME_UTC_COLUMN_NAME: validation_datetime_utc,
            PASSED_VALIDATION_COLUMN_NAME: passed_validation,
            WILL_RETRY_COLUMN_NAME: will_retry,
            IS_RELEVANT_COLUMN_NAME: is_relevant,
            VALIDATION_ISSUES_JSON_COLUMN_NAME: (
                json.dumps(audit_issues_json) if audit_issues_json else None
            ),
        }
