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
"""Column name constants and schema definitions for document extraction result BQ tables."""

from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.enums import SqlTypeNames

STATE_CODE_COLUMN_NAME = "state_code"
EXTRACTION_JOB_ID_COLUMN_NAME = "extraction_job_id"
EXTRACTOR_ID_COLUMN_NAME = "extractor_id"
EXTRACTOR_VERSION_ID_COLUMN_NAME = "extractor_version_id"
DOCUMENT_CONTENTS_ID_COLUMN_NAME = "document_contents_id"
RESULT_DATETIME_UTC_COLUMN_NAME = "result_datetime_utc"
RESULT_JSON_COLUMN_NAME = "result_json"
VALIDATION_CONFIG_VERSION_ID_COLUMN_NAME = "validation_config_version_id"
VALIDATION_DATETIME_UTC_COLUMN_NAME = "validation_datetime_utc"
IS_RELEVANT_COLUMN_NAME = "is_relevant"
PASSED_VALIDATION_COLUMN_NAME = "passed_validation"
WILL_RETRY_COLUMN_NAME = "will_retry"
VALIDATION_ISSUES_JSON_COLUMN_NAME = "validation_issues_json"


def build_raw_results_schema() -> list[SchemaField]:
    """Returns the schema for a raw results table: one row per successful model call,
    holding the raw JSON exactly as the model returned it, before any validation or
    quality filtering.
    """
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


def build_validated_results_schema() -> list[SchemaField]:
    """Returns the schema for a validated results table: the model results that
    passed all extraction-error checks, with quality-filter corrections applied,
    one row per (document, extractor version).
    """
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


def build_validation_audit_schema() -> list[SchemaField]:
    """Returns the schema for a validation audit table: one row per document per
    validation run recording the outcome and the specific problems found, holding
    no extracted values.
    """
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
            mode="REQUIRED",
            description="Model's relevance determination",
        ),
        SchemaField(
            name=VALIDATION_ISSUES_JSON_COLUMN_NAME,
            field_type=SqlTypeNames.STRING.value,
            mode="NULLABLE",
            description="JSON array of issues found (null if none)",
        ),
    ]
