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
"""The BigQuery table holding the scored output of evaluating an extractor against
its human-labeled golden set, one table per extractor collection.

The table owns its address, its schema, and the row it derives from a set of
primitive column values, so the columns a row supplies and the columns the schema
declares live in one place and can't drift apart.
"""
import datetime

from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.enums import SqlTypeNames

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.dataset_config import (
    document_extraction_golden_eval_results_dataset,
)
from recidiviz.documents.extraction.models.llm_document_extraction_golden_eval_config import (
    GoldenEvalTestType,
)

STATE_CODE_COLUMN_NAME = "state_code"
EXTRACTOR_ID_COLUMN_NAME = "extractor_id"
EXTRACTOR_VERSION_ID_COLUMN_NAME = "extractor_version_id"
OUTPUT_SCHEMA_VERSION_COLUMN_NAME = "output_schema_version"
RUN_DATETIME_UTC_COLUMN_NAME = "run_datetime_utc"
GOLDEN_DOCUMENT_ID_COLUMN_NAME = "golden_document_id"
TEST_TYPE_COLUMN_NAME = "test_type"
TEST_CASE_COLUMN_NAME = "test_case"
FIELD_NAME_COLUMN_NAME = "field_name"
ELEMENT_INDEX_COLUMN_NAME = "element_index"
EXPECTED_COLUMN_NAME = "expected"
ACTUAL_COLUMN_NAME = "actual"
IS_CORRECT_COLUMN_NAME = "is_correct"


class GoldenEvalResultsBQTable:
    """The golden eval results table: one row per (run, document, field) comparing
    the expected value to what the extractor actually produced.

    The regression-test record for extractor quality across versions — what the CI
    golden eval check and the quality dashboards read to catch a version that
    degraded results. Append-only: repeated runs of the same extractor version add
    rows rather than replacing them.
    """

    @staticmethod
    def table_id(collection_name: str) -> str:
        return collection_name.lower()

    @classmethod
    def address(
        cls, *, collection_name: str, sandbox_prefix: str | None = None
    ) -> BigQueryAddress:
        return BigQueryAddress(
            dataset_id=document_extraction_golden_eval_results_dataset(sandbox_prefix),
            table_id=cls.table_id(collection_name),
        )

    @staticmethod
    def description(*, collection_name: str) -> str:
        return (
            f"Golden eval results for the [{collection_name}] collection. One row "
            "per (run, document, field) comparing the expected value to what the "
            "extractor actually produced, across every state that runs the "
            "extractor. Append-only."
        )

    @staticmethod
    def schema() -> list[SchemaField]:
        return [
            SchemaField(
                name=STATE_CODE_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="State code of the extractor that was evaluated",
            ),
            SchemaField(
                name=EXTRACTOR_ID_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="The logical extractor that was evaluated",
            ),
            SchemaField(
                name=EXTRACTOR_VERSION_ID_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="Version ID of the extractor config used for this run",
            ),
            SchemaField(
                name=OUTPUT_SCHEMA_VERSION_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="Hash of the output schema at the time of the run",
            ),
            SchemaField(
                name=RUN_DATETIME_UTC_COLUMN_NAME,
                field_type=SqlTypeNames.TIMESTAMP.value,
                mode="REQUIRED",
                description="When this eval run was executed",
            ),
            SchemaField(
                name=GOLDEN_DOCUMENT_ID_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description=(
                    "Identifier of the test document, real or synthetic, as "
                    "assigned in the collection's eval sheet"
                ),
            ),
            SchemaField(
                name=TEST_TYPE_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description=(
                    f"One of "
                    f"{sorted(test_type.value for test_type in GoldenEvalTestType)}"
                ),
            ),
            SchemaField(
                name=TEST_CASE_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description=(
                    "Category within the test type (e.g. base_case, "
                    "missing_fields), so we can check coverage across scenarios"
                ),
            ),
            SchemaField(
                name=FIELD_NAME_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description=(
                    "Field being scored. Sub-fields of an array field are named "
                    "{array_field}.{sub_field}, e.g. employers.employer_name"
                ),
            ),
            SchemaField(
                name=ELEMENT_INDEX_COLUMN_NAME,
                field_type=SqlTypeNames.INTEGER.value,
                mode="NULLABLE",
                description=(
                    "For array sub-fields, the index of the matched element pair. "
                    "Null for flat fields and array-level summary rows"
                ),
            ),
            SchemaField(
                name=EXPECTED_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="NULLABLE",
                description=(
                    "Expected value, stringified. Null for an unmatched actual "
                    "element (a false positive)"
                ),
            ),
            SchemaField(
                name=ACTUAL_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="NULLABLE",
                description=(
                    "Actual extracted value, stringified. Null for an unmatched "
                    "expected element (a miss)"
                ),
            ),
            SchemaField(
                name=IS_CORRECT_COLUMN_NAME,
                field_type=SqlTypeNames.BOOLEAN.value,
                mode="REQUIRED",
                description=(
                    "Whether the actual value was correct for the expected value, "
                    "using fuzzy matching for strings"
                ),
            ),
        ]

    @staticmethod
    def to_row(
        *,
        state_code: StateCode,
        extractor_id: str,
        extractor_version_id: str,
        output_schema_version: str,
        run_datetime_utc: datetime.datetime,
        golden_document_id: str,
        test_type: GoldenEvalTestType,
        test_case: str,
        field_name: str,
        element_index: int | None,
        expected: str | None,
        actual: str | None,
        is_correct: bool,
    ) -> dict[str, str | int | bool | datetime.datetime | None]:
        """Returns the BigQuery row for one scored field comparison."""
        return {
            STATE_CODE_COLUMN_NAME: state_code.value,
            EXTRACTOR_ID_COLUMN_NAME: extractor_id,
            EXTRACTOR_VERSION_ID_COLUMN_NAME: extractor_version_id,
            OUTPUT_SCHEMA_VERSION_COLUMN_NAME: output_schema_version,
            RUN_DATETIME_UTC_COLUMN_NAME: run_datetime_utc,
            GOLDEN_DOCUMENT_ID_COLUMN_NAME: golden_document_id,
            TEST_TYPE_COLUMN_NAME: test_type.value,
            TEST_CASE_COLUMN_NAME: test_case,
            FIELD_NAME_COLUMN_NAME: field_name,
            ELEMENT_INDEX_COLUMN_NAME: element_index,
            EXPECTED_COLUMN_NAME: expected,
            ACTUAL_COLUMN_NAME: actual,
            IS_CORRECT_COLUMN_NAME: is_correct,
        }
