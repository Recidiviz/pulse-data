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
"""Tests for GoldenEvalResultsBQTable."""
import datetime
from typing import Any
from unittest import TestCase

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.eval.golden_eval_results_table import (
    GoldenEvalResultsBQTable,
)
from recidiviz.documents.extraction.models.llm_document_extraction_golden_eval_config import (
    GoldenEvalTestType,
)

_RUN_DATETIME = datetime.datetime(2026, 7, 24, 17, 5, tzinfo=datetime.UTC)


def _row(**overrides: Any) -> dict[str, Any]:
    """Returns a scored row, defaulting to a flat-field row that |overrides|
    overrides key by key.
    """
    kwargs: dict[str, Any] = {
        "state_code": StateCode.US_XX,
        "extractor_id": "US_XX_FAKE_EXTRACTOR_COLLECTION",
        "extractor_version_id": "abc123",
        "output_schema_version": "def456",
        "run_datetime_utc": _RUN_DATETIME,
        "golden_document_id": "unit_base_case_1",
        "test_type": GoldenEvalTestType.UNIT,
        "test_case": "base_case",
        "field_name": "primary_status",
        "element_index": None,
        "expected": "ACTIVE",
        "actual": "ACTIVE",
        "is_correct": True,
    }
    kwargs.update(overrides)
    return GoldenEvalResultsBQTable.to_row(**kwargs)


class GoldenEvalResultsBQTableTest(TestCase):
    """Tests for GoldenEvalResultsBQTable."""

    def test_table_id(self) -> None:
        self.assertEqual(
            "playground_employment_info",
            GoldenEvalResultsBQTable.table_id("PLAYGROUND_EMPLOYMENT_INFO"),
        )

    def test_address(self) -> None:
        self.assertEqual(
            BigQueryAddress(
                dataset_id="document_extraction_golden_eval_results",
                table_id="playground_employment_info",
            ),
            GoldenEvalResultsBQTable.address(
                collection_name="PLAYGROUND_EMPLOYMENT_INFO"
            ),
        )

    def test_address_with_sandbox_prefix(self) -> None:
        self.assertEqual(
            BigQueryAddress(
                dataset_id="my_prefix_document_extraction_golden_eval_results",
                table_id="playground_employment_info",
            ),
            GoldenEvalResultsBQTable.address(
                collection_name="PLAYGROUND_EMPLOYMENT_INFO",
                sandbox_prefix="my_prefix",
            ),
        )

    def test_schema(self) -> None:
        # The column list, order, types, and modes are pinned by the design doc's
        # golden-eval results table — downstream CI checks and quality dashboards
        # read these columns by name.
        self.assertEqual(
            [
                ("state_code", "STRING", "REQUIRED"),
                ("extractor_id", "STRING", "REQUIRED"),
                ("extractor_version_id", "STRING", "REQUIRED"),
                ("output_schema_version", "STRING", "REQUIRED"),
                ("run_datetime_utc", "TIMESTAMP", "REQUIRED"),
                ("golden_document_id", "STRING", "REQUIRED"),
                ("test_type", "STRING", "REQUIRED"),
                ("test_case", "STRING", "REQUIRED"),
                ("field_name", "STRING", "REQUIRED"),
                ("element_index", "INTEGER", "NULLABLE"),
                ("expected", "STRING", "NULLABLE"),
                ("actual", "STRING", "NULLABLE"),
                ("is_correct", "BOOLEAN", "REQUIRED"),
            ],
            [
                (field.name, field.field_type, field.mode)
                for field in GoldenEvalResultsBQTable.schema()
            ],
        )

    def test_every_schema_field_has_a_description(self) -> None:
        for field in GoldenEvalResultsBQTable.schema():
            with self.subTest(field=field.name):
                self.assertTrue(field.description)

    def test_to_row_flat_field(self) -> None:
        self.assertEqual(
            {
                "state_code": "US_XX",
                "extractor_id": "US_XX_FAKE_EXTRACTOR_COLLECTION",
                "extractor_version_id": "abc123",
                "output_schema_version": "def456",
                "run_datetime_utc": _RUN_DATETIME,
                "golden_document_id": "unit_base_case_1",
                "test_type": "unit",
                "test_case": "base_case",
                "field_name": "primary_status",
                "element_index": None,
                "expected": "ACTIVE",
                "actual": "ACTIVE",
                "is_correct": True,
            },
            _row(),
        )

    def test_to_row_array_sub_field(self) -> None:
        # Sub-field rows name the array they came from and carry the index of the
        # matched element pair.
        row = _row(
            field_name="employers.employer_name",
            element_index=1,
            expected="Globex",
            actual="Globex Corp",
            is_correct=True,
        )
        self.assertEqual("employers.employer_name", row["field_name"])
        self.assertEqual(1, row["element_index"])

    def test_to_row_array_level_summary(self) -> None:
        # The array-level row records whether the count matched and every element
        # paired, so `expected`/`actual` hold a rendered count rather than a value.
        row = _row(
            field_name="employers",
            element_index=None,
            expected="count:2",
            actual="count:1",
            is_correct=False,
        )
        self.assertEqual("count:2", row["expected"])
        self.assertEqual("count:1", row["actual"])
        self.assertFalse(row["is_correct"])

    def test_to_row_unmatched_expected_element_is_a_miss(self) -> None:
        row = _row(expected="Globex", actual=None, is_correct=False)
        self.assertIsNone(row["actual"])

    def test_to_row_unmatched_actual_element_is_a_false_positive(self) -> None:
        row = _row(expected=None, actual="Globex", is_correct=False)
        self.assertIsNone(row["expected"])

    def test_to_row_keys_exactly_match_schema(self) -> None:
        # A row that names a column the schema doesn't declare (or omits one it
        # does) fails the BQ insert at runtime, so pin them together here.
        self.assertEqual(
            [field.name for field in GoldenEvalResultsBQTable.schema()],
            list(_row()),
        )

    def test_description_names_the_collection(self) -> None:
        self.assertIn(
            "PLAYGROUND_EMPLOYMENT_INFO",
            GoldenEvalResultsBQTable.description(
                collection_name="PLAYGROUND_EMPLOYMENT_INFO"
            ),
        )
