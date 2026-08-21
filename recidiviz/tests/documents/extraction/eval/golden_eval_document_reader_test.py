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
"""Tests for GoldenEvalDocumentReader.

Parses against FAKE_EXTRACTOR_COLLECTION's output schema, which covers a
STRUCTURAL BOOLEAN (`is_relevant`), an INFERRED ENUM (`primary_status`), a
STRUCTURAL STRING (`status_note`), an INFERRED STRING (`location`), and an
ARRAY_OF_STRUCT (`assignments`, keyed on `assignment_name`) whose sub-fields span
STRING, ENUM, and FLOAT. The INTEGER and top-level FLOAT cases it does not
declare are exercised against the minimal schema `_visits_output_schema()` builds.
"""
import csv
import re
from typing import Any
from unittest import TestCase
from unittest.mock import create_autospec

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.eval.golden_eval_document import GoldenEvalDocument
from recidiviz.documents.extraction.eval.golden_eval_document_parser import (
    DOCUMENT_TEXT_COLUMN_NAME,
    EXPECTED_VALUE_COLUMN_SUFFIX,
    GOLDEN_DOCUMENT_ID_COLUMN_NAME,
    TEST_CASE_COLUMN_NAME,
    TEST_TYPE_COLUMN_NAME,
    expected_value_column_name,
)
from recidiviz.documents.extraction.eval.golden_eval_document_reader import (
    GoldenEvalDocumentReader,
    GoldenEvalDocumentReadResult,
)
from recidiviz.documents.extraction.eval.golden_eval_document_source import (
    GoldenEvalDocumentSourceError,
    GoldenEvalGoogleSheetDocumentSource,
)
from recidiviz.documents.extraction.eval.golden_eval_scorer import (
    LLMDocumentExtractionGoldenEvalScorer,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_document_extraction_golden_eval_config import (
    GoldenEvalTestType,
    LLMDocumentExtractionGoldenEvalConfig,
)
from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ArrayOfStructLLMRequestOutputSchemaField,
    LLMOutputFieldType,
    PrimitiveScalarLLMRequestOutputSchemaField,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    IS_RELEVANT_FIELD_NAME,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    build_fake_extractor_assignment_result_json,
    build_fake_extractor_irrelevant_result_content,
    build_fake_extractor_result_content,
    build_inferred_field_result_json,
    build_null_inferred_field_result_json,
    wrap_in_result_key,
)
from recidiviz.tests.ingest import fixtures
from recidiviz.utils.google_sheets_reader import (
    GoogleSheetReader,
    GoogleSheetTable,
    GoogleSheetTableRow,
)
from recidiviz.utils.types import assert_type

_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
_SOURCE_SHEET_URI = "https://docs.google.com/spreadsheets/d/abc123"
_HAPPY_PATH_FIXTURE = "golden_eval_sheet.csv"

_DISH_DUTY_ELEMENT_JSON = (
    '{"assignment_name": "Dish duty", "assignment_type": "internal", '
    '"rate_amount": 12.5, "rate_period": "hourly"}'
)
_DISH_DUTY_ELEMENT = {
    "assignment_name": "Dish duty",
    "assignment_type": "internal",
    "rate_amount": 12.5,
    "rate_period": "hourly",
}


def _load_sheet(fixture_file_name: str) -> tuple[list[str], list[GoogleSheetTableRow]]:
    """Returns (column names, rows) of a CSV fixture standing in for the raw
    cell text of a golden eval sheet, 1-indexed.
    """
    with open(
        fixtures.as_filepath(fixture_file_name), encoding="utf-8"
    ) as fixture_file:
        column_names, *data_rows = list(csv.reader(fixture_file))
    return column_names, [
        GoogleSheetTableRow(
            row_number=row_number, values_by_column=dict(zip(column_names, cells))
        )
        for row_number, cells in enumerate(data_rows, start=2)
    ]


def _cells(**overrides: str) -> dict[str, str]:
    """Returns one row's cell text keyed by column, defaulting to a valid row
    and allowing passed-in overrides.
    """
    cells = {
        TEST_TYPE_COLUMN_NAME: "unit",
        TEST_CASE_COLUMN_NAME: "base_case",
        GOLDEN_DOCUMENT_ID_COLUMN_NAME: "unit_1",
        DOCUMENT_TEXT_COLUMN_NAME: "The record is active.",
        expected_value_column_name(IS_RELEVANT_FIELD_NAME): "true",
        expected_value_column_name("primary_status"): "active",
        expected_value_column_name("status_note"): "Currently active.",
        expected_value_column_name("location"): "Kitchen",
        expected_value_column_name("assignments"): f"[{_DISH_DUTY_ELEMENT_JSON}]",
    }
    cells.update(overrides)
    return cells


def _visits_cells(**overrides: str) -> dict[str, str]:
    """Returns one row's cell text keyed by column, defaulting to a valid row
    against `_visits_output_schema()` and allowing overrides.
    """
    cells = {
        TEST_TYPE_COLUMN_NAME: "unit",
        TEST_CASE_COLUMN_NAME: "base_case",
        GOLDEN_DOCUMENT_ID_COLUMN_NAME: "unit_1",
        DOCUMENT_TEXT_COLUMN_NAME: "Two visits at a rate of 12.5.",
        expected_value_column_name(IS_RELEVANT_FIELD_NAME): "true",
        expected_value_column_name("visit_count"): "2",
        expected_value_column_name("visit_rate"): "12.5",
        expected_value_column_name(
            "visits"
        ): '[{"visit_name": "Intake", "visit_number": 1}]',
    }
    cells.update(overrides)
    return cells


def _cells_without(column_name: str) -> dict[str, str]:
    """Returns the default row's cell text with |column_name| left out of the
    header, standing in for a sheet that never carried the column.
    """
    return {name: value for name, value in _cells().items() if name != column_name}


def _rows(*cell_dicts: dict[str, str]) -> list[GoogleSheetTableRow]:
    """Returns one sheet row per cell dict, starting from row 2."""
    return [
        GoogleSheetTableRow(row_number=row_number, values_by_column=cells)
        for row_number, cells in enumerate(cell_dicts, start=2)
    ]


def _tab(*cell_dicts: dict[str, str]) -> GoogleSheetTable:
    """Returns the US_XX tab holding |cell_dicts| as its data rows, numbered from
    row 2 as the sheet's first data row.
    """
    return GoogleSheetTable(
        title=StateCode.US_XX.value,
        column_names=list(_cells()),
        rows=_rows(*cell_dicts),
    )


def _visits_output_schema() -> LLMRequestOutputSchema:
    """Returns a minimal output schema declaring an INTEGER field, a FLOAT field,
    and an ARRAY_OF_STRUCT that sets `min_items` and holds a required INTEGER
    sub-field.
    """
    return LLMRequestOutputSchema(
        full_batch_description="Fictional visit records for a batch of documents.",
        result_level_description="Fictional visit record for a single document.",
        relevance_criteria="Whether the document mentions a fictional visit",
        user_defined_fields=[
            PrimitiveScalarLLMRequestOutputSchemaField(
                name="visit_count",
                description="How many visits the document mentions.",
                required=False,
                inferred_field_config=None,
                scalar_type=LLMOutputFieldType.INTEGER,
            ),
            PrimitiveScalarLLMRequestOutputSchemaField(
                name="visit_rate",
                description="The rate charged for a visit.",
                required=False,
                inferred_field_config=None,
                scalar_type=LLMOutputFieldType.FLOAT,
            ),
            ArrayOfStructLLMRequestOutputSchemaField(
                name="visits",
                description="The visits the document mentions.",
                required=False,
                inferred_field_config=None,
                primary_keys=["visit_name"],
                min_items=1,
                fields=[
                    PrimitiveScalarLLMRequestOutputSchemaField(
                        name="visit_name",
                        description="Name of the visit.",
                        required=False,
                        inferred_field_config=None,
                        scalar_type=LLMOutputFieldType.STRING,
                    ),
                    PrimitiveScalarLLMRequestOutputSchemaField(
                        name="visit_number",
                        description="Position of the visit in the sequence.",
                        required=True,
                        inferred_field_config=None,
                        scalar_type=LLMOutputFieldType.INTEGER,
                    ),
                ],
            ),
        ],
    )


def _one_cell_issue(
    *, column_name: str, value: str, reason: str, row_number: int = 2
) -> str:
    """Returns the exact issue string expected for a row with exactly one
    malformed cell.
    """
    return f"row [{row_number}] column [{column_name}] value [{value}]: {reason}"


class GoldenEvalDocumentReaderTest(TestCase):
    """Tests for GoldenEvalDocumentReader."""

    def setUp(self) -> None:
        config = get_first_order_llm_extractor_config(
            StateCode.US_XX, _COLLECTION_NAME, config_module=fake_config
        )
        self.config = attr.evolve(
            config,
            golden_eval=attr.evolve(
                assert_type(config.golden_eval, LLMDocumentExtractionGoldenEvalConfig),
                source_sheet_uri=_SOURCE_SHEET_URI,
            ),
        )
        self.output_schema = self.config.extractor_collection.output_schema
        self.sheets_reader = create_autospec(GoogleSheetReader)
        source = GoldenEvalGoogleSheetDocumentSource(
            sheets_reader=self.sheets_reader,
            state_code=StateCode.US_XX,
            source_sheet_uri=_SOURCE_SHEET_URI,
        )
        self.reader = GoldenEvalDocumentReader.for_extractor(
            source=source, output_schema=self.output_schema
        )
        self.visits_reader = GoldenEvalDocumentReader.for_extractor(
            source=source, output_schema=_visits_output_schema()
        )

    def _read_rows(
        self, *cell_dicts: dict[str, str]
    ) -> list[GoldenEvalDocumentReadResult]:
        """Returns the results parsed from inline rows against
        FAKE_EXTRACTOR_COLLECTION's schema.
        """
        return [
            self.reader.read_document_from_row(row=row) for row in _rows(*cell_dicts)
        ]

    def _read_visits_rows(
        self, *cell_dicts: dict[str, str]
    ) -> list[GoldenEvalDocumentReadResult]:
        """Returns the results parsed from inline rows against
        `_visits_output_schema()`.
        """
        return [
            self.visits_reader.read_document_from_row(row=row)
            for row in _rows(*cell_dicts)
        ]

    def _read_happy_path_fixture(self) -> list[GoldenEvalDocumentReadResult]:
        """Returns the results parsed from the happy-path CSV fixture."""
        _, rows = _load_sheet(_HAPPY_PATH_FIXTURE)
        return [self.reader.read_document_from_row(row=row) for row in rows]

    def _read_one_document(self, *cell_dicts: dict[str, str]) -> GoldenEvalDocument:
        """Returns the single document parsed from one row, asserting it parsed
        with no issues.
        """
        (result,) = self._read_rows(*cell_dicts)
        self.assertEqual([], result.issues)
        assert result.document is not None
        return result.document

    def _read_one_row_issues(self, *cell_dicts: dict[str, str]) -> list[str]:
        """Returns the rendered issues from reading one row, asserting it produced
        no document.
        """
        (result,) = self._read_rows(*cell_dicts)
        self.assertIsNone(result.document)
        return [issue.rendered for issue in result.issues]

    def _read_one_visits_document(
        self, *cell_dicts: dict[str, str]
    ) -> GoldenEvalDocument:
        """Returns the single document parsed from one visits row, asserting it
        parsed with no issues.
        """
        (result,) = self._read_visits_rows(*cell_dicts)
        self.assertEqual([], result.issues)
        assert result.document is not None
        return result.document

    def _read_one_visits_row_issues(self, *cell_dicts: dict[str, str]) -> list[str]:
        """Returns the rendered issues from reading one visits row, asserting it
        produced no document.
        """
        (result,) = self._read_visits_rows(*cell_dicts)
        self.assertIsNone(result.document)
        return [issue.rendered for issue in result.issues]

    # ------------------------------------------------------------------
    # Happy path
    # ------------------------------------------------------------------

    def test_read_document(self) -> None:
        results = self._read_happy_path_fixture()
        self.assertEqual([], [issue for result in results for issue in result.issues])
        self.assertEqual(
            [
                GoldenEvalDocument(
                    golden_document_id="unit_1",
                    test_type=GoldenEvalTestType.UNIT,
                    test_case="base_case",
                    state_code=StateCode.US_XX,
                    document_text=(
                        "The record is active. Assigned to dish duty at $12.50/hour."
                    ),
                    expected_values={
                        IS_RELEVANT_FIELD_NAME: True,
                        "primary_status": "active",
                        "status_note": "Currently active.",
                        "location": "Kitchen",
                        "assignments": [_DISH_DUTY_ELEMENT],
                    },
                ),
                GoldenEvalDocument(
                    golden_document_id="unit_2",
                    test_type=GoldenEvalTestType.UNIT,
                    test_case="missing_fields",
                    state_code=StateCode.US_XX,
                    document_text="No fictional assignment record here.",
                    # Every blank cell means the field is not expected at all.
                    expected_values={
                        IS_RELEVANT_FIELD_NAME: False,
                        "primary_status": None,
                        "status_note": None,
                        "location": None,
                        "assignments": None,
                    },
                ),
                GoldenEvalDocument(
                    golden_document_id="sample_1",
                    test_type=GoldenEvalTestType.SAMPLE,
                    test_case="realistic_note",
                    state_code=StateCode.US_XX,
                    document_text=(
                        "Record active. Assignments: dish duty at $12.50/hour, and "
                        "laundry."
                    ),
                    expected_values={
                        IS_RELEVANT_FIELD_NAME: True,
                        "primary_status": "active",
                        "status_note": "Currently active.",
                        "location": None,
                        "assignments": [
                            _DISH_DUTY_ELEMENT,
                            # A sub-field an element omits is not expected present.
                            {"assignment_name": "Laundry"},
                        ],
                    },
                ),
                GoldenEvalDocument(
                    golden_document_id="unit_3",
                    test_type=GoldenEvalTestType.UNIT,
                    test_case="no_assignments",
                    state_code=StateCode.US_XX,
                    document_text="The record is active with no assignments.",
                    expected_values={
                        IS_RELEVANT_FIELD_NAME: True,
                        "primary_status": "active",
                        "status_note": "Currently active.",
                        "location": "Kitchen",
                        # An empty array is expected present with zero elements,
                        # which a blank cell is not.
                        "assignments": [],
                    },
                ),
            ],
            [result.document for result in results],
        )

    def test_read_document_ignores_annotator_columns(self) -> None:
        # The `notes` column of the fixture is neither metadata nor an
        # expected-value column, and must not fail the column check.
        column_names, _ = _load_sheet(_HAPPY_PATH_FIXTURE)

        self.assertIn("notes", column_names)
        self.reader.verify_columns(column_names=column_names)
        self.assertEqual(4, len(self._read_happy_path_fixture()))

    # ------------------------------------------------------------------
    # Output schema coverage
    # ------------------------------------------------------------------

    def test_verify_columns_accepts_the_happy_path_fixture(self) -> None:
        column_names, _ = _load_sheet(_HAPPY_PATH_FIXTURE)

        self.reader.verify_columns(column_names=column_names)

    def test_verify_columns_missing_expected_column_for_schema_field(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Golden eval sheet [{_SOURCE_SHEET_URI}] cannot be read against "
                f"the extractor's output schema:\n"
                f"  - has no [location__expected] column, so output schema field "
                f"[location] would have no expectation"
            ),
        ):
            self.reader.verify_columns(
                column_names=list(_cells_without("location__expected"))
            )

    def test_verify_columns_missing_metadata_column(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"  - has no [{DOCUMENT_TEXT_COLUMN_NAME}] column, which every "
                f"golden eval sheet requires"
            ),
        ):
            self.reader.verify_columns(
                column_names=list(_cells_without(DOCUMENT_TEXT_COLUMN_NAME))
            )

    def test_verify_columns_stale_expected_value_column(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "  - column [removed_field__expected] names no field the output "
                "schema declares: ['assignments', 'is_relevant', 'location', "
                "'primary_status', 'status_note']"
            ),
        ):
            self.reader.verify_columns(
                column_names=[*_cells(), "removed_field__expected"]
            )

    def test_verify_columns_reports_every_problem_at_once(self) -> None:
        with self.assertRaises(ValueError) as caught:
            self.reader.verify_columns(
                column_names=[
                    *_cells_without(DOCUMENT_TEXT_COLUMN_NAME),
                    "removed_field__expected",
                ]
            )

        # One header line plus one line per problem: the missing metadata column
        # and the stale expected-value column.
        self.assertEqual(2, str(caught.exception).count("\n"))

    def test_expected_value_columns_match_output_schema(self) -> None:
        # Adding a field to FAKE_EXTRACTOR_COLLECTION's config must fail here,
        # rather than silently leaving the new field unlabeled in the fixture.
        column_names, _ = _load_sheet(_HAPPY_PATH_FIXTURE)

        self.assertEqual(
            {
                expected_value_column_name(field.name)
                for field in self.output_schema.all_fields
            },
            {c for c in column_names if c.endswith(EXPECTED_VALUE_COLUMN_SUFFIX)},
        )

    # ------------------------------------------------------------------
    # Type coercion
    # ------------------------------------------------------------------

    def test_read_document_coerces_expected_value_types(self) -> None:
        # `assertEqual` cannot make these assertions: `5 == 5.0` and `True == 1`,
        # but the scorer compares most types raw, so the Python type must match
        # what the extractor emits.
        document = self._read_happy_path_fixture()[0].document
        assert document is not None
        expected_values = document.expected_values
        self.assertIsInstance(expected_values[IS_RELEVANT_FIELD_NAME], bool)
        self.assertIsInstance(expected_values["primary_status"], str)
        self.assertIsInstance(expected_values["status_note"], str)
        self.assertIsInstance(expected_values["assignments"], list)
        rate_amount = expected_values["assignments"][0]["rate_amount"]
        self.assertIsInstance(rate_amount, float)
        self.assertNotIsInstance(rate_amount, bool)

        visits_document = self._read_one_visits_document(_visits_cells())
        visits_expected_values = visits_document.expected_values
        self.assertIsInstance(visits_expected_values["visit_count"], int)
        self.assertNotIsInstance(visits_expected_values["visit_count"], bool)
        self.assertIsInstance(visits_expected_values["visit_rate"], float)
        visit_number = visits_expected_values["visits"][0]["visit_number"]
        self.assertIsInstance(visit_number, int)
        self.assertNotIsInstance(visit_number, bool)

    def test_read_document_scores_as_correct_against_matching_output(self) -> None:
        # The end-to-end guard on coercion: a cell left as its raw string would
        # score as incorrect against the extractor's typed output.
        documents = [
            result.document
            for result in self._read_happy_path_fixture()
            if result.document is not None
        ]

        scores = LLMDocumentExtractionGoldenEvalScorer().score(
            output_schema=self.output_schema,
            documents=documents,
            actual_output_values_by_document_id={
                document_id: LLMRequestOutputValues(
                    output_schema=self.output_schema, output_json=output_json
                )
                for document_id, output_json in _matching_actual_output_json().items()
            },
        )

        self.assertTrue(scores)
        self.assertEqual(
            [],
            [
                (score.golden_document_id, score.field_name, score.expected_value)
                for score in scores
                if not score.is_correct
            ],
        )

    # ------------------------------------------------------------------
    # Malformed scalar cells
    # ------------------------------------------------------------------

    def test_read_document_invalid_boolean(self) -> None:
        self.assertEqual(
            [
                _one_cell_issue(
                    column_name="is_relevant__expected",
                    value="yes",
                    reason="could not be parsed as a boolean; expected true or false",
                )
            ],
            self._read_one_row_issues(_cells(is_relevant__expected="yes")),
        )

    def test_read_document_blank_is_relevant(self) -> None:
        self.assertEqual(
            [
                _one_cell_issue(
                    column_name="is_relevant__expected",
                    value="",
                    reason="is a required boolean field for golden evals.",
                )
            ],
            self._read_one_row_issues(_cells(is_relevant__expected="")),
        )

    def test_read_document_blank_required_field(self) -> None:
        self.assertEqual(
            [
                _one_cell_issue(
                    column_name="primary_status__expected",
                    value="",
                    reason=(
                        "is blank, but field [primary_status] is required of every "
                        "relevant document"
                    ),
                )
            ],
            self._read_one_row_issues(_cells(primary_status__expected="")),
        )

    def test_read_document_blank_required_field_of_irrelevant_document(self) -> None:
        document = self._read_one_document(
            _cells(
                is_relevant__expected="false",
                primary_status__expected="",
                status_note__expected="",
                location__expected="",
                assignments__expected="",
            )
        )

        self.assertIsNone(document.expected_values["primary_status"])

    def test_read_document_invalid_integer(self) -> None:
        self.assertEqual(
            [
                _one_cell_issue(
                    column_name="visit_count__expected",
                    value="5.0",
                    reason=(
                        "could not be parsed as an integer; ensure the number has no "
                        "formatting in the cell."
                    ),
                )
            ],
            self._read_one_visits_row_issues(
                _visits_cells(visit_count__expected="5.0")
            ),
        )

    def test_read_document_invalid_float(self) -> None:
        self.assertEqual(
            [
                _one_cell_issue(
                    column_name="visit_rate__expected",
                    value="$12.50",
                    reason=(
                        "could not be parsed as a float; ensure the number has no "
                        "formatting in the cell."
                    ),
                )
            ],
            self._read_one_visits_row_issues(
                _visits_cells(visit_rate__expected="$12.50")
            ),
        )

    def test_read_document_non_finite_float(self) -> None:
        self.assertEqual(
            [
                _one_cell_issue(
                    column_name="visit_rate__expected",
                    value="inf",
                    reason="must be a finite number.",
                )
            ],
            self._read_one_visits_row_issues(_visits_cells(visit_rate__expected="inf")),
        )

    def test_read_document_unknown_enum_value(self) -> None:
        self.assertEqual(
            [
                _one_cell_issue(
                    column_name="primary_status__expected",
                    value="pending",
                    reason=(
                        "is not one of the values field [primary_status] declares: "
                        "['active', 'inactive', 'other']"
                    ),
                )
            ],
            self._read_one_row_issues(_cells(primary_status__expected="pending")),
        )

    def test_read_document_enum_value_spelled_differently_than_declared(self) -> None:
        # The sheet must spell the value exactly as the field declares it.
        self.assertEqual(
            [
                _one_cell_issue(
                    column_name="primary_status__expected",
                    value="ACTIVE",
                    reason=(
                        "is not one of the values field [primary_status] declares: "
                        "['active', 'inactive', 'other']"
                    ),
                )
            ],
            self._read_one_row_issues(_cells(primary_status__expected="ACTIVE")),
        )

    # ------------------------------------------------------------------
    # Malformed ARRAY_OF_STRUCT cells
    # ------------------------------------------------------------------

    def test_read_document_invalid_array_json(self) -> None:
        (issue,) = self._read_one_row_issues(
            _cells(assignments__expected="[{assignment_name: Dish duty}]")
        )
        self.assertRegex(
            issue,
            r"^"
            + re.escape(
                "row [2] column [assignments__expected] value "
                "[[{assignment_name: Dish duty}]]: is not valid JSON ("
            )
            + r".*"
            + re.escape(
                "); expected a JSON array of objects keyed by the sub-field "
                "names of field [assignments]"
            )
            + r"$",
        )

    def test_read_document_array_cell_is_not_an_array(self) -> None:
        self.assertEqual(
            [
                _one_cell_issue(
                    column_name="assignments__expected",
                    value=_DISH_DUTY_ELEMENT_JSON,
                    reason=(
                        "is a JSON [dict], but ARRAY_OF_STRUCT field [assignments] "
                        "expects a JSON array of objects"
                    ),
                )
            ],
            self._read_one_row_issues(
                _cells(assignments__expected=_DISH_DUTY_ELEMENT_JSON)
            ),
        )

    def test_read_document_array_element_is_not_an_object(self) -> None:
        self.assertEqual(
            [
                _one_cell_issue(
                    column_name="assignments__expected",
                    value='["Dish duty"]',
                    reason=(
                        "element [0] is a JSON [str], but every element of "
                        "ARRAY_OF_STRUCT field [assignments] must be a JSON object "
                        "keyed by sub-field name"
                    ),
                )
            ],
            self._read_one_row_issues(_cells(assignments__expected='["Dish duty"]')),
        )

    def test_read_document_array_element_has_unknown_sub_field(self) -> None:
        cell = '[{"assignment_name": "Dish duty", "assignment_nmae": "Dish duty"}]'

        self.assertEqual(
            [
                _one_cell_issue(
                    column_name="assignments__expected",
                    value=cell,
                    reason=(
                        "element [0] holds key(s) ['assignment_nmae'] that are not "
                        "sub-fields of field [assignments]: ['assignment_name', "
                        "'assignment_type', 'rate_amount', 'rate_period']"
                    ),
                )
            ],
            self._read_one_row_issues(_cells(assignments__expected=cell)),
        )

    def test_read_document_string_sub_field_given_a_number(self) -> None:
        self.assertEqual(
            [
                _one_cell_issue(
                    column_name="assignments__expected",
                    value='[{"assignment_name": 12}]',
                    reason=(
                        "element [0] sub-field [assignment_name] of type [STRING] "
                        "expects a string, but the element holds [12]"
                    ),
                )
            ],
            self._read_one_row_issues(
                _cells(assignments__expected='[{"assignment_name": 12}]')
            ),
        )

    def test_read_document_float_sub_field_accepts_an_integer(self) -> None:
        # A sub-field defined as a FLOAT accepts an integer
        document = self._read_one_document(
            _cells(
                assignments__expected='[{"assignment_name": "Dish duty", "rate_amount": 12}]'
            )
        )

        rate_amount = document.expected_values["assignments"][0]["rate_amount"]
        self.assertIsInstance(rate_amount, int)
        self.assertNotIsInstance(rate_amount, bool)

    def test_read_document_integer_sub_field_accepts_an_integral_float(self) -> None:
        # A sub-field defined as an INTEGER accepts a float with a zero fractional part.
        document = self._read_one_visits_document(
            _visits_cells(
                visits__expected='[{"visit_name": "Intake", "visit_number": 1.0}]'
            )
        )

        visit_number = document.expected_values["visits"][0]["visit_number"]
        self.assertIsInstance(visit_number, float)
        self.assertEqual(1, visit_number)

    def test_read_document_integer_sub_field_rejects_a_fractional_number(self) -> None:
        # A sub-field defined as an INTEGER rejects a float with a non-zero fractional part.
        cell = '[{"visit_name": "Intake", "visit_number": 1.5}]'

        self.assertEqual(
            [
                _one_cell_issue(
                    column_name="visits__expected",
                    value=cell,
                    reason=(
                        "element [0] sub-field [visit_number] of type [INTEGER] "
                        "expects an unquoted integer, but the element holds [1.5]"
                    ),
                )
            ],
            self._read_one_visits_row_issues(_visits_cells(visits__expected=cell)),
        )

    def test_read_document_integer_sub_field_given_a_boolean(self) -> None:
        # A sub-field defined as an INTEGER rejects a boolean
        cell = '[{"visit_name": "Intake", "visit_number": true}]'

        self.assertEqual(
            [
                _one_cell_issue(
                    column_name="visits__expected",
                    value=cell,
                    reason=(
                        "element [0] sub-field [visit_number] of type [INTEGER] "
                        "expects an unquoted integer, but the element holds [true]"
                    ),
                )
            ],
            self._read_one_visits_row_issues(_visits_cells(visits__expected=cell)),
        )

    def test_read_document_array_element_missing_primary_key(self) -> None:
        cell = '[{"assignment_type": "internal"}]'

        self.assertEqual(
            [
                _one_cell_issue(
                    column_name="assignments__expected",
                    value=cell,
                    reason=(
                        "element [0] has no value for primary key sub-field(s) "
                        "['assignment_name'] of field [assignments], which expected "
                        "and actual elements pair on"
                    ),
                )
            ],
            self._read_one_row_issues(_cells(assignments__expected=cell)),
        )

    def test_read_document_array_elements_share_a_primary_key(self) -> None:
        # The pairing key is normalized the way the scorer's comparison key is,
        # so two spellings of one name are duplicates here too.
        cell = (
            '[{"assignment_name": "Dish duty"}, {"assignment_name": "  dish   DUTY "}]'
        )

        self.assertEqual(
            [
                _one_cell_issue(
                    column_name="assignments__expected",
                    value=cell,
                    reason=(
                        "element [1] has the same primary key values as element [0], "
                        "so the two cannot be told apart when pairing on "
                        "['assignment_name'] of field [assignments]"
                    ),
                )
            ],
            self._read_one_row_issues(_cells(assignments__expected=cell)),
        )

    def test_read_document_array_element_missing_required_sub_field(self) -> None:
        cell = '[{"visit_name": "Intake"}]'

        self.assertEqual(
            [
                _one_cell_issue(
                    column_name="visits__expected",
                    value=cell,
                    reason=(
                        "element [0] has no value for required sub-field(s) "
                        "['visit_number'] of field [visits]"
                    ),
                )
            ],
            self._read_one_visits_row_issues(_visits_cells(visits__expected=cell)),
        )

    def test_read_document_array_holds_fewer_elements_than_min_items(self) -> None:
        self.assertEqual(
            [
                _one_cell_issue(
                    column_name="visits__expected",
                    value="[]",
                    reason=(
                        "holds [0] element(s), but ARRAY_OF_STRUCT field [visits] "
                        "requires at least [1]"
                    ),
                )
            ],
            self._read_one_visits_row_issues(_visits_cells(visits__expected="[]")),
        )

    # ------------------------------------------------------------------
    # Error accumulation
    # ------------------------------------------------------------------

    def test_read_document_reports_every_unreadable_cell_in_one_row(self) -> None:
        # Ordered by the sheet's header order, not by when they were read.
        self.assertEqual(
            [
                _one_cell_issue(
                    column_name=TEST_TYPE_COLUMN_NAME,
                    value="monthly",
                    reason=(
                        "is not one of the golden eval test types ['sample', 'unit']"
                    ),
                ),
                _one_cell_issue(
                    column_name=DOCUMENT_TEXT_COLUMN_NAME,
                    value="",
                    reason="is blank, but is required",
                ),
                _one_cell_issue(
                    column_name="is_relevant__expected",
                    value="yes",
                    reason="could not be parsed as a boolean; expected true or false",
                ),
            ],
            self._read_one_row_issues(
                _cells(
                    test_type="monthly",
                    document_text="",
                    is_relevant__expected="yes",
                )
            ),
        )

    def test_read_document_reports_every_invalid_value_in_one_row(self) -> None:
        cell = '[{"assignment_name": 12}]'

        self.assertEqual(
            [
                _one_cell_issue(
                    column_name="primary_status__expected",
                    value="pending",
                    reason=(
                        "is not one of the values field [primary_status] declares: "
                        "['active', 'inactive', 'other']"
                    ),
                ),
                _one_cell_issue(
                    column_name="assignments__expected",
                    value=cell,
                    reason=(
                        "element [0] sub-field [assignment_name] of type [STRING] "
                        "expects a string, but the element holds [12]"
                    ),
                ),
            ],
            self._read_one_row_issues(
                _cells(
                    primary_status__expected="pending",
                    assignments__expected=cell,
                )
            ),
        )

    def test_read_document_unreadable_cell_is_not_validated(self) -> None:
        # Validation runs only once every cell has been read, so the values it
        # judges are never the ones that failed to parse. `pending` is a value the
        # schema does not declare, and goes unreported while the row holds a cell
        # that could not be read at all.
        self.assertEqual(
            [
                _one_cell_issue(
                    column_name=DOCUMENT_TEXT_COLUMN_NAME,
                    value="",
                    reason="is blank, but is required",
                )
            ],
            self._read_one_row_issues(
                _cells(document_text="", primary_status__expected="pending")
            ),
        )

    # ------------------------------------------------------------------
    # Malformed metadata cells
    # ------------------------------------------------------------------

    def test_read_document_invalid_test_type(self) -> None:
        self.assertEqual(
            [
                _one_cell_issue(
                    column_name=TEST_TYPE_COLUMN_NAME,
                    value="regression",
                    reason=(
                        "is not one of the golden eval test types ['sample', 'unit']"
                    ),
                )
            ],
            self._read_one_row_issues(_cells(test_type="regression")),
        )

    def test_read_document_blank_document_text(self) -> None:
        self.assertEqual(
            [
                _one_cell_issue(
                    column_name=DOCUMENT_TEXT_COLUMN_NAME,
                    value="   ",
                    reason="is blank, but is required",
                )
            ],
            self._read_one_row_issues(_cells(document_text="   ")),
        )

    def test_read_document_blank_golden_document_id(self) -> None:
        self.assertEqual(
            [
                _one_cell_issue(
                    column_name=GOLDEN_DOCUMENT_ID_COLUMN_NAME,
                    value="",
                    reason="is blank, but is required",
                )
            ],
            self._read_one_row_issues(_cells(golden_document_id="")),
        )

    # ------------------------------------------------------------------
    # Construction from an extractor config
    # ------------------------------------------------------------------

    def test_from_config_builds_reader_for_the_extractor(self) -> None:
        reader = GoldenEvalDocumentReader.from_config(
            config=self.config, sheets_reader=self.sheets_reader
        )

        self.assertEqual(StateCode.US_XX, reader.state_code)
        self.assertEqual(self.output_schema, reader.parser.output_schema)
        self.assertEqual(_SOURCE_SHEET_URI, reader.source_sheet_uri)

    def test_from_config_without_golden_eval_raises(self) -> None:
        config = attr.evolve(self.config, golden_eval=None)

        with self.assertRaisesRegex(
            ValueError,
            rf"^Extractor \[{re.escape(config.extractor_id)}\] declares no "
            r"golden_eval config, so it cannot be used to run a golden eval\.$",
        ):
            GoldenEvalDocumentReader.from_config(
                config=config, sheets_reader=self.sheets_reader
            )

    # ------------------------------------------------------------------
    # Reading the whole sheet
    # ------------------------------------------------------------------

    def test_load_all_documents_reads_the_configured_sheet(self) -> None:
        self.sheets_reader.read_tab_as_table.return_value = _tab(_cells())

        documents = self.reader.load_all_documents()

        self.sheets_reader.read_tab_as_table.assert_called_once_with(
            sheet_uri=_SOURCE_SHEET_URI
        )
        self.assertEqual(1, len(documents))
        document = documents[0]
        self.assertEqual("unit_1", document.golden_document_id)
        self.assertEqual(GoldenEvalTestType.UNIT, document.test_type)
        self.assertEqual("base_case", document.test_case)
        self.assertEqual(StateCode.US_XX, document.state_code)
        self.assertEqual("The record is active.", document.document_text)

    def test_load_all_documents_fails_on_a_column_problem(self) -> None:
        # Column problems are sheet-wide, so they raise before any row is read.
        self.sheets_reader.read_tab_as_table.return_value = attr.evolve(
            _tab(_cells()), column_names=[*_cells(), "no_such_field__expected"]
        )

        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Golden eval sheet [{_SOURCE_SHEET_URI}] cannot be read against "
                f"the extractor's output schema:\n"
                f"  - column [no_such_field__expected] names no field the output "
                f"schema declares:"
            ),
        ):
            self.reader.load_all_documents()

    def test_documents_yields_one_result_at_a_time(self) -> None:
        self.sheets_reader.read_tab_as_table.return_value = _tab(
            _cells(golden_document_id="unit_1"),
            _cells(golden_document_id="unit_2"),
        )

        results = self.reader.documents()

        first = next(results)
        self.assertEqual(2, first.row_number)
        self.assertEqual(
            "unit_1", assert_type(first.document, GoldenEvalDocument).golden_document_id
        )
        second = next(results)
        self.assertEqual(3, second.row_number)
        self.assertEqual(
            "unit_2",
            assert_type(second.document, GoldenEvalDocument).golden_document_id,
        )
        with self.assertRaises(StopIteration):
            next(results)

    def test_documents_yields_issues_and_keeps_going(self) -> None:
        # The whole point of yielding results rather than raising: a malformed row
        # does not kill the iterator, so a caller still sees every later row.
        self.sheets_reader.read_tab_as_table.return_value = _tab(
            _cells(golden_document_id="unit_1", is_relevant__expected="yes"),
            _cells(golden_document_id="unit_2"),
        )

        results = list(self.reader.documents())

        self.assertIsNone(results[0].document)
        self.assertEqual(
            [
                "row [2] column [is_relevant__expected] value [yes]: could not "
                "be parsed as a boolean; expected true or false"
            ],
            [issue.rendered for issue in results[0].issues],
        )
        self.assertEqual(
            "unit_2",
            assert_type(results[1].document, GoldenEvalDocument).golden_document_id,
        )
        self.assertEqual([], results[1].issues)

    def test_load_all_documents_returns_rows_in_sheet_order(self) -> None:
        self.sheets_reader.read_tab_as_table.return_value = _tab(
            _cells(golden_document_id="unit_1"),
            _cells(golden_document_id="unit_2"),
            _cells(golden_document_id="unit_3"),
        )

        documents = self.reader.load_all_documents()

        self.assertEqual(
            ["unit_1", "unit_2", "unit_3"],
            [document.golden_document_id for document in documents],
        )

    def test_load_all_documents_reports_every_malformed_row_at_once(self) -> None:
        self.sheets_reader.read_tab_as_table.return_value = _tab(
            _cells(golden_document_id="unit_1", primary_status__expected="bogus"),
            _cells(golden_document_id="unit_2"),
            _cells(golden_document_id="unit_3", is_relevant__expected="yes"),
        )

        with self.assertRaises(GoldenEvalDocumentSourceError) as caught:
            self.reader.load_all_documents()

        self.assertEqual(
            [
                "row [2] column [primary_status__expected] value [bogus]: is not "
                "one of the values field [primary_status] declares: "
                "['active', 'inactive', 'other']",
                "row [4] column [is_relevant__expected] value [yes]: could not "
                "be parsed as a boolean; expected true or false",
            ],
            caught.exception.issues,
        )
        self.assertIn(
            f"Found [2] problem(s) in golden eval sheet [{_SOURCE_SHEET_URI}]",
            str(caught.exception),
        )

    def test_load_all_documents_reports_duplicate_document_ids(self) -> None:
        self.sheets_reader.read_tab_as_table.return_value = _tab(
            _cells(golden_document_id="unit_1"),
            _cells(golden_document_id="unit_2"),
            _cells(golden_document_id="unit_1"),
        )

        with self.assertRaises(GoldenEvalDocumentSourceError) as caught:
            self.reader.load_all_documents()

        self.assertEqual(
            ["golden_document_id [unit_1] is reused in multiple rows [2, 4]"],
            caught.exception.issues,
        )

    def test_load_all_documents_reports_cell_and_duplicate_issues_together(
        self,
    ) -> None:
        self.sheets_reader.read_tab_as_table.return_value = _tab(
            _cells(golden_document_id="unit_1"),
            _cells(golden_document_id="unit_2", is_relevant__expected="yes"),
            _cells(golden_document_id="unit_1"),
        )

        with self.assertRaises(GoldenEvalDocumentSourceError) as caught:
            self.reader.load_all_documents()

        self.assertEqual(
            [
                "row [3] column [is_relevant__expected] value [yes]: could not "
                "be parsed as a boolean; expected true or false",
                "golden_document_id [unit_1] is reused in multiple rows [2, 4]",
            ],
            caught.exception.issues,
        )

    def test_load_all_documents_warns_when_a_test_type_is_unrepresented(self) -> None:
        self.sheets_reader.read_tab_as_table.return_value = _tab(
            _cells(test_type="unit")
        )

        with self.assertLogs(level="WARNING") as logs:
            self.reader.load_all_documents()

        self.assertEqual(
            1,
            len([log for log in logs.output if "['sample']" in log]),
        )

    def test_load_all_documents_does_not_warn_when_every_test_type_is_present(
        self,
    ) -> None:
        self.sheets_reader.read_tab_as_table.return_value = _tab(
            _cells(golden_document_id="unit_1", test_type="unit"),
            _cells(golden_document_id="sample_1", test_type="sample"),
        )

        with self.assertNoLogs(level="WARNING"):
            self.reader.load_all_documents()

    def test_load_all_documents_collapses_an_issue_every_row_shares(self) -> None:
        # A mistake repeated down a whole column puts the same issue on every
        # row, and repeating it once per row would bury everything else.
        self.sheets_reader.read_tab_as_table.return_value = _tab(
            _cells(golden_document_id="unit_1", is_relevant__expected="yes"),
            _cells(golden_document_id="unit_2", is_relevant__expected="yes"),
        )

        with self.assertRaises(GoldenEvalDocumentSourceError) as caught:
            self.reader.load_all_documents()

        self.assertEqual(
            [
                "every row: column [is_relevant__expected]: could not be parsed as "
                "a boolean; expected true or false"
            ],
            caught.exception.issues,
        )

    def test_load_all_documents_leaves_a_one_row_issue_alone(self) -> None:
        self.sheets_reader.read_tab_as_table.return_value = _tab(
            _cells(golden_document_id="unit_1", is_relevant__expected="yes"),
            _cells(golden_document_id="unit_2"),
        )

        with self.assertRaises(GoldenEvalDocumentSourceError) as caught:
            self.reader.load_all_documents()

        self.assertEqual(
            [
                "row [2] column [is_relevant__expected] value [yes]: could not be "
                "parsed as a boolean; expected true or false"
            ],
            caught.exception.issues,
        )


def _matching_actual_output_json() -> dict[str, dict[str, Any]]:
    """Returns the extractor output that matches the happy-path fixture's expected
    values on every field, keyed by golden document id.
    """
    dish_duty = build_fake_extractor_assignment_result_json(
        "Dish duty", "internal", 12.5, "hourly"
    )
    return {
        "unit_1": wrap_in_result_key(
            build_fake_extractor_result_content(
                primary_status="active",
                status_note="Currently active.",
                location="Kitchen",
                assignments=[dish_duty],
            )
        ),
        "unit_2": wrap_in_result_key(build_fake_extractor_irrelevant_result_content()),
        "sample_1": wrap_in_result_key(
            build_fake_extractor_result_content(
                primary_status="active",
                status_note="Currently active.",
                location=None,
                assignments=[
                    dish_duty,
                    {
                        "assignment_name": build_inferred_field_result_json("Laundry"),
                        "assignment_type": build_null_inferred_field_result_json(),
                        "rate_amount": build_null_inferred_field_result_json(),
                        "rate_period": build_null_inferred_field_result_json(),
                    },
                ],
            )
        ),
        "unit_3": wrap_in_result_key(
            build_fake_extractor_result_content(
                primary_status="active",
                status_note="Currently active.",
                location="Kitchen",
                assignments=[],
            )
        ),
    }
