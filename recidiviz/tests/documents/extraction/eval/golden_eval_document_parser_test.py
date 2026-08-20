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
"""Tests for GoldenEvalDocumentParser.

Covers the contract GoldenEvalDocumentReader depends on and its own tests cannot
see: which cells the parser fails on, and which readable-but-illegal values it
passes through for GoldenEvalDocumentValidator to judge. The rendered text of
each issue is asserted in golden_eval_document_reader_test.py.
"""
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.eval.golden_eval_document_parser import (
    DOCUMENT_TEXT_COLUMN_NAME,
    GOLDEN_DOCUMENT_ID_COLUMN_NAME,
    TEST_CASE_COLUMN_NAME,
    TEST_TYPE_COLUMN_NAME,
    GoldenEvalDocumentParseError,
    GoldenEvalDocumentParser,
    expected_value_column_name,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    IS_RELEVANT_FIELD_NAME,
)
from recidiviz.tests.documents import fake_config
from recidiviz.utils.google_sheets_reader import GoogleSheetTableRow

_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"

_DISH_DUTY_ELEMENT_JSON = (
    '{"assignment_name": "Dish duty", "assignment_type": "internal", '
    '"rate_amount": 12.5, "rate_period": "hourly"}'
)


def _row(**overrides: str) -> GoogleSheetTableRow:
    """Returns one sheet row, defaulting to cell text that parses against
    FAKE_EXTRACTOR_COLLECTION's schema and allowing passed-in overrides.
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
    return GoogleSheetTableRow(row_number=2, values_by_column=cells)


def _column_names_without(column_name: str) -> list[str]:
    """Returns the default row's column names with |column_name| left out,
    standing in for a sheet that never carried the column.
    """
    return [name for name in _row().values_by_column if name != column_name]


class GoldenEvalDocumentParserTest(TestCase):
    """Tests for GoldenEvalDocumentParser."""

    def setUp(self) -> None:
        self.parser = GoldenEvalDocumentParser(
            state_code=StateCode.US_XX,
            output_schema=get_first_order_llm_extractor_config(
                StateCode.US_XX, _COLLECTION_NAME, config_module=fake_config
            ).extractor_collection.output_schema,
        )

    def _parse_error(self, row: GoogleSheetTableRow) -> GoldenEvalDocumentParseError:
        """Returns the error raised by parsing |row|."""
        with self.assertRaises(GoldenEvalDocumentParseError) as caught:
            self.parser.parse(row=row)
        return caught.exception

    def test_parse_stamps_the_extractors_state_onto_the_document(self) -> None:
        self.assertEqual(StateCode.US_XX, self.parser.parse(row=_row()).state_code)

    def test_parse_reports_every_unreadable_cell_at_once(self) -> None:
        error = self._parse_error(
            _row(
                test_type="monthly",
                document_text="",
                **{expected_value_column_name(IS_RELEVANT_FIELD_NAME): "yes"},
            )
        )

        self.assertEqual(
            [
                TEST_TYPE_COLUMN_NAME,
                DOCUMENT_TEXT_COLUMN_NAME,
                expected_value_column_name(IS_RELEVANT_FIELD_NAME),
            ],
            list(error.issues_by_column),
        )

    def test_parse_fails_on_a_cell_no_declared_type_can_read(self) -> None:
        error = self._parse_error(
            _row(**{expected_value_column_name("assignments"): "not json"})
        )

        self.assertEqual(
            [expected_value_column_name("assignments")], list(error.issues_by_column)
        )

    def test_column_issues_accepts_a_complete_sheet(self) -> None:
        self.assertEqual(
            [], self.parser.column_issues(column_names=list(_row().values_by_column))
        )

    def test_column_issues_ignores_annotator_columns(self) -> None:
        self.assertEqual(
            [],
            self.parser.column_issues(column_names=[*_row().values_by_column, "notes"]),
        )

    def test_column_issues_missing_metadata_column(self) -> None:
        self.assertEqual(
            [
                f"has no [{DOCUMENT_TEXT_COLUMN_NAME}] column, which every golden "
                f"eval sheet requires"
            ],
            self.parser.column_issues(
                column_names=_column_names_without(DOCUMENT_TEXT_COLUMN_NAME)
            ),
        )

    def test_column_issues_missing_expected_column_for_declared_field(self) -> None:
        self.assertEqual(
            [
                "has no [location__expected] column, so output schema field "
                "[location] would have no expectation"
            ],
            self.parser.column_issues(
                column_names=_column_names_without(
                    expected_value_column_name("location")
                )
            ),
        )

    def test_column_issues_expected_column_naming_no_declared_field(self) -> None:
        self.assertEqual(
            [
                "column [no_such_field__expected] names no field the output schema "
                "declares: ['assignments', 'is_relevant', 'location', "
                "'primary_status', 'status_note']"
            ],
            self.parser.column_issues(
                column_names=[*_row().values_by_column, "no_such_field__expected"]
            ),
        )

    def test_column_issues_reports_every_problem_at_once(self) -> None:
        self.assertEqual(
            3,
            len(
                self.parser.column_issues(
                    column_names=[
                        *_column_names_without(TEST_CASE_COLUMN_NAME)[:-1],
                        "no_such_field__expected",
                    ]
                )
            ),
        )

    def test_parse_passes_through_an_undeclared_enum_value(self) -> None:
        # Readable as a string, so the parser hands it on rather than failing —
        # GoldenEvalDocumentValidator is what rejects it.
        document = self.parser.parse(
            row=_row(**{expected_value_column_name("primary_status"): "pending"})
        )

        self.assertEqual("pending", document.expected_values["primary_status"])

    def test_parse_passes_through_a_wrongly_typed_sub_field_value(self) -> None:
        document = self.parser.parse(
            row=_row(
                **{
                    expected_value_column_name(
                        "assignments"
                    ): '[{"assignment_name": 12}]'
                }
            )
        )

        self.assertEqual(
            [{"assignment_name": 12}], document.expected_values["assignments"]
        )

    def test_parse_passes_through_json_that_is_not_an_array_of_objects(self) -> None:
        document = self.parser.parse(
            row=_row(**{expected_value_column_name("assignments"): '{"a": 1}'})
        )

        self.assertEqual({"a": 1}, document.expected_values["assignments"])

    def test_parse_leaves_a_blank_is_relevant_null(self) -> None:
        # Required-ness is the validator's call, so a blank cell parses cleanly.
        document = self.parser.parse(
            row=_row(**{expected_value_column_name(IS_RELEVANT_FIELD_NAME): ""})
        )

        self.assertIsNone(document.expected_values[IS_RELEVANT_FIELD_NAME])

    def test_parse_keeps_enum_spelling_as_written(self) -> None:
        # The sheet must spell the value exactly as the field declares it;
        # GoldenEvalDocumentValidator is what rejects this spelling.
        document = self.parser.parse(
            row=_row(**{expected_value_column_name("primary_status"): "ACTIVE"})
        )

        self.assertEqual("ACTIVE", document.expected_values["primary_status"])

    def test_parse_keeps_array_sub_field_values_as_json_reads_them(self) -> None:
        # JSON already types every sub-field value, so the parser hands the
        # array cell on exactly as json.loads reads it — 12 stays an int, and
        # GoldenEvalDocumentValidator is what checks it against the FLOAT field.
        document = self.parser.parse(
            row=_row(
                **{
                    expected_value_column_name("assignments"): (
                        '[{"assignment_name": "Dish duty", "rate_amount": 12}]'
                    )
                }
            )
        )

        rate_amount = document.expected_values["assignments"][0]["rate_amount"]
        self.assertIsInstance(rate_amount, int)
        self.assertNotIsInstance(rate_amount, bool)

    def test_parse_leaves_an_unknown_element_key_untouched(self) -> None:
        document = self.parser.parse(
            row=_row(
                **{
                    expected_value_column_name("assignments"): (
                        '[{"assignment_name": "Dish duty", "assignment_nmae": "x"}]'
                    )
                }
            )
        )

        self.assertEqual(
            [{"assignment_name": "Dish duty", "assignment_nmae": "x"}],
            document.expected_values["assignments"],
        )
