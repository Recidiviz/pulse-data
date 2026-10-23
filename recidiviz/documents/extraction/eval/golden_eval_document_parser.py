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
"""Turns the raw cells of one golden eval Google Sheet row into a
GoldenEvalDocument, or of one entity-resolution golden eval row into a
GoldenEvalEntityResolutionDocument.
"""
import datetime
import json
from typing import Any

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.eval.golden_eval_document import (
    GoldenEvalDocument,
    GoldenEvalEntityResolutionDocument,
)
from recidiviz.documents.extraction.llm_extraction_view_graph_stage import (
    LLMExtractionViewGraphStage,
)
from recidiviz.documents.extraction.models.llm_document_extraction_golden_eval_config import (
    GoldenEvalTestType,
)
from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ArrayOfStructLLMRequestOutputSchemaField,
    LLMOutputFieldType,
    LLMRequestOutputSchemaField,
    ScalarValuedLLMRequestOutputSchemaField,
)
from recidiviz.utils.google_sheets_reader import GoogleSheetTableRow
from recidiviz.utils.params import str_to_bool
from recidiviz.utils.types import assert_type

# The metadata column headers a golden eval sheet carries, alongside one
# `{field}__expected` column per output schema field.
TEST_TYPE_COLUMN_NAME = "test_type"
TEST_CASE_COLUMN_NAME = "test_case"
GOLDEN_DOCUMENT_ID_COLUMN_NAME = "document_id"
DOCUMENT_TEXT_COLUMN_NAME = "document_text"

REQUIRED_METADATA_COLUMN_NAMES = (
    TEST_TYPE_COLUMN_NAME,
    TEST_CASE_COLUMN_NAME,
    GOLDEN_DOCUMENT_ID_COLUMN_NAME,
    DOCUMENT_TEXT_COLUMN_NAME,
)

EXPECTED_VALUE_COLUMN_SUFFIX = "__expected"

# The extra metadata columns an entity-resolution golden eval sheet
# carries.
TEST_GROUP_ID_COLUMN_NAME = "test_group_id"
DOCUMENT_DATE_COLUMN_NAME = "document_date"

REQUIRED_ENTITY_RESOLUTION_METADATA_COLUMN_NAMES = (
    TEST_GROUP_ID_COLUMN_NAME,
    DOCUMENT_DATE_COLUMN_NAME,
)

EXPECTED_RESOLVED_VALUE_COLUMN_SUFFIX = f"{EXPECTED_VALUE_COLUMN_SUFFIX}__er"


def expected_value_column_name(field_name: str) -> str:
    """Returns the name column holding the expected value of field |field_name|."""
    return f"{field_name}{EXPECTED_VALUE_COLUMN_SUFFIX}"


def expected_resolved_value_column_name(field_name: str) -> str:
    """Returns the name of the column holding the value of field |field_name|
    expected after entity resolution.
    """
    return f"{field_name}{EXPECTED_RESOLVED_VALUE_COLUMN_SUFFIX}"


def expected_column_name_for_stage(
    *, field_name: str, stage: LLMExtractionViewGraphStage
) -> str:
    """Returns the name of the column holding the value of field |field_name|
    expected after |stage|.
    """
    if stage is LLMExtractionViewGraphStage.FIRST_ORDER:
        return expected_value_column_name(field_name)
    if stage is LLMExtractionViewGraphStage.ENTITY_RESOLUTION:
        return expected_resolved_value_column_name(field_name)
    raise ValueError(f"Unexpected stage: [{stage}]")


def _cell(*, row: GoogleSheetTableRow, column_name: str) -> str:
    """Returns the stripped text of |row|'s |column_name| cell."""
    return row.values_by_column[column_name].strip()


class GoldenEvalDocumentParseError(ValueError):
    """Every cell of one golden eval sheet row whose text could not be read as the
    type its column declares.
    """

    def __init__(self, *, row_number: int, issues_by_column: dict[str, str]) -> None:
        super().__init__(
            f"Could not read golden eval sheet cell(s) in row [{row_number}]: "
            + ", ".join(
                f"[{column_name}] {issue}"
                for column_name, issue in issues_by_column.items()
            )
        )
        self.row_number = row_number
        """1-indexed position of the sheet row the unreadable cells sit in."""
        self.issues_by_column = issues_by_column
        """One issue per unreadable cell, keyed by column name."""


class GoldenEvalCellError(ValueError):
    """One golden eval sheet cell whose text could not be read."""


@attr.define(frozen=True, kw_only=True)
class GoldenEvalDocumentParser:
    """Turns the raw cells of one golden eval Google Sheet row into a
    GoldenEvalDocument, coercing each expected cell to the Python type its output
    schema field declares.
    """

    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))
    """The state of the extractor being evaluated, stamped onto every document
    parsed. Each state's eval set has its own sheet tab.
    """

    output_schema: LLMRequestOutputSchema = attr.ib(
        validator=attr.validators.instance_of(LLMRequestOutputSchema)
    )
    """The output schema of the extractor being evaluated, which fixes the Python
    type each expected cell is coerced to.
    """

    def column_issues(self, *, column_names: list[str]) -> list[str]:
        """Returns one issue per column problem that stops a sheet with
        |column_names| from being parsed against the output schema: a missing
        metadata column, a declared field with no `__expected` column, or an
        `__expected` column naming no declared field. Columns that are neither
        metadata nor `__expected` are ignored.
        """
        columns = set(column_names)
        declared_field_names = sorted(
            field.name for field in self.output_schema.all_fields
        )
        issues = [
            f"has no [{column_name}] column, which every golden eval sheet requires"
            for column_name in REQUIRED_METADATA_COLUMN_NAMES
            if column_name not in columns
        ]
        issues.extend(
            f"has no [{expected_value_column_name(field_name)}] column, so output "
            f"schema field [{field_name}] would have no expectation"
            for field_name in declared_field_names
            if expected_value_column_name(field_name) not in columns
        )
        issues.extend(
            f"column [{column_name}] names no field the output schema declares: "
            f"{declared_field_names}"
            for column_name in column_names
            if column_name.endswith(EXPECTED_VALUE_COLUMN_SUFFIX)
            and column_name[: -len(EXPECTED_VALUE_COLUMN_SUFFIX)]
            not in declared_field_names
        )
        return issues

    def parse(self, *, row: GoogleSheetTableRow) -> GoldenEvalDocument:
        """Returns the document |row| describes, raising a
        GoldenEvalDocumentParseError naming every cell of |row| whose text could
        not be read. The row's columns must first have passed column_issues.
        """
        issues_by_column: dict[str, str] = {}

        test_type: GoldenEvalTestType | None = None
        try:
            test_type = GoldenEvalTestType(
                _cell(row=row, column_name=TEST_TYPE_COLUMN_NAME)
            )
        except ValueError:
            issues_by_column[TEST_TYPE_COLUMN_NAME] = (
                f"is not one of the golden eval test types "
                f"{sorted(t.value for t in GoldenEvalTestType)}"
            )

        for column_name in (
            TEST_CASE_COLUMN_NAME,
            GOLDEN_DOCUMENT_ID_COLUMN_NAME,
            DOCUMENT_TEXT_COLUMN_NAME,
        ):
            if not _cell(row=row, column_name=column_name):
                issues_by_column[column_name] = "is blank, but is required"

        expected_values, expected_value_issues = self._parse_expected_values(row=row)
        issues_by_column.update(expected_value_issues)

        if issues_by_column:
            raise GoldenEvalDocumentParseError(
                row_number=row.row_number, issues_by_column=issues_by_column
            )

        return GoldenEvalDocument(
            golden_document_id=_cell(
                row=row, column_name=GOLDEN_DOCUMENT_ID_COLUMN_NAME
            ),
            test_type=assert_type(test_type, GoldenEvalTestType),
            test_case=_cell(row=row, column_name=TEST_CASE_COLUMN_NAME),
            state_code=self.state_code,
            # Kept verbatim: the document text is the extractor's input, and
            # trimming it would evaluate against text no extractor ever sees.
            document_text=row.values_by_column[DOCUMENT_TEXT_COLUMN_NAME],
            expected_values=expected_values,
        )

    def _parse_expected_values(
        self, *, row: GoogleSheetTableRow
    ) -> tuple[dict[str, Any], dict[str, str]]:
        """Returns one expected value per `__expected` column of |row|, and one
        issue per column whose text could not be read, both keyed by name — values
        by field name, issues by column name.
        """
        fields_by_name = {field.name: field for field in self.output_schema.all_fields}
        expected_values: dict[str, Any] = {}
        issues_by_column: dict[str, str] = {}
        for column_name, cell_text in row.values_by_column.items():
            if not column_name.endswith(EXPECTED_VALUE_COLUMN_SUFFIX):
                continue
            field_name = column_name[: -len(EXPECTED_VALUE_COLUMN_SUFFIX)]
            try:
                expected_values[field_name] = self.parse_expected_value_cell(
                    field=fields_by_name[field_name], cell=cell_text.strip()
                )
            except GoldenEvalCellError as e:
                issues_by_column[column_name] = str(e)
        return expected_values, issues_by_column

    @classmethod
    def parse_expected_value_cell(
        cls, *, field: LLMRequestOutputSchemaField, cell: str
    ) -> Any:
        """Returns the expected value |cell| holds for |field|, coerced to the
        field's Python type, raising a GoldenEvalCellError when the text cannot
        be read as that type.
        """
        if isinstance(field, ArrayOfStructLLMRequestOutputSchemaField):
            return cls._parse_array_of_struct_cell(field=field, cell=cell)
        if isinstance(field, ScalarValuedLLMRequestOutputSchemaField):
            return cls._parse_scalar_cell(field=field, cell=cell)
        raise ValueError(
            f"Cannot parse golden eval score field [{field.name}] of type "
            f"[{field.field_type.value}]."
        )

    @classmethod
    def _parse_scalar_cell(
        cls, *, field: ScalarValuedLLMRequestOutputSchemaField, cell: str
    ) -> Any:
        """Returns the expected value |cell| holds for scalar-valued |field|,
        coerced to the field's Python type. Blank cells parse to None.
        """
        if not cell:
            return None

        field_type = field.field_type
        if field_type is LLMOutputFieldType.STRING:
            return cell
        if field_type is LLMOutputFieldType.ENUM:
            # Kept as spelled; GoldenEvalDocumentValidator flags a value the
            # field does not declare exactly.
            return cell
        if field_type is LLMOutputFieldType.BOOLEAN:
            try:
                return str_to_bool(cell)
            except ValueError as e:
                raise GoldenEvalCellError(
                    "could not be parsed as a boolean; expected true or false"
                ) from e
        if field_type is LLMOutputFieldType.INTEGER:
            try:
                return int(cell)
            except ValueError as e:
                raise GoldenEvalCellError(
                    "could not be parsed as an integer; ensure the number has no formatting in the cell."
                ) from e
        if field_type is LLMOutputFieldType.FLOAT:
            try:
                return float(cell)
            except ValueError as e:
                raise GoldenEvalCellError(
                    "could not be parsed as a float; ensure the number has no formatting in the cell."
                ) from e
        raise ValueError(
            f"Cannot parse golden eval score field [{field.name}] of type "
            f"[{field_type.value}]."
        )

    @classmethod
    def _parse_array_of_struct_cell(
        cls, *, field: ArrayOfStructLLMRequestOutputSchemaField, cell: str
    ) -> Any:
        """Returns the JSON |cell| holds for ARRAY_OF_STRUCT |field|, as
        json.loads reads it — JSON already types every sub-field value, and
        GoldenEvalDocumentValidator checks those types. Blank cells parse to None.
        """
        if not cell:
            return None
        try:
            return json.loads(cell)
        except json.JSONDecodeError as e:
            raise GoldenEvalCellError(
                f"is not valid JSON ({e.msg}, at position [{e.pos}]); expected a "
                f"JSON array of objects keyed by the sub-field names of field "
                f"[{field.name}]"
            ) from e


@attr.define(frozen=True, kw_only=True)
class GoldenEvalEntityResolutionDocumentParser(GoldenEvalDocumentParser):
    """Turns the raw cells of one entity-resolution golden eval Google Sheet row
    into a GoldenEvalEntityResolutionDocument: the first-order columns as
    GoldenEvalDocumentParser reads them, plus the test group, document date, and
    one __expected__er column per resolver field.
    """

    resolver_fields: list[LLMRequestOutputSchemaField] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(LLMRequestOutputSchemaField),
        ]
    )
    """The output schema fields whose values entity resolution resolves, as
    LLMExtractorCollectionConfig.resolver_fields lists them. The sheet carries
    one __expected__er column per resolver field.
    """

    def __attrs_post_init__(self) -> None:
        declared_field_names = {field.name for field in self.output_schema.all_fields}
        for field in self.resolver_fields:
            if field.name not in declared_field_names:
                raise ValueError(
                    f"Resolver field [{field.name}] is not a field of the output "
                    f"schema: {sorted(declared_field_names)}"
                )

    @property
    def _resolver_fields_by_name(self) -> dict[str, LLMRequestOutputSchemaField]:
        return {field.name: field for field in self.resolver_fields}

    def column_issues(self, *, column_names: list[str]) -> list[str]:
        """Returns one issue per column problem that stops an entity-resolution
        sheet with |column_names| from being parsed.
        """
        columns = set(column_names)
        resolver_field_names = sorted(self._resolver_fields_by_name)
        issues = super().column_issues(column_names=column_names)
        issues.extend(
            f"has no [{column_name}] column, which every entity-resolution golden "
            f"eval sheet requires"
            for column_name in REQUIRED_ENTITY_RESOLUTION_METADATA_COLUMN_NAMES
            if column_name not in columns
        )
        issues.extend(
            f"has no [{expected_resolved_value_column_name(field_name)}] column, "
            f"so resolver field [{field_name}] would have no resolved expectation"
            for field_name in resolver_field_names
            if expected_resolved_value_column_name(field_name) not in columns
        )
        issues.extend(
            f"column [{column_name}] names no resolver field: {resolver_field_names}"
            for column_name in column_names
            if column_name.endswith(EXPECTED_RESOLVED_VALUE_COLUMN_SUFFIX)
            and column_name[: -len(EXPECTED_RESOLVED_VALUE_COLUMN_SUFFIX)]
            not in resolver_field_names
        )
        return issues

    def parse(self, *, row: GoogleSheetTableRow) -> GoldenEvalEntityResolutionDocument:
        """Returns the document |row| describes, raising a
        GoldenEvalDocumentParseError naming every first-order and
        entity-resolution cell of |row| whose text could not be read. The row's
        columns must first have passed column_issues.
        """
        issues_by_column: dict[str, str] = {}

        first_order_document: GoldenEvalDocument | None = None
        try:
            first_order_document = super().parse(row=row)
        except GoldenEvalDocumentParseError as e:
            issues_by_column.update(e.issues_by_column)

        test_group_id: int | None = None
        try:
            test_group_id = self._parse_test_group_id_cell(
                _cell(row=row, column_name=TEST_GROUP_ID_COLUMN_NAME)
            )
        except GoldenEvalCellError as e:
            issues_by_column[TEST_GROUP_ID_COLUMN_NAME] = str(e)

        document_date: datetime.date | None = None
        try:
            document_date = self._parse_document_date_cell(
                _cell(row=row, column_name=DOCUMENT_DATE_COLUMN_NAME)
            )
        except GoldenEvalCellError as e:
            issues_by_column[DOCUMENT_DATE_COLUMN_NAME] = str(e)

        expected_resolved_values: dict[str, Any] = {}
        for field_name, field in self._resolver_fields_by_name.items():
            column_name = expected_resolved_value_column_name(field_name)
            try:
                expected_resolved_values[field_name] = self.parse_expected_value_cell(
                    field=field, cell=_cell(row=row, column_name=column_name)
                )
            except GoldenEvalCellError as e:
                issues_by_column[column_name] = str(e)

        if issues_by_column:
            raise GoldenEvalDocumentParseError(
                row_number=row.row_number, issues_by_column=issues_by_column
            )

        return GoldenEvalEntityResolutionDocument(
            **attr.asdict(
                assert_type(first_order_document, GoldenEvalDocument), recurse=False
            ),
            test_group_id=assert_type(test_group_id, int),
            document_date=assert_type(document_date, datetime.date),
            expected_resolved_values=expected_resolved_values,
        )

    @staticmethod
    def _parse_test_group_id_cell(cell: str) -> int:
        """Returns the test group id |cell| holds, a positive integer."""
        try:
            test_group_id = int(cell)
        except ValueError as e:
            raise GoldenEvalCellError(
                "is not a positive integer; test_group_id is a small synthetic "
                "integer shared by the rows resolved together as one person's "
                "documents"
            ) from e
        if test_group_id <= 0:
            raise GoldenEvalCellError(
                "is not a positive integer; test_group_id is a small synthetic "
                "integer shared by the rows resolved together as one person's "
                "documents"
            )
        return test_group_id

    @staticmethod
    def _parse_document_date_cell(cell: str) -> datetime.date:
        """Returns the date |cell| holds, written as YYYY-MM-DD."""
        try:
            return datetime.date.fromisoformat(cell)
        except ValueError as e:
            raise GoldenEvalCellError(
                "is not a date written as YYYY-MM-DD; ensure the cell holds plain "
                "text rather than a formatted date"
            ) from e
