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
"""Reads a golden eval Google Sheet into GoldenEvalDocuments for one extractor,
orchestrating the source that reads the sheet's rows, the parser that turns each
row into a document, and the validator that checks the values parsed from it.
"""
import logging
from collections import defaultdict
from collections.abc import Iterator

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.eval.golden_eval_document import GoldenEvalDocument
from recidiviz.documents.extraction.eval.golden_eval_document_parser import (
    GoldenEvalDocumentParseError,
    GoldenEvalDocumentParser,
    expected_value_column_name,
)
from recidiviz.documents.extraction.eval.golden_eval_document_source import (
    GoldenEvalDocumentSourceError,
    GoldenEvalGoogleSheetDocumentSource,
)
from recidiviz.documents.extraction.eval.golden_eval_document_validator import (
    GoldenEvalDocumentIssue,
    GoldenEvalDocumentValidator,
)
from recidiviz.documents.extraction.models.llm_document_extraction_golden_eval_config import (
    GoldenEvalTestType,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.utils.google_sheets_reader import GoogleSheetReader, GoogleSheetTableRow


@attr.define(frozen=True, kw_only=True)
class GoldenEvalRowIssue:
    """One cell of a golden eval sheet row that stops it describing a document,
    located in the sheet the author has to go fix.
    """

    row_number: int = attr.ib(validator=attr_validators.is_positive_int)
    """1-based position of the row the cell sits in."""

    column_name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Header of the column the cell sits in."""

    cell_value: str = attr.ib(validator=attr_validators.is_str)
    """Raw text of the cell."""

    detail: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """What is wrong with the cell, phrased to follow the position naming it."""

    @property
    def rendered(self) -> str:
        """Returns the issue as one line, naming where in the sheet it sits and
        what that cell holds.
        """
        return (
            f"row [{self.row_number}] column [{self.column_name}] "
            f"value [{self.cell_value}]: {self.detail}"
        )


@attr.define(frozen=True, kw_only=True)
class GoldenEvalDocumentReadResult:
    """One golden eval sheet row's outcome: the document it describes, or the
    reason every unusable cell in it could not be read.
    """

    row_number: int = attr.ib(validator=attr_validators.is_positive_int)
    """1-based position of the row this outcome describes."""

    document: GoldenEvalDocument | None = attr.ib(
        validator=attr_validators.is_opt(GoldenEvalDocument)
    )
    """The document the row describes, or None if any of its cells was unusable."""

    issues: list[GoldenEvalRowIssue] = attr.ib(
        validator=[
            attr_validators.is_list_of(GoldenEvalRowIssue),
            attr_validators.is_list,
        ]
    )
    """One issue per unusable cell in the row, empty if it read cleanly."""

    def __attrs_post_init__(self) -> None:
        if (self.document is None) == (len(self.issues) == 0):
            raise ValueError(
                f"Read result for row [{self.row_number}] must hold either a "
                f"document or the issues that stopped it from being read, not "
                f"both and not neither."
            )


@attr.define(frozen=True, kw_only=True)
class GoldenEvalDocumentReader:
    """Reads a golden eval Google Sheet into GoldenEvalDocuments for one
    extractor: the source reads and vets the sheet's rows, the parser turns each
    row's cell text into a document, and the validator checks the values parsed
    from it, locating every problem at the cell it came from.
    """

    source: GoldenEvalGoogleSheetDocumentSource = attr.ib(
        validator=attr.validators.instance_of(GoldenEvalGoogleSheetDocumentSource)
    )
    """Reads the sheet's rows and verifies the sheet is the eval set expected."""

    parser: GoldenEvalDocumentParser = attr.ib(
        validator=attr.validators.instance_of(GoldenEvalDocumentParser)
    )
    """Reads each row's cell text into a document."""

    validator: GoldenEvalDocumentValidator = attr.ib(
        validator=attr.validators.instance_of(GoldenEvalDocumentValidator)
    )
    """Flags the expected values of a parsed document that the output schema does
    not allow.
    """

    @property
    def state_code(self) -> StateCode:
        """Returns the state of the extractor being evaluated."""
        return self.source.state_code

    @property
    def source_sheet_uri(self) -> str:
        """Returns the URI of the sheet the rows come from."""
        return self.source.source_sheet_uri

    @classmethod
    def for_extractor(
        cls,
        *,
        source: GoldenEvalGoogleSheetDocumentSource,
        output_schema: LLMRequestOutputSchema,
    ) -> "GoldenEvalDocumentReader":
        """Returns the reader for the eval sheet |source| reads, wired to read it
        against |output_schema|.
        """
        return cls(
            source=source,
            parser=GoldenEvalDocumentParser(
                state_code=source.state_code, output_schema=output_schema
            ),
            validator=GoldenEvalDocumentValidator(output_schema=output_schema),
        )

    @classmethod
    def from_config(
        cls, *, config: LLMExtractorConfig, sheets_reader: GoogleSheetReader
    ) -> "GoldenEvalDocumentReader":
        """Returns the reader for |config|'s golden eval set."""
        if config.golden_eval is None:
            raise ValueError(
                f"Extractor [{config.extractor_id}] declares no golden_eval config, "
                f"so it cannot be used to run a golden eval."
            )
        return cls.for_extractor(
            source=GoldenEvalGoogleSheetDocumentSource(
                sheets_reader=sheets_reader,
                state_code=config.state_code,
                source_sheet_uri=config.golden_eval.source_sheet_uri,
            ),
            output_schema=config.extractor_collection.output_schema,
        )

    def verify_columns(self, *, column_names: list[str]) -> None:
        """Verifies |column_names| lets every row of the sheet be read against the
        extractor's output schema, raising a ValueError naming every problem at
        once. Must pass before rows are handed to read_document_from_row.
        """
        if issues := self.parser.column_issues(column_names=column_names):
            raise ValueError(
                "\n".join(
                    [
                        f"Golden eval sheet [{self.source_sheet_uri}] cannot be "
                        f"read against the extractor's output schema:",
                        *(f"  - {issue}" for issue in issues),
                    ]
                )
            )

    def documents(self) -> Iterator[GoldenEvalDocumentReadResult]:
        """Yields the outcome of reading each row of the sheet, in sheet order. A
        malformed row yields its issues rather than raising, so a caller sees every
        row.
        """
        table = self.source.read_table()
        self.verify_columns(column_names=table.column_names)
        for row in table.rows:
            yield self.read_document_from_row(row=row)

    def load_all_documents(self) -> list[GoldenEvalDocument]:
        """Returns every golden eval document the source's sheet holds, in order,
        having reported every problem at once rather than stopping at the first —
        so an eval run fails before it reaches the LLM and an author fixes the
        whole eval set in one pass.
        """
        documents: list[GoldenEvalDocument] = []
        row_numbers_by_document_id: dict[str, list[int]] = defaultdict(list)
        row_issues: list[GoldenEvalRowIssue] = []
        row_count = 0
        for result in self.documents():
            row_count += 1
            if result.document is None:
                row_issues.extend(result.issues)
                continue
            documents.append(result.document)
            row_numbers_by_document_id[result.document.golden_document_id].append(
                result.row_number
            )

        issues = self._rendered_row_issues(row_issues=row_issues, row_count=row_count)
        issues.extend(
            self._duplicate_document_id_issues(
                row_numbers_by_document_id=row_numbers_by_document_id
            )
        )
        if issues:
            raise GoldenEvalDocumentSourceError(
                f"Found [{len(issues)}] problem(s) in golden eval sheet "
                f"[{self.source_sheet_uri}]",
                issues,
            )
        self._warn_if_a_test_type_is_unrepresented(documents=documents)
        return documents

    def read_document_from_row(
        self, *, row: GoogleSheetTableRow
    ) -> GoldenEvalDocumentReadResult:
        """Returns the outcome of reading |row|: the GoldenEvalDocument it
        describes, or every problem that stops it from being one. The row's
        columns must first have passed verify_columns.
        """
        try:
            document = self.parser.parse(row=row)
        except GoldenEvalDocumentParseError as e:
            return self._failed_result(row=row, issues_by_column=e.issues_by_column)

        if issues := self.validator.issues(document=document):
            return self._failed_result(
                row=row,
                issues_by_column={
                    column_name: "; ".join(details)
                    for column_name, details in self._issues_for_row(issues).items()
                },
            )

        return GoldenEvalDocumentReadResult(
            row_number=row.row_number, document=document, issues=[]
        )

    @staticmethod
    def _issues_for_row(
        issues: list[GoldenEvalDocumentIssue],
    ) -> dict[str, list[str]]:
        """Returns the text of every issue in one row, keyed by the sheet column
        the value it flags was read from, each opened with that value's position
        within the column's cell.
        """
        issues_by_column: dict[str, list[str]] = defaultdict(list)
        for issue in issues:
            detail = (
                issue.detail
                if issue.element_index is None
                else f"element [{issue.element_index}] {issue.detail}"
            )
            issues_by_column[expected_value_column_name(issue.field_name)].append(
                detail
            )
        return dict(issues_by_column)

    @staticmethod
    def _failed_result(
        *, row: GoogleSheetTableRow, issues_by_column: dict[str, str]
    ) -> GoldenEvalDocumentReadResult:
        """Returns the outcome of a row that produced no document, ordering the
        issues by the sheet's columns rather than by the order the cells were
        checked.
        """
        column_order = list(row.values_by_column)
        return GoldenEvalDocumentReadResult(
            row_number=row.row_number,
            document=None,
            issues=[
                GoldenEvalRowIssue(
                    row_number=row.row_number,
                    column_name=column_name,
                    cell_value=row.values_by_column[column_name],
                    detail=issues_by_column[column_name],
                )
                for column_name in sorted(issues_by_column, key=column_order.index)
            ],
        )

    @staticmethod
    def _rendered_row_issues(
        *, row_issues: list[GoldenEvalRowIssue], row_count: int
    ) -> list[str]:
        """Returns one rendered line per row issue, in sheet order, collapsing to a
        single line any issue that every row of the sheet holds — a mistake
        repeated down a whole column, rendered once per row, buries everything
        else.
        """
        matching_rows_by_cause: dict[tuple[str, str], int] = defaultdict(int)
        for row_issue in row_issues:
            matching_rows_by_cause[(row_issue.column_name, row_issue.detail)] += 1

        collapsed_causes = {
            cause
            for cause, matching_rows in matching_rows_by_cause.items()
            if row_count > 1 and matching_rows == row_count
        }

        rendered: list[str] = []
        already_collapsed: set[tuple[str, str]] = set()
        for row_issue in row_issues:
            cause = (row_issue.column_name, row_issue.detail)
            if cause not in collapsed_causes:
                rendered.append(row_issue.rendered)
                continue
            if cause in already_collapsed:
                continue
            already_collapsed.add(cause)
            column_name, detail = cause
            rendered.append(f"every row: column [{column_name}]: {detail}")
        return rendered

    @staticmethod
    def _duplicate_document_id_issues(
        *, row_numbers_by_document_id: dict[str, list[int]]
    ) -> list[str]:
        """Returns one issue per golden_document_id claimed by more than one row.
        The id is what an extraction result is looked up by, so a duplicate would
        score one document's output against another document's expectations.
        """
        return [
            f"golden_document_id [{document_id}] is reused in multiple rows {row_numbers}"
            for document_id, row_numbers in sorted(row_numbers_by_document_id.items())
            if len(row_numbers) > 1
        ]

    def _warn_if_a_test_type_is_unrepresented(
        self, *, documents: list[GoldenEvalDocument]
    ) -> None:
        """Logs a warning for each GoldenEvalTestType the sheet does not cover,
        since a test type with no documents scores as a vacuous pass rather than as
        the coverage gap it is.
        """
        if unrepresented_test_types := set(GoldenEvalTestType) - {
            document.test_type for document in documents
        }:
            logging.warning(
                "Golden eval sheet [%s] holds no [%s] rows, so those test types "
                "will be scored over zero documents.",
                self.source_sheet_uri,
                sorted(test_type.value for test_type in unrepresented_test_types),
            )
