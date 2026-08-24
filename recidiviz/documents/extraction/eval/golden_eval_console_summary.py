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
"""Renders a golden eval run's result as the console summary a developer reads
while iterating on a prompt.

Lives beside the runner rather than inside the CLI that prints it, so the
rendering is unit-testable on its own and sits where the CI entry point's
PR-comment renderer will sit.
"""
from collections.abc import Sequence
from typing import Any

import attr
from tabulate import tabulate

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common import attr_validators
from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMExtractionJobDocumentResultType,
)
from recidiviz.documents.extraction.eval.golden_eval_result import (
    GoldenEvalFieldScore,
    GoldenEvalResult,
)
from recidiviz.documents.extraction.models.llm_document_extraction_golden_eval_config import (
    GoldenEvalTestType,
    LLMDocumentExtractionGoldenEvalConfig,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.utils.list_helpers import group_by
from recidiviz.utils.types import assert_type

NO_DOCUMENTS_SCORED_MESSAGE = "No documents were scored."
"""Rendered in place of the accuracy sections when a run scored nothing, so an
empty eval set reads as an empty eval set rather than as 0% accuracy.
"""

_PASS_MARKER = "✅"  # nosec B105
_FAIL_MARKER = "❌"
_WARNING_MARKER = "⚠️"

# How many expected-vs-actual examples the miss examples section shows per field.
_MAX_MISS_EXAMPLES_PER_FIELD = 3

# Rendered in an example miss's Expected column when the field was not expected
# to be present, and in its Actual column when the extractor did not produce it.
_MISSING_VALUE_MARKER = "<none>"

# Indents the key/value lines of the header block under its title. The tables below
# it are laid out by tabulate, which is aware that a marker glyph occupies two
# terminal columns where `len()` counts one.
_LINE_INDENT = "  "

# The console table format the sibling sandbox scripts render with. Chosen over the
# markdown-emitting formats deliberately: this output is read in a terminal, and the
# CI entry point renders the same rows as a PR comment by passing `github` instead.
_TABLE_FORMAT = "simple"


def render_golden_eval_console_summary(
    *,
    config: LLMExtractorConfig,
    result: GoldenEvalResult,
    results_table_address: BigQueryAddress,
) -> str:
    """Returns the console summary of golden eval |result| for extractor |config|,
    whose scored rows were written to |results_table_address|.

    Reports the documents whose extraction request failed outright (which were
    never scored), accuracy per test type against that test type's configured
    threshold, accuracy per scored field with example misses, how each document's
    extraction was ultimately classified, and the run's total token usage.
    """
    sections = [
        _render_header(config=config, results_table_address=results_table_address)
    ]
    if result.request_failure_by_document_id:
        sections.append(_render_request_failures(result=result))
    if not result.field_scores:
        sections.append(NO_DOCUMENTS_SCORED_MESSAGE)
    else:
        sections.append(_render_accuracy_by_test_type(config=config, result=result))
        sections.append(_render_accuracy_by_field(result=result))
        if any(not score.is_correct for score in result.field_scores):
            sections.append(_render_example_misses(result=result))
        sections.append(_render_document_outcomes(result=result))
    sections.append(_render_token_usage(result=result))
    return "\n\n".join(sections)


def _render_request_failures(*, result: GoldenEvalResult) -> str:
    """Returns the block naming each document whose extraction request failed
    outright, with the failure's category and message. These documents were never
    scored, so the accuracy sections read only over documents that produced output.
    """
    failure_count = len(result.request_failure_by_document_id)
    document_count = len(result.actual_llm_result_type_by_document_id)
    return _render_section(
        title=(
            f"{_WARNING_MARKER} {failure_count}/{document_count} extraction "
            f"requests failed and were NOT scored:"
        ),
        headers=["Document", "Error type", "Error message"],
        rows=[
            [
                document_id,
                failure.error_type.value,
                # Flattened so one failure stays one table row.
                " ".join(failure.error_message.split()),
            ]
            for document_id, failure in sorted(
                result.request_failure_by_document_id.items()
            )
        ],
    )


def _render_header(
    *, config: LLMExtractorConfig, results_table_address: BigQueryAddress
) -> str:
    """Returns the lines identifying which extractor version was evaluated, the
    model config it ran with, and where its scored rows landed.
    """
    return "\n".join(
        [
            f"Golden eval results for extractor [{config.extractor_id}]",
            f"{_LINE_INDENT}extractor version:      {config.extractor_version_id}",
            f"{_LINE_INDENT}output schema version:  "
            f"{config.extractor_collection.output_schema_version}",
            f"{_LINE_INDENT}model config:           "
            f"{config.model_config.name} ({config.model_config.model})",
            f"{_LINE_INDENT}scored rows written to: {results_table_address.to_str()}",
        ]
    )


def _render_accuracy_by_test_type(
    *, config: LLMExtractorConfig, result: GoldenEvalResult
) -> str:
    """Returns the per-test-type accuracy table, each row marked against the
    threshold the extractor's golden eval config sets for that test type.

    Test types read in declaration order — the targeted unit documents before the
    realistic samples — and a test type the run scored no documents for is omitted
    rather than reported as 0%, which would read as a failure the extractor did not
    cause.
    """
    accuracy_thresholds = assert_type(
        config.golden_eval, LLMDocumentExtractionGoldenEvalConfig
    ).accuracy_thresholds
    tallies = {
        test_type: _Tally.of(scores)
        for test_type, scores in group_by(
            result.field_scores, key_fn=lambda score: score.test_type
        ).items()
    }

    rows = []
    for test_type in GoldenEvalTestType:
        if test_type not in tallies:
            continue
        tally = tallies[test_type]
        threshold = accuracy_thresholds[test_type]
        marker = _PASS_MARKER if tally.accuracy >= threshold else _FAIL_MARKER
        rows.append([f"{marker} {test_type.value}", tally.render(), f"{threshold:.1%}"])

    return _render_section(
        title="Accuracy by test type:",
        headers=["Test type", "Accuracy", "Threshold"],
        rows=rows,
    )


def _render_accuracy_by_field(*, result: GoldenEvalResult) -> str:
    """Returns the per-field accuracy table, alphabetized so an ARRAY_OF_STRUCT
    field's array-level accuracy reads directly above its sub-fields'.
    """
    tallies = {
        field_name: _Tally.of(scores)
        for field_name, scores in sorted(
            group_by(result.field_scores, key_fn=lambda score: score.field_name).items()
        )
    }

    return _render_section(
        title="Accuracy by field:",
        headers=["Field", "Accuracy"],
        rows=[
            [
                f"{_PASS_MARKER if tally.is_all_correct else _FAIL_MARKER} "
                f"{field_name}",
                tally.render(),
            ]
            for field_name, tally in tallies.items()
        ],
    )


def _render_example_misses(*, result: GoldenEvalResult) -> str:
    """Returns the table of example misses: for each field with any incorrect
    comparison, up to _MAX_MISS_EXAMPLES_PER_FIELD expected-vs-actual pairs and the
    documents they came from.
    """
    misses_by_field_name = group_by(
        [score for score in result.field_scores if not score.is_correct],
        key_fn=lambda score: score.field_name,
    )
    rows = []
    for field_name, misses in sorted(misses_by_field_name.items()):
        for miss in misses[:_MAX_MISS_EXAMPLES_PER_FIELD]:
            rows.append(
                [
                    field_name,
                    miss.golden_document_id,
                    _render_compared_value(miss.expected_value),
                    _render_compared_value(miss.actual_value),
                ]
            )
    return _render_section(
        title=f"Example misses (up to {_MAX_MISS_EXAMPLES_PER_FIELD} per field):",
        headers=["Field", "Document", "Expected", "Actual"],
        rows=rows,
    )


def _render_compared_value(value: str | None) -> str:
    """Returns one side of a scored comparison as an example miss renders it."""
    return value if value is not None else _MISSING_VALUE_MARKER


def _render_token_usage(*, result: GoldenEvalResult) -> str:
    """Returns the run's total token usage across every request it made, including
    failed ones.
    """
    token_counts = result.total_token_counts
    return "\n".join(
        [
            "Token usage:",
            f"{_LINE_INDENT}input tokens:    {token_counts.input_token_count} "
            f"({token_counts.cached_input_token_count} cached)",
            f"{_LINE_INDENT}output tokens:   {token_counts.output_token_count}",
            f"{_LINE_INDENT}thinking tokens: {token_counts.thinking_token_count}",
        ]
    )


def _render_document_outcomes(*, result: GoldenEvalResult) -> str:
    """Returns the document outcomes table: one row per processed result type,
    counting the documents that landed in it and naming those that did not succeed.

    Naming them is the point of the section: a failed request and a document the
    extractor simply got wrong both score as a miss on every field, and only this
    tells them apart.
    """
    document_ids_by_result_type = group_by(
        sorted(result.actual_llm_result_type_by_document_id),
        key_fn=lambda document_id: result.actual_llm_result_type_by_document_id[
            document_id
        ],
    )

    rows = []
    for result_type in LLMExtractionJobDocumentResultType:
        if result_type not in document_ids_by_result_type:
            continue
        document_ids = document_ids_by_result_type[result_type]
        is_success = LLMExtractionJobDocumentResultType.is_success_result_type(
            result_type
        )
        rows.append(
            [
                f"{_PASS_MARKER if is_success else _FAIL_MARKER} {result_type.value}",
                len(document_ids),
                # A successful document needs no calling out; the whole point of the
                # column is naming the ones whose fields cannot be read as misses.
                "" if is_success else ", ".join(document_ids),
            ]
        )

    return _render_section(
        title="Document outcomes:",
        headers=["Result type", "Documents", "Document IDs"],
        rows=rows,
    )


def _render_section(*, title: str, headers: list[str], rows: list[list[Any]]) -> str:
    """Returns |title| above |rows| laid out as a console table under |headers|."""
    return f"{title}\n" + tabulate(rows, headers=headers, tablefmt=_TABLE_FORMAT)


@attr.define(frozen=True, kw_only=True)
class _Tally:
    """How many of one group of scored comparisons were correct."""

    correct_count: int = attr.ib(validator=attr_validators.is_non_negative_int)
    """The comparisons in the group the extractor got right."""

    total_count: int = attr.ib(validator=attr_validators.is_positive_int)
    """Every comparison in the group."""

    @classmethod
    def of(cls, scores: Sequence[GoldenEvalFieldScore]) -> "_Tally":
        """Returns the tally of |scores|, which must be non-empty — a group only
        exists because a score fell into it.
        """
        return cls(
            correct_count=len([score for score in scores if score.is_correct]),
            total_count=len(scores),
        )

    @property
    def accuracy(self) -> float:
        return self.correct_count / self.total_count

    @property
    def is_all_correct(self) -> bool:
        return self.correct_count == self.total_count

    def render(self) -> str:
        """Returns the tally as `{correct}/{total} ({accuracy})`."""
        return f"{self.correct_count}/{self.total_count} ({self.accuracy:.1%})"
