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
"""Tests for render_golden_eval_console_summary.

Runs against FAKE_EXTRACTOR_COLLECTION's extractor config, whose golden eval
config requires 100% accuracy for both test types.
"""
from unittest import TestCase

import attr

from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMExtractionJobDocumentResultType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.eval.golden_eval_console_summary import (
    render_golden_eval_console_summary,
)
from recidiviz.documents.extraction.eval.golden_eval_result import (
    GoldenEvalFieldScore,
    GoldenEvalRequestFailure,
    GoldenEvalResult,
    array_sub_field_score_name,
)
from recidiviz.documents.extraction.eval.golden_eval_results_table import (
    GoldenEvalResultsBQTable,
)
from recidiviz.documents.extraction.llm_client.types import (
    LLMDocumentExtractionTokenCounts,
    LLMRequestErrorType,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_document_extraction_golden_eval_config import (
    GoldenEvalTestType,
    LLMDocumentExtractionGoldenEvalConfig,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.tests.documents import fake_config
from recidiviz.utils.types import assert_type

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
_SANDBOX_PREFIX = "my_prefix"

_UNIT = GoldenEvalTestType.UNIT
_SAMPLE = GoldenEvalTestType.SAMPLE

_SUCCESS = LLMExtractionJobDocumentResultType.SUCCESS
_TRANSIENT_FAILURE = LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT

_ARRAY_FIELD_NAME = "assignments"
_ARRAY_SUB_FIELD_NAME = array_sub_field_score_name(
    array_field_name=_ARRAY_FIELD_NAME, sub_field_name="assignment_name"
)

# The summary reports how many comparisons in a group were correct, never what
# was compared, so every score a test builds carries the same placeholder values.
_PLACEHOLDER_ELEMENT_INDEX = None
_PLACEHOLDER_EXPECTED_VALUE = "expected"
_PLACEHOLDER_ACTUAL_VALUE = "actual"


def _config() -> LLMExtractorConfig:
    """Returns the fake state's extractor config."""
    return get_first_order_llm_extractor_config(
        _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
    )


def _config_with_thresholds(
    thresholds: dict[GoldenEvalTestType, float]
) -> LLMExtractorConfig:
    """Returns the fake state's extractor config with |thresholds| in place of its
    declared accuracy thresholds.
    """
    config = _config()
    return attr.evolve(
        config,
        golden_eval=attr.evolve(
            assert_type(config.golden_eval, LLMDocumentExtractionGoldenEvalConfig),
            accuracy_thresholds=thresholds,
        ),
    )


def _score(
    golden_document_id: str,
    test_type: GoldenEvalTestType,
    field_name: str,
    is_correct: bool,
    *,
    expected_value: str | None = _PLACEHOLDER_EXPECTED_VALUE,
    actual_value: str | None = _PLACEHOLDER_ACTUAL_VALUE,
) -> GoldenEvalFieldScore:
    """Returns one scored comparison for the summary to aggregate."""
    return GoldenEvalFieldScore(
        golden_document_id=golden_document_id,
        test_type=test_type,
        test_case="base_case",
        field_name=field_name,
        element_index=_PLACEHOLDER_ELEMENT_INDEX,
        expected_value=expected_value,
        actual_value=actual_value,
        is_correct=is_correct,
    )


def _request_failure(error_message: str) -> GoldenEvalRequestFailure:
    """Returns a request failure with |error_message|, categorized SERVER_ERROR."""
    return GoldenEvalRequestFailure(
        error_type=LLMRequestErrorType.SERVER_ERROR, error_message=error_message
    )


def _scores(
    *rows: tuple[str, GoldenEvalTestType, str, bool]
) -> list[GoldenEvalFieldScore]:
    """Returns one scored comparison per
    (golden_document_id, test_type, field_name, is_correct) row.
    """
    return [_score(*row) for row in rows]


def _result(
    scores: list[GoldenEvalFieldScore],
    *,
    result_type_overrides: dict[str, LLMExtractionJobDocumentResultType] | None = None,
    request_failure_by_document_id: dict[str, GoldenEvalRequestFailure] | None = None,
    total_token_counts: LLMDocumentExtractionTokenCounts | None = None,
) -> GoldenEvalResult:
    """Returns the eval result holding |scores|. Every scored document is
    classified SUCCESS and every request-failed document TRANSIENT unless
    |result_type_overrides| classifies it otherwise.
    """
    overrides = result_type_overrides or {}
    request_failures = request_failure_by_document_id or {}
    document_ids = {score.golden_document_id for score in scores} | set(
        request_failures
    )
    return GoldenEvalResult(
        field_scores=scores,
        actual_llm_result_type_by_document_id={
            document_id: overrides.get(
                document_id,
                _TRANSIENT_FAILURE if document_id in request_failures else _SUCCESS,
            )
            for document_id in document_ids
        },
        request_failure_by_document_id=request_failures,
        total_token_counts=(
            total_token_counts or LLMDocumentExtractionTokenCounts.empty()
        ),
    )


def _render(config: LLMExtractorConfig, result: GoldenEvalResult) -> str:
    """Returns the console summary of |result|, as the sandbox CLI renders it."""
    return render_golden_eval_console_summary(
        config=config,
        result=result,
        results_table_address=GoldenEvalResultsBQTable.address(
            collection_name=_COLLECTION_NAME, sandbox_prefix=_SANDBOX_PREFIX
        ),
    )


class TestRenderGoldenEvalConsoleSummary(TestCase):
    """Tests for render_golden_eval_console_summary."""

    def test_renders_full_summary(self) -> None:
        """The canonical summary: a header identifying what ran and where its rows
        landed, accuracy per test type against its threshold, accuracy per field with
        example misses, each document's processed outcome, and total token usage.
        """
        config = _config()
        result = _result(
            _scores(
                ("doc_1", _UNIT, "location", True),
                ("doc_1", _UNIT, _ARRAY_SUB_FIELD_NAME, False),
                ("doc_2", _SAMPLE, "location", True),
                ("doc_2", _SAMPLE, _ARRAY_SUB_FIELD_NAME, True),
            ),
            total_token_counts=LLMDocumentExtractionTokenCounts(
                input_token_count=1200,
                output_token_count=340,
                cached_input_token_count=1000,
                thinking_token_count=56,
            ),
        )

        summary = _render(config, result)

        results_table_address = GoldenEvalResultsBQTable.address(
            collection_name=_COLLECTION_NAME, sandbox_prefix=_SANDBOX_PREFIX
        ).to_str()
        self.assertEqual(
            f"""Golden eval results for extractor [US_XX_FAKE_EXTRACTOR_COLLECTION]
  extractor version:      {config.extractor_version_id}
  output schema version:  {config.extractor_collection.output_schema_version}
  model config:           {config.model_config.name} ({config.model_config.model})
  scored rows written to: {results_table_address}

Accuracy by test type:
Test type    Accuracy      Threshold
-----------  ------------  -----------
❌ unit      1/2 (50.0%)   100.0%
✅ sample    2/2 (100.0%)  100.0%

Accuracy by field:
Field                           Accuracy
------------------------------  ------------
❌ assignments.assignment_name  1/2 (50.0%)
✅ location                     2/2 (100.0%)

Example misses (up to 3 per field):
Field                        Document    Expected    Actual
---------------------------  ----------  ----------  --------
assignments.assignment_name  doc_1       expected    actual

Document outcomes:
Result type      Documents  Document IDs
-------------  -----------  --------------
✅ SUCCESS               2

Token usage:
  input tokens:    1200 (1000 cached)
  output tokens:   340
  thinking tokens: 56""",
            summary,
        )

    def test_marks_test_type_meeting_its_threshold(self) -> None:
        """A test type at or above its threshold reads as a pass, even below 100%."""
        config = _config_with_thresholds({_UNIT: 0.5, _SAMPLE: 1.0})
        result = _result(
            _scores(
                ("doc_1", _UNIT, "location", True),
                ("doc_1", _UNIT, "status_note", False),
            )
        )

        summary = _render(config, result)

        self.assertIn(
            """Accuracy by test type:
Test type    Accuracy     Threshold
-----------  -----------  -----------
✅ unit      1/2 (50.0%)  50.0%""",
            summary,
        )

    def test_omits_test_type_with_no_scored_documents(self) -> None:
        """A test type the sheet holds no documents for is left out rather than
        reported as 0%, which would read as a failure the extractor did not cause.
        """
        result = _result(_scores(("doc_1", _UNIT, "location", True)))

        summary = _render(_config(), result)

        self.assertIn(
            """Accuracy by test type:
Test type    Accuracy      Threshold
-----------  ------------  -----------
✅ unit      1/1 (100.0%)  100.0%""",
            summary,
        )
        self.assertNotIn(_SAMPLE.value, summary)

    def test_names_documents_behind_a_non_success_outcome(self) -> None:
        """A document whose request failed is reported as a failed request, naming
        the document, rather than disappearing into its fields' misses.
        """
        result = _result(
            _scores(
                ("doc_1", _UNIT, "location", True),
                ("doc_2", _UNIT, "location", False),
                ("doc_3", _UNIT, "location", False),
            ),
            result_type_overrides={
                "doc_2": _TRANSIENT_FAILURE,
                "doc_3": _TRANSIENT_FAILURE,
            },
        )

        summary = _render(_config(), result)

        self.assertIn(
            """Document outcomes:
Result type                            Documents  Document IDs
-----------------------------------  -----------  --------------
✅ SUCCESS                                     1
❌ DOCUMENT_LEVEL_FAILURE_TRANSIENT            2  doc_2, doc_3""",
            summary,
        )

    def test_renders_array_level_score_alongside_its_sub_fields(self) -> None:
        """An ARRAY_OF_STRUCT field's array-level accuracy is reported separately
        from each of its sub-fields', as the scorer emits them.
        """
        result = _result(
            _scores(
                ("doc_1", _UNIT, _ARRAY_FIELD_NAME, False),
                ("doc_1", _UNIT, _ARRAY_SUB_FIELD_NAME, True),
            )
        )

        summary = _render(_config(), result)

        self.assertIn(
            """Accuracy by field:
Field                           Accuracy
------------------------------  ------------
❌ assignments                  0/1 (0.0%)
✅ assignments.assignment_name  1/1 (100.0%)""",
            summary,
        )

    def test_reported_accuracy_matches_the_result_object(self) -> None:
        """The rendered percentages are the same accuracies GoldenEvalResult derives,
        so the console and the BQ-backed dashboards can never disagree.
        """
        result = _result(
            _scores(
                ("doc_1", _UNIT, "location", True),
                ("doc_1", _UNIT, "status_note", False),
                ("doc_2", _SAMPLE, "location", False),
            )
        )

        summary = _render(_config(), result)

        for test_type, accuracy in result.accuracy_by_test_type.items():
            self.assertIn(f"{test_type.value}", summary)
            self.assertIn(f"({accuracy:.1%})", summary)
        for field_name, accuracy in result.accuracy_by_field.items():
            self.assertIn(f"{field_name}", summary)
            self.assertIn(f"({accuracy:.1%})", summary)

    def test_reports_a_run_that_scored_nothing(self) -> None:
        """An eval set with no documents renders a summary that says so rather than
        dividing by zero.
        """
        summary = _render(_config(), _result([]))

        self.assertIn("No documents were scored.", summary)

    def test_flags_failed_requests_before_accuracy(self) -> None:
        """A document whose extraction request failed outright is named at the top
        with the failure's category and message, above the accuracy sections it is
        excluded from.
        """
        result = _result(
            _scores(("doc_1", _UNIT, "location", True)),
            request_failure_by_document_id={
                "doc_3": _request_failure("403 Forbidden:\n  no access"),
                "doc_2": _request_failure("503 Service Unavailable"),
            },
        )

        summary = _render(_config(), result)

        failures_section = """⚠️ 2/3 extraction requests failed and were NOT scored:
Document    Error type    Error message
----------  ------------  ------------------------
doc_2       SERVER_ERROR  503 Service Unavailable
doc_3       SERVER_ERROR  403 Forbidden: no access"""
        self.assertIn(failures_section, summary)
        self.assertLess(
            summary.index(failures_section), summary.index("Accuracy by test type:")
        )
        self.assertIn("✅ unit      1/1 (100.0%)", summary)

    def test_reports_a_run_where_every_request_failed(self) -> None:
        """A run whose every request failed reads as that, not as an empty eval
        set.
        """
        result = _result(
            [],
            request_failure_by_document_id={
                "doc_1": _request_failure("503 Service Unavailable")
            },
        )

        summary = _render(_config(), result)

        self.assertIn("1/1 extraction requests failed and were NOT scored:", summary)
        self.assertIn("No documents were scored.", summary)

    def test_example_misses_render_missing_values_and_cap_at_three(self) -> None:
        """Example misses show at most three expected-vs-actual pairs per field and
        render an absent side as <none>.
        """
        result = _result(
            [
                _score("doc_1", _UNIT, "location", False, actual_value=None),
                _score("doc_2", _UNIT, "location", False, expected_value=None),
                _score("doc_3", _UNIT, "location", False),
                _score("doc_4", _UNIT, "location", False),
                _score("doc_1", _UNIT, "status_note", False),
            ]
        )

        summary = _render(_config(), result)

        self.assertIn(
            """Example misses (up to 3 per field):
Field        Document    Expected    Actual
-----------  ----------  ----------  --------
location     doc_1       expected    <none>
location     doc_2       <none>      actual
location     doc_3       expected    actual
status_note  doc_1       expected    actual""",
            summary,
        )
        self.assertNotIn("doc_4", summary)

    def test_renders_token_usage(self) -> None:
        """The run's total token usage renders with the cached share of the input."""
        result = _result(
            _scores(("doc_1", _UNIT, "location", True)),
            total_token_counts=LLMDocumentExtractionTokenCounts(
                input_token_count=100,
                output_token_count=50,
                cached_input_token_count=20,
                thinking_token_count=5,
            ),
        )

        summary = _render(_config(), result)

        self.assertIn(
            """Token usage:
  input tokens:    100 (20 cached)
  output tokens:   50
  thinking tokens: 5""",
            summary,
        )
