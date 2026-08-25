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
"""Tests for GoldenEvalRunner.

Runs against FAKE_EXTRACTOR_COLLECTION's output schema, whose fields cover every
shape a scored row can take: a STRUCTURAL BOOLEAN (`is_relevant`), an INFERRED
ENUM (`primary_status`), a STRUCTURAL STRING (`status_note`), an INFERRED STRING
(`location`), and an ARRAY_OF_STRUCT (`assignments`, keyed on `assignment_name`).

The unit tests stub out the sheet read, so the Sheets API fluent chain is only
mocked in the sheet-reading tests.
"""
import datetime
from typing import Any
from unittest import TestCase
from unittest.mock import create_autospec, patch

import attr
from freezegun import freeze_time
from googleapiclient.discovery import Resource

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMExtractionJobDocumentResultType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.eval.golden_eval_document_parser import (
    DOCUMENT_TEXT_COLUMN_NAME,
    GOLDEN_DOCUMENT_ID_COLUMN_NAME,
    TEST_CASE_COLUMN_NAME,
    TEST_TYPE_COLUMN_NAME,
    expected_value_column_name,
)
from recidiviz.documents.extraction.eval.golden_eval_result import (
    GoldenEvalFieldScore,
    GoldenEvalResult,
)
from recidiviz.documents.extraction.eval.golden_eval_results_table import (
    GoldenEvalResultsBQTable,
)
from recidiviz.documents.extraction.eval.golden_eval_runner import GoldenEvalRunner
from recidiviz.documents.extraction.extraction_results_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    EXTRACTION_JOB_ID_COLUMN_NAME,
)
from recidiviz.documents.extraction.llm_client.types import (
    BILLING_LABELS_EXTRACTION_REQUEST_PARAMETER_NAME,
    LLMClientDocumentExtractionResult,
    LLMDocumentExtractionRequest,
    LLMDocumentExtractionTokenCounts,
    LLMRequestErrorType,
)
from recidiviz.documents.extraction.llm_extraction_results_tables import (
    ExtractionRawResultsBQTable,
    ExtractionValidatedResultsBQTable,
    ExtractionValidationAuditBQTable,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_document_extraction_golden_eval_config import (
    GoldenEvalTestType,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.extraction.models.llm_model_registry import LLMModelConfig
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    IS_RELEVANT_FIELD_NAME,
)
from recidiviz.source_tables.extraction_results_source_table_collection import (
    collect_extraction_results_source_table_collections,
    collect_golden_eval_results_source_table_collection,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    build_fake_extractor_assignment_result_json,
    build_fake_extractor_result_content,
    ground_citations_in_fake_source_text,
    wrap_in_result_key,
)
from recidiviz.tests.documents.extraction.llm_client.fake_sync_llm_client import (
    FakeSyncLLMClient,
)
from recidiviz.utils import metadata
from recidiviz.utils.google_sheets_reader import (
    GoogleSheetReader,
    GoogleSheetTable,
    GoogleSheetTableRow,
)

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
_SANDBOX_PREFIX = "my_prefix"
_REQUESTER = "test-user"

# One frozen instant for every run, so the synthetic job id and the run_datetime_utc
# stamped on each scored row are exact.
_RUN_DATETIME_STR = "2026-08-03 12:34:56.789012"
_RUN_DATETIME = datetime.datetime(2026, 8, 3, 12, 34, 56, 789012, tzinfo=datetime.UTC)
_RUN_DATETIME_JOB_ID_SUFFIX = "20260803T123456789012"

_DISH_DUTY_ELEMENT_JSON = (
    '[{"assignment_name": "Dish duty", "assignment_type": "internal", '
    '"rate_amount": 12.5, "rate_period": "hourly"}]'
)

# The scored comparisons of one document whose output matched every expectation,
# as (field_name, element_index, expected, actual, is_correct) in score order.
_ALL_CORRECT_SCORE_VALUES: list[
    tuple[str, int | None, str | None, str | None, bool]
] = [
    (IS_RELEVANT_FIELD_NAME, None, "True", "True", True),
    ("primary_status", None, "active", "active", True),
    ("status_note", None, "Currently active.", "Currently active.", True),
    ("location", None, "Kitchen", "Kitchen", True),
    ("assignments", None, "count:1", "count:1", True),
    ("assignments.assignment_name", 0, "Dish duty", "Dish duty", True),
    ("assignments.assignment_type", 0, "internal", "internal", True),
    ("assignments.rate_amount", 0, "12.5", "12.5", True),
    ("assignments.rate_period", 0, "hourly", "hourly", True),
]

# The same document's comparisons when the extractor produced no `location`.
_MISSING_LOCATION_SCORE_VALUES: list[
    tuple[str, int | None, str | None, str | None, bool]
] = [
    (IS_RELEVANT_FIELD_NAME, None, "True", "True", True),
    ("primary_status", None, "active", "active", True),
    ("status_note", None, "Currently active.", "Currently active.", True),
    ("location", None, "Kitchen", None, False),
    ("assignments", None, "count:1", "count:1", True),
    ("assignments.assignment_name", 0, "Dish duty", "Dish duty", True),
    ("assignments.assignment_type", 0, "internal", "internal", True),
    ("assignments.rate_amount", 0, "12.5", "12.5", True),
    ("assignments.rate_period", 0, "hourly", "hourly", True),
]

# The same document's comparisons when the extractor produced nothing usable —
# every expected value is a miss with no actual value to show.
_ALL_MISSED_SCORE_VALUES: list[tuple[str, int | None, str | None, str | None, bool]] = [
    (IS_RELEVANT_FIELD_NAME, None, "True", None, False),
    ("primary_status", None, "active", None, False),
    ("status_note", None, "Currently active.", None, False),
    ("location", None, "Kitchen", None, False),
    ("assignments", None, "count:1", None, False),
    ("assignments.assignment_name", 0, "Dish duty", None, False),
    ("assignments.assignment_type", 0, "internal", None, False),
    ("assignments.rate_amount", 0, "12.5", None, False),
    ("assignments.rate_period", 0, "hourly", None, False),
]


def _config() -> LLMExtractorConfig:
    """Returns the fake US_XX extractor config every test evaluates."""
    return get_first_order_llm_extractor_config(
        _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
    )


def _cells(**overrides: str) -> dict[str, str]:
    """Returns one sheet row's cell text keyed by column, defaulting to a valid
    US_XX row against FAKE_EXTRACTOR_COLLECTION's schema whose every expected value
    is non-null, and overriding it column by column.
    """
    cells = {
        TEST_TYPE_COLUMN_NAME: "unit",
        TEST_CASE_COLUMN_NAME: "base_case",
        GOLDEN_DOCUMENT_ID_COLUMN_NAME: "unit_1",
        DOCUMENT_TEXT_COLUMN_NAME: _DOCUMENT_TEXT,
        expected_value_column_name(IS_RELEVANT_FIELD_NAME): "true",
        expected_value_column_name("primary_status"): "active",
        expected_value_column_name("status_note"): "Currently active.",
        expected_value_column_name("location"): "Kitchen",
        expected_value_column_name("assignments"): _DISH_DUTY_ELEMENT_JSON,
    }
    cells.update(overrides)
    return cells


def _sheet(*cell_dicts: dict[str, str]) -> GoogleSheetTable:
    """Returns the sheet tab a read would produce for |cell_dicts|, numbered from
    row 2 as the sheet's first data row.
    """
    return GoogleSheetTable(
        title=_STATE_CODE.value,
        column_names=list(cell_dicts[0]),
        rows=[
            GoogleSheetTableRow(row_number=row_number, values_by_column=cells)
            for row_number, cells in enumerate(cell_dicts, start=2)
        ],
    )


def _relevant_result_json(*, location: str | None) -> dict[str, Any]:
    """Returns the wrapped result JSON of a relevant document holding one
    assignment, taking `location`'s null branch when |location| is None.
    """
    return wrap_in_result_key(
        build_fake_extractor_result_content(
            primary_status="active",
            status_note="Currently active.",
            location=location,
            assignments=[
                build_fake_extractor_assignment_result_json(
                    "Dish duty", "internal", 12.5, "hourly"
                )
            ],
        )
    )


# The success results the tests return, each grounded in the source document text
# that quotes every citation it carries, so the results survive validation's
# citation checks.
_GROUNDED_RESULT = ground_citations_in_fake_source_text(
    _relevant_result_json(location="Kitchen")
)
_GROUNDED_NO_LOCATION_RESULT = ground_citations_in_fake_source_text(
    _relevant_result_json(location=None)
)

# The document text of the default sheet row, grounding `_GROUNDED_RESULT`'s
# citations.
_DOCUMENT_TEXT = _GROUNDED_RESULT.source_document_text


def _success(
    *, request: LLMDocumentExtractionRequest, result_json: dict[str, Any]
) -> LLMClientDocumentExtractionResult:
    """Returns the client result a successful extraction of |request| produces."""
    return LLMClientDocumentExtractionResult.from_success(
        document_contents_id=request.document_contents_id,
        result_json=result_json,
        token_counts=LLMDocumentExtractionTokenCounts.empty(),
    )


def _content_filtered_error(
    request: LLMDocumentExtractionRequest,
) -> LLMClientDocumentExtractionResult:
    """Returns a permanent (never retried) request failure for |request|."""
    return LLMClientDocumentExtractionResult.from_error(
        document_contents_id=request.document_contents_id,
        error_type=LLMRequestErrorType.CONTENT_FILTERED,
        error_message="filtered",
    )


def _fake_client(
    *,
    config: LLMExtractorConfig,
    result_fn: Any,
) -> FakeSyncLLMClient:
    """Returns a fake sync client for |config|'s model that defers to |result_fn|."""
    return FakeSyncLLMClient(model_config=config.model_config, result_fn=result_fn)


def _client_factory(client: FakeSyncLLMClient) -> Any:
    """Returns a client factory that hands back |client| for any model config."""
    return lambda _model_config: client


def _expected_scores(
    *,
    golden_document_id: str,
    test_type: GoldenEvalTestType,
    test_case: str,
    score_values: list[tuple[str, int | None, str | None, str | None, bool]],
) -> list[GoldenEvalFieldScore]:
    """Returns the scored comparisons |score_values| describes for one document."""
    return [
        GoldenEvalFieldScore(
            golden_document_id=golden_document_id,
            test_type=test_type,
            test_case=test_case,
            field_name=field_name,
            element_index=element_index,
            expected_value=expected_value,
            actual_value=actual_value,
            is_correct=is_correct,
        )
        for (
            field_name,
            element_index,
            expected_value,
            actual_value,
            is_correct,
        ) in score_values
    ]


def _expected_rows(
    *,
    config: LLMExtractorConfig,
    golden_document_id: str,
    test_type: GoldenEvalTestType,
    test_case: str,
    score_values: list[tuple[str, int | None, str | None, str | None, bool]],
) -> list[dict[str, Any]]:
    """Returns the golden eval results rows |score_values| describes for one
    document of a run at `_RUN_DATETIME`.
    """
    return [
        GoldenEvalResultsBQTable.to_row(
            state_code=_STATE_CODE,
            extractor_id=config.extractor_id,
            extractor_version_id=config.extractor_version_id,
            output_schema_version=config.extractor_collection.output_schema_version,
            run_datetime_utc=_RUN_DATETIME,
            golden_document_id=golden_document_id,
            test_type=test_type,
            test_case=test_case,
            field_name=field_name,
            element_index=element_index,
            expected=expected_value,
            actual=actual_value,
            is_correct=is_correct,
        )
        for (
            field_name,
            element_index,
            expected_value,
            actual_value,
            is_correct,
        ) in score_values
    ]


class GoldenEvalRunnerTest(TestCase):
    """Tests the golden eval run itself, with the sheet read stubbed out."""

    def setUp(self) -> None:
        self.config = _config()
        self.bq_client = create_autospec(BigQueryClient)

    def _runner(
        self,
        *,
        client: FakeSyncLLMClient,
        sandbox_prefix: str = _SANDBOX_PREFIX,
        persist_processed_results: bool = False,
    ) -> GoldenEvalRunner:
        """Returns a runner that extracts through |client| and writes through this
        test's mock BQ client.
        """
        return GoldenEvalRunner(
            sandbox_prefix=sandbox_prefix,
            requester=_REQUESTER,
            persist_processed_results=persist_processed_results,
            sheets_service=create_autospec(Resource, instance=True),
            sync_llm_client_factory=_client_factory(client),
            bq_client=self.bq_client,
        )

    def test_no_golden_eval_config_raises(self) -> None:
        client = _fake_client(
            config=self.config,
            result_fn=lambda request: _success(
                request=request, result_json=_GROUNDED_RESULT.result_json
            ),
        )
        config = attr.evolve(self.config, golden_eval=None)

        with self.assertRaisesRegex(
            ValueError,
            r"^Extractor \[US_XX_FAKE_EXTRACTOR_COLLECTION\] declares no "
            r"golden_eval config, so it cannot be used to run a golden eval\.$",
        ):
            self._runner(client=client).run_eval(config=config)

        self.assertEqual([], client.requests)
        self.bq_client.stream_into_table.assert_not_called()

    def test_missing_expected_value_column_fails_before_any_llm_call(self) -> None:
        cells = _cells()
        del cells[expected_value_column_name("location")]
        client = _fake_client(
            config=self.config,
            result_fn=lambda request: _success(
                request=request, result_json=_GROUNDED_RESULT.result_json
            ),
        )

        with patch.object(
            GoogleSheetReader,
            "read_tab_as_table",
            return_value=_sheet(cells),
        ):
            with self.assertRaisesRegex(
                ValueError,
                r"(?s)^Golden eval sheet \[.*\] cannot be read against the "
                r"extractor's output schema:\n"
                r"  - has no \[location__expected\] column, so output schema field "
                r"\[location\] would have no expectation$",
            ):
                self._runner(client=client).run_eval(config=self.config)

        self.assertEqual([], client.requests)
        self.bq_client.stream_into_table.assert_not_called()

    def test_requests_built_from_document_text(self) -> None:
        client = _fake_client(
            config=self.config,
            result_fn=lambda request: _success(
                request=request, result_json=_GROUNDED_RESULT.result_json
            ),
        )
        model_configs: list[LLMModelConfig] = []

        def factory(model_config: LLMModelConfig) -> FakeSyncLLMClient:
            model_configs.append(model_config)
            return client

        runner = GoldenEvalRunner(
            sandbox_prefix=_SANDBOX_PREFIX,
            requester=_REQUESTER,
            persist_processed_results=False,
            sync_llm_client_factory=factory,
            sheets_service=create_autospec(Resource, instance=True),
            bq_client=self.bq_client,
        )
        with patch.object(
            GoogleSheetReader,
            "read_tab_as_table",
            return_value=_sheet(_cells()),
        ):
            runner.run_eval(config=self.config)

        self.assertEqual([self.config.model_config], model_configs)
        self.assertEqual(1, len(client.requests))
        request = client.requests[0]
        self.assertEqual("unit_1", request.document_contents_id)
        self.assertEqual(_DOCUMENT_TEXT, request.document_text)
        self.assertEqual(self.config.instructions_prompt, request.system_prompt)
        self.assertEqual(
            self.config.extractor_collection.generate_json_schema(),
            request.response_json_schema,
        )
        self.assertEqual(
            {
                "state_code": "us_xx",
                "job_type": "golden-eval",
                "model": "acme_large_deterministic",
                "requester": _REQUESTER,
            },
            request.request_parameters[
                BILLING_LABELS_EXTRACTION_REQUEST_PARAMETER_NAME
            ],
        )

    def test_billing_labels_sanitized(self) -> None:
        runner = GoldenEvalRunner(
            sandbox_prefix=_SANDBOX_PREFIX,
            requester="Testy.Tester@Recidiviz.org",
            persist_processed_results=False,
            bq_client=self.bq_client,
        )

        self.assertEqual(
            {
                "state_code": "us_xx",
                "job_type": "golden-eval",
                "model": "acme_large_deterministic",
                "requester": "testy_tester_recidiviz_org",
            },
            runner.billing_labels(config=self.config),
        )

    @freeze_time(_RUN_DATETIME_STR)
    def test_run_job_id(self) -> None:
        job_id = GoldenEvalRunner.build_run_job_id(
            config=self.config,
            run_datetime_utc=datetime.datetime.now(tz=datetime.UTC),
        )

        self.assertEqual(
            f"golden_eval_{self.config.extractor_version_id}_"
            f"{_RUN_DATETIME_JOB_ID_SUFFIX}",
            job_id,
        )

    def test_matching_output_scores_as_correct(self) -> None:
        client = _fake_client(
            config=self.config,
            result_fn=lambda request: _success(
                request=request, result_json=_GROUNDED_RESULT.result_json
            ),
        )

        with patch.object(
            GoogleSheetReader,
            "read_tab_as_table",
            return_value=_sheet(_cells()),
        ):
            result = self._runner(client=client).run_eval(config=self.config)

        self.assertEqual(
            _expected_scores(
                golden_document_id="unit_1",
                test_type=GoldenEvalTestType.UNIT,
                test_case="base_case",
                score_values=_ALL_CORRECT_SCORE_VALUES,
            ),
            result.field_scores,
        )
        self.assertEqual(
            {"unit_1": LLMExtractionJobDocumentResultType.SUCCESS},
            result.actual_llm_result_type_by_document_id,
        )

    def test_request_error_scores_as_miss(self) -> None:
        client = _fake_client(config=self.config, result_fn=_content_filtered_error)

        with patch.object(
            GoogleSheetReader,
            "read_tab_as_table",
            return_value=_sheet(_cells()),
        ):
            result = self._runner(client=client).run_eval(config=self.config)

        self.assertEqual(
            _expected_scores(
                golden_document_id="unit_1",
                test_type=GoldenEvalTestType.UNIT,
                test_case="base_case",
                score_values=_ALL_MISSED_SCORE_VALUES,
            ),
            result.field_scores,
        )
        self.assertEqual(
            {
                "unit_1": (
                    LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT
                )
            },
            result.actual_llm_result_type_by_document_id,
        )

    def test_validation_downgrade_scores_as_miss(self) -> None:
        # `primary_status` is required, so a result omitting it fails the
        # structural conformance check and leaves no validated content.
        client = _fake_client(
            config=self.config,
            result_fn=lambda request: _success(
                request=request,
                result_json=wrap_in_result_key(
                    {
                        IS_RELEVANT_FIELD_NAME: True,
                        "status_note": "Currently active.",
                    }
                ),
            ),
        )

        with patch.object(
            GoogleSheetReader,
            "read_tab_as_table",
            return_value=_sheet(_cells()),
        ):
            result = self._runner(client=client).run_eval(config=self.config)

        self.assertEqual(
            _expected_scores(
                golden_document_id="unit_1",
                test_type=GoldenEvalTestType.UNIT,
                test_case="base_case",
                score_values=_ALL_MISSED_SCORE_VALUES,
            ),
            result.field_scores,
        )
        self.assertEqual(
            {
                "unit_1": (
                    LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT
                )
            },
            result.actual_llm_result_type_by_document_id,
        )

    def test_unexpected_result_ids_raise(self) -> None:
        client = _fake_client(
            config=self.config,
            result_fn=lambda request: LLMClientDocumentExtractionResult.from_success(
                document_contents_id=f"bogus_{request.document_contents_id}",
                result_json=_GROUNDED_RESULT.result_json,
                token_counts=LLMDocumentExtractionTokenCounts.empty(),
            ),
        )

        with patch.object(
            GoogleSheetReader,
            "read_tab_as_table",
            return_value=_sheet(_cells()),
        ):
            with self.assertRaisesRegex(
                ValueError,
                r"^Golden eval run of extractor \[US_XX_FAKE_EXTRACTOR_COLLECTION\] "
                r"got extraction results that do not match its requests\. Missing "
                r"result\(s\) for \['unit_1'\]; unexpected result\(s\) for "
                r"\['bogus_unit_1'\]\.$",
            ):
                self._runner(client=client).run_eval(config=self.config)

        self.bq_client.stream_into_table.assert_not_called()

    @freeze_time(_RUN_DATETIME_STR)
    def test_scored_rows_written_to_sandbox_table(self) -> None:
        client = _fake_client(
            config=self.config,
            result_fn=lambda request: _success(
                request=request, result_json=_GROUNDED_RESULT.result_json
            ),
        )

        with patch.object(
            GoogleSheetReader,
            "read_tab_as_table",
            return_value=_sheet(_cells()),
        ):
            self._runner(client=client, sandbox_prefix=_SANDBOX_PREFIX).run_eval(
                config=self.config
            )

        self.bq_client.stream_into_table.assert_called_once_with(
            address=GoldenEvalResultsBQTable.address(
                collection_name=_COLLECTION_NAME, sandbox_prefix=_SANDBOX_PREFIX
            ),
            rows=_expected_rows(
                config=self.config,
                golden_document_id="unit_1",
                test_type=GoldenEvalTestType.UNIT,
                test_case="base_case",
                score_values=_ALL_CORRECT_SCORE_VALUES,
            ),
        )


class GoldenEvalRunnerEmulatorTest(BigQueryEmulatorTestCase):
    """Tests that a golden eval run's scored rows — and, in a sandbox, its
    processed results — land in the BQ tables.
    """

    # The result tables are created once in setUpClass; keep them across tests and
    # only clear their rows between tests.
    wipe_emulator_data_on_teardown = False

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        golden_eval_collection = collect_golden_eval_results_source_table_collection(
            config_module=fake_config
        )
        return [
            golden_eval_collection,
            golden_eval_collection.as_sandbox_collection(_SANDBOX_PREFIX),
            *[
                collection.as_sandbox_collection(_SANDBOX_PREFIX)
                for collection in collect_extraction_results_source_table_collections(
                    configs={
                        _STATE_CODE: {
                            _COLLECTION_NAME: get_first_order_llm_extractor_config(
                                _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
                            )
                        }
                    }
                )
            ],
        ]

    def setUp(self) -> None:
        super().setUp()
        self.config = _config()

    def tearDown(self) -> None:
        self._clear_emulator_table_data()
        super().tearDown()

    @property
    def _sheet(self) -> GoogleSheetTable:
        """Returns the three-document eval sheet both end-to-end tests evaluate:
        one document the extractor gets exactly right, one it misses a single field
        on, and one whose request fails outright.
        """
        return _sheet(
            _cells(),
            _cells(
                document_id="unit_2",
                test_case="missing_location",
                document_text=_GROUNDED_NO_LOCATION_RESULT.source_document_text,
            ),
            _cells(
                document_id="sample_1",
                test_type="sample",
                test_case="request_error",
            ),
        )

    @staticmethod
    def _result_fn(
        request: LLMDocumentExtractionRequest,
    ) -> LLMClientDocumentExtractionResult:
        """Returns the canned extraction result for |request|'s document."""
        if request.document_contents_id == "unit_1":
            return _success(request=request, result_json=_GROUNDED_RESULT.result_json)
        if request.document_contents_id == "unit_2":
            return _success(
                request=request, result_json=_GROUNDED_NO_LOCATION_RESULT.result_json
            )
        if request.document_contents_id == "sample_1":
            return _content_filtered_error(request)
        raise ValueError(f"Unexpected document [{request.document_contents_id}].")

    def _expected_rows(self) -> list[dict[str, Any]]:
        """Returns every golden eval results row the three-document run writes."""
        return [
            *_expected_rows(
                config=self.config,
                golden_document_id="unit_1",
                test_type=GoldenEvalTestType.UNIT,
                test_case="base_case",
                score_values=_ALL_CORRECT_SCORE_VALUES,
            ),
            *_expected_rows(
                config=self.config,
                golden_document_id="unit_2",
                test_type=GoldenEvalTestType.UNIT,
                test_case="missing_location",
                score_values=_MISSING_LOCATION_SCORE_VALUES,
            ),
            *_expected_rows(
                config=self.config,
                golden_document_id="sample_1",
                test_type=GoldenEvalTestType.SAMPLE,
                test_case="request_error",
                score_values=_ALL_MISSED_SCORE_VALUES,
            ),
        ]

    def _run_eval(
        self, *, sandbox_prefix: str, persist_processed_results: bool
    ) -> GoldenEvalResult:
        """Runs the three-document eval and returns its result."""
        runner = GoldenEvalRunner(
            sandbox_prefix=sandbox_prefix,
            requester=_REQUESTER,
            persist_processed_results=persist_processed_results,
            sheets_service=create_autospec(Resource, instance=True),
            sync_llm_client_factory=_client_factory(
                _fake_client(config=self.config, result_fn=self._result_fn)
            ),
            bq_client=self.bq_client,
        )
        with patch.object(
            GoogleSheetReader,
            "read_tab_as_table",
            return_value=self._sheet,
        ):
            with freeze_time(_RUN_DATETIME_STR):
                return runner.run_eval(config=self.config)

    def test_end_to_end_writes_scored_rows(self) -> None:
        result = self._run_eval(
            sandbox_prefix=_SANDBOX_PREFIX, persist_processed_results=False
        )

        self.compare_table_to_results(
            GoldenEvalResultsBQTable.address(
                collection_name=_COLLECTION_NAME, sandbox_prefix=_SANDBOX_PREFIX
            ),
            self._expected_rows(),
        )
        self.assertEqual(
            [
                *_expected_scores(
                    golden_document_id="unit_1",
                    test_type=GoldenEvalTestType.UNIT,
                    test_case="base_case",
                    score_values=_ALL_CORRECT_SCORE_VALUES,
                ),
                *_expected_scores(
                    golden_document_id="unit_2",
                    test_type=GoldenEvalTestType.UNIT,
                    test_case="missing_location",
                    score_values=_MISSING_LOCATION_SCORE_VALUES,
                ),
                *_expected_scores(
                    golden_document_id="sample_1",
                    test_type=GoldenEvalTestType.SAMPLE,
                    test_case="request_error",
                    score_values=_ALL_MISSED_SCORE_VALUES,
                ),
            ],
            result.field_scores,
        )
        self.assertEqual(
            {
                "unit_1": LLMExtractionJobDocumentResultType.SUCCESS,
                "unit_2": LLMExtractionJobDocumentResultType.SUCCESS,
                "sample_1": (
                    LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT
                ),
            },
            result.actual_llm_result_type_by_document_id,
        )
        # 18 UNIT comparisons, one of which (unit_2's location) missed; the SAMPLE
        # document's request failed, so all 9 of its comparisons missed.
        self.assertEqual(
            {GoldenEvalTestType.UNIT: 17 / 18, GoldenEvalTestType.SAMPLE: 0.0},
            result.accuracy_by_test_type,
        )

    def test_end_to_end_sandbox_persists_processed_results(self) -> None:
        self._run_eval(sandbox_prefix=_SANDBOX_PREFIX, persist_processed_results=True)

        self.compare_table_to_results(
            GoldenEvalResultsBQTable.address(
                collection_name=_COLLECTION_NAME, sandbox_prefix=_SANDBOX_PREFIX
            ),
            self._expected_rows(),
        )
        self.compare_table_to_results(
            GoldenEvalResultsBQTable.address(collection_name=_COLLECTION_NAME), []
        )

        expected_job_id = GoldenEvalRunner.build_run_job_id(
            config=self.config, run_datetime_utc=_RUN_DATETIME
        )
        # The two documents that produced JSON have raw rows; only they validated
        # cleanly, so validated and audit rows cover the same two.
        self.assertEqual(
            [("unit_1", expected_job_id), ("unit_2", expected_job_id)],
            self._document_ids_and_job_ids(
                ExtractionRawResultsBQTable.address(
                    state_code=_STATE_CODE,
                    collection_name=_COLLECTION_NAME,
                    sandbox_prefix=_SANDBOX_PREFIX,
                )
            ),
        )
        self.assertEqual(
            [("unit_1", expected_job_id), ("unit_2", expected_job_id)],
            self._document_ids_and_job_ids(
                ExtractionValidatedResultsBQTable.address(
                    state_code=_STATE_CODE,
                    collection_name=_COLLECTION_NAME,
                    sandbox_prefix=_SANDBOX_PREFIX,
                )
            ),
        )
        self.assertEqual(
            [("unit_1", expected_job_id), ("unit_2", expected_job_id)],
            self._document_ids_and_job_ids(
                ExtractionValidationAuditBQTable.address(
                    state_code=_STATE_CODE,
                    collection_name=_COLLECTION_NAME,
                    sandbox_prefix=_SANDBOX_PREFIX,
                )
            ),
        )

    def _document_ids_and_job_ids(
        self, address: BigQueryAddress
    ) -> list[tuple[str, str]]:
        """Returns the (document_contents_id, job_id) of every row of |address|,
        sorted by document id.
        """
        results = self.query(
            address.to_project_specific_address(metadata.project_id()).select_query(
                select_statement=f"SELECT {DOCUMENT_CONTENTS_ID_COLUMN_NAME}, "
                f"{EXTRACTION_JOB_ID_COLUMN_NAME}"
            )
        ).to_dict(orient="records")
        return sorted(
            (row[DOCUMENT_CONTENTS_ID_COLUMN_NAME], row[EXTRACTION_JOB_ID_COLUMN_NAME])
            for row in results
        )
