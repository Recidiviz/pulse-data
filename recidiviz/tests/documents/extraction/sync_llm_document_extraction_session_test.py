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
"""Tests for SyncLLMDocumentExtractionSession.

The two harnesses (the sandbox extraction script and the golden eval runner) cover
their own consumption of the session end-to-end; these tests state the session's
own contract directly — the finish_session lifecycle, the outcome and token
tallies, the delegate event dispatch, and the requests-pair-with-results
invariant — through a FakeSyncLLMClient and in-memory documents.
"""
import copy
from collections.abc import Callable
from unittest import TestCase

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMExtractionJobDocumentResultType,
)
from recidiviz.documents.extraction.expected_entry_nums_source import (
    InMemoryExpectedEntryNumsSource,
)
from recidiviz.documents.extraction.llm_client.types import (
    LLMClientDocumentExtractionResult,
    LLMDocumentExtractionRequest,
    LLMDocumentExtractionTokenCounts,
    LLMRequestErrorType,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    IS_RELEVANT_FIELD_NAME,
)
from recidiviz.documents.extraction.sync_llm_document_extraction_session import (
    SyncLLMDocumentExtractionSession,
    SyncLLMDocumentExtractionSessionDelegate,
    SyncLLMDocumentExtractionSessionSummary,
)
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    fake_entity_resolution_extractor_config,
    fake_first_order_extractor_config,
    patch_fake_entity_resolution_model_config_name,
)
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    fake_minimal_relevant_result_json,
    ground_citations_in_fake_source_text,
    wrap_in_result_key,
)
from recidiviz.tests.documents.extraction.llm_client.fake_sync_llm_client import (
    FakeSyncLLMClient,
)

_JOB_ID = "job-1"
_BILLING_LABELS = {"job_type": "test"}
_ASSIGNMENT_GROUP_NAME = "assignment"
_DOC_A = "CID_A"
_DOC_B = "CID_B"
_DOC_C = "CID_C"

# A result that survives validation, paired with the source text that grounds its
# citations; every in-memory document below carries that text.
_GROUNDED_SUCCESS_RESULT = ground_citations_in_fake_source_text(
    fake_minimal_relevant_result_json()
)
_DOCUMENT_TEXT = _GROUNDED_SUCCESS_RESULT.source_document_text

# A result that parses but fails validation: the required `primary_status` field
# is missing, so structural conformance rejects it.
_INVALID_RESULT_JSON = wrap_in_result_key(
    {IS_RELEVANT_FIELD_NAME: True, "status_note": "Currently active."}
)

_PER_DOCUMENT_TOKEN_COUNTS = LLMDocumentExtractionTokenCounts(
    input_token_count=10,
    output_token_count=5,
    cached_input_token_count=2,
    thinking_token_count=0,
)


def _success_result_fn(
    request: LLMDocumentExtractionRequest,
) -> LLMClientDocumentExtractionResult:
    """Returns a grounded, validation-passing success result for |request|."""
    return LLMClientDocumentExtractionResult.from_success(
        document_contents_id=request.document_contents_id,
        result_json=copy.deepcopy(_GROUNDED_SUCCESS_RESULT.result_json),
        token_counts=_PER_DOCUMENT_TOKEN_COUNTS,
    )


@attr.define(frozen=True, kw_only=True)
class _InMemoryExtractionDocument:
    """A DocumentTextSource that already holds its text, like a golden eval
    document does."""

    document_contents_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    document_text: str = attr.ib(validator=attr_validators.is_str)

    def fetch_document_text(self) -> str:
        return self.document_text


@attr.define(frozen=True, kw_only=True)
class _UnfetchableExtractionDocument:
    """A DocumentTextSource whose text fetch always fails, like a GCS-backed
    document whose blob is missing."""

    document_contents_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    error: Exception = attr.ib(validator=attr.validators.instance_of(Exception))

    def fetch_document_text(self) -> str:
        raise self.error


class _RecordingSessionDelegate:
    """Delegate that records every event it receives, so tests can assert on
    exactly what the session dispatched."""

    def __init__(self) -> None:
        self.empty_document_ids: list[str] = []
        self.build_failures: list[tuple[str, Exception]] = []
        self.raw_results: list[LLMClientDocumentExtractionResult] = []

    def on_empty_document(self, *, document_contents_id: str) -> None:
        self.empty_document_ids.append(document_contents_id)

    def on_document_request_build_failure(
        self, *, document_contents_id: str, error: Exception
    ) -> None:
        self.build_failures.append((document_contents_id, error))

    def on_raw_document_extraction_result(
        self, result: LLMClientDocumentExtractionResult
    ) -> None:
        self.raw_results.append(result)


class _RaisingOnBuildFailureDelegate(_RecordingSessionDelegate):
    """Recording delegate whose build-failure event is fatal, like the golden
    eval's strict delegate."""

    def on_document_request_build_failure(
        self, *, document_contents_id: str, error: Exception
    ) -> None:
        raise ValueError(f"Build failed for [{document_contents_id}].") from error


class SyncLLMDocumentExtractionSessionTest(TestCase):
    """Tests for SyncLLMDocumentExtractionSession."""

    def setUp(self) -> None:
        self.config = fake_first_order_extractor_config()
        self.delegate = _RecordingSessionDelegate()

    def _session(
        self,
        *,
        client: FakeSyncLLMClient,
        delegate: SyncLLMDocumentExtractionSessionDelegate | None = None,
    ) -> SyncLLMDocumentExtractionSession:
        return SyncLLMDocumentExtractionSession(
            config=self.config,
            job_id=_JOB_ID,
            billing_labels=_BILLING_LABELS,
            sync_client=client,
            delegate=delegate if delegate is not None else self.delegate,
            expected_entry_nums_source=None,
            request_build_concurrency=2,
        )

    def _client(
        self,
        result_fn: Callable[
            [LLMDocumentExtractionRequest], LLMClientDocumentExtractionResult
        ] = _success_result_fn,
    ) -> FakeSyncLLMClient:
        return FakeSyncLLMClient(
            model_config=self.config.model_config, result_fn=result_fn
        )

    @staticmethod
    def _document(document_contents_id: str) -> _InMemoryExtractionDocument:
        return _InMemoryExtractionDocument(
            document_contents_id=document_contents_id, document_text=_DOCUMENT_TEXT
        )

    def test_finish_session_returns_accumulated_summary(self) -> None:
        session = self._session(client=self._client())

        results = list(
            session.extract(documents=[self._document(_DOC_A), self._document(_DOC_B)])
        )

        self.assertEqual(
            {_DOC_A, _DOC_B},
            {result.document_contents_id for result in results},
        )
        self.assertEqual(
            SyncLLMDocumentExtractionSessionSummary(
                processed=2,
                succeeded=2,
                token_counts=LLMDocumentExtractionTokenCounts(
                    input_token_count=20,
                    output_token_count=10,
                    cached_input_token_count=4,
                    thinking_token_count=0,
                ),
            ),
            session.finish_session(),
        )

    def test_entity_resolution_config_without_entry_nums_source_raises(self) -> None:
        with patch_fake_entity_resolution_model_config_name():
            er_config = fake_entity_resolution_extractor_config(_ASSIGNMENT_GROUP_NAME)
            with self.assertRaisesRegex(
                ValueError,
                r"^Extractor \[.*\] is an entity-resolution extractor, so an "
                r"expected_entry_nums_source must be provided\.$",
            ):
                SyncLLMDocumentExtractionSession(
                    config=er_config,
                    job_id=_JOB_ID,
                    billing_labels=_BILLING_LABELS,
                    sync_client=FakeSyncLLMClient(
                        model_config=er_config.model_config,
                        result_fn=_success_result_fn,
                    ),
                    delegate=self.delegate,
                    expected_entry_nums_source=None,
                    request_build_concurrency=2,
                )

    def test_first_order_config_with_entry_nums_source_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Extractor \[.*\] is a first-order extractor, so "
            r"expected_entry_nums_source must be None — its documents have no "
            r"numbered entries\.$",
        ):
            SyncLLMDocumentExtractionSession(
                config=self.config,
                job_id=_JOB_ID,
                billing_labels=_BILLING_LABELS,
                sync_client=self._client(),
                delegate=self.delegate,
                expected_entry_nums_source=InMemoryExpectedEntryNumsSource(
                    entry_nums_by_document={_DOC_A: {1, 2}}
                ),
                request_build_concurrency=2,
            )

    def test_finish_session_twice_raises(self) -> None:
        session = self._session(client=self._client())
        list(session.extract(documents=[self._document(_DOC_A)]))
        session.finish_session()

        with self.assertRaisesRegex(
            ValueError,
            r"^Extraction session for job \[job-1\] is already finished; no "
            r"further extraction can run in it\.$",
        ):
            session.finish_session()

    def test_extract_after_finish_raises(self) -> None:
        session = self._session(client=self._client())
        list(session.extract(documents=[self._document(_DOC_A)]))
        session.finish_session()

        with self.assertRaisesRegex(
            ValueError,
            r"^Extraction session for job \[job-1\] is already finished; no "
            r"further extraction can run in it\.$",
        ):
            # extract is a generator, so the guard fires at the first pull.
            list(session.extract(documents=[self._document(_DOC_B)]))

    def test_two_extract_calls_accumulate_into_one_summary(self) -> None:
        session = self._session(client=self._client())

        list(session.extract(documents=[self._document(_DOC_A)]))
        list(session.extract(documents=[self._document(_DOC_B)]))

        self.assertEqual(
            SyncLLMDocumentExtractionSessionSummary(
                processed=2,
                succeeded=2,
                token_counts=LLMDocumentExtractionTokenCounts(
                    input_token_count=20,
                    output_token_count=10,
                    cached_input_token_count=4,
                    thinking_token_count=0,
                ),
            ),
            session.finish_session(),
        )

    def test_summary_counts_success_failed_request_and_failed_validation(
        self,
    ) -> None:
        def result_fn(
            request: LLMDocumentExtractionRequest,
        ) -> LLMClientDocumentExtractionResult:
            if request.document_contents_id == _DOC_A:
                return _success_result_fn(request)
            if request.document_contents_id == _DOC_B:
                return LLMClientDocumentExtractionResult.from_error(
                    document_contents_id=request.document_contents_id,
                    error_type=LLMRequestErrorType.CONTENT_FILTERED,
                    error_message="filtered",
                )
            if request.document_contents_id == _DOC_C:
                return LLMClientDocumentExtractionResult.from_success(
                    document_contents_id=request.document_contents_id,
                    result_json=copy.deepcopy(_INVALID_RESULT_JSON),
                    token_counts=_PER_DOCUMENT_TOKEN_COUNTS,
                )
            raise ValueError(f"Unexpected document [{request.document_contents_id}].")

        session = self._session(client=self._client(result_fn))

        results = list(
            session.extract(
                documents=[
                    self._document(_DOC_A),
                    self._document(_DOC_B),
                    self._document(_DOC_C),
                ]
            )
        )

        self.assertEqual(
            {
                _DOC_A: LLMExtractionJobDocumentResultType.SUCCESS,
                _DOC_B: LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT,
                _DOC_C: LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
            },
            {result.document_contents_id: result.result_type for result in results},
        )
        # The error result carries no tokens, so only the two results that came
        # back with JSON contribute to the token totals.
        self.assertEqual(
            SyncLLMDocumentExtractionSessionSummary(
                processed=3,
                succeeded=1,
                failed_llm_request=1,
                failed_validation=1,
                token_counts=LLMDocumentExtractionTokenCounts(
                    input_token_count=20,
                    output_token_count=10,
                    cached_input_token_count=4,
                    thinking_token_count=0,
                ),
            ),
            session.finish_session(),
        )

    def test_token_counts_summed_across_results(self) -> None:
        session = self._session(client=self._client())
        list(
            session.extract(documents=[self._document(_DOC_A), self._document(_DOC_B)])
        )

        self.assertEqual(
            LLMDocumentExtractionTokenCounts(
                input_token_count=20,
                output_token_count=10,
                cached_input_token_count=4,
                thinking_token_count=0,
            ),
            session.finish_session().token_counts,
        )

    def test_empty_document_dispatches_event_and_yields_no_result(self) -> None:
        # Pins the status quo TODO(OBT-42807) will change: an empty-text document
        # is skipped without any terminal result, so nothing reaches the LLM and
        # nothing is yielded for the consumer to persist.
        client = self._client()
        session = self._session(client=client)

        results = list(
            session.extract(
                documents=[
                    _InMemoryExtractionDocument(
                        document_contents_id=_DOC_A, document_text=""
                    )
                ]
            )
        )

        self.assertEqual([], results)
        self.assertEqual([], client.requests)
        self.assertEqual([_DOC_A], self.delegate.empty_document_ids)
        self.assertEqual(
            SyncLLMDocumentExtractionSessionSummary(skipped_empty=1),
            session.finish_session(),
        )

    def test_build_failure_dispatches_event_with_error(self) -> None:
        error = RuntimeError("text fetch failed")
        session = self._session(client=self._client())

        results = list(
            session.extract(
                documents=[
                    _UnfetchableExtractionDocument(
                        document_contents_id=_DOC_A, error=error
                    ),
                    self._document(_DOC_B),
                ]
            )
        )

        # The failed document goes to the delegate (with the original error) and
        # the rest of the run survives it.
        self.assertEqual([(_DOC_A, error)], self.delegate.build_failures)
        self.assertEqual([_DOC_B], [result.document_contents_id for result in results])
        self.assertEqual(
            SyncLLMDocumentExtractionSessionSummary(
                processed=1,
                succeeded=1,
                failed_to_build=1,
                token_counts=LLMDocumentExtractionTokenCounts(
                    input_token_count=10,
                    output_token_count=5,
                    cached_input_token_count=2,
                    thinking_token_count=0,
                ),
            ),
            session.finish_session(),
        )

    def test_raw_result_event_fires_once_per_result(self) -> None:
        session = self._session(client=self._client())

        list(
            session.extract(documents=[self._document(_DOC_A), self._document(_DOC_B)])
        )

        self.assertEqual(
            {_DOC_A, _DOC_B},
            {result.document_contents_id for result in self.delegate.raw_results},
        )
        self.assertEqual(2, len(self.delegate.raw_results))

    def test_delegate_raise_aborts_the_run(self) -> None:
        session = self._session(
            client=self._client(), delegate=_RaisingOnBuildFailureDelegate()
        )

        with self.assertRaisesRegex(ValueError, r"^Build failed for \[CID_A\]\.$"):
            list(
                session.extract(
                    documents=[
                        _UnfetchableExtractionDocument(
                            document_contents_id=_DOC_A,
                            error=RuntimeError("text fetch failed"),
                        )
                    ]
                )
            )

    def test_result_with_unknown_id_raises(self) -> None:
        def result_fn(
            request: LLMDocumentExtractionRequest,
        ) -> LLMClientDocumentExtractionResult:
            return LLMClientDocumentExtractionResult.from_success(
                document_contents_id=f"bogus_{request.document_contents_id}",
                result_json=copy.deepcopy(_GROUNDED_SUCCESS_RESULT.result_json),
                token_counts=LLMDocumentExtractionTokenCounts.empty(),
            )

        session = self._session(client=self._client(result_fn))

        with self.assertRaisesRegex(
            ValueError,
            r"^Extraction run \[job-1\] of extractor "
            r"\[US_XX_FAKE_EXTRACTOR_COLLECTION\] got a result for document "
            r"\[bogus_CID_A\] with no matching in-flight request — the document "
            r"was never requested, or its result already arrived\.$",
        ):
            list(session.extract(documents=[self._document(_DOC_A)]))
