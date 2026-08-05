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
"""Tests for SyncLLMDocumentExtractionRequestRunner."""

import functools
import threading
from collections.abc import Iterator
from typing import Any
from unittest import TestCase
from unittest.mock import patch

from recidiviz.documents.extraction.llm_client.sync_llm_document_extraction_request_runner import (
    RequestsPerSecondLimiter,
    SyncLLMDocumentExtractionRequestRunner,
)
from recidiviz.documents.extraction.llm_client.types import (
    LLMClientDocumentExtractionResult,
    LLMDocumentExtractionRequest,
    LLMDocumentExtractionTokenCounts,
    LLMRequestErrorType,
)
from recidiviz.documents.extraction.models.llm_model_registry import (
    load_llm_model_registry,
)
from recidiviz.tests.documents.extraction.llm_client.fake_sync_llm_client import (
    FakeSyncLLMClient,
)

_RESPONSE_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"result": {"type": "object"}},
    "required": ["result"],
}


def _request(document_contents_id: str) -> LLMDocumentExtractionRequest:
    return LLMDocumentExtractionRequest(
        document_contents_id=document_contents_id,
        system_prompt="You extract employment info.",
        document_text=f"Text for {document_contents_id}.",
        response_json_schema=_RESPONSE_JSON_SCHEMA,
        request_parameters={},
    )


def _success(
    request: LLMDocumentExtractionRequest,
) -> LLMClientDocumentExtractionResult:
    return LLMClientDocumentExtractionResult.from_success(
        document_contents_id=request.document_contents_id,
        result_json={"result": {"is_relevant": True}},
        token_counts=LLMDocumentExtractionTokenCounts(
            input_token_count=10,
            output_token_count=5,
            cached_input_token_count=0,
            thinking_token_count=0,
        ),
    )


def _error(
    request: LLMDocumentExtractionRequest, *, error_type: LLMRequestErrorType
) -> LLMClientDocumentExtractionResult:
    return LLMClientDocumentExtractionResult.from_error(
        document_contents_id=request.document_contents_id,
        error_type=error_type,
        error_message=f"{error_type} for {request.document_contents_id}.",
    )


class SyncLLMDocumentExtractionRequestRunnerTest(TestCase):
    """Tests for SyncLLMDocumentExtractionRequestRunner, driven by a fake client
    whose result function stands in for various provider behaviors. Tests default
    to an effectively-unthrottled rate limiter and a zero retry backoff so retry
    and concurrency logic is exercised without real delay.
    """

    def setUp(self) -> None:
        self.model_config = load_llm_model_registry().get_model_config(
            "GEMINI_3_5_FLASH_MINIMAL_THINKING"
        )

    def _runner(
        self, client: FakeSyncLLMClient, **kwargs: Any
    ) -> SyncLLMDocumentExtractionRequestRunner:
        kwargs.setdefault("max_requests_per_second", 1e6)
        kwargs.setdefault("retry_base_delay_seconds", 0.0)
        return SyncLLMDocumentExtractionRequestRunner(client=client, **kwargs)

    def _collect(
        self,
        runner: SyncLLMDocumentExtractionRequestRunner,
        requests: list[LLMDocumentExtractionRequest],
        **kwargs: Any,
    ) -> list[LLMClientDocumentExtractionResult]:
        with runner.execute_document_extraction_requests(
            requests=requests, **kwargs
        ) as results:
            return list(results)

    def test_all_requests_executed_and_results_returned(self) -> None:
        client = FakeSyncLLMClient(model_config=self.model_config, result_fn=_success)
        runner = self._runner(client)
        requests = [_request(f"doc_{i}") for i in range(10)]

        results = self._collect(runner, requests)

        self.assertEqual(10, len(results))
        # Every request is executed exactly once (all succeed, no retries), and
        # every document is represented in the results.
        self.assertEqual(10, len(client.requests))
        self.assertEqual(
            {f"doc_{i}" for i in range(10)},
            {r.document_contents_id for r in results},
        )
        self.assertTrue(all(r.error_type is None for r in results))

    def test_transient_error_retried_until_success(self) -> None:
        # RATE_LIMITED on the first two attempts, then success on the third.
        attempts: dict[str, int] = {}

        def result_fn(
            request: LLMDocumentExtractionRequest,
        ) -> LLMClientDocumentExtractionResult:
            attempts[request.document_contents_id] = (
                attempts.get(request.document_contents_id, 0) + 1
            )
            if attempts[request.document_contents_id] <= 2:
                return _error(request, error_type=LLMRequestErrorType.RATE_LIMITED)
            return _success(request)

        client = FakeSyncLLMClient(model_config=self.model_config, result_fn=result_fn)
        runner = self._runner(client, max_retries=5)

        results = self._collect(runner, [_request("doc_0")])

        self.assertEqual(1, len(results))
        self.assertIsNone(results[0].error_type)
        # Two transient failures then success => three total attempts.
        self.assertEqual(3, attempts["doc_0"])

    def test_transient_error_surfaces_after_retries_exhausted(self) -> None:
        client = FakeSyncLLMClient(
            model_config=self.model_config,
            result_fn=lambda r: _error(r, error_type=LLMRequestErrorType.TIMEOUT),
        )
        runner = self._runner(client, max_retries=3)

        results = self._collect(runner, [_request("doc_0")])

        self.assertEqual(1, len(results))
        self.assertEqual(LLMRequestErrorType.TIMEOUT, results[0].error_type)
        # max_retries + 1 total attempts.
        self.assertEqual(4, len(client.requests))

    def test_permanent_errors_pass_through_without_retry(self) -> None:
        for error_type in (
            LLMRequestErrorType.MALFORMED_RESPONSE,
            LLMRequestErrorType.EMPTY_RESPONSE,
            LLMRequestErrorType.CONTENT_FILTERED,
            LLMRequestErrorType.UNKNOWN_ERROR,
        ):
            with self.subTest(error_type=error_type):
                client = FakeSyncLLMClient(
                    model_config=self.model_config,
                    result_fn=functools.partial(_error, error_type=error_type),
                )
                runner = self._runner(client, max_retries=5)

                results = self._collect(runner, [_request("doc_0")])

                self.assertEqual(error_type, results[0].error_type)
                # Executed exactly once — permanent conditions are not retried.
                self.assertEqual(1, len(client.requests))

    def test_progress_callback_invoked_per_result(self) -> None:
        client = FakeSyncLLMClient(model_config=self.model_config, result_fn=_success)
        runner = self._runner(client)
        requests = [_request(f"doc_{i}") for i in range(5)]
        seen: list[str] = []

        results = self._collect(
            runner,
            requests,
            progress_callback=lambda result: seen.append(result.document_contents_id),
        )

        self.assertEqual(5, len(results))
        # The callback fires exactly once per completed result.
        self.assertCountEqual([r.document_contents_id for r in results], seen)

    def test_requests_run_concurrently(self) -> None:
        # Each call blocks on a barrier until `max_concurrency` calls are in
        # flight, so the batch can only complete if that many run concurrently.
        max_concurrency = 4
        barrier = threading.Barrier(max_concurrency, timeout=10)

        def result_fn(
            request: LLMDocumentExtractionRequest,
        ) -> LLMClientDocumentExtractionResult:
            barrier.wait()
            return _success(request)

        client = FakeSyncLLMClient(model_config=self.model_config, result_fn=result_fn)
        runner = self._runner(client, max_concurrency=max_concurrency)
        requests = [_request(f"doc_{i}") for i in range(max_concurrency)]

        results = self._collect(runner, requests)

        self.assertEqual(max_concurrency, len(results))

    def test_early_exit_cancels_pending_requests(self) -> None:
        # Breaking out of the results loop must not execute (or wait on) the
        # rest of the backlog: exiting the context cancels everything that has
        # not yet started.
        client = FakeSyncLLMClient(model_config=self.model_config, result_fn=_success)
        runner = self._runner(client, max_concurrency=2)
        requests = [_request(f"doc_{i}") for i in range(500)]

        with runner.execute_document_extraction_requests(requests=requests) as results:
            next(results)

        # Only the requests already submitted around the time of the early exit
        # ran; the hundreds of pending ones were cancelled, not executed.
        self.assertLess(len(client.requests), 20)

    def test_lazy_requests_iterable_consumed_incrementally(self) -> None:
        # A generator of requests is pulled from only as concurrency slots free
        # up, never drained up front.
        client = FakeSyncLLMClient(model_config=self.model_config, result_fn=_success)
        runner = self._runner(client, max_concurrency=2)
        pulled = 0

        def request_generator() -> Iterator[LLMDocumentExtractionRequest]:
            nonlocal pulled
            for i in range(100):
                pulled += 1
                yield _request(f"doc_{i}")

        with runner.execute_document_extraction_requests(
            requests=request_generator()
        ) as results:
            next(results)
            self.assertLess(pulled, 20)
            list(results)

        self.assertEqual(100, pulled)

    def test_exit_interrupts_retry_backoff(self) -> None:
        # A worker sleeping in an hours-long retry backoff observes the
        # cancellation event when the context exits and returns instead of
        # sleeping out the delay (which would otherwise keep its non-daemon
        # thread alive long past the run).
        doc_1_attempted = threading.Event()
        worker_threads: list[threading.Thread] = []

        def result_fn(
            request: LLMDocumentExtractionRequest,
        ) -> LLMClientDocumentExtractionResult:
            worker_threads.append(threading.current_thread())
            if request.document_contents_id == "doc_1":
                doc_1_attempted.set()
                return _error(request, error_type=LLMRequestErrorType.RATE_LIMITED)
            return _success(request)

        client = FakeSyncLLMClient(model_config=self.model_config, result_fn=result_fn)
        runner = self._runner(
            client,
            max_concurrency=2,
            max_retries=5,
            retry_base_delay_seconds=3600.0,
        )

        with runner.execute_document_extraction_requests(
            requests=[_request("doc_0"), _request("doc_1")]
        ) as results:
            # Consume doc_0's success and confirm doc_1 has entered its retry
            # loop before tearing the run down.
            next(results)
            self.assertTrue(doc_1_attempted.wait(timeout=10))

        for thread in worker_threads:
            thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
        # doc_1 gave up during its first backoff rather than retrying.
        self.assertEqual(
            1,
            len([r for r in client.requests if r.document_contents_id == "doc_1"]),
        )

    def test_rate_limiter_spaces_out_dispatch(self) -> None:
        # Drive the limiter with a fake clock so we can assert the exact waits it
        # computes without any real sleeping. Each acquire() advances the clock by
        # the amount it decided to sleep, mimicking real time passing.
        limiter = RequestsPerSecondLimiter(max_requests_per_second=20.0)
        min_interval = 1.0 / 20.0
        fake_now = 0.0
        sleeps: list[float] = []

        def fake_sleep(seconds: float) -> None:
            nonlocal fake_now
            sleeps.append(seconds)
            fake_now += seconds

        with (
            patch(
                "recidiviz.documents.extraction.llm_client."
                "sync_llm_document_extraction_request_runner.time.monotonic",
                side_effect=lambda: fake_now,
            ),
            patch(
                "recidiviz.documents.extraction.llm_client."
                "sync_llm_document_extraction_request_runner.time.sleep",
                side_effect=fake_sleep,
            ),
        ):
            for _ in range(10):
                limiter.acquire()

        # The first acquire dispatches immediately; each of the next nine waits one
        # min_interval, so no back-to-back dispatch exceeds the configured rate.
        self.assertEqual(9, len(sleeps))
        for actual in sleeps:
            self.assertAlmostEqual(min_interval, actual)
