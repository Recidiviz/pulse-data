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
"""Runner that submits a collection of document extraction requests through a
provider-agnostic `SyncLLMClient`, managing parallelization and retries for
transient errors.
"""

import contextlib
import itertools
import logging
import random
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait

import attr

from recidiviz.common import attr_validators
from recidiviz.documents.extraction.llm_client.sync_llm_client import SyncLLMClient
from recidiviz.documents.extraction.llm_client.types import (
    LLMClientDocumentExtractionResult,
    LLMDocumentExtractionRequest,
    LLMRequestErrorType,
)

# Maximum number of extraction requests in flight against the provider at once.
# The provider's per-minute quota is the true ceiling; this caps how quickly we
# approach it. Sized from the prototype's ~900K-document runs.
DEFAULT_MAX_CONCURRENCY = 200

# Maximum requests per second dispatched to the provider. Caps the burst rate
# independently of concurrency: Vertex AI enforces per-second burst limits well
# below the per-minute quota, and without this the pool would fire
# `max_concurrency` requests simultaneously at job start and overrun the burst
# limit.
DEFAULT_MAX_REQUESTS_PER_SECOND = 50.0

# Number of times a request is retried after a transient failure (so a request
# is attempted up to `max_retries + 1` times total).
DEFAULT_MAX_RETRIES = 5

# Base delay, in seconds, for the exponential backoff between retries.
DEFAULT_RETRY_BASE_DELAY_SECONDS = 5.0

# Error types that indicate a transient provider condition worth retrying. Every
# other error type (malformed/empty/filtered responses, unknown errors) reflects
# a permanent condition and is passed straight through for the caller to classify.
_TRANSIENT_ERROR_TYPES = frozenset(
    {
        LLMRequestErrorType.RATE_LIMITED,
        LLMRequestErrorType.TIMEOUT,
        LLMRequestErrorType.SERVER_ERROR,
    }
)

# Callback invoked with each result as it completes, e.g. so a script can print
# progress. Called once per request, serially from the thread consuming the
# results iterator (never from a worker thread), so it does not need to be
# thread-safe.
ProgressCallback = Callable[[LLMClientDocumentExtractionResult], None]


@attr.define(kw_only=True)
class RequestsPerSecondLimiter:
    """A thread-safe limiter that blocks until dispatching another request would
    stay within `max_requests_per_second`, by pacing dispatches a fixed minimum
    interval apart (no burst allowance). `aiolimiter` covers this for asyncio,
    but the runner drives the synchronous client from a thread pool, so we need
    a blocking, thread-safe limiter instead.
    """

    max_requests_per_second: float = attr.ib(
        validator=attr_validators.is_positive_float
    )
    """Maximum requests per second the limiter will allow to be dispatched."""

    _lock: threading.Lock = attr.ib(
        factory=threading.Lock, init=False, eq=False, repr=False
    )
    # Monotonic timestamp at which the next request may be dispatched.
    _next_allowed_time: float = attr.ib(default=0.0, init=False, eq=False, repr=False)

    @property
    def _min_interval_seconds(self) -> float:
        return 1.0 / self.max_requests_per_second

    def acquire(self) -> None:
        """Blocks until the caller may dispatch a request without exceeding the
        configured rate.
        """
        with self._lock:
            now = time.monotonic()
            wait_seconds = self._next_allowed_time - now
            start_time = max(now, self._next_allowed_time)
            self._next_allowed_time = start_time + self._min_interval_seconds
        if wait_seconds > 0:
            time.sleep(wait_seconds)


@attr.define(frozen=True, kw_only=True)
class SyncLLMDocumentExtractionRequestRunner:
    """Wrapper around the provider-agnostic `SyncLLMClient` that submits requests
    and waits for their results, managing parallelization and retries for
    transient errors.

    A "pure" class: the only I/O it does is with the LLM provider (through the
    client). It does not read from or write to any data store, and it does not
    interpret results beyond deciding whether a transient failure is worth
    retrying — the caller routes every result onward.
    """

    _client: SyncLLMClient = attr.ib(
        # SyncLLMClient is abstract; mypy flags it where a concrete type is
        # expected, but instance_of accepts an ABC fine at runtime.
        validator=attr.validators.instance_of(SyncLLMClient)  # type: ignore[type-abstract]
    )
    """Client that executes a single extraction request against the provider."""

    _max_concurrency: int = attr.ib(
        default=DEFAULT_MAX_CONCURRENCY, validator=attr_validators.is_positive_int
    )
    """Maximum requests in flight against the provider at once."""

    _max_requests_per_second: float = attr.ib(
        default=DEFAULT_MAX_REQUESTS_PER_SECOND,
        validator=attr_validators.is_positive_float,
    )
    """Maximum requests per second dispatched to the provider."""

    _max_retries: int = attr.ib(
        default=DEFAULT_MAX_RETRIES, validator=attr_validators.is_non_negative_int
    )
    """Times a request is retried after a transient failure."""

    _retry_base_delay_seconds: float = attr.ib(
        default=DEFAULT_RETRY_BASE_DELAY_SECONDS,
        validator=attr_validators.is_non_negative_float,
    )
    """Base delay, in seconds, for the exponential backoff between retries."""

    @contextlib.contextmanager
    def execute_document_extraction_requests(
        self,
        *,
        requests: Iterable[LLMDocumentExtractionRequest],
        progress_callback: ProgressCallback | None = None,
    ) -> Iterator[Iterator[LLMClientDocumentExtractionResult]]:
        """Context manager whose entered value is an iterator that executes every
        request against the provider with bounded concurrency and transient-error
        retries, producing each `LLMClientDocumentExtractionResult` as it
        completes (not in request order). A failed request that exhausts its
        retries is surfaced as an error result, never raised.

        Exiting the context — including via an early `break` or an exception in
        the consuming loop — cancels all not-yet-started requests and signals any
        worker sleeping between retries to give up, so partial consumption never
        blocks on the unprocessed backlog:

            with runner.execute_document_extraction_requests(requests=...) as results:
                for result in results:
                    ...

        The iterator is only valid inside the `with` block. `progress_callback`,
        if given, is invoked with each result as it completes, serially from the
        consuming thread.
        """
        rate_limiter = RequestsPerSecondLimiter(
            max_requests_per_second=self._max_requests_per_second
        )
        cancel_event = threading.Event()
        executor = ThreadPoolExecutor(max_workers=self._max_concurrency)
        try:
            yield self._result_iterator(
                executor=executor,
                requests=requests,
                rate_limiter=rate_limiter,
                cancel_event=cancel_event,
                progress_callback=progress_callback,
            )
        finally:
            # On abnormal exit this tears the run down promptly: queued futures
            # are cancelled before they start, and workers mid-backoff observe
            # the event and return instead of re-hitting the provider. On normal
            # exit (iterator fully consumed) both are no-ops. shutdown does not
            # wait, so at most the in-flight provider calls finish in the
            # background.
            cancel_event.set()
            executor.shutdown(wait=False, cancel_futures=True)

    def _result_iterator(
        self,
        *,
        executor: ThreadPoolExecutor,
        requests: Iterable[LLMDocumentExtractionRequest],
        rate_limiter: RequestsPerSecondLimiter,
        cancel_event: threading.Event,
        progress_callback: ProgressCallback | None,
    ) -> Iterator[LLMClientDocumentExtractionResult]:
        """Generator that submits requests to |executor| with at most
        `max_concurrency` in flight and yields results as they complete. Keeping
        the in-flight set bounded — pulling from |requests| only as slots free
        up, rather than submitting everything up front — keeps peak memory
        proportional to `max_concurrency` rather than the total request count,
        and preserves the laziness of a generator passed as |requests|.
        """
        request_iter = iter(requests)

        def submit(
            request: LLMDocumentExtractionRequest,
        ) -> Future[LLMClientDocumentExtractionResult]:
            return executor.submit(
                self._execute_with_retries,
                request=request,
                rate_limiter=rate_limiter,
                cancel_event=cancel_event,
            )

        in_flight = {
            submit(request)
            for request in itertools.islice(request_iter, self._max_concurrency)
        }
        while in_flight:
            done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
            # Refill before yielding so workers stay busy while the consumer
            # processes this batch of results.
            in_flight |= {
                submit(request) for request in itertools.islice(request_iter, len(done))
            }
            for future in done:
                result = future.result()
                if progress_callback is not None:
                    progress_callback(result)
                yield result

    def _execute_with_retries(
        self,
        *,
        request: LLMDocumentExtractionRequest,
        rate_limiter: RequestsPerSecondLimiter,
        cancel_event: threading.Event,
    ) -> LLMClientDocumentExtractionResult:
        """Executes a single request, retrying transient failures with exponential
        backoff and jitter. Returns the first non-transient result, or the last
        transient result once retries are exhausted or the run is cancelled
        mid-backoff. Every attempt (including the first) acquires a rate-limit
        slot before hitting the provider.
        """
        attempt = 0
        while True:
            rate_limiter.acquire()
            result = self._client.execute_document_extraction_request(request=request)

            if result.error_type not in _TRANSIENT_ERROR_TYPES:
                return result
            if attempt >= self._max_retries:
                return result

            # Exponential backoff with jitter, to avoid a thundering herd when
            # many concurrent requests get rate-limited at once.
            base_delay = self._retry_base_delay_seconds * (2**attempt)
            delay = base_delay * (0.5 + random.random())
            logging.warning(
                "Transient error [%s] for document [%s] (attempt %d/%d), retrying "
                "in %.1fs: %s",
                result.error_type,
                request.document_contents_id,
                attempt + 1,
                self._max_retries + 1,
                delay,
                result.error_message,
            )
            # Waiting on the cancellation event doubles as the backoff sleep: it
            # returns True immediately if the run is torn down mid-backoff, in
            # which case this result will never be consumed and we give up
            # rather than issuing another provider call.
            if cancel_event.wait(delay):
                return result
            attempt += 1
