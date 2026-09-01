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
"""The core extraction loop for a single-process caller that runs synchronous LLM
extraction over a collection of documents and accumulates the results for downstream
use.
"""
from collections.abc import Iterator, Sequence
from typing import Protocol

import attr

from recidiviz.common import attr_validators
from recidiviz.documents.extraction.document_text_source import DocumentTextSource
from recidiviz.documents.extraction.expected_entry_nums_source import (
    ExpectedEntryNumsSource,
)
from recidiviz.documents.extraction.llm_client.llm_document_extraction_request_builder import (
    LLMDocumentExtractionRequestBuilder,
)
from recidiviz.documents.extraction.llm_client.sync_llm_client import SyncLLMClient
from recidiviz.documents.extraction.llm_client.sync_llm_document_extraction_request_runner import (
    SyncLLMDocumentExtractionRequestRunner,
)
from recidiviz.documents.extraction.llm_client.types import (
    LLMClientDocumentExtractionResult,
    LLMDocumentExtractionRequest,
    LLMDocumentExtractionTokenCounts,
)
from recidiviz.documents.extraction.llm_extraction_job_manager import (
    LLMJobDocumentExtractionResult,
)
from recidiviz.documents.extraction.llm_extraction_result_processor import (
    LLMExtractionResultProcessor,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.extraction.validation.llm_extraction_result_validator import (
    LLMExtractionResultValidator,
)
from recidiviz.utils.future_executor import map_with_bounded_concurrency


class SyncLLMDocumentExtractionSessionDelegate(Protocol):
    """Receives the session's per-document events. Implementations decide whether
    an event is survivable (count it, log it, and continue) or fatal (raise).
    """

    def on_empty_document(self, *, document_contents_id: str) -> None:
        """Called when a document's text is empty, so no request was built for it.
        The session skips the document when this returns.
        """

    def on_document_request_build_failure(
        self, *, document_contents_id: str, error: Exception
    ) -> None:
        """Called when a document's request could not be built (e.g. its text was
        not found). The session skips the document when this returns.
        """

    def on_raw_document_extraction_result(
        self, result: LLMClientDocumentExtractionResult
    ) -> None:
        """Called with each raw result as it completes, before classification,
        serially from the thread consuming the results — a progress hook.
        """


@attr.define(kw_only=True)
class SyncLLMDocumentExtractionSessionSummary:
    """The per-document outcome counts and token usage one extraction session
    accumulates as results complete, for the caller to display once the session
    finishes.
    """

    processed: int = attr.ib(default=0, validator=attr_validators.is_non_negative_int)
    """Documents that reached the LLM and were classified (success or failure)."""

    succeeded: int = attr.ib(default=0, validator=attr_validators.is_non_negative_int)
    """Processed documents that extracted and validated cleanly."""

    failed_llm_request: int = attr.ib(
        default=0, validator=attr_validators.is_non_negative_int
    )
    """Processed documents whose LLM request itself failed (timeout, rate limit,
    server error, content filter, malformed/empty response) — no result JSON came
    back to validate."""

    failed_validation: int = attr.ib(
        default=0, validator=attr_validators.is_non_negative_int
    )
    """Processed documents whose LLM request returned a result that then failed
    validation."""

    skipped_empty: int = attr.ib(
        default=0, validator=attr_validators.is_non_negative_int
    )
    """Documents skipped before the LLM because their text was empty."""

    failed_to_build: int = attr.ib(
        default=0, validator=attr_validators.is_non_negative_int
    )
    """Documents that could not be assembled into a request (e.g. missing GCS
    text)."""

    token_counts: LLMDocumentExtractionTokenCounts = attr.ib(
        factory=LLMDocumentExtractionTokenCounts.empty,
        validator=attr.validators.instance_of(LLMDocumentExtractionTokenCounts),
    )
    """The run's total token usage across every processed document."""


@attr.define(kw_only=True)
class SyncLLMDocumentExtractionSession:
    """The core extraction loop for a single-process caller that runs synchronous LLM
    extraction over a collection of documents and accumulates the results for downstream
    use.

    One session builds one request per document with bounded concurrency, runs the requests
    through the LLM, classifies each raw result, and folds every outcome into the session
    summary, yielding each processed result as it completes.

    Lifecycle: `extract` may run any number of document batches, all accumulating
    into one summary; `finish_session` then returns that summary and closes the
    session, so any further `extract` or `finish_session` call raises.
    """

    config: LLMExtractorConfig = attr.ib(
        validator=attr.validators.instance_of(LLMExtractorConfig)
    )
    """The narrowed extractor config every stage reads from."""

    job_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The extraction job id stamped on every processed result."""

    billing_labels: dict[str, str] = attr.ib(
        validator=attr_validators.is_dict_of(str, str)
    )
    """Billing labels attached to every LLM request this session makes, for cost
    attribution."""

    sync_client: SyncLLMClient = attr.ib(
        # SyncLLMClient is abstract; mypy flags it where a concrete type is
        # expected, but instance_of accepts an ABC fine at runtime.
        validator=attr.validators.instance_of(SyncLLMClient)  # type: ignore[type-abstract]
    )
    """Client that executes each extraction request against the provider — the
    production Vertex AI client, or a fake in tests."""

    delegate: SyncLLMDocumentExtractionSessionDelegate = attr.ib()
    """Receives the per-document events; decides survivable versus fatal."""

    expected_entry_nums_source: ExpectedEntryNumsSource | None = attr.ib()
    """Source of the complete entry set of each composite document, for an
    entity-resolution extractor — or None for a first-order extractor, whose
    documents have no numbered entries.

    TODO(OBT-47158) The current only implementation of this protocol,
    InMemoryExpectedEntryNumsSource, requires preloading the ER collection's
    entire entry→source map table into memory. This is fine at sandbox scale,
    but the map could grow without bound in production. When we start using
    SyncLLMDocumentExtractionSession in prod, we will need a dynamic version
    that scopes the query to the job's pending documents. If we find that we
    need the production ExpectedEntryNumsSource to dynamically fetch the
    mapping for batches of documents, we may need to move the entry num mapping
    lookup from the classify step onto the request-build pool so fetches
    overlap under its bounded concurrency.
    """

    request_build_concurrency: int = attr.ib(validator=attr_validators.is_positive_int)
    """How many requests to build (fetching their document text) concurrently."""

    _request_builder: LLMDocumentExtractionRequestBuilder = attr.ib(init=False)
    """Assembles the per-document extraction request the same way the pipeline
    does, from the config's prompt, output schema, and model parameters."""

    @_request_builder.default
    def _build_request_builder(self) -> LLMDocumentExtractionRequestBuilder:
        """Returns the request builder for this session's config and billing
        labels."""
        return LLMDocumentExtractionRequestBuilder.for_config(
            config=self.config, billing_labels=self.billing_labels
        )

    _request_runner: SyncLLMDocumentExtractionRequestRunner = attr.ib(init=False)
    """Executes the requests against the provider with bounded concurrency and
    transient-error retries."""

    @_request_runner.default
    def _build_request_runner(self) -> SyncLLMDocumentExtractionRequestRunner:
        """Returns the request runner wrapping this session's LLM client."""
        return SyncLLMDocumentExtractionRequestRunner(client=self.sync_client)

    _result_processor: LLMExtractionResultProcessor = attr.ib(
        factory=lambda: LLMExtractionResultProcessor(
            validator=LLMExtractionResultValidator()
        ),
        init=False,
    )
    """Classifies and validates each raw result the way the pipeline would
    persist it."""

    _summary: SyncLLMDocumentExtractionSessionSummary | None = attr.ib(
        factory=SyncLLMDocumentExtractionSessionSummary,
        init=False,
        validator=attr_validators.is_opt(SyncLLMDocumentExtractionSessionSummary),
    )
    """The session's accumulating summary, or None once the session is finished."""

    def __attrs_post_init__(self) -> None:
        if (
            self.config.entity_group is not None
            and self.expected_entry_nums_source is None
        ):
            raise ValueError(
                f"Extractor [{self.config.extractor_id}] is an entity-resolution "
                f"extractor, so an expected_entry_nums_source must be provided."
            )
        if (
            self.config.entity_group is None
            and self.expected_entry_nums_source is not None
        ):
            raise ValueError(
                f"Extractor [{self.config.extractor_id}] is a first-order "
                f"extractor, so expected_entry_nums_source must be None — its "
                f"documents have no numbered entries."
            )

    def extract(
        self, *, documents: Sequence[DocumentTextSource]
    ) -> Iterator[LLMJobDocumentExtractionResult]:
        """Runs every document through the LLM and yields each processed result as
        it completes (not in document order). Raises when the results do not pair
        one-to-one with the requests the run issued, or when the session is
        already finished.
        """
        self._active_summary()

        # Each buildable document's source text, keyed by document_contents_id.
        # The request generator writes an entry as it builds each request and the
        # classify step pops it once the result comes back, so this holds only the
        # in-flight window's worth of text (bounded by the runner's concurrency),
        # not every document's text at once.
        source_text_by_document: dict[str, str] = {}

        with self._request_runner.execute_document_extraction_requests(
            requests=self._iter_requests(
                documents=documents,
                source_text_by_document=source_text_by_document,
            ),
        ) as results:
            for raw_result in results:
                self.delegate.on_raw_document_extraction_result(raw_result)
                document_contents_id = raw_result.document_contents_id
                if document_contents_id not in source_text_by_document:
                    raise ValueError(
                        f"Extraction run [{self.job_id}] of extractor "
                        f"[{self.config.extractor_id}] got a result for document "
                        f"[{document_contents_id}] with no matching in-flight "
                        f"request — the document was never requested, or its "
                        f"result already arrived."
                    )
                # The document is done once its result is classified, so popping
                # its source text here bounds the map to the in-flight window
                # rather than the whole run.
                yield self._classify_result(
                    raw_result=raw_result,
                    source_document_text=source_text_by_document.pop(
                        document_contents_id
                    ),
                )
        if source_text_by_document:
            raise ValueError(
                f"Extraction run [{self.job_id}] of extractor "
                f"[{self.config.extractor_id}] got no results for requested "
                f"document(s) {sorted(source_text_by_document)}."
            )

    def finish_session(self) -> SyncLLMDocumentExtractionSessionSummary:
        """Returns the summary accumulated across every `extract` call and closes
        the session: any further `extract` or `finish_session` call raises.
        """
        summary = self._active_summary()
        self._summary = None
        return summary

    def _active_summary(self) -> SyncLLMDocumentExtractionSessionSummary:
        """Returns the session's accumulating summary.

        Raises when the session is already finished, so no processing can slip in
        after `finish_session` handed the summary to the caller.
        """
        if self._summary is None:
            raise ValueError(
                f"Extraction session for job [{self.job_id}] is already finished; "
                f"no further extraction can run in it."
            )
        return self._summary

    def _iter_requests(
        self,
        *,
        documents: Sequence[DocumentTextSource],
        source_text_by_document: dict[str, str],
    ) -> Iterator[LLMDocumentExtractionRequest]:
        """Yields one extraction request per buildable document, lazily, recording
        each document's source text in |source_text_by_document| for the classify
        step to read back.

        Builds requests on a small thread pool rather than inline. A build can do
        I/O to fetch its document's text (e.g. two sequential HTTP round trips to
        GCS), and this generator is consumed on the thread driving the request
        runner — so building inline serialized every fetch and starved the runner,
        holding in-flight LLM requests far below its max_concurrency. Building on
        a pool overlaps those fetches with each other and with the LLM calls.

        The summary counters mutated below are only ever touched from this thread
        (the one consuming the completed builds), not from the build pool.
        """
        with map_with_bounded_concurrency(
            work_fn=lambda document: self._request_builder.build_request(
                document=document
            ),
            items=documents,
            max_concurrency=self.request_build_concurrency,
        ) as completed_builds:
            for completed in completed_builds:
                document_contents_id = completed.item.document_contents_id
                try:
                    request = completed.result
                except Exception as e:  # pylint: disable=broad-except
                    # Any failure building one document's request — the expected
                    # LLMDocumentExtractionRequestError, or an unexpected error
                    # like a transient GCS read failure — goes to the delegate,
                    # which decides whether it is survivable. A systematic build
                    # bug still surfaces loudly as every document landing in
                    # failed_to_build.
                    self.delegate.on_document_request_build_failure(
                        document_contents_id=document_contents_id, error=e
                    )
                    self._active_summary().failed_to_build += 1
                    continue
                if request is None:
                    # TODO(OBT-42807) An empty-text document gets no terminal
                    # result, so every run re-selects it forever. Give it a
                    # terminal result through the classify/persist path instead
                    # of this skip event.
                    self.delegate.on_empty_document(
                        document_contents_id=document_contents_id
                    )
                    self._active_summary().skipped_empty += 1
                    continue
                source_text_by_document[document_contents_id] = request.document_text
                yield request

    def _classify_result(
        self,
        *,
        raw_result: LLMClientDocumentExtractionResult,
        source_document_text: str,
    ) -> LLMJobDocumentExtractionResult:
        """Returns the processed result for one raw extraction result, classified
        and validated, and folds its counts and token usage into the session
        summary.
        """
        result = self._result_processor.validate_and_classify(
            config=self.config,
            raw_result=raw_result,
            job_id=self.job_id,
            source_document_text=source_document_text,
            expected_entry_nums=(
                None
                if self.expected_entry_nums_source is None
                else self.expected_entry_nums_source.get_expected_entry_nums(
                    document_contents_id=raw_result.document_contents_id
                )
            ),
            # TODO(OBT-41779) prior_transient_failure_count is hardcoded to 0, so
            # escalation to RETRIES_EXHAUSTED never happens across runs and a
            # transient-failed document is re-selected forever. The session should
            # accept caller-supplied per-document prior counts; the Airflow caller
            # reads them from Postgres in one batched query.
            prior_transient_failure_count=0,
        )

        summary = self._active_summary()
        summary.processed += 1
        if result.is_validated_result:
            summary.succeeded += 1
        elif result.raw_result.is_error_result:
            # The LLM request itself failed, so no result JSON reached the
            # validator.
            summary.failed_llm_request += 1
        else:
            # The request returned a result that then failed validation.
            summary.failed_validation += 1

        summary.token_counts = LLMDocumentExtractionTokenCounts.sum(
            [summary.token_counts, result.raw_result.token_counts]
        )
        return result
