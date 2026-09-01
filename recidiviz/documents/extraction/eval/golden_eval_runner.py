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
"""Runs one extractor against its human-labeled golden eval set: reads the eval
sheet, extracts every document through the real LLM path, scores the results, and
writes the scored rows to the golden eval results table.
"""
import datetime
import re
from collections.abc import Callable, Sequence

from googleapiclient.discovery import Resource

from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.documents.extraction.eval.golden_eval_document import GoldenEvalDocument
from recidiviz.documents.extraction.eval.golden_eval_document_reader import (
    GoldenEvalDocumentReader,
)
from recidiviz.documents.extraction.eval.golden_eval_result import (
    GoldenEvalFieldScore,
    GoldenEvalResult,
)
from recidiviz.documents.extraction.eval.golden_eval_results_table import (
    GoldenEvalResultsBQTable,
)
from recidiviz.documents.extraction.eval.golden_eval_scorer import (
    LLMDocumentExtractionGoldenEvalScorer,
)
from recidiviz.documents.extraction.llm_client.sync_llm_client import SyncLLMClient
from recidiviz.documents.extraction.llm_client.types import (
    LLMClientDocumentExtractionResult,
)
from recidiviz.documents.extraction.llm_client.vertex_ai_sync_llm_client import (
    VertexAISyncLLMClient,
)
from recidiviz.documents.extraction.llm_extraction_job_manager import (
    LLMJobDocumentExtractionResult,
)
from recidiviz.documents.extraction.llm_extraction_results_persister import (
    LLMExtractionResultsPersister,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.extraction.models.llm_model_registry import LLMModelConfig
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.documents.extraction.sync_llm_document_extraction_session import (
    SyncLLMDocumentExtractionSession,
)
from recidiviz.utils.google_sheets_reader import GoogleSheetReader

GOLDEN_EVAL_JOB_ID_PREFIX = "golden_eval"
"""Prefix for the job id of a golden eval run."""

# Billing label keys attached to every request an eval run makes, for cost
# attribution.
# TODO(OBT-42711): Use formal ResourceLabel classes
_STATE_CODE_BILLING_LABEL_KEY = "state_code"
_JOB_TYPE_BILLING_LABEL_KEY = "job_type"
_MODEL_BILLING_LABEL_KEY = "model"
_REQUESTER_BILLING_LABEL_KEY = "requester"

# Value of the job-type billing label
_SYNC_JOB_TYPE_BILLING_LABEL_VALUE = "golden-eval"

# Golden eval documents hold their text in memory, so a request build does no I/O
# and needs no build parallelism.
_REQUEST_BUILD_CONCURRENCY = 1


def _sanitize_label_value(value: str) -> str:
    """Returns |value| coerced into a valid GCP label value: lowercased, with any
    character outside [a-z0-9_-] replaced by '_', truncated to GCP's 63-character
    limit."""
    sanitized = re.sub(r"[^a-z0-9_-]", "_", value.lower())
    return sanitized[:63]


def _sanitize_label_key(key: str) -> str:
    """Returns |key| coerced into a valid GCP label key. Sanitizes like a value
    but additionally enforces GCP's stricter key rules — a key must be non-empty
    and start with a lowercase letter — raising loudly rather than sending an
    invalid key that would fail every Vertex request in the run."""
    sanitized = _sanitize_label_value(key)
    if not re.fullmatch(r"[a-z][a-z0-9_-]*", sanitized):
        raise ValueError(
            f"Label key [{key}] sanitizes to [{sanitized}], which is not a valid "
            f"GCP label key (must be non-empty and start with a lowercase letter)."
        )
    return sanitized


def build_vertex_ai_sync_llm_client(model_config: LLMModelConfig) -> SyncLLMClient:
    """Returns the production synchronous LLM client for |model_config|."""
    return VertexAISyncLLMClient(model_config=model_config)


class _StrictGoldenEvalSessionDelegate:
    """Session delegate for golden eval runs, where every document must produce a
    result: a skipped or unbuildable document aborts the run instead of being
    counted and passed over.
    """

    def on_empty_document(self, *, document_contents_id: str) -> None:
        """Raises: a golden eval document must have text. Unreachable today —
        GoldenEvalDocument validates its text non-empty — but kept loud in case
        that invariant ever changes.
        """
        raise ValueError(
            f"Golden eval document [{document_contents_id}] has empty text, so no "
            f"extraction request could be built for it."
        )

    def on_document_request_build_failure(
        self, *, document_contents_id: str, error: Exception
    ) -> None:
        """Raises: every golden eval document must build a request."""
        raise ValueError(
            f"Could not build an extraction request for golden eval document "
            f"[{document_contents_id}]."
        ) from error

    def on_raw_document_extraction_result(
        self, result: LLMClientDocumentExtractionResult
    ) -> None:
        """Does nothing: an eval run reports no per-result progress."""


class GoldenEvalRunner:
    """Runs one extractor against its human-labeled golden eval set: reads documents and
    expected outputs from the golden eval sheet, runs each document through the
    extractor, scores the results by comparing actual extractor output against expected
    output, and writes the document scores to the golden eval results table.
    """

    def __init__(
        self,
        *,
        # The sandbox prefix of the tables this run writes results to.
        # TODO(OBT-33692): Give a real CI entry point its own output-table
        # strategy (e.g. a sandbox context object) so a golden eval run started
        # from CI can write somewhere other than a developer-named sandbox.
        sandbox_prefix: str,
        # Identifier for who is running this eval, attached to every LLM
        # request's billing labels for cost attribution.
        requester: str,
        # Whether to also write results to the extraction result tables. A
        # sandbox debugging aid.
        persist_processed_results: bool,
        # The Sheets API service the eval sheet is read through, or None to
        # build one from application default credentials on first read.
        sheets_service: Resource | None = None,
        # Builds the LLM client for an LLMModelConfig, which arrives with the
        # LLMExtractorConfig passed to run_eval.
        sync_llm_client_factory: Callable[
            [LLMModelConfig], SyncLLMClient
        ] = build_vertex_ai_sync_llm_client,
        # The client used to write the scored rows.
        bq_client: BigQueryClient | None = None,
        # Compares each document's actual output to its expected values.
        scorer: LLMDocumentExtractionGoldenEvalScorer | None = None,
    ) -> None:
        if not sandbox_prefix:
            raise ValueError("sandbox_prefix must be non-empty.")
        if not requester:
            raise ValueError("requester must be non-empty.")

        self.sandbox_prefix = sandbox_prefix
        self.requester = requester
        self.persist_processed_results = persist_processed_results
        self.sheets_service = sheets_service
        self.sync_llm_client_factory = sync_llm_client_factory
        self.bq_client = bq_client or BigQueryClientImpl()
        self.scorer = scorer or LLMDocumentExtractionGoldenEvalScorer()
        # Writes the processed results to the extraction result tables when
        # persist_processed_results is set.
        self.persister = LLMExtractionResultsPersister(
            sandbox_prefix=self.sandbox_prefix, bq_client=self.bq_client
        )

    def run_eval(self, *, config: LLMExtractorConfig) -> GoldenEvalResult:
        """Returns the scored result of evaluating |config| against its golden eval
        set, having written one row per scored comparison to the golden eval results
        table.
        """
        # One timestamp for the whole run: the synthetic job id and every scored
        # row share it, so all of a run's rows are keyed identically.
        run_datetime_utc = datetime.datetime.now(tz=datetime.UTC)
        job_id = self.build_run_job_id(config=config, run_datetime_utc=run_datetime_utc)

        documents = self._read_golden_eval_documents(config=config)

        session = SyncLLMDocumentExtractionSession(
            config=config,
            job_id=job_id,
            billing_labels=self.billing_labels(config=config),
            sync_client=self.sync_llm_client_factory(config.model_config),
            delegate=_StrictGoldenEvalSessionDelegate(),
            # Golden eval documents have no numbered entries to check.
            expected_entry_nums_source=None,
            request_build_concurrency=_REQUEST_BUILD_CONCURRENCY,
        )
        # TODO(OBT-45749): Add an optional max-retries flag (default 0; never
        # set by CI — a hand-picked case the model misses must surface as a
        # failure) that re-calls session.extract, before finish_session, with
        # the documents whose results classified as validation-failure
        # transient. Production retries those across DAG runs, so the flag is a
        # prompt-iteration diagnostic for whether a miss would converge there.
        # Request-failure transients are excluded — the request runner already
        # retries those in-run.
        processed_results_by_document_id = {
            result.document_contents_id: result
            for result in session.extract(documents=documents)
        }
        session_summary = session.finish_session()

        if self.persist_processed_results:
            # Written before scoring, so a scoring bug still leaves the raw and
            # validated results behind to debug from.
            self.persister.persist_results(
                config=config, results=list(processed_results_by_document_id.values())
            )

        field_scores = self.scorer.score(
            output_schema=config.extractor_collection.output_schema,
            documents=documents,
            actual_output_values_by_document_id={
                document_id: self._actual_output_values(result=result)
                for document_id, result in processed_results_by_document_id.items()
            },
        )
        self._write_field_scores(
            config=config,
            run_datetime_utc=run_datetime_utc,
            field_scores=field_scores,
        )

        return GoldenEvalResult(
            field_scores=field_scores,
            actual_llm_result_type_by_document_id={
                document_id: result.result_type
                for document_id, result in processed_results_by_document_id.items()
            },
            total_token_counts=session_summary.token_counts,
        )

    @classmethod
    def build_run_job_id(
        cls, *, config: LLMExtractorConfig, run_datetime_utc: datetime.datetime
    ) -> str:
        """Returns the job id for a golden eval run, which includes the extractor
        version id and timestamp.
        """
        return (
            f"{GOLDEN_EVAL_JOB_ID_PREFIX}_{config.extractor_version_id}_"
            f"{run_datetime_utc.strftime('%Y%m%dT%H%M%S%f')}"
        )

    def billing_labels(self, *, config: LLMExtractorConfig) -> dict[str, str]:
        """Returns the billing labels attached to every LLM request this run
        makes, for cost attribution. Keys and values are sanitized to satisfy
        GCP's label character restrictions.
        """
        labels = {
            _STATE_CODE_BILLING_LABEL_KEY: config.state_code.value,
            _JOB_TYPE_BILLING_LABEL_KEY: _SYNC_JOB_TYPE_BILLING_LABEL_VALUE,
            _MODEL_BILLING_LABEL_KEY: config.model_config.name,
            _REQUESTER_BILLING_LABEL_KEY: self.requester,
        }
        return {
            _sanitize_label_key(key): _sanitize_label_value(value)
            for key, value in labels.items()
        }

    def _read_golden_eval_documents(
        self, *, config: LLMExtractorConfig
    ) -> list[GoldenEvalDocument]:
        """Returns every golden eval document |config| declares, having reported
        every problem in the eval set at once.
        """
        sheets_reader = (
            GoogleSheetReader(sheets_service=self.sheets_service)
            if self.sheets_service is not None
            else GoogleSheetReader.from_application_default_credentials()
        )
        return GoldenEvalDocumentReader.from_config(
            config=config, sheets_reader=sheets_reader
        ).load_all_documents()

    @staticmethod
    def _actual_output_values(
        *, result: LLMJobDocumentExtractionResult
    ) -> LLMRequestOutputValues | None:
        """Returns the validated output the scorer compares against |result|'s
        expected values, or None when the request failed or validation left no
        usable content — the scorer scores every expected field as a miss.
        """
        if result.validation_results is None:
            return None
        return result.validation_results.validated_output

    def _write_field_scores(
        self,
        *,
        config: LLMExtractorConfig,
        run_datetime_utc: datetime.datetime,
        field_scores: Sequence[GoldenEvalFieldScore],
    ) -> None:
        """Writes field scores to a table, one per row, stamping each with the run-level
        values a score does not carry.
        """
        rows = [
            GoldenEvalResultsBQTable.to_row(
                state_code=config.state_code,
                extractor_id=config.extractor_id,
                extractor_version_id=config.extractor_version_id,
                output_schema_version=config.extractor_collection.output_schema_version,
                run_datetime_utc=run_datetime_utc,
                golden_document_id=score.golden_document_id,
                test_type=score.test_type,
                test_case=score.test_case,
                field_name=score.field_name,
                element_index=score.element_index,
                expected=score.expected_value,
                actual=score.actual_value,
                is_correct=score.is_correct,
            )
            for score in field_scores
        ]
        # TODO(OBT-33692): This writes through a streaming insert, whose rows can
        # sit in the streaming buffer and be invisible to a query for minutes. A
        # caller that reads the golden eval results table right after run_eval
        # returns can see a short or empty table even though the run reported
        # success. Moving to load jobs (which commit atomically) is the real
        # fix; see the same tradeoff on LLMExtractionResultsPersister.
        self.bq_client.stream_into_table(
            address=GoldenEvalResultsBQTable.address(
                collection_name=config.extractor_collection.name,
                sandbox_prefix=self.sandbox_prefix,
            ),
            rows=rows,
        )
