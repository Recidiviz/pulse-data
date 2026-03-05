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
"""Interface and data classes for LLM clients used in document extraction.

The LLM extraction workflow is split into two phases:
1. Submission: LLMClient.submit_extraction_job() orchestrates the full submission,
   calling submit_batch() to send requests to the LLM
2. Result retrieval: LLMResultReader.get_results() retrieves results for a batch

For real LLM services, there may be a delay between submission and result availability.
For testing, FakeLLMClient generates results immediately but stores them for later retrieval.
"""
import abc
import collections
import datetime
import enum
import logging
from typing import Any

import attr
import pytz

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.document_extractor_configs import (
    get_extractor_collection,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.document_provider import (
    GCSDocument,
    GCSDocumentProvider,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.extraction_output_schema import (
    ExtractionOutputSchema,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_extraction_error_type import (
    DocumentExtractionErrorType,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_extractor_collection_metadata import (
    DocumentExtractorCollectionMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_job_error_type import (
    ExtractionJobErrorType,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_job_metadata import (
    ExtractionJobMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_job_submitted_document_metadata import (
    ExtractionJobSubmittedDocumentMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.llm_prompt_extractor_metadata import (
    LLMPromptExtractorMetadata,
)


@attr.define
class LLMExtractionRequest:
    """A request to extract structured data from a document using an LLM."""

    extractor: LLMPromptExtractorMetadata
    document: GCSDocument

    @property
    def llm_provider(self) -> str:
        return self.extractor.llm_provider

    @property
    def model(self) -> str:
        return self.extractor.model

    @property
    def output_schema(self) -> ExtractionOutputSchema:
        return get_extractor_collection(self.extractor.collection_name).output_schema

    def build_messages(self) -> list[dict[str, str]]:
        """Build the messages array for submission to the LLM."""
        # TODO(#61702): How to handle read errors?
        with self.document.open() as f:
            document_content = f.read()

        return [
            # Using system role for instructions enables prompt caching across
            # requests - the system message prefix can be cached while only the
            # user message (document content) varies per request.
            {"role": "system", "content": self.extractor.instructions_prompt},
            {"role": "user", "content": document_content},
        ]


class LLMExtractionStatus(enum.Enum):
    """Status of an LLM extraction result."""

    SUCCESS = "SUCCESS"
    TRANSIENT_FAILURE = "TRANSIENT_FAILURE"
    PERMANENT_FAILURE = "PERMANENT_FAILURE"


@attr.define
class LLMExtractionResult:
    """The result of an LLM extraction request."""

    # The document ID that was processed
    document_id: str

    # The status of the extraction
    status: LLMExtractionStatus

    # The extracted data as a single dictionary (one result per document).
    # None if status is not SUCCESS.
    extracted_data: dict[str, Any] | None

    # Error message if status is not SUCCESS
    error_message: str | None

    # Structured error type if status is not SUCCESS
    error_type: DocumentExtractionErrorType | None

    def __attrs_post_init__(self) -> None:
        if self.status == LLMExtractionStatus.SUCCESS:
            if self.extracted_data is None:
                raise ValueError("extracted_data must be set when status is SUCCESS")
            if self.error_type is not None or self.error_message is not None:
                raise ValueError(
                    "error_type and error_message must be None when status is SUCCESS"
                )
        else:
            if self.extracted_data is not None:
                raise ValueError(
                    "extracted_data must be None when status is not SUCCESS"
                )
            if self.error_type is None or self.error_message is None:
                raise ValueError(
                    "error_type and error_message must both be set when status is not SUCCESS"
                )


class BatchNotFoundError(ValueError):
    """Raised when attempting to retrieve results for a batch that doesn't exist."""

    def __init__(self, job_id: str) -> None:
        super().__init__(f"Batch for job {job_id} not found")
        self.job_id = job_id


@attr.define
class LLMExtractionInProgressBatchStatus:
    """Status returned when a batch is still being processed."""

    # The job ID returned by submit_batch()
    job_id: str

    # Number of requests that have completed processing
    completed_count: int

    # Total number of requests in the batch
    total_count: int


@attr.define
class LLMExtractionBatchResult:
    """Final result of a completed batch extraction."""

    # The job ID returned by submit_batch()
    job_id: str

    # The extraction results for each document. None if batch_error_type is set.
    results: list[LLMExtractionResult] | None

    # Structured error type if the entire batch failed
    batch_error_type: ExtractionJobErrorType | None

    # Error message if the entire batch failed (e.g., GCS read error)
    batch_error_message: str | None

    def __attrs_post_init__(self) -> None:
        if (self.results is None) == (self.batch_error_type is None):
            raise ValueError("Exactly one of results or batch_error_type must be set")
        if self.batch_error_type is not None and self.batch_error_message is None:
            raise ValueError(
                "batch_error_message must be set when batch_error_type is set"
            )
        if self.results is not None and self.batch_error_message is not None:
            raise ValueError(
                "batch_error_message must be None when results are present"
            )


def _validate_no_duplicate_document_ids(documents: list[GCSDocument]) -> None:
    """Validates that no document IDs are duplicated within the input list.

    Raises ValueError if any document_ids appear more than once.
    """
    doc_id_counts = collections.Counter(doc.document_id for doc in documents)
    duplicate_ids = [doc_id for doc_id, count in doc_id_counts.items() if count > 1]
    if duplicate_ids:
        sample = duplicate_ids[:10]
        raise ValueError(
            f"Found {len(duplicate_ids)} duplicate document_id(s) in input: "
            f"{sample}"
            + (
                f" (and {len(duplicate_ids) - 10} more)"
                if len(duplicate_ids) > 10
                else ""
            )
        )


def _filter_already_submitted_documents(
    documents: list[GCSDocument],
    extractor: LLMPromptExtractorMetadata,
    bq_client: BigQueryClient,
    sandbox_dataset_prefix: str | None,
) -> list[GCSDocument]:
    """Filters out documents that have already been submitted for this extractor version.

    Returns the subset of documents that have NOT yet been submitted.
    """
    submitted_docs_address = (
        ExtractionJobSubmittedDocumentMetadata.metadata_table_address(
            sandbox_dataset_prefix
        )
    )
    jobs_address = ExtractionJobMetadata.metadata_table_address(sandbox_dataset_prefix)
    submitted_docs_full = submitted_docs_address.to_project_specific_address(
        bq_client.project_id
    )
    jobs_full = jobs_address.to_project_specific_address(bq_client.project_id)
    already_submitted_query = f"""
        SELECT DISTINCT sd.document_id
        FROM `{submitted_docs_full.to_str()}` sd
        JOIN `{jobs_full.to_str()}` j
            ON sd.job_id = j.job_id
        WHERE j.extractor_version_id = '{extractor.extractor_version_id()}'
    """
    already_submitted_results = bq_client.run_query_async(
        query_str=already_submitted_query,
        use_query_cache=True,
    )
    already_submitted_doc_ids = {
        row["document_id"] for row in already_submitted_results
    }

    if already_submitted_doc_ids:
        new_documents = [
            doc for doc in documents if doc.document_id not in already_submitted_doc_ids
        ]
        skipped = len(documents) - len(new_documents)
        logging.info(
            "Skipping %d document(s) already submitted with extractor version '%s'",
            skipped,
            extractor.extractor_version_id(),
        )
        return new_documents

    return documents


class LLMClient(abc.ABC):
    """Abstract interface for submitting extraction requests to an LLM service."""

    @abc.abstractmethod
    def submit_batch(self, requests: list[LLMExtractionRequest]) -> str:
        """Submits a batch of extraction requests to the LLM service.

        Args:
            requests: The extraction requests to submit.

        Returns:
            The job_id for tracking this batch.
        """

    def submit_extraction_job(
        self,
        bq_client: BigQueryClient,
        extractor: LLMPromptExtractorMetadata,
        document_provider: GCSDocumentProvider,
        sandbox_dataset_prefix: str | None = None,
    ) -> ExtractionJobMetadata | None:
        """Submits an extraction job and writes metadata to BigQuery.

        This is a concrete method that orchestrates the full job submission:
        1. Queries for documents to process via document_provider
        2. Writes extractor collection metadata to BQ
        3. Writes extractor metadata to BQ
        4. Builds LLMExtractionRequests
        5. Calls self.submit_batch() (abstract, implemented by subclass)
        6. Creates ExtractionJob with all fields populated
        7. Writes job metadata to BQ
        8. Writes submitted documents to BQ

        Args:
            bq_client: BigQuery client for writing metadata.
            extractor: The LLMPromptExtractor to use for extraction.
            document_provider: Provider for documents to process. Contains the BQ
                query for document IDs, state code, and optional sandbox bucket.
            sandbox_dataset_prefix: If set, prefixes the default dataset IDs
                for extraction metadata tables with this sandbox prefix.

        Returns:
            ExtractionJob if documents were found and submitted, None otherwise.
        """
        # Write extractor collection metadata
        collection = get_extractor_collection(extractor.collection_name)
        collection_address = DocumentExtractorCollectionMetadata.metadata_table_address(
            sandbox_dataset_prefix
        )
        bq_client.stream_into_table(collection_address, [collection.as_metadata_row()])
        logging.info(
            "Wrote collection metadata for '%s' to %s",
            collection.name,
            collection_address.to_str(),
        )

        # Write extractor metadata
        extractor_metadata_address = extractor.metadata_table_address(
            sandbox_dataset_prefix
        )
        bq_client.stream_into_table(
            extractor_metadata_address, [extractor.as_metadata_row()]
        )
        logging.info(
            "Wrote extractor metadata for '%s' to %s",
            extractor.extractor_id,
            extractor_metadata_address.to_str(),
        )

        # Get documents from provider
        logging.info("Querying for documents to process...")
        documents = list(document_provider.document_iterator(bq_client))
        logging.info("Found %d documents to process", len(documents))

        if not documents:
            logging.warning("No documents found to process")
            return None

        _validate_no_duplicate_document_ids(documents)
        documents = _filter_already_submitted_documents(
            documents, extractor, bq_client, sandbox_dataset_prefix
        )

        if not documents:
            logging.warning(
                "All documents have already been submitted for extractor version '%s'",
                extractor.extractor_version_id(),
            )
            return None

        # Build extraction requests and submit to LLM
        logging.info("Submitting %d documents for extraction...", len(documents))
        requests = [
            LLMExtractionRequest(extractor=extractor, document=doc) for doc in documents
        ]

        # Capture start time right before submission
        start_datetime = datetime.datetime.now(tz=pytz.UTC)

        # submit_batch returns the job_id
        job_id = self.submit_batch(requests)
        logging.info("Job submitted with ID: %s", job_id)

        # Create the ExtractionJobMetadata with flat fields
        job = ExtractionJobMetadata(
            job_id=job_id,
            start_datetime=start_datetime,
            extractor_id=extractor.extractor_id,
            extractor_version_id=extractor.extractor_version_id(),
            state_code=document_provider.state_code,
            num_documents_submitted=len(documents),
        )

        # Write job metadata
        jobs_address = ExtractionJobMetadata.metadata_table_address(
            sandbox_dataset_prefix
        )
        bq_client.stream_into_table(jobs_address, [job.as_metadata_row()])

        # Write submitted documents
        submitted_address = (
            ExtractionJobSubmittedDocumentMetadata.metadata_table_address(
                sandbox_dataset_prefix
            )
        )
        job_submitted_documents = [
            {
                "job_id": job.job_id,
                "document_id": doc.document_id,
                "submitted_datetime": start_datetime,
                "job_index": idx,
                "gcs_uri": doc.gcs_file_path.uri(),
                "mime_type": doc.mime_type,
            }
            for idx, doc in enumerate(documents)
        ]
        bq_client.stream_into_table(submitted_address, job_submitted_documents)
        logging.info(
            "Wrote %d submitted document records to %s",
            len(job_submitted_documents),
            submitted_address.to_str(),
        )

        return job


class LLMResultReader(abc.ABC):
    """Abstract interface for reading extraction results from an LLM service."""

    @abc.abstractmethod
    def get_results(
        self,
        extraction_job: ExtractionJobMetadata,
        submitted_documents: list[ExtractionJobSubmittedDocumentMetadata],
    ) -> LLMExtractionBatchResult | LLMExtractionInProgressBatchStatus:
        """Retrieves the results for a previously submitted extraction job.

        Args:
            extraction_job: The ExtractionJob (read from BQ). Provides job_id
                and config.
            submitted_documents: Documents submitted with this job (read from BQ).
                Must be ordered by job_index. Used for positional correlation
                with batch results.

        Returns:
            Either LLMExtractionBatchResult if the batch is complete (or failed),
            or LLMExtractionInProgressBatchStatus if still processing.

        Raises:
            BatchNotFoundError: If the job_id is not found.
        """
