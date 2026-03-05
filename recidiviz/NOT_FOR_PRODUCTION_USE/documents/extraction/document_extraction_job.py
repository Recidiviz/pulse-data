# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Classes for processing document extraction job results."""
import abc
import datetime
import json
import logging

import pytz

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.document_extractor_configs import (
    get_extractor,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.llm_client import (
    LLMExtractionBatchResult,
    LLMExtractionInProgressBatchStatus,
    LLMExtractionStatus,
    LLMResultReader,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_extraction_error_type import (
    DocumentExtractionErrorType,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_extraction_result_metadata import (
    DocumentExtractionResultMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_job_error_type import (
    ExtractionJobErrorType,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_job_metadata import (
    ExtractionJobMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_job_result_metadata import (
    ExtractionJobResultMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_job_submitted_document_metadata import (
    ExtractionJobSubmittedDocumentMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.llm_prompt_extractor_metadata import (
    LLMPromptExtractorMetadata,
)


class ExtractionJobResultProcessor(abc.ABC):
    """Base class for processing extraction job results."""

    @abc.abstractmethod
    def process_results(
        self,
        job: ExtractionJobMetadata,
        bq_client: BigQueryClient,
    ) -> ExtractionJobResultMetadata | None:
        """Processes the results from extraction.

        Writes per-document results to document_extraction_results table and
        job-level results to extraction_job_results table.

        Returns the ExtractionJobResultMetadata with summary statistics, or None if the
        batch is still being processed.
        """


class LLMPromptExtractionJobResultProcessor(ExtractionJobResultProcessor):
    """Processes results from LLM prompt extraction jobs."""

    def __init__(
        self,
        llm_result_reader: LLMResultReader,
        sandbox_dataset_prefix: str | None,
        extractor: LLMPromptExtractorMetadata,
    ) -> None:
        self.llm_result_reader = llm_result_reader
        self.sandbox_dataset_prefix = sandbox_dataset_prefix
        self.extractor = extractor

    def _write_extraction_results(
        self,
        results: list[DocumentExtractionResultMetadata],
        bq_client: BigQueryClient,
    ) -> None:
        """Writes extraction results to the per-extractor raw table."""
        if not results:
            return

        raw_address = DocumentExtractionResultMetadata.raw_table_address(
            state_code=self.extractor.state_code,
            collection_name=self.extractor.collection_name,
            sandbox_dataset_prefix=self.sandbox_dataset_prefix,
        )
        bq_client.stream_into_table(raw_address, [r.as_metadata_row() for r in results])

    def _query_submitted_documents(
        self,
        job: ExtractionJobMetadata,
        bq_client: BigQueryClient,
    ) -> list[ExtractionJobSubmittedDocumentMetadata]:
        """Query submitted documents from BQ in submission order."""
        address = ExtractionJobSubmittedDocumentMetadata.metadata_table_address(
            self.sandbox_dataset_prefix
        )
        query = f"""
            SELECT * FROM `{address.to_str()}`
            WHERE job_id = '{job.job_id}'
            ORDER BY job_index
        """
        rows = bq_client.run_query_async(query_str=query, use_query_cache=True)
        return [
            ExtractionJobSubmittedDocumentMetadata.from_metadata_row(dict(row))
            for row in rows
        ]

    def _process_job_level_error(
        self,
        job: ExtractionJobMetadata,
        batch_error_type: ExtractionJobErrorType,
        batch_error_message: str | None,
        submitted_docs: list[ExtractionJobSubmittedDocumentMetadata],
        bq_client: BigQueryClient,
    ) -> ExtractionJobResultMetadata:
        """Handles a job-level error by writing per-document failure rows and
        a job-level result row to BigQuery."""
        logging.error(
            "Job %s failed with error type %s: %s",
            job.job_id,
            batch_error_type.value,
            batch_error_message,
        )
        result_datetime = datetime.datetime.now(tz=pytz.UTC)

        # Write per-document failure rows so every document gets a result
        doc_error_message = f"Job-level failure: {batch_error_message}"
        per_doc_results = [
            DocumentExtractionResultMetadata(
                job_id=job.job_id,
                document_id=doc.document_id,
                extractor_id=job.extractor_id,
                extractor_version_id=job.extractor_version_id,
                extraction_datetime=result_datetime,
                status=LLMExtractionStatus.PERMANENT_FAILURE.value,
                result_json=None,
                error_type=DocumentExtractionErrorType.JOB_LEVEL_FAILURE,
                error_message=doc_error_message,
            )
            for doc in submitted_docs
        ]
        self._write_extraction_results(per_doc_results, bq_client)

        job_result = ExtractionJobResultMetadata(
            job_id=job.job_id,
            result_datetime=result_datetime,
            num_documents_originally_submitted=job.num_documents_submitted,
            num_successful_extractions=0,
            num_failed_extractions=job.num_documents_submitted,
            error_type=batch_error_type,
            error_message=batch_error_message,
        )
        # Write job result to BQ
        job_results_address = ExtractionJobResultMetadata.metadata_table_address(
            self.sandbox_dataset_prefix
        )
        bq_client.stream_into_table(job_results_address, [job_result.as_metadata_row()])
        return job_result

    def process_results(
        self,
        job: ExtractionJobMetadata,
        bq_client: BigQueryClient,
    ) -> ExtractionJobResultMetadata | None:
        """Retrieves results from LLM and writes to BigQuery.

        Returns None if the batch is still being processed.
        """
        # Query submitted documents from BQ
        submitted_docs = self._query_submitted_documents(job, bq_client)

        # Retrieve results from LLM
        logging.info("Retrieving extraction results for job %s...", job.job_id)
        batch_status = self.llm_result_reader.get_results(job, submitted_docs)

        # Handle in-progress batch
        if isinstance(batch_status, LLMExtractionInProgressBatchStatus):
            logging.info(
                "Job %s still in progress: %d/%d completed",
                job.job_id,
                batch_status.completed_count,
                batch_status.total_count,
            )
            return None

        # Handle completed batch (with or without error)
        assert isinstance(batch_status, LLMExtractionBatchResult)

        # Handle batch-level error
        if batch_status.batch_error_type is not None:
            return self._process_job_level_error(
                job,
                batch_status.batch_error_type,
                batch_status.batch_error_message,
                submitted_docs,
                bq_client,
            )

        # Process individual results
        llm_results = batch_status.results
        assert llm_results is not None

        extraction_results: list[DocumentExtractionResultMetadata] = []
        num_failed = 0
        for llm_result in llm_results:
            if llm_result.status == LLMExtractionStatus.SUCCESS:
                # Single result per document — no list expansion.
                result_data = llm_result.extracted_data or {}
                extraction_results.append(
                    DocumentExtractionResultMetadata(
                        job_id=job.job_id,
                        document_id=llm_result.document_id,
                        extractor_id=job.extractor_id,
                        extractor_version_id=job.extractor_version_id,
                        extraction_datetime=datetime.datetime.now(tz=pytz.UTC),
                        status=llm_result.status.value,
                        result_json=json.dumps(result_data),
                        error_message=None,
                        error_type=None,
                    )
                )
            else:
                num_failed += 1
                logging.warning(
                    "Extraction failed for document %s: %s",
                    llm_result.document_id,
                    llm_result.error_message,
                )
                extraction_results.append(
                    DocumentExtractionResultMetadata(
                        job_id=job.job_id,
                        document_id=llm_result.document_id,
                        extractor_id=job.extractor_id,
                        extractor_version_id=job.extractor_version_id,
                        extraction_datetime=datetime.datetime.now(tz=pytz.UTC),
                        status=llm_result.status.value,
                        result_json=None,
                        error_message=llm_result.error_message,
                        error_type=llm_result.error_type,
                    )
                )

        # Write extraction results to BQ
        if extraction_results:
            logging.info(
                "Writing %d extraction results for extractor %s",
                len(extraction_results),
                job.extractor_id,
            )
            self._write_extraction_results(extraction_results, bq_client)

        # Create job result
        job_result = ExtractionJobResultMetadata(
            job_id=job.job_id,
            result_datetime=datetime.datetime.now(tz=pytz.UTC),
            num_documents_originally_submitted=job.num_documents_submitted,
            num_successful_extractions=len(llm_results) - num_failed,
            num_failed_extractions=num_failed,
            error_type=None,
            error_message=None,
        )

        # Write job result to BQ
        job_results_address = ExtractionJobResultMetadata.metadata_table_address(
            self.sandbox_dataset_prefix
        )
        bq_client.stream_into_table(job_results_address, [job_result.as_metadata_row()])

        return job_result


class ExtractionJobResultsFinder:
    """Finds and processes pending extraction job results."""

    @classmethod
    def find_pending_jobs(
        cls, bq_client: BigQueryClient
    ) -> list[ExtractionJobMetadata]:
        """Query metadata tables to find jobs that have been started but not yet
        completed (i.e. no row in the top-level results metadata table).
        """
        raise NotImplementedError(
            "TODO(#61715): Pending job discovery not yet implemented"
        )

    @classmethod
    def get_processor_for_job(
        cls,
        job: ExtractionJobMetadata,
        llm_result_reader: LLMResultReader,
        sandbox_dataset_prefix: str | None,
    ) -> ExtractionJobResultProcessor:
        """Returns the appropriate result processor for the given job."""
        # Look up the extractor to determine its type
        extractor = get_extractor(job.extractor_id)
        if isinstance(extractor, LLMPromptExtractorMetadata):
            return LLMPromptExtractionJobResultProcessor(
                llm_result_reader,
                sandbox_dataset_prefix=sandbox_dataset_prefix,
                extractor=extractor,
            )
        raise ValueError(f"No processor for extractor type: {type(extractor)}")
