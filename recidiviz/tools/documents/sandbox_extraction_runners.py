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
"""Runners for sandbox extraction script"""

import logging
from datetime import datetime, timezone

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extraction_eligible_document_query_builder import (
    LLMExtractionEligibleDocumentQueryBuilder,
)
from recidiviz.documents.extraction.llm_extraction_job_generator import (
    LLMExtractionJobGenerator,
)
from recidiviz.documents.extraction.llm_extraction_job_manager import (
    LLMExtractionJobManager,
)
from recidiviz.documents.extraction.llm_extractor_metadata_manager import (
    LLMExtractorMetadataManager,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
)
from recidiviz.documents.store.document_store_sandbox_context import (
    DocumentStoreSandboxContext,
)
from recidiviz.documents.store.document_upload_batching import build_document_batches
from recidiviz.documents.store.gcs_document_uploader import GcsDocumentUploader
from recidiviz.documents.store.new_document_discovery import NewDocumentDiscoverer
from recidiviz.documents.store.record_document_upload_results import (
    DocumentUploadResultRecorder,
)
from recidiviz.tools.documents.sandbox_document_extraction_processor import (
    DocumentExtractionProcessor,
    SandboxExtractionSummary,
)

# Refuse to start a run whose job holds more than this many documents.
# TODO(OBT-42789) decrease this in the future since this script is intended
# for bounded sandbox runs, not large-scale production runs.
MAX_DOCUMENTS = 500_000


class SandboxDocumentStoreRunner:
    """Generates and uploads one collection's documents into the sandbox document
    store so the extraction has sandbox input to read.
    """

    def __init__(
        self,
        *,
        # The collection whose documents are generated and uploaded.
        document_collection: DocumentCollectionConfig,
        # Maps the collection (and, for an ER collection, the first-order source
        # collection) to where its documents are read and written.
        document_store_sandbox: DocumentStoreSandboxContext,
        # Client for the discovery, batching, and recording queries.
        bq_client: BigQueryClient,
        # Filesystem the uploaded document text is written to.
        fs: GCSFileSystem,
        # Identifier tying the discovery, upload, and recording steps to one run.
        run_id: str,
    ) -> None:
        self.document_collection = document_collection
        self.document_store_sandbox = document_store_sandbox
        self.bq_client = bq_client
        self.fs = fs
        self.run_id = run_id

    def run(self) -> None:
        """Runs discovery → batching → GCS upload → recording for the collection.
        Discovery diffs the generated documents against wherever the document store
        sandbox maps the collection to reading."""
        output_prefix = self.document_store_sandbox.output_prefix_for_writing(
            self.document_collection.name
        )
        logging.info(
            "Generating and uploading documents for collection [%s] into the sandbox "
            "document store under prefix [%s].",
            self.document_collection.name,
            output_prefix,
        )
        # One coherent timestamp for both the upload status rows and the metadata
        # rows they gate.
        upload_datetime = datetime.now(tz=timezone.utc)

        discovery_result = NewDocumentDiscoverer(
            state_code=self.document_collection.state_code,
            collection_name=self.document_collection.name,
            project_id=self.bq_client.project_id,
            big_query_client=self.bq_client,
            run_id=self.run_id,
            sandbox_context=self.document_store_sandbox,
        ).run()

        if discovery_result is None:
            logging.info(
                "No documents discovered for collection [%s]; nothing to upload.",
                self.document_collection.name,
            )
            return

        # Discovery can return a result with metadata updates (added/updated/deleted
        # rows) but no new document contents to upload — e.g. a metadata-only change,
        # or documents whose text was already uploaded on a prior run. Only the GCS
        # upload is skipped in that case; the recorder still runs to record the
        # metadata rows (it skips only the upload-status CSV load internally).
        if discovery_result.num_new_document_contents_rows:
            task_batches = build_document_batches(
                collection_result=discovery_result,
                num_upload_task_instances=1,
                big_query_client=self.bq_client,
            )
            GcsDocumentUploader(
                state_code=self.document_collection.state_code,
                project_id=self.bq_client.project_id,
                big_query_client=self.bq_client,
                fs=self.fs,
                run_id=self.run_id,
                task_index=0,
                upload_datetime=upload_datetime,
                sandbox_prefix=output_prefix,
            ).run(task_batches[0])
        else:
            logging.info(
                "No new document contents to upload for collection [%s]; recording "
                "metadata updates only.",
                self.document_collection.name,
            )

        DocumentUploadResultRecorder(
            state_code=self.document_collection.state_code,
            project_id=self.bq_client.project_id,
            big_query_client=self.bq_client,
            run_id=self.run_id,
            metadata_row_create_datetime=upload_datetime,
            output_sandbox_prefix=output_prefix,
        ).run(discovery_result)


class SandboxExtractionRunner:
    """Owns the extraction job's lifecycle for one extractor and sandbox: generates
    the job, enforces the MAX_DOCUMENTS ceiling, marks the job started/completed/
    failed, and delegates running the pending documents through the LLM to a
    DocumentExtractionProcessor. Runs unchanged for a first-order or an
    entity-resolution config.
    """

    def __init__(
        self,
        *,
        # The narrowed extractor config every stage reads from.
        config: LLMExtractorConfig,
        # The document store the eligible-document query reads; maps the input
        # collection to the production document store when unseeded. None reads the
        # input collection from production, matching an unsandboxed input collection.
        document_store_sandbox: DocumentStoreSandboxContext | None,
        # Client for the eligible-document query.
        bq_client: BigQueryClient,
        # Tracks the job lifecycle (generation, start, completion, failure).
        job_manager: LLMExtractionJobManager,
        # Runs the job's pending documents through the LLM.
        processor: DocumentExtractionProcessor,
    ) -> None:
        self.config = config
        self.document_store_sandbox = document_store_sandbox
        self.bq_client = bq_client
        self.job_manager = job_manager
        self.processor = processor

    @property
    def state_code(self) -> StateCode:
        return self.config.state_code

    def run(self) -> SandboxExtractionSummary | None:
        """Runs the extraction thread end-to-end and returns the run's summary, or
        None if there was no work to do (no eligible documents, or a resumed job
        whose documents all finished before a crash).

        Expects the sandbox result tables to already exist; deploying the parsed
        views over the tables this writes is left to the caller.
        """
        active_version = LLMExtractorMetadataManager().set_active_extractor_version(
            config=self.config
        )
        logging.info(
            "Running sandbox extraction for state [%s], collection [%s], "
            "extractor version [%s].",
            self.state_code.value,
            self.config.extractor_collection.name,
            active_version.extractor_version_id,
        )
        # A resumed job (with --keep-postgres) is keyed only on the extractor
        # version, which excludes the --document-limit / --root-entity-ids
        # narrowing. So a resume with different narrowing flags continues the
        # original job's already-selected documents rather than the newly
        # requested ones; the narrowing appears applied but is effectively
        # ignored on resume. TODO(OBT-42806) surface or key on the narrowing.
        source_sandbox_prefix = (
            self.document_store_sandbox.source_read_prefix_for_document_collection(
                self.config.input_document_collection.name
            )
            if self.document_store_sandbox is not None
            else None
        )
        job = LLMExtractionJobGenerator(
            eligible_documents_query_builder=LLMExtractionEligibleDocumentQueryBuilder(
                document_filter=self.config.document_filter,
                input_document_collection=self.config.input_document_collection,
                source_sandbox_prefix=source_sandbox_prefix,
            ),
            job_manager=self.job_manager,
            bq_client=self.bq_client,
        ).generate_job(config=self.config, active_version=active_version)

        if job is None:
            logging.info("No documents need processing; nothing to do.")
            return None

        # mark_job_started is a no-op on an already-started (resumed) job, so
        # calling it before the pending-document read lets the try/except own the
        # whole job lifecycle: any failure from here on marks the job failed
        # rather than leaving it open, which under --keep-postgres would block
        # every future run for this extractor version.
        self.job_manager.mark_job_started(state_code=self.state_code, job_id=job.job_id)
        try:
            pending_documents = self.job_manager.get_pending_job_documents(
                state_code=self.state_code, job_id=job.job_id
            )
            if len(pending_documents) > MAX_DOCUMENTS:
                raise ValueError(
                    f"Job [{job.job_id}] holds [{len(pending_documents)}] pending "
                    f"documents, above the MAX_DOCUMENTS ceiling of "
                    f"[{MAX_DOCUMENTS}]. This script's write path is not yet sized "
                    f"for runs this large; narrow the run (e.g. --document-limit) "
                    f"or raise MAX_DOCUMENTS deliberately."
                )
            if pending_documents:
                summary = self.processor.process(
                    job_id=job.job_id, pending_documents=pending_documents
                )
            else:
                # A resumed job whose documents all finished before a crash still
                # needs completing, or it stays open and blocks every future run.
                logging.info(
                    "Job [%s] has no pending documents; completing it.", job.job_id
                )
                summary = None
            self.job_manager.mark_job_completed(
                state_code=self.state_code, job_id=job.job_id
            )
        except Exception as e:
            self.job_manager.mark_job_failed(
                state_code=self.state_code, job_id=job.job_id, error_message=str(e)
            )
            raise

        return summary
