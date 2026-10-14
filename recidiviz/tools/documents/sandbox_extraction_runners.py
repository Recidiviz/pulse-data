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
from recidiviz.documents.extraction.document_extraction_processor import (
    DocumentExtractionProcessor,
    ExtractionRunSummary,
)
from recidiviz.documents.extraction.llm_extraction_job_manager import (
    LLMExtractionJobManager,
)
from recidiviz.documents.extraction.llm_extraction_job_runner import (
    run_sync_extraction_job,
)
from recidiviz.documents.extraction.llm_extraction_job_tasks import (
    identify_job_documents,
    sync_extractor_metadata,
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
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import (
    SchemaType,
    SQLAlchemyDatabaseKey,
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
    """Generates the extraction job for one extractor and sandbox, then runs it to
    completion via run_sync_extraction_job (capped at MAX_DOCUMENTS). Runs unchanged
    for a first-order or an entity-resolution config.
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
        self.db_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)

    @property
    def state_code(self) -> StateCode:
        return self.config.state_code

    def run(self) -> ExtractionRunSummary | None:
        """Runs the extraction thread end-to-end and returns the run's summary, or
        None if there was no work to do (no eligible documents, or a resumed job
        whose documents all finished before a crash).

        Expects the sandbox result tables to already exist; deploying the parsed
        views over the tables this writes is left to the caller.
        """
        with SessionFactory.using_database(self.db_key) as session:
            active_version_id = sync_extractor_metadata(
                config=self.config, session=session
            )
        logging.info(
            "Running sandbox extraction for state [%s], collection [%s], "
            "extractor version [%s].",
            self.state_code.value,
            self.config.extractor_collection.name,
            active_version_id,
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
        with SessionFactory.using_database(self.db_key) as session:
            job_id = identify_job_documents(
                config=self.config,
                extractor_version_id=active_version_id,
                job_manager=self.job_manager,
                bq_client=self.bq_client,
                source_sandbox_prefix=source_sandbox_prefix,
                session=session,
            )

        if job_id is None:
            logging.info("No documents need processing; nothing to do.")
            return None

        return run_sync_extraction_job(
            config=self.config,
            job_id=job_id,
            job_manager=self.job_manager,
            processor=self.processor,
            max_documents=MAX_DOCUMENTS,
        )
