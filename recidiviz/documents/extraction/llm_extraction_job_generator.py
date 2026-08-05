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
"""Turns an eligible-document query into a persisted extraction job."""

import logging

import attr

from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.documents.extraction.llm_extraction_eligible_document_query_builder import (
    LLMExtractionEligibleDocumentQueryBuilder,
)
from recidiviz.documents.extraction.llm_extraction_job_manager import (
    LLMExtractionEligibleDocumentRecord,
    LLMExtractionJobManager,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    DOCUMENT_LENGTH_BYTES_COLUMN_NAME,
    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
)
from recidiviz.persistence.entity.operations.entities import (
    LLMExtractionJob,
    LLMExtractorVersion,
)


@attr.define(frozen=True, kw_only=True)
class LLMExtractionJobGenerator:
    """Turns an eligible-document query into a persisted extraction job.

    All Postgres I/O runs through the LLMExtractionJobManager; this class issues
    no SQL of its own beyond the eligible-document query it runs against BQ.
    """

    eligible_documents_query_builder: LLMExtractionEligibleDocumentQueryBuilder = (
        attr.ib(
            validator=attr.validators.instance_of(
                LLMExtractionEligibleDocumentQueryBuilder
            )
        )
    )
    """Builds the SQL that selects this extractor's eligible documents."""

    job_manager: LLMExtractionJobManager = attr.ib(
        validator=attr.validators.instance_of(LLMExtractionJobManager)
    )
    """The single owner of all job-lifecycle Postgres I/O."""

    bq_client: BigQueryClient = attr.ib(factory=BigQueryClientImpl)
    """Executes the eligible-document query."""

    def generate_job(
        self, *, config: LLMExtractorConfig, active_version: LLMExtractorVersion
    ) -> LLMExtractionJob | None:
        """Returns the open job for the active version if one exists (resume — a
        crashed run's job must be finished before new work is selected, since the
        one-open-job-per-version unique index forbids a second), otherwise the
        newly created job, or None when there's no work. Either way the job's
        remaining documents are read back via
        LLMExtractionJobManager.get_pending_job_documents. Sandbox narrowing
        arrives already applied to the config (and to the query builder built
        from it) — this class has no narrowing surface of its own.
        """
        state_code = config.state_code
        extractor_version_id = active_version.extractor_version_id

        open_job = self.job_manager.get_open_job(
            state_code=state_code, extractor_version_id=extractor_version_id
        )
        if open_job is not None:
            logging.info(
                "Resuming open job [%s] for state [%s], extractor version [%s].",
                open_job.job_id,
                state_code.value,
                extractor_version_id,
            )
            return open_job

        self.job_manager.record_eligible_documents(
            state_code=state_code,
            extractor_version_id=extractor_version_id,
            document_filter_id=config.document_filter_id,
            eligible_documents=self._query_eligible_documents(),
        )

        document_contents_ids = (
            self.job_manager.get_document_contents_ids_needing_processing(
                state_code=state_code, extractor_version_id=extractor_version_id
            )
        )
        if not document_contents_ids:
            logging.info(
                "No eligible documents for state [%s], extractor version [%s].",
                state_code.value,
                extractor_version_id,
            )
            return None

        logging.info(
            "Creating new job for state [%s], extractor version [%s] with [%d] eligible documents.",
            state_code.value,
            extractor_version_id,
            len(document_contents_ids),
        )
        return self.job_manager.create_job(
            state_code=state_code,
            extractor_version_id=extractor_version_id,
            document_contents_ids=document_contents_ids,
        )

    def _query_eligible_documents(self) -> list[LLMExtractionEligibleDocumentRecord]:
        """Returns the eligible documents produced by running the extractor's
        eligible-document query against BQ, one record per document_contents_id.
        """
        logging.info("Querying eligible documents from BQ")
        query = self.eligible_documents_query_builder.build_query(
            project_id=self.bq_client.project_id
        )
        query_job = self.bq_client.run_query_async(
            query_str=query, use_query_cache=False
        )
        return [
            LLMExtractionEligibleDocumentRecord(
                document_contents_id=row[DOCUMENT_CONTENTS_ID_COLUMN_NAME],
                document_length_bytes=row[DOCUMENT_LENGTH_BYTES_COLUMN_NAME],
                document_update_datetime=row[DOCUMENT_UPDATE_DATETIME_COLUMN_NAME],
            )
            for row in query_job.result()
        ]
