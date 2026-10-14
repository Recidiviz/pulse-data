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
"""Shared LLM extraction job-lifecycle tasks."""

from recidiviz.big_query.big_query_client import BigQueryClient
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
from recidiviz.persistence.database.session import Session


def sync_extractor_metadata(config: LLMExtractorConfig, session: Session) -> str:
    active_version = (
        LLMExtractorMetadataManager().set_active_extractor_version_with_session(
            config=config, session=session
        )
    )
    return active_version.extractor_version_id


def identify_job_documents(
    *,
    config: LLMExtractorConfig,
    extractor_version_id: str,
    job_manager: LLMExtractionJobManager,
    bq_client: BigQueryClient,
    session: Session,
    source_sandbox_prefix: str | None = None,
) -> str | None:
    job = LLMExtractionJobGenerator(
        eligible_documents_query_builder=LLMExtractionEligibleDocumentQueryBuilder(
            document_filter=config.document_filter,
            input_document_collection=config.input_document_collection,
            source_sandbox_prefix=source_sandbox_prefix,
        ),
        job_manager=job_manager,
        bq_client=bq_client,
    ).generate_job(
        session=session, config=config, extractor_version_id=extractor_version_id
    )

    return job.job_id if job is not None else None
