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

import logging
from collections.abc import Callable
from contextlib import AbstractContextManager

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMDocumentExtractionErrorType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extraction_eligible_document_query_builder import (
    LLMExtractionEligibleDocumentQueryBuilder,
)
from recidiviz.documents.extraction.llm_extraction_job_manager import (
    LLMExtractionEligibleDocumentRecord,
    LLMExtractionJobManager,
)
from recidiviz.documents.extraction.llm_extractor_metadata_manager import (
    LLMExtractorMetadataManager,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.persistence.database.session import Session
from recidiviz.utils.types import assert_type


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
    session_cm: Callable[[], AbstractContextManager[Session]],
    source_sandbox_prefix: str | None = None,
) -> str | None:
    """Returns the id of the job holding this extractor's documents to process, or
    None when there's no work.
    """
    logging.info("Querying eligible documents from BQ")
    query_builder = LLMExtractionEligibleDocumentQueryBuilder(
        document_filter=config.document_filter,
        input_document_collection=config.input_document_collection,
        source_sandbox_prefix=source_sandbox_prefix,
    )
    query_job = bq_client.run_query_async(
        query_str=query_builder.build_query(project_id=bq_client.project_id),
        use_query_cache=False,
    )
    eligible_documents = [
        LLMExtractionEligibleDocumentRecord.from_row(row) for row in query_job.result()
    ]

    logging.info("Generating job with %d eligible documents", len(eligible_documents))
    with session_cm() as session:
        job = job_manager.generate_job_with_session(
            session,
            state_code=config.state_code,
            extractor_version_id=extractor_version_id,
            document_filter_id=config.document_filter_id,
            eligible_documents=eligible_documents,
        )

    return job.job_id if job is not None else None


def raise_on_attention_required_extraction_failures(
    *,
    state_code: StateCode,
    job_id: str,
    job_manager: LLMExtractionJobManager,
    session: Session,
) -> None:
    """Raises if |job_id| produced any attention-required failure — a terminal
    failure that originated from an LLM request error — naming each failing
    document and its result/error type.
    """
    terminal_failures = job_manager.get_terminal_failures_with_session(
        session, state_code=state_code, job_id=job_id
    )
    # TODO(OBT-56097) we currently only raise on terminal failures that originated
    # from an LLM request error (a non-null error_type), filtering out
    # validation-originated terminal failures. Because we have a large volume of
    # validation failures, including them would be too noisy.
    failures = [
        failure for failure in terminal_failures if failure.error_type is not None
    ]
    if not failures:
        return
    failure_details = ", ".join(
        f"[{failure.document_contents_id}: {failure.result_type.value} "
        f"({assert_type(failure.error_type, LLMDocumentExtractionErrorType).value}) "
        f"[{assert_type(failure.error_message, str)}]]"
        for failure in failures
    )
    raise ValueError(
        f"Job [{job_id}] for state [{state_code.value}] produced "
        f"[{len(failures)}] document failure(s) requiring attention: "
        f"{failure_details}."
    )
