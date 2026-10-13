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

from recidiviz.documents.extraction.document_extraction_processor import (
    DocumentExtractionProcessor,
    ExtractionRunSummary,
)
from recidiviz.documents.extraction.llm_extraction_job_manager import (
    LLMExtractionJobManager,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)


def run_sync_extraction_job(
    *,
    config: LLMExtractorConfig,
    job_id: str,
    job_manager: LLMExtractionJobManager,
    processor: DocumentExtractionProcessor,
    # Ceiling on how many pending documents the run will accept before refusing to
    # start, or None for no ceiling.
    max_documents: int | None = None,
) -> ExtractionRunSummary | None:
    """Runs an already-created |job_id| to completion and returns its summary, or None if
    there was no work to do. Reads the job's pending documents, processes them through the LLM, and
    marks the job completed — or, on any failure, marks the job failed and re-raises.
    """
    logging.info(
        "Running sync extraction job [%s] for state [%s], collection [%s].",
        job_id,
        config.state_code.value,
        config.extractor_collection.name,
    )

    # mark_job_started is a no-op on an already-started (resumed) job, so calling it
    # before the pending-document read lets the try/except own the whole job
    # lifecycle: any failure from here on marks the job failed rather than leaving it
    # open, which would block every future run for this extractor version.
    job_manager.mark_job_started(state_code=config.state_code, job_id=job_id)
    try:
        pending_documents = job_manager.get_pending_job_documents(
            state_code=config.state_code, job_id=job_id
        )
        # TODO(OBT-46617) this is intended to prevent us from accidentally running very large
        # sandbox runs, we should replace with the production SKIPPED_OVER_CAP check
        # once that is built
        if max_documents is not None and len(pending_documents) > max_documents:
            raise ValueError(
                f"Job [{job_id}] holds [{len(pending_documents)}] pending documents, "
                f"above the ceiling of [{max_documents}]."
            )
        if pending_documents:
            summary = processor.process(
                job_id=job_id, pending_documents=pending_documents
            )
        else:
            # A resumed job whose documents all finished before a crash still needs
            # completing, or it stays open and blocks every future run.
            logging.info("Job [%s] has no pending documents; completing it.", job_id)
            summary = None
        job_manager.mark_job_completed(state_code=config.state_code, job_id=job_id)
    except Exception as e:
        job_manager.mark_job_failed(
            state_code=config.state_code, job_id=job_id, error_message=str(e)
        )
        raise

    return summary
