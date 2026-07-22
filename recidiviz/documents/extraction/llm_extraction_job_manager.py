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
"""Postgres I/O layer over the LLM extraction job-processing lifecycle tables.

`LLMExtractionJobManager` is the single owner of reads and writes against the
eligible-document tables and the job + job-document tables, so the selection
logic and result processing never issue raw SQL. It never hands a session-bound
ORM row across its boundary: reads return validated attrs value objects (the
operations `entities` types) and writes take primitives plus small attrs
objects.
"""

import datetime
import uuid
from typing import Any

import attr
from sqlalchemy import and_, func
from sqlalchemy.dialects.postgresql import insert

from recidiviz.common import attr_validators
from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMDocumentExtractionErrorType,
    LLMExtractionJobDocumentResultType,
    LLMExtractionJobResultType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_client.types import (
    LLMClientDocumentExtractionResult,
)
from recidiviz.documents.extraction.llm_document_validation_result import (
    LLMDocumentValidationResult,
)
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_schema_object_to_entity,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.operations.entities import (
    LLMExtractionJob,
    LLMExtractionJobDocument,
)

# The per-document result types that permanently remove a document from job
# selection for an extractor version — a document with any of these is "done"
# and is not re-selected.
TERMINAL_JOB_DOCUMENT_RESULT_TYPES = frozenset(
    result_type
    for result_type in LLMExtractionJobDocumentResultType
    if LLMExtractionJobDocumentResultType.is_terminal_result_type(result_type)
)

# The document-level result types that count as a failure when deriving a
# completed job's terminal result: any per-document result other than SUCCESS.
_FAILURE_JOB_DOCUMENT_RESULT_TYPES = frozenset(
    result_type
    for result_type in LLMExtractionJobDocumentResultType
    if not LLMExtractionJobDocumentResultType.is_success_result_type(result_type)
)


@attr.define(frozen=True, kw_only=True)
class LLMExtractionEligibleDocumentRecord:
    """One document from an extractor's eligible-document query"""

    document_contents_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Content-addressed (SHA256) identifier of the document."""

    # TODO(OBT-39477): The eligible-document query builder now emits
    # document_length_bytes rather than a char count; reconcile this field (and
    # the persisted char_count column) with that output when the query is wired
    # into record_eligible_documents.
    char_count: int = attr.ib(validator=attr_validators.is_non_negative_int)
    """Character count of the document text; write-once because the id is a hash
    of the text."""

    document_update_datetime: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    """The document's date; used to order oldest-first. Write-once for the same
    reason as `char_count`."""


@attr.define(frozen=True, kw_only=True)
class LLMJobDocumentExtractionResult:
    """The output side of an `llm_extraction_job_document` row: the full outcome
    of processing one document, produced by the result processor."""

    job_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The job this result belongs to."""

    document_contents_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The document this result is for."""

    result_datetime_utc: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    """When this document's result was produced."""

    raw_result: LLMClientDocumentExtractionResult = attr.ib(
        validator=attr.validators.instance_of(LLMClientDocumentExtractionResult)
    )
    """The provider-agnostic result from the LLM client, source of the token
    counts persisted here (its content goes to BQ, not Postgres)."""

    result_type: LLMExtractionJobDocumentResultType = attr.ib(
        validator=attr.validators.in_(LLMExtractionJobDocumentResultType)
    )
    """The effective per-document classification — validation can downgrade a raw
    SUCCESS before it is stamped here."""

    is_relevant: bool | None = attr.ib(validator=attr_validators.is_opt_bool)
    """The model's relevance determination. None unless validation produced usable
    content: a raw call can return JSON that is incomplete or missing the
    is_relevant field, so relevance is only known once validated_content exists."""

    error_type: LLMDocumentExtractionErrorType | None = attr.ib(
        validator=attr_validators.is_opt(LLMDocumentExtractionErrorType)
    )
    """The per-document error category, or None on success."""

    error_message: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """A human-readable description of the failure, or None on success."""

    validation_results: LLMDocumentValidationResult | None = attr.ib(
        validator=attr_validators.is_opt(LLMDocumentValidationResult)
    )
    """The validation outcome. Set whenever the raw call produced JSON to
    validate, None when the raw call failed."""

    @property
    def is_validated_result(self) -> bool:
        """Returns whether validation produced usable content for this document.
        A raw call can return JSON that is incomplete or malformed, which
        validation rejects (leaving no validated_content); only a populated
        validated_content means the result is usable."""
        return (
            self.validation_results is not None
            and self.validation_results.validated_content is not None
        )

    def __attrs_post_init__(self) -> None:
        raw_call_succeeded = self.raw_result.result_json is not None
        if (self.validation_results is not None) != raw_call_succeeded:
            raise ValueError(
                f"validation_results must be set iff the raw call produced JSON for "
                f"document [{self.document_contents_id}] in job [{self.job_id}]: got "
                f"validation_results=[{self.validation_results}], "
                f"raw_call_succeeded=[{raw_call_succeeded}]."
            )
        if (self.is_relevant is not None) != self.is_validated_result:
            raise ValueError(
                f"is_relevant must be set iff validation produced usable content for "
                f"document [{self.document_contents_id}] in job [{self.job_id}]: got "
                f"is_relevant=[{self.is_relevant}], "
                f"is_validated_result=[{self.is_validated_result}]."
            )
        if self.result_type is LLMExtractionJobDocumentResultType.SUCCESS and (
            self.error_type is not None or self.error_message is not None
        ):
            raise ValueError(
                f"A SUCCESS result must not carry an error for document "
                f"[{self.document_contents_id}] in job [{self.job_id}]: got "
                f"error_type=[{self.error_type}], error_message=[{self.error_message}]."
            )


class LLMExtractionJobManager:
    """Postgres I/O for the job-processing lifecycle: which documents are eligible
    for an extractor, which jobs processed them, and each document's outcome.
    Never hands a session-bound ORM row across its boundary.
    """

    def __init__(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)

    def record_eligible_documents(
        self,
        *,
        state_code: StateCode,
        extractor_version_id: str,
        document_filter_id: str,
        eligible_documents: list[LLMExtractionEligibleDocumentRecord],
    ) -> None:
        """Writes to `llm_extraction_eligible_document` (insert-if-absent per
        (extractor_version, filter, document)) and, for documents never seen
        before, `llm_extraction_eligible_document_metadata`.

        Idempotent: re-recording the same documents adds no duplicate rows, and
        a metadata row is written only the first time a document is seen.
        """
        if not eligible_documents:
            return

        now = datetime.datetime.now(tz=datetime.UTC)
        with SessionFactory.using_database(self.database_key) as session:
            metadata_rows = [
                {
                    schema.LLMExtractionEligibleDocumentMetadata.state_code: state_code.value,
                    schema.LLMExtractionEligibleDocumentMetadata.document_contents_id: doc.document_contents_id,
                    schema.LLMExtractionEligibleDocumentMetadata.char_count: doc.char_count,
                    schema.LLMExtractionEligibleDocumentMetadata.document_update_datetime: doc.document_update_datetime,
                    schema.LLMExtractionEligibleDocumentMetadata.row_creation_datetime_utc: now,
                }
                for doc in eligible_documents
            ]
            session.execute(
                insert(schema.LLMExtractionEligibleDocumentMetadata)
                .values(metadata_rows)
                .on_conflict_do_nothing(
                    constraint="llm_extraction_eligible_document_metadata_pkey",
                )
            )

            eligible_rows = [
                {
                    schema.LLMExtractionEligibleDocument.state_code: state_code.value,
                    schema.LLMExtractionEligibleDocument.extractor_version_id: extractor_version_id,
                    schema.LLMExtractionEligibleDocument.document_filter_id: document_filter_id,
                    schema.LLMExtractionEligibleDocument.document_contents_id: doc.document_contents_id,
                    schema.LLMExtractionEligibleDocument.row_creation_datetime_utc: now,
                }
                for doc in eligible_documents
            ]
            session.execute(
                insert(schema.LLMExtractionEligibleDocument)
                .values(eligible_rows)
                .on_conflict_do_nothing(
                    constraint="llm_extraction_eligible_document_pkey",
                )
            )

    def get_document_contents_ids_needing_processing(
        self, *, state_code: StateCode, extractor_version_id: str
    ) -> list[str]:
        """Returns the eligible documents that lack a terminal result across
        prior jobs for this extractor version — the input to `create_job`.

        The eligible-documents x job-documents anti-join runs entirely in SQL;
        the eligible set grows without bound over an extractor's lifetime, so it
        is never diffed in memory.
        """
        with SessionFactory.using_database(self.database_key) as session:
            terminal_document_ids = (
                session.query(schema.LLMExtractionJobDocument.document_contents_id)
                .join(
                    schema.LLMExtractionJob,
                    and_(
                        schema.LLMExtractionJob.state_code
                        == schema.LLMExtractionJobDocument.state_code,
                        schema.LLMExtractionJob.job_id
                        == schema.LLMExtractionJobDocument.job_id,
                    ),
                )
                .filter(
                    schema.LLMExtractionJobDocument.state_code == state_code.value,
                    schema.LLMExtractionJob.extractor_version_id
                    == extractor_version_id,
                    schema.LLMExtractionJobDocument.result_type.in_(
                        [t.value for t in TERMINAL_JOB_DOCUMENT_RESULT_TYPES]
                    ),
                )
                .scalar_subquery()
            )

            needing_processing = (
                session.query(schema.LLMExtractionEligibleDocument.document_contents_id)
                .filter(
                    schema.LLMExtractionEligibleDocument.state_code == state_code.value,
                    schema.LLMExtractionEligibleDocument.extractor_version_id
                    == extractor_version_id,
                    schema.LLMExtractionEligibleDocument.document_contents_id.notin_(
                        terminal_document_ids
                    ),
                )
                .distinct()
                .all()
            )
            return [
                document_contents_id for (document_contents_id,) in needing_processing
            ]

    def get_open_job(
        self, *, state_code: StateCode, extractor_version_id: str
    ) -> LLMExtractionJob | None:
        """Returns the open (not-yet-completed) job for this extractor version, or
        None. At most one exists — enforced by the
        `one_open_llm_extraction_job_per_extractor_version` partial unique index,
        which is also why callers resume it rather than create another.
        """
        with SessionFactory.using_database(self.database_key) as session:
            job = (
                session.query(schema.LLMExtractionJob)
                .filter(
                    schema.LLMExtractionJob.state_code == state_code.value,
                    schema.LLMExtractionJob.extractor_version_id
                    == extractor_version_id,
                    schema.LLMExtractionJob.completion_datetime_utc.is_(None),
                )
                .one_or_none()
            )
            if job is None:
                return None
            return convert_schema_object_to_entity(
                job, LLMExtractionJob, populate_direct_back_edges=False
            )

    def create_job(
        self,
        *,
        state_code: StateCode,
        extractor_version_id: str,
        document_contents_ids: list[str],
    ) -> LLMExtractionJob:
        """Writes the `llm_extraction_job` row and one `llm_extraction_job_document`
        row per document, returning the created job.

        Everything but the inputs is manager-assigned: the job_id UUID, each
        row's job_index ordering (0-indexed, in the given order), and the
        result/token columns (all null until the document is processed).
        """
        if not document_contents_ids:
            raise ValueError(
                f"Cannot create a job with no documents for state "
                f"[{state_code.value}], extractor version [{extractor_version_id}]."
            )

        job_id = str(uuid.uuid4())
        with SessionFactory.using_database(self.database_key) as session:
            job = schema.LLMExtractionJob(
                state_code=state_code.value,
                job_id=job_id,
                extractor_version_id=extractor_version_id,
                start_datetime_utc=None,
                completion_datetime_utc=None,
                result_type=None,
                error_message=None,
            )
            session.add(job)
            session.add_all(
                schema.LLMExtractionJobDocument(
                    state_code=state_code.value,
                    job_id=job_id,
                    document_contents_id=document_contents_id,
                    batch_index=None,
                    job_index=job_index,
                    result_datetime_utc=None,
                    result_type=None,
                    is_relevant=None,
                    error_message=None,
                    input_token_count=None,
                    output_token_count=None,
                    cached_input_token_count=None,
                    thinking_token_count=None,
                )
                for job_index, document_contents_id in enumerate(document_contents_ids)
            )
            return convert_schema_object_to_entity(
                job, LLMExtractionJob, populate_direct_back_edges=False
            )

    def mark_job_started(self, *, state_code: StateCode, job_id: str) -> None:
        """Writes `start_datetime_utc` to `llm_extraction_job`. A no-op if the
        job is already started (a resumed job)."""
        with SessionFactory.using_database(self.database_key) as session:
            session.query(schema.LLMExtractionJob).filter(
                schema.LLMExtractionJob.state_code == state_code.value,
                schema.LLMExtractionJob.job_id == job_id,
                schema.LLMExtractionJob.start_datetime_utc.is_(None),
            ).update(
                {
                    schema.LLMExtractionJob.start_datetime_utc: datetime.datetime.now(
                        tz=datetime.UTC
                    )
                },
                synchronize_session=False,
            )

    def get_pending_job_documents(
        self, *, state_code: StateCode, job_id: str
    ) -> list[LLMExtractionJobDocument]:
        """Returns the job's documents still needing processing (no result yet) —
        the read that feeds the request builder.

        These are the input side of a row: identity and creation-time context,
        result columns still null. The document text is not loaded here.
        """
        with SessionFactory.using_database(self.database_key) as session:
            pending = (
                session.query(schema.LLMExtractionJobDocument)
                .filter(
                    schema.LLMExtractionJobDocument.state_code == state_code.value,
                    schema.LLMExtractionJobDocument.job_id == job_id,
                    schema.LLMExtractionJobDocument.result_type.is_(None),
                )
                .all()
            )
            return [
                convert_schema_object_to_entity(
                    document,
                    LLMExtractionJobDocument,
                    populate_direct_back_edges=False,
                )
                for document in pending
            ]

    def set_job_document_result(
        self, *, state_code: StateCode, result: LLMJobDocumentExtractionResult
    ) -> None:
        """Persists one document's outcome (its non-PII columns) to
        `llm_extraction_job_document`.

        Callers must persist the result's content to BQ *first*: a terminal
        result here permanently removes the document from job selection, so
        marking before the BQ write turns a crash between the two writes into
        silent data loss.
        """
        token_counts = result.raw_result.token_counts
        with SessionFactory.using_database(self.database_key) as session:
            updated = (
                session.query(schema.LLMExtractionJobDocument)
                .filter(
                    schema.LLMExtractionJobDocument.state_code == state_code.value,
                    schema.LLMExtractionJobDocument.job_id == result.job_id,
                    schema.LLMExtractionJobDocument.document_contents_id
                    == result.document_contents_id,
                )
                .update(
                    {
                        schema.LLMExtractionJobDocument.result_datetime_utc: result.result_datetime_utc,
                        schema.LLMExtractionJobDocument.result_type: result.result_type.value,
                        schema.LLMExtractionJobDocument.is_relevant: result.is_relevant,
                        schema.LLMExtractionJobDocument.error_type: (
                            result.error_type.value
                            if result.error_type is not None
                            else None
                        ),
                        schema.LLMExtractionJobDocument.error_message: result.error_message,
                        schema.LLMExtractionJobDocument.input_token_count: token_counts.input_token_count,
                        schema.LLMExtractionJobDocument.output_token_count: token_counts.output_token_count,
                        schema.LLMExtractionJobDocument.cached_input_token_count: token_counts.cached_input_token_count,
                        schema.LLMExtractionJobDocument.thinking_token_count: token_counts.thinking_token_count,
                    },
                    synchronize_session=False,
                )
            )
            if not updated:
                raise ValueError(
                    f"No job document found for state [{state_code.value}], job "
                    f"[{result.job_id}], document [{result.document_contents_id}]."
                )

    def mark_job_completed(self, *, state_code: StateCode, job_id: str) -> None:
        """Closes the job, deriving the job-level result from its document rows:
        SUCCESS when no document failed, PARTIAL_FAILURE when at least one did.

        The document rows are the authoritative outcomes — on a resumed job, no
        in-memory results list covers the documents processed before a crash.
        The job must already be started (a completed job with documents was, by
        definition, run); the DB completion-requires-start constraint enforces
        this. A job that fails before it starts goes through mark_job_failed.
        """
        with SessionFactory.using_database(self.database_key) as session:
            has_failure = (
                session.query(schema.LLMExtractionJobDocument)
                .filter(
                    schema.LLMExtractionJobDocument.state_code == state_code.value,
                    schema.LLMExtractionJobDocument.job_id == job_id,
                    schema.LLMExtractionJobDocument.result_type.in_(
                        [t.value for t in _FAILURE_JOB_DOCUMENT_RESULT_TYPES]
                    ),
                )
                .first()
                is not None
            )
            result_type = (
                LLMExtractionJobResultType.PARTIAL_FAILURE
                if has_failure
                else LLMExtractionJobResultType.SUCCESS
            )
            self._close_job(
                session=session,
                state_code=state_code,
                job_id=job_id,
                result_type=result_type,
                error_message=None,
            )

    def mark_job_failed(
        self, *, state_code: StateCode, job_id: str, error_message: str
    ) -> None:
        """Closes the job as a job-level FAILURE with the given `error_message`
        (the one result type that cannot be derived from document rows).

        Its unprocessed documents lack a terminal result, so they are re-selected
        into a future job.
        """
        with SessionFactory.using_database(self.database_key) as session:
            self._close_job(
                session=session,
                state_code=state_code,
                job_id=job_id,
                result_type=LLMExtractionJobResultType.FAILURE,
                error_message=error_message,
                # A job can fail at startup, before mark_job_started runs. The
                # completion-requires-start constraint still applies, so treat
                # the failure moment as the (instantaneous) start in that case.
                backfill_start_if_unstarted=True,
            )

    @staticmethod
    def _close_job(
        *,
        session: Session,
        state_code: StateCode,
        job_id: str,
        result_type: LLMExtractionJobResultType,
        error_message: str | None,
        backfill_start_if_unstarted: bool = False,
    ) -> None:
        """Sets completion_datetime_utc, result_type, and error_message on the
        job, failing loudly if the job does not exist or is already closed.

        `backfill_start_if_unstarted` also stamps start_datetime_utc when it is
        null, so the completion-requires-start check constraint holds even for a
        job that is closed before it was ever marked started.
        """
        now = datetime.datetime.now(tz=datetime.UTC)
        values: dict[Any, Any] = {
            schema.LLMExtractionJob.completion_datetime_utc: now,
            schema.LLMExtractionJob.result_type: result_type.value,
            schema.LLMExtractionJob.error_message: error_message,
        }
        if backfill_start_if_unstarted:
            values[schema.LLMExtractionJob.start_datetime_utc] = func.coalesce(
                schema.LLMExtractionJob.start_datetime_utc, now
            )
        updated = (
            session.query(schema.LLMExtractionJob)
            .filter(
                schema.LLMExtractionJob.state_code == state_code.value,
                schema.LLMExtractionJob.job_id == job_id,
                schema.LLMExtractionJob.completion_datetime_utc.is_(None),
            )
            .update(values, synchronize_session=False)
        )
        if not updated:
            raise ValueError(
                f"No open job found to close for state [{state_code.value}], job "
                f"[{job_id}] (it may not exist or may already be completed)."
            )
