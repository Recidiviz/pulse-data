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
"""Tests for LLMExtractionJobManager against a real test Postgres."""

import datetime
import unittest
from typing import Any

import pytest
import pytz

from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMDocumentExtractionErrorType,
    LLMExtractionJobDocumentResultType,
    LLMExtractionJobResultType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_client.types import (
    LLMClientDocumentExtractionResult,
    LLMDocumentExtractionTokenCounts,
    LLMRequestErrorType,
)
from recidiviz.documents.extraction.llm_document_validation_result import (
    LLMDocumentValidationResult,
    ValidationCheckType,
    ValidationIssue,
)
from recidiviz.documents.extraction.llm_extraction_job_manager import (
    LLMExtractionEligibleDocumentRecord,
    LLMExtractionJobManager,
    LLMJobDocumentExtractionResult,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    IS_RELEVANT_FIELD_NAME,
    RESULT_KEY,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_schema_object_to_entity,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.operations.entities import (
    LLMExtractionJob,
    LLMExtractionJobDocument,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult

_STATE = StateCode.US_XX
_EXTRACTOR_VERSION = "ev1"
_FILTER = "filter1"
_NOW = datetime.datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
_OUTPUT_SCHEMA = get_first_order_llm_extractor_config(
    _STATE, "FAKE_EXTRACTOR_COLLECTION", config_module=fake_config
).extractor_collection.output_schema


def _eligible_record(
    document_contents_id: str, **overrides: Any
) -> LLMExtractionEligibleDocumentRecord:
    return LLMExtractionEligibleDocumentRecord(
        document_contents_id=document_contents_id,
        document_length_bytes=overrides.pop("document_length_bytes", 100),
        document_update_datetime=overrides.pop("document_update_datetime", _NOW),
    )


def _client_result(
    document_contents_id: str, *, error_type: LLMRequestErrorType | None = None
) -> LLMClientDocumentExtractionResult:
    token_counts = LLMDocumentExtractionTokenCounts(
        input_token_count=10,
        output_token_count=5,
        cached_input_token_count=2,
        thinking_token_count=1,
    )
    if error_type is not None:
        return LLMClientDocumentExtractionResult.from_error(
            document_contents_id=document_contents_id,
            error_type=error_type,
            error_message=f"error for {document_contents_id}",
            token_counts=token_counts,
        )
    return LLMClientDocumentExtractionResult.from_success(
        document_contents_id=document_contents_id,
        result_json={"is_relevant": True},
        token_counts=token_counts,
    )


def _validation_result(
    *,
    validated_output_json: dict[str, Any] | None,
    result_type_override: LLMExtractionJobDocumentResultType | None,
    audit_issues: list[ValidationIssue] | None = None,
) -> LLMDocumentValidationResult:
    return LLMDocumentValidationResult(
        validated_output=(
            None
            if validated_output_json is None
            else LLMRequestOutputValues(
                output_schema=_OUTPUT_SCHEMA, output_json=validated_output_json
            )
        ),
        audit_issues=audit_issues if audit_issues is not None else [],
        result_type_override=result_type_override,
        validation_config_version_id="vc1",
        validation_datetime_utc=_NOW,
    )


def _success_result(
    *, job_id: str, document_contents_id: str, is_relevant: bool = True
) -> LLMJobDocumentExtractionResult:
    return LLMJobDocumentExtractionResult.for_success(
        job_id=job_id,
        raw_result=_client_result(document_contents_id),
        result_datetime_utc=_NOW,
        validation_results=_validation_result(
            validated_output_json={RESULT_KEY: {IS_RELEVANT_FIELD_NAME: is_relevant}},
            result_type_override=None,
        ),
    )


def _failure_result(
    *,
    job_id: str,
    document_contents_id: str,
    result_type: LLMExtractionJobDocumentResultType,
    error_type: LLMDocumentExtractionErrorType,
) -> LLMJobDocumentExtractionResult:
    return LLMJobDocumentExtractionResult.for_failed_request(
        job_id=job_id,
        raw_result=_client_result(
            document_contents_id, error_type=LLMRequestErrorType.RATE_LIMITED
        ),
        result_datetime_utc=_NOW,
        result_type=result_type,
        error_type=error_type,
    )


class LLMJobDocumentExtractionResultFactoryTest(unittest.TestCase):
    """Tests the classification/construction the LLMJobDocumentExtractionResult
    factories encapsulate — no Postgres involved."""

    def test_for_success(self) -> None:
        # is_relevant is taken from the validated output, not passed in.
        for is_relevant in (True, False):
            with self.subTest(is_relevant=is_relevant):
                result = LLMJobDocumentExtractionResult.for_success(
                    job_id="job1",
                    raw_result=_client_result("doc1"),
                    result_datetime_utc=_NOW,
                    validation_results=_validation_result(
                        validated_output_json={"result": {"is_relevant": is_relevant}},
                        result_type_override=None,
                    ),
                )
                self.assertEqual(
                    LLMExtractionJobDocumentResultType.SUCCESS, result.result_type
                )
                self.assertEqual(is_relevant, result.is_relevant)
                self.assertIsNone(result.error_type)
                self.assertIsNone(result.error_message)
                self.assertTrue(result.is_validated_result)

    def test_for_success_requires_validated_output(self) -> None:
        with self.assertRaisesRegex(ValueError, r"requires validated output"):
            LLMJobDocumentExtractionResult.for_success(
                job_id="job1",
                raw_result=_client_result("doc1"),
                result_datetime_utc=_NOW,
                validation_results=_validation_result(
                    validated_output_json=None,
                    result_type_override=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
                    audit_issues=[
                        ValidationIssue(
                            check_type=ValidationCheckType.SCHEMA_CONFORMANCE,
                            field_name=None,
                            detail="bad",
                        )
                    ],
                ),
            )

    def test_for_failed_validation(self) -> None:
        result = LLMJobDocumentExtractionResult.for_failed_validation(
            job_id="job1",
            raw_result=_client_result("doc1"),
            result_datetime_utc=_NOW,
            result_type=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
            validation_results=_validation_result(
                validated_output_json=None,
                result_type_override=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
                audit_issues=[
                    ValidationIssue(
                        check_type=ValidationCheckType.SCHEMA_CONFORMANCE,
                        field_name=None,
                        detail="bad",
                    )
                ],
            ),
        )
        self.assertEqual(
            LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
            result.result_type,
        )
        self.assertIsNone(result.is_relevant)
        self.assertIsNone(result.error_type)
        self.assertIsNone(result.error_message)
        self.assertFalse(result.is_validated_result)

    def test_for_failed_validation_requires_failed_validation(self) -> None:
        # A validation result that passed is not a failure — so the factory refuses
        # it rather than silently stamping a SUCCESS.
        with self.assertRaisesRegex(ValueError, r"requires a failed validation result"):
            LLMJobDocumentExtractionResult.for_failed_validation(
                job_id="job1",
                raw_result=_client_result("doc1"),
                result_datetime_utc=_NOW,
                result_type=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
                validation_results=_validation_result(
                    validated_output_json={"result": {"is_relevant": True}},
                    result_type_override=None,
                ),
            )

    def test_for_failed_request(self) -> None:
        # The caller supplies the classified result/error types; the factory stamps
        # them, carries the message from the raw result, and leaves nothing
        # validated. (The request-error -> type classification itself lives in, and
        # is tested by, LLMExtractionResultProcessor.)
        result = LLMJobDocumentExtractionResult.for_failed_request(
            job_id="job1",
            raw_result=_client_result(
                "doc1", error_type=LLMRequestErrorType.RATE_LIMITED
            ),
            result_datetime_utc=_NOW,
            result_type=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
            error_type=LLMDocumentExtractionErrorType.LLM_REQUEST_RATE_LIMITED,
        )
        self.assertEqual(
            LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
            result.result_type,
        )
        self.assertEqual(
            LLMDocumentExtractionErrorType.LLM_REQUEST_RATE_LIMITED, result.error_type
        )
        self.assertEqual("error for doc1", result.error_message)
        self.assertIsNone(result.is_relevant)
        self.assertIsNone(result.validation_results)

    def test_for_failed_request_requires_error_result(self) -> None:
        with self.assertRaisesRegex(ValueError, r"requires an error raw_result"):
            LLMJobDocumentExtractionResult.for_failed_request(
                job_id="job1",
                raw_result=_client_result("doc1"),
                result_datetime_utc=_NOW,
                result_type=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT,
                error_type=LLMDocumentExtractionErrorType.LLM_REQUEST_UNKNOWN_ERROR,
            )


@pytest.mark.uses_db
class LLMExtractionJobManagerTest(unittest.TestCase):
    """Tests for LLMExtractionJobManager."""

    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    def setUp(self) -> None:
        self.manager = LLMExtractionJobManager()
        self.database_key = SQLAlchemyDatabaseKey.for_schema(
            self.manager.database_key.schema_type
        )
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.database_key
        )
        self._seed_extractor_version_chain()

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def _seed_extractor_version_chain(self) -> None:
        """Seeds the collection → extractor → extractor_version parent rows the
        job and eligible-document tables' foreign keys require."""
        with SessionFactory.using_database(self.database_key) as session:
            session.add(
                schema.LLMExtractorCollection(
                    collection_name="CASE_NOTE_EMPLOYMENT_INFO",
                    collection_version_id="cv1",
                    output_schema_version="sv1",
                    output_schema_json='{"type":"object"}',
                    description="desc",
                    minimum_confidence_level="inferred",
                    row_creation_datetime_utc=_NOW,
                )
            )
            session.add(
                schema.LLMExtractor(
                    state_code=_STATE.value,
                    extractor_id="US_XX_CASE_NOTE_EMPLOYMENT_INFO",
                    extractor_collection_name="CASE_NOTE_EMPLOYMENT_INFO",
                    input_document_collection_name="case_notes",
                    row_creation_datetime_utc=_NOW,
                )
            )
            session.flush()
            session.add(
                schema.LLMExtractorVersion(
                    state_code=_STATE.value,
                    extractor_version_id=_EXTRACTOR_VERSION,
                    extractor_id="US_XX_CASE_NOTE_EMPLOYMENT_INFO",
                    extractor_collection_name="CASE_NOTE_EMPLOYMENT_INFO",
                    extractor_collection_version_id="cv1",
                    instructions_prompt="prompt",
                    instructions_prompt_hash="hash",
                    model_config_name="model",
                    model_config_version_id="mv1",
                    invalidated_datetime_utc=None,
                    invalidation_reason=None,
                    row_creation_datetime_utc=_NOW,
                )
            )

    def _record_eligible(self, document_contents_ids: list[str]) -> None:
        self.manager.record_eligible_documents(
            state_code=_STATE,
            extractor_version_id=_EXTRACTOR_VERSION,
            document_filter_id=_FILTER,
            eligible_documents=[_eligible_record(d) for d in document_contents_ids],
        )

    def _count_rows(self, schema_class: Any) -> int:
        with SessionFactory.using_database(self.database_key) as session:
            return session.query(schema_class).count()

    def test_record_eligible_documents_idempotent(self) -> None:
        # First recording writes both eligible and metadata rows; re-recording
        # the same documents (even under a second filter) adds no metadata
        # duplicates and no eligible duplicates for the same (version, filter).
        self._record_eligible(["doc1", "doc2"])
        self._record_eligible(["doc1", "doc2"])

        self.assertEqual(2, self._count_rows(schema.LLMExtractionEligibleDocument))
        self.assertEqual(
            2, self._count_rows(schema.LLMExtractionEligibleDocumentMetadata)
        )

        # A new filter over the same documents adds new eligible rows but reuses
        # the existing (write-once) metadata rows.
        self.manager.record_eligible_documents(
            state_code=_STATE,
            extractor_version_id=_EXTRACTOR_VERSION,
            document_filter_id="filter2",
            eligible_documents=[_eligible_record("doc1"), _eligible_record("doc3")],
        )
        self.assertEqual(4, self._count_rows(schema.LLMExtractionEligibleDocument))
        self.assertEqual(
            3, self._count_rows(schema.LLMExtractionEligibleDocumentMetadata)
        )

    def test_record_eligible_documents_empty_is_noop(self) -> None:
        self.manager.record_eligible_documents(
            state_code=_STATE,
            extractor_version_id=_EXTRACTOR_VERSION,
            document_filter_id=_FILTER,
            eligible_documents=[],
        )
        self.assertEqual(0, self._count_rows(schema.LLMExtractionEligibleDocument))

    def test_create_and_round_trip_job_and_documents(self) -> None:
        self._record_eligible(["doc1", "doc2"])
        job = self.manager.create_job(
            state_code=_STATE,
            extractor_version_id=_EXTRACTOR_VERSION,
            document_contents_ids=["doc1", "doc2"],
        )

        self.assertEqual(_STATE.value, job.state_code)
        self.assertEqual(_EXTRACTOR_VERSION, job.extractor_version_id)
        self.assertIsNone(job.start_datetime_utc)
        self.assertIsNone(job.completion_datetime_utc)
        self.assertIsNone(job.result_type)

        pending = self.manager.get_pending_job_documents(
            state_code=_STATE, job_id=job.job_id
        )
        self.assertEqual({"doc1", "doc2"}, {d.document_contents_id for d in pending})
        # Documents are written with identity only — no results yet, job_index
        # assigned in the given order.
        self.assertEqual({0, 1}, {d.job_index for d in pending})
        for document in pending:
            self.assertIsNone(document.result_type)
            self.assertIsNone(document.result_datetime_utc)
            self.assertIsNone(document.batch_index)

    def test_create_job_requires_documents(self) -> None:
        with self.assertRaisesRegex(ValueError, r"Cannot create a job with no docume"):
            self.manager.create_job(
                state_code=_STATE,
                extractor_version_id=_EXTRACTOR_VERSION,
                document_contents_ids=[],
            )

    def test_get_open_job(self) -> None:
        self.assertIsNone(
            self.manager.get_open_job(
                state_code=_STATE, extractor_version_id=_EXTRACTOR_VERSION
            )
        )

        self._record_eligible(["doc1"])
        created = self.manager.create_job(
            state_code=_STATE,
            extractor_version_id=_EXTRACTOR_VERSION,
            document_contents_ids=["doc1"],
        )
        open_job = self.manager.get_open_job(
            state_code=_STATE, extractor_version_id=_EXTRACTOR_VERSION
        )
        self.assertIsNotNone(open_job)
        assert open_job is not None
        self.assertEqual(created.job_id, open_job.job_id)

        # Once completed, there is no open job for the version.
        self.manager.mark_job_started(state_code=_STATE, job_id=created.job_id)
        self.manager.set_job_document_results(
            state_code=_STATE,
            results=[
                _success_result(job_id=created.job_id, document_contents_id="doc1")
            ],
        )
        self.manager.mark_job_completed(state_code=_STATE, job_id=created.job_id)
        self.assertIsNone(
            self.manager.get_open_job(
                state_code=_STATE, extractor_version_id=_EXTRACTOR_VERSION
            )
        )

    def test_mark_job_started_is_idempotent(self) -> None:
        self._record_eligible(["doc1"])
        job = self.manager.create_job(
            state_code=_STATE,
            extractor_version_id=_EXTRACTOR_VERSION,
            document_contents_ids=["doc1"],
        )

        self.manager.mark_job_started(state_code=_STATE, job_id=job.job_id)
        first_start = self._get_job_row(job.job_id).start_datetime_utc
        self.assertIsNotNone(first_start)

        # A second call on an already-started (resumed) job is a no-op — it does
        # not overwrite the original start time.
        self.manager.mark_job_started(state_code=_STATE, job_id=job.job_id)
        self.assertEqual(first_start, self._get_job_row(job.job_id).start_datetime_utc)

    def test_set_job_document_result_persists(self) -> None:
        self._record_eligible(["doc1"])
        job = self.manager.create_job(
            state_code=_STATE,
            extractor_version_id=_EXTRACTOR_VERSION,
            document_contents_ids=["doc1"],
        )
        self.manager.set_job_document_results(
            state_code=_STATE,
            results=[_success_result(job_id=job.job_id, document_contents_id="doc1")],
        )

        document = self._get_job_document_row(job.job_id, "doc1")
        self.assertEqual(
            LLMExtractionJobDocumentResultType.SUCCESS, document.result_type
        )
        self.assertTrue(document.is_relevant)
        self.assertEqual(_NOW, document.result_datetime_utc)
        self.assertEqual(10, document.input_token_count)
        self.assertEqual(5, document.output_token_count)
        self.assertEqual(2, document.cached_input_token_count)
        self.assertEqual(1, document.thinking_token_count)
        self.assertIsNone(document.error_type)
        self.assertIsNone(document.error_message)

        # A processed document is no longer pending.
        self.assertEqual(
            [],
            self.manager.get_pending_job_documents(
                state_code=_STATE, job_id=job.job_id
            ),
        )

    def test_set_job_document_result_unknown_document_raises(self) -> None:
        self._record_eligible(["doc1"])
        job = self.manager.create_job(
            state_code=_STATE,
            extractor_version_id=_EXTRACTOR_VERSION,
            document_contents_ids=["doc1"],
        )
        with self.assertRaisesRegex(ValueError, r"No job document found"):
            self.manager.set_job_document_results(
                state_code=_STATE,
                results=[
                    _success_result(job_id=job.job_id, document_contents_id="not_a_doc")
                ],
            )

    def test_get_document_contents_ids_needing_processing(self) -> None:
        # doc_new: never processed. doc_transient: transient failure (retryable).
        # doc_success / doc_permanent / doc_exhausted: terminal, excluded.
        eligible = [
            "doc_new",
            "doc_transient",
            "doc_success",
            "doc_permanent",
            "doc_exhausted",
        ]
        self._record_eligible(eligible)
        job = self.manager.create_job(
            state_code=_STATE,
            extractor_version_id=_EXTRACTOR_VERSION,
            document_contents_ids=eligible,
        )

        self.manager.set_job_document_results(
            state_code=_STATE,
            results=[
                _failure_result(
                    job_id=job.job_id,
                    document_contents_id="doc_transient",
                    result_type=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
                    error_type=LLMDocumentExtractionErrorType.LLM_REQUEST_RATE_LIMITED,
                )
            ],
        )
        self.manager.set_job_document_results(
            state_code=_STATE,
            results=[
                _success_result(job_id=job.job_id, document_contents_id="doc_success")
            ],
        )
        self.manager.set_job_document_results(
            state_code=_STATE,
            results=[
                _failure_result(
                    job_id=job.job_id,
                    document_contents_id="doc_permanent",
                    result_type=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT,
                    error_type=LLMDocumentExtractionErrorType.LLM_REQUEST_CONTENT_FILTERED,
                )
            ],
        )
        self.manager.set_job_document_results(
            state_code=_STATE,
            results=[
                _failure_result(
                    job_id=job.job_id,
                    document_contents_id="doc_exhausted",
                    result_type=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_RETRIES_EXHAUSTED,
                    error_type=LLMDocumentExtractionErrorType.LLM_REQUEST_RATE_LIMITED,
                )
            ],
        )

        needing = self.manager.get_document_contents_ids_needing_processing(
            state_code=_STATE, extractor_version_id=_EXTRACTOR_VERSION
        )
        self.assertEqual({"doc_new", "doc_transient"}, set(needing))

    def test_needing_processing_scoped_to_version(self) -> None:
        # A terminal result under a different extractor version does not exclude
        # the document from the version under test.
        self._record_eligible(["doc1"])
        other_version = self._seed_second_extractor_version("ev2")
        self.manager.record_eligible_documents(
            state_code=_STATE,
            extractor_version_id=other_version,
            document_filter_id=_FILTER,
            eligible_documents=[_eligible_record("doc1")],
        )
        other_job = self.manager.create_job(
            state_code=_STATE,
            extractor_version_id=other_version,
            document_contents_ids=["doc1"],
        )
        self.manager.set_job_document_results(
            state_code=_STATE,
            results=[
                _success_result(job_id=other_job.job_id, document_contents_id="doc1")
            ],
        )

        needing = self.manager.get_document_contents_ids_needing_processing(
            state_code=_STATE, extractor_version_id=_EXTRACTOR_VERSION
        )
        self.assertEqual(["doc1"], needing)

    def test_set_job_document_results_batch_persists_all(self) -> None:
        # A single batched call persists a success and a failure to their
        # respective rows, and leaves both no longer pending.
        self._record_eligible(["doc_ok", "doc_bad"])
        job = self.manager.create_job(
            state_code=_STATE,
            extractor_version_id=_EXTRACTOR_VERSION,
            document_contents_ids=["doc_ok", "doc_bad"],
        )
        self.manager.set_job_document_results(
            state_code=_STATE,
            results=[
                _success_result(job_id=job.job_id, document_contents_id="doc_ok"),
                _failure_result(
                    job_id=job.job_id,
                    document_contents_id="doc_bad",
                    result_type=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT,
                    error_type=LLMDocumentExtractionErrorType.LLM_REQUEST_CONTENT_FILTERED,
                ),
            ],
        )

        self.assertEqual(
            LLMExtractionJobDocumentResultType.SUCCESS,
            self._get_job_document_row(job.job_id, "doc_ok").result_type,
        )
        self.assertEqual(
            LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT,
            self._get_job_document_row(job.job_id, "doc_bad").result_type,
        )
        self.assertEqual(
            [],
            self.manager.get_pending_job_documents(
                state_code=_STATE, job_id=job.job_id
            ),
        )

    def test_set_job_document_results_unknown_document_rolls_back_batch(self) -> None:
        # If any result in the batch has no matching row, the whole batch rolls
        # back — the valid result's row is not persisted either.
        self._record_eligible(["doc_ok"])
        job = self.manager.create_job(
            state_code=_STATE,
            extractor_version_id=_EXTRACTOR_VERSION,
            document_contents_ids=["doc_ok"],
        )
        with self.assertRaisesRegex(ValueError, r"No job document found"):
            self.manager.set_job_document_results(
                state_code=_STATE,
                results=[
                    _success_result(job_id=job.job_id, document_contents_id="doc_ok"),
                    _success_result(
                        job_id=job.job_id, document_contents_id="not_a_doc"
                    ),
                ],
            )

        # The valid result was rolled back with the failing one.
        self.assertEqual(
            ["doc_ok"],
            [
                d.document_contents_id
                for d in self.manager.get_pending_job_documents(
                    state_code=_STATE, job_id=job.job_id
                )
            ],
        )

    def test_set_job_document_results_empty_is_noop(self) -> None:
        self.manager.set_job_document_results(state_code=_STATE, results=[])

    def test_mark_job_completed_derives_success_vs_partial_failure(self) -> None:
        # All documents succeed → SUCCESS.
        self._record_eligible(["doc1", "doc2"])
        success_job = self.manager.create_job(
            state_code=_STATE,
            extractor_version_id=_EXTRACTOR_VERSION,
            document_contents_ids=["doc1", "doc2"],
        )
        self.manager.mark_job_started(state_code=_STATE, job_id=success_job.job_id)
        for doc in ["doc1", "doc2"]:
            self.manager.set_job_document_results(
                state_code=_STATE,
                results=[
                    _success_result(job_id=success_job.job_id, document_contents_id=doc)
                ],
            )
        self.manager.mark_job_completed(state_code=_STATE, job_id=success_job.job_id)
        completed = self._get_job_row(success_job.job_id)
        self.assertEqual(LLMExtractionJobResultType.SUCCESS, completed.result_type)
        self.assertIsNotNone(completed.completion_datetime_utc)
        self.assertIsNone(completed.error_message)

        # At least one document fails → PARTIAL_FAILURE.
        version = self._seed_second_extractor_version("ev2")
        self.manager.record_eligible_documents(
            state_code=_STATE,
            extractor_version_id=version,
            document_filter_id=_FILTER,
            eligible_documents=[_eligible_record("doc3"), _eligible_record("doc4")],
        )
        partial_job = self.manager.create_job(
            state_code=_STATE,
            extractor_version_id=version,
            document_contents_ids=["doc3", "doc4"],
        )
        self.manager.mark_job_started(state_code=_STATE, job_id=partial_job.job_id)
        self.manager.set_job_document_results(
            state_code=_STATE,
            results=[
                _success_result(job_id=partial_job.job_id, document_contents_id="doc3")
            ],
        )
        self.manager.set_job_document_results(
            state_code=_STATE,
            results=[
                _failure_result(
                    job_id=partial_job.job_id,
                    document_contents_id="doc4",
                    result_type=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT,
                    error_type=LLMDocumentExtractionErrorType.LLM_REQUEST_MALFORMED_RESPONSE,
                )
            ],
        )
        self.manager.mark_job_completed(state_code=_STATE, job_id=partial_job.job_id)
        self.assertEqual(
            LLMExtractionJobResultType.PARTIAL_FAILURE,
            self._get_job_row(partial_job.job_id).result_type,
        )
        failed_document = self._get_job_document_row(partial_job.job_id, "doc4")
        self.assertEqual(
            LLMDocumentExtractionErrorType.LLM_REQUEST_MALFORMED_RESPONSE,
            failed_document.error_type,
        )
        self.assertEqual("error for doc4", failed_document.error_message)

    def test_mark_job_failed(self) -> None:
        self._record_eligible(["doc1"])
        job = self.manager.create_job(
            state_code=_STATE,
            extractor_version_id=_EXTRACTOR_VERSION,
            document_contents_ids=["doc1"],
        )
        self.manager.mark_job_failed(
            state_code=_STATE, job_id=job.job_id, error_message="the whole job died"
        )
        row = self._get_job_row(job.job_id)
        self.assertEqual(LLMExtractionJobResultType.FAILURE, row.result_type)
        self.assertEqual("the whole job died", row.error_message)
        self.assertIsNotNone(row.completion_datetime_utc)

        # A job's unprocessed documents remain selectable after a job-level
        # failure (no terminal result was written for them).
        needing = self.manager.get_document_contents_ids_needing_processing(
            state_code=_STATE, extractor_version_id=_EXTRACTOR_VERSION
        )
        self.assertEqual(["doc1"], needing)

    def test_close_already_closed_job_raises(self) -> None:
        self._record_eligible(["doc1"])
        job = self.manager.create_job(
            state_code=_STATE,
            extractor_version_id=_EXTRACTOR_VERSION,
            document_contents_ids=["doc1"],
        )
        self.manager.mark_job_failed(
            state_code=_STATE, job_id=job.job_id, error_message="died"
        )
        with self.assertRaisesRegex(ValueError, r"No open job found to close"):
            self.manager.mark_job_failed(
                state_code=_STATE, job_id=job.job_id, error_message="again"
            )

    def _seed_second_extractor_version(self, extractor_version_id: str) -> str:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(
                schema.LLMExtractorVersion(
                    state_code=_STATE.value,
                    extractor_version_id=extractor_version_id,
                    extractor_id="US_XX_CASE_NOTE_EMPLOYMENT_INFO",
                    extractor_collection_name="CASE_NOTE_EMPLOYMENT_INFO",
                    extractor_collection_version_id="cv1",
                    instructions_prompt="prompt",
                    instructions_prompt_hash="hash2",
                    model_config_name="model",
                    model_config_version_id="mv1",
                    invalidated_datetime_utc=None,
                    invalidation_reason=None,
                    row_creation_datetime_utc=_NOW,
                )
            )
        return extractor_version_id

    def _get_job_row(self, job_id: str) -> LLMExtractionJob:
        with SessionFactory.using_database(self.database_key) as session:
            row = (
                session.query(schema.LLMExtractionJob)
                .filter(
                    schema.LLMExtractionJob.state_code == _STATE.value,
                    schema.LLMExtractionJob.job_id == job_id,
                )
                .one()
            )
            return convert_schema_object_to_entity(
                row, LLMExtractionJob, populate_direct_back_edges=False
            )

    def _get_job_document_row(
        self, job_id: str, document_contents_id: str
    ) -> LLMExtractionJobDocument:
        with SessionFactory.using_database(self.database_key) as session:
            row = (
                session.query(schema.LLMExtractionJobDocument)
                .filter(
                    schema.LLMExtractionJobDocument.state_code == _STATE.value,
                    schema.LLMExtractionJobDocument.job_id == job_id,
                    schema.LLMExtractionJobDocument.document_contents_id
                    == document_contents_id,
                )
                .one()
            )
            return convert_schema_object_to_entity(
                row, LLMExtractionJobDocument, populate_direct_back_edges=False
            )
