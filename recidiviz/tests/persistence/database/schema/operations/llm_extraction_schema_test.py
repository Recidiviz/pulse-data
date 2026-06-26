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
"""Tests for the LLM extraction tables defined in operations/schema.py."""

import datetime
import re
import unittest
from typing import Any

import pytest
import pytz
from sqlalchemy.exc import IntegrityError

from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMExtractionJobDocumentResultType,
    LLMExtractionJobResultType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult

_STATE = StateCode.US_XX.value
_NOW = datetime.datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
_LATER = datetime.datetime(2026, 1, 1, 13, 0, tzinfo=pytz.UTC)


def _collection(**overrides: Any) -> schema.LLMExtractorCollection:
    return schema.LLMExtractorCollection(
        collection_name=overrides.pop("collection_name", "CASE_NOTE_EMPLOYMENT_INFO"),
        collection_version_id=overrides.pop("collection_version_id", "cv1"),
        output_schema_version="sv1",
        output_schema_json='{"type":"object"}',
        description="desc",
        minimum_confidence_level="inferred",
        row_creation_datetime_utc=_NOW,
        **overrides,
    )


def _extractor(**overrides: Any) -> schema.LLMExtractor:
    return schema.LLMExtractor(
        state_code=overrides.pop("state_code", _STATE),
        extractor_id=overrides.pop("extractor_id", "US_XX_CASE_NOTE_EMPLOYMENT_INFO"),
        extractor_collection_name=overrides.pop(
            "extractor_collection_name", "CASE_NOTE_EMPLOYMENT_INFO"
        ),
        input_document_collection_name="case_notes",
        row_creation_datetime_utc=_NOW,
        **overrides,
    )


def _extractor_version(**overrides: Any) -> schema.LLMExtractorVersion:
    return schema.LLMExtractorVersion(
        state_code=overrides.pop("state_code", _STATE),
        extractor_version_id=overrides.pop("extractor_version_id", "ev1"),
        extractor_id=overrides.pop("extractor_id", "US_XX_CASE_NOTE_EMPLOYMENT_INFO"),
        extractor_collection_name=overrides.pop(
            "extractor_collection_name", "CASE_NOTE_EMPLOYMENT_INFO"
        ),
        extractor_collection_version_id=overrides.pop(
            "extractor_collection_version_id", "cv1"
        ),
        instructions_prompt="prompt",
        instructions_prompt_hash="hash",
        model_config_name="model",
        model_config_version_id="mv1",
        invalidated_datetime_utc=overrides.pop("invalidated_datetime_utc", None),
        invalidation_reason=overrides.pop("invalidation_reason", None),
        row_creation_datetime_utc=_NOW,
        **overrides,
    )


def _eligible_metadata(
    **overrides: Any,
) -> schema.LLMExtractionEligibleDocumentMetadata:
    return schema.LLMExtractionEligibleDocumentMetadata(
        state_code=overrides.pop("state_code", _STATE),
        document_contents_id=overrides.pop("document_contents_id", "doc1"),
        char_count=100,
        document_update_datetime=_NOW,
        row_creation_datetime_utc=_NOW,
        **overrides,
    )


def _job(**overrides: Any) -> schema.LLMExtractionJob:
    return schema.LLMExtractionJob(
        state_code=overrides.pop("state_code", _STATE),
        job_id=overrides.pop("job_id", "job1"),
        extractor_version_id=overrides.pop("extractor_version_id", "ev1"),
        start_datetime_utc=overrides.pop("start_datetime_utc", None),
        completion_datetime_utc=overrides.pop("completion_datetime_utc", None),
        result_type=overrides.pop("result_type", None),
        error_message=overrides.pop("error_message", None),
        **overrides,
    )


@pytest.mark.uses_db
class LLMExtractionSchemaTest(unittest.TestCase):
    """Tests for the LLM extraction tables in operations/schema.py."""

    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.database_key
        )

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def _seed_chain(self, session: Any) -> None:
        """Seed one valid row for each parent table referenced by the FK tests."""
        session.add(_collection())
        session.add(_extractor())
        session.flush()
        session.add(_extractor_version())
        session.add(_eligible_metadata())
        session.flush()

    def test_happy_path_all_tables(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._seed_chain(session)

            session.add(
                schema.LLMExtractorDocumentFilter(
                    state_code=_STATE,
                    extractor_id="US_XX_CASE_NOTE_EMPLOYMENT_INFO",
                    document_filter_id="f1",
                    document_metadata_filter_query_template="SELECT id FROM `{project_id}.t`",
                    row_creation_datetime_utc=_NOW,
                )
            )
            session.add(
                schema.LLMExtractionEligibleDocument(
                    state_code=_STATE,
                    extractor_version_id="ev1",
                    document_filter_id="f1",
                    document_contents_id="doc1",
                    row_creation_datetime_utc=_NOW,
                )
            )
            session.add(_job(start_datetime_utc=_NOW))
            session.flush()
            session.add(
                schema.LLMExtractionBatchJobMetadata(
                    state_code=_STATE,
                    job_id="job1",
                    batch_index=0,
                    external_job_id="x1",
                    external_job_provider="VERTEX_AI_BATCH",
                    gcs_input_uri="gs://bucket/in.jsonl",
                    gcs_output_uri=None,
                    row_creation_datetime_utc=_NOW,
                )
            )
            session.flush()
            session.add(
                schema.LLMExtractionJobDocument(
                    state_code=_STATE,
                    job_id="job1",
                    document_contents_id="doc1",
                    batch_index=0,
                    job_index=0,
                    result_datetime_utc=None,
                    result_type=None,
                    is_relevant=None,
                    error_message=None,
                    input_token_count=None,
                    output_token_count=None,
                    cached_input_token_count=None,
                    thinking_token_count=None,
                )
            )
            session.add(
                schema.LLMExtractionCapOverride(
                    state_code=_STATE,
                    extractor_version_id="ev1",
                    document_filter_id="f1",
                    override_document_count_cap=1_000_000,
                    expires_datetime_utc=_LATER,
                    row_creation_datetime_utc=_NOW,
                )
            )
            session.commit()

            # Complete the job successfully and finalize the job document with
            # populated result fields, exercising the positive side of every
            # iff-style check on these tables.
            (
                session.query(schema.LLMExtractionJob)
                .filter_by(job_id="job1")
                .update(
                    {
                        "completion_datetime_utc": _LATER,
                        "result_type": LLMExtractionJobResultType.SUCCESS.value,
                    }
                )
            )
            (
                session.query(schema.LLMExtractionJobDocument)
                .filter_by(job_id="job1", document_contents_id="doc1")
                .update(
                    {
                        "result_datetime_utc": _LATER,
                        "result_type": LLMExtractionJobDocumentResultType.SUCCESS.value,
                        "is_relevant": True,
                        "input_token_count": 100,
                        "output_token_count": 50,
                        "cached_input_token_count": 10,
                        "thinking_token_count": 0,
                    }
                )
            )
            session.commit()

    def _assert_fk_violation(self, _session: Any) -> Any:
        return self.assertRaisesRegex(
            IntegrityError, r"\(psycopg2\.errors\.ForeignKeyViolation\).*"
        )

    def _assert_check_violation(self, _session: Any, constraint_name: str) -> Any:
        return self.assertRaisesRegex(
            IntegrityError,
            r"\(psycopg2\.errors\.CheckViolation\).*"
            + re.escape(f'violates check constraint "{constraint_name}"'),
        )

    def test_extractor_version_fks_and_invalidation_check(self) -> None:
        # Missing collection FK
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(_extractor())
            session.flush()
            session.add(_extractor_version())
            with self._assert_fk_violation(session):
                session.commit()

        # Missing extractor FK
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(_collection())
            session.flush()
            session.add(_extractor_version())
            with self._assert_fk_violation(session):
                session.commit()

        # invalidated_datetime_utc set without reason
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(_collection())
            session.add(_extractor())
            session.flush()
            session.add(
                _extractor_version(
                    invalidated_datetime_utc=_NOW, invalidation_reason=None
                )
            )
            with self._assert_check_violation(
                session, "llm_extractor_version_invalidation_fields_consistent"
            ):
                session.commit()

        # reason set without invalidated_datetime_utc
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(_collection())
            session.add(_extractor())
            session.flush()
            session.add(
                _extractor_version(
                    invalidated_datetime_utc=None, invalidation_reason="why"
                )
            )
            with self._assert_check_violation(
                session, "llm_extractor_version_invalidation_fields_consistent"
            ):
                session.commit()

    def test_extractor_document_filter_extractor_fk(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(
                schema.LLMExtractorDocumentFilter(
                    state_code=_STATE,
                    extractor_id="MISSING",
                    document_filter_id="f1",
                    document_metadata_filter_query_template="SELECT 1",
                    row_creation_datetime_utc=_NOW,
                )
            )
            with self._assert_fk_violation(session):
                session.commit()

    def test_eligible_documents_fks(self) -> None:
        # Missing extractor_version FK
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(_eligible_metadata())
            session.flush()
            session.add(
                schema.LLMExtractionEligibleDocument(
                    state_code=_STATE,
                    extractor_version_id="ev_missing",
                    document_filter_id="f1",
                    document_contents_id="doc1",
                    row_creation_datetime_utc=_NOW,
                )
            )
            with self._assert_fk_violation(session):
                session.commit()

        # Missing eligible_documents_metadata FK
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(_collection())
            session.add(_extractor())
            session.flush()
            session.add(_extractor_version())
            session.flush()
            session.add(
                schema.LLMExtractionEligibleDocument(
                    state_code=_STATE,
                    extractor_version_id="ev1",
                    document_filter_id="f1",
                    document_contents_id="doc_missing",
                    row_creation_datetime_utc=_NOW,
                )
            )
            with self._assert_fk_violation(session):
                session.commit()

    def test_job_extractor_version_fk(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(_job())
            with self._assert_fk_violation(session):
                session.commit()

    def test_job_only_one_open_per_extractor_version(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._seed_chain(session)
            session.add(_job(job_id="job1", start_datetime_utc=_NOW))
            session.commit()

            # Second open job with same (state_code, extractor_version_id) — blocked
            session.add(_job(job_id="job2", start_datetime_utc=_NOW))
            with self.assertRaisesRegex(
                IntegrityError,
                re.escape(
                    '(psycopg2.errors.UniqueViolation) duplicate key value violates unique constraint "one_open_llm_extraction_job_per_extractor_version"'
                ),
            ):
                session.commit()
            session.rollback()

            # Completing the first job lets a second open job exist
            (
                session.query(schema.LLMExtractionJob)
                .filter_by(job_id="job1")
                .update(
                    {
                        "completion_datetime_utc": _LATER,
                        "result_type": LLMExtractionJobResultType.SUCCESS.value,
                    }
                )
            )
            session.commit()
            session.add(_job(job_id="job2", start_datetime_utc=_LATER))
            session.commit()

    def test_job_completion_and_result_type_consistent(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._seed_chain(session)

            # completion set but result_type null
            session.add(
                _job(
                    job_id="bad1",
                    start_datetime_utc=_NOW,
                    completion_datetime_utc=_LATER,
                    result_type=None,
                )
            )
            with self._assert_check_violation(
                session, "llm_extraction_job_completion_and_result_type_consistent"
            ):
                session.commit()
            session.rollback()

            # result_type set but completion null
            session.add(
                _job(
                    job_id="bad2",
                    start_datetime_utc=_NOW,
                    completion_datetime_utc=None,
                    result_type=LLMExtractionJobResultType.SUCCESS.value,
                )
            )
            with self._assert_check_violation(
                session, "llm_extraction_job_completion_and_result_type_consistent"
            ):
                session.commit()

    def test_job_completion_requires_start(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._seed_chain(session)
            session.add(
                _job(
                    job_id="bad1",
                    start_datetime_utc=None,
                    completion_datetime_utc=_LATER,
                    result_type=LLMExtractionJobResultType.SUCCESS.value,
                )
            )
            with self._assert_check_violation(
                session, "llm_extraction_job_completion_requires_start"
            ):
                session.commit()

    def test_job_completion_after_start(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._seed_chain(session)
            session.add(
                _job(
                    job_id="bad1",
                    start_datetime_utc=_LATER,
                    completion_datetime_utc=_NOW,
                    result_type=LLMExtractionJobResultType.SUCCESS.value,
                )
            )
            with self._assert_check_violation(
                session, "llm_extraction_job_completion_after_start"
            ):
                session.commit()

    def test_job_error_message_only_for_failure(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._seed_chain(session)
            session.add(
                _job(
                    job_id="bad1",
                    start_datetime_utc=_NOW,
                    completion_datetime_utc=_LATER,
                    result_type=LLMExtractionJobResultType.SUCCESS.value,
                    error_message="boom",
                )
            )
            with self._assert_check_violation(
                session, "llm_extraction_job_error_message_only_for_failure"
            ):
                session.commit()

    def test_batch_job_metadata_job_fk(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(
                schema.LLMExtractionBatchJobMetadata(
                    state_code=_STATE,
                    job_id="missing",
                    batch_index=0,
                    external_job_id="x1",
                    external_job_provider="VERTEX_AI_BATCH",
                    gcs_input_uri="gs://bucket/in.jsonl",
                    gcs_output_uri=None,
                    row_creation_datetime_utc=_NOW,
                )
            )
            with self._assert_fk_violation(session):
                session.commit()

    def test_job_documents_fks_and_unique(self) -> None:
        # job_id FK missing
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(
                schema.LLMExtractionJobDocument(
                    state_code=_STATE,
                    job_id="missing",
                    document_contents_id="doc1",
                    batch_index=None,
                    job_index=0,
                )
            )
            with self._assert_fk_violation(session):
                session.commit()

        # batch_index FK missing
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._seed_chain(session)
            session.add(_job(start_datetime_utc=_NOW))
            session.flush()
            session.add(
                schema.LLMExtractionJobDocument(
                    state_code=_STATE,
                    job_id="job1",
                    document_contents_id="doc1",
                    batch_index=99,
                    job_index=0,
                )
            )
            with self._assert_fk_violation(session):
                session.commit()

        # Unique constraint on (state_code, job_id, batch_index, job_index)
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._seed_chain(session)
            session.add(_eligible_metadata(document_contents_id="doc2"))
            session.add(_job(start_datetime_utc=_NOW))
            session.flush()
            session.add(
                schema.LLMExtractionBatchJobMetadata(
                    state_code=_STATE,
                    job_id="job1",
                    batch_index=0,
                    external_job_id="x1",
                    external_job_provider="VERTEX_AI_BATCH",
                    gcs_input_uri="gs://b/in.jsonl",
                    gcs_output_uri=None,
                    row_creation_datetime_utc=_NOW,
                )
            )
            session.flush()
            session.add(
                schema.LLMExtractionJobDocument(
                    state_code=_STATE,
                    job_id="job1",
                    document_contents_id="doc1",
                    batch_index=0,
                    job_index=0,
                )
            )
            session.commit()
            session.add(
                schema.LLMExtractionJobDocument(
                    state_code=_STATE,
                    job_id="job1",
                    document_contents_id="doc2",
                    batch_index=0,
                    job_index=0,
                )
            )
            with self.assertRaisesRegex(
                IntegrityError,
                re.escape(
                    '(psycopg2.errors.UniqueViolation) duplicate key value violates unique constraint "llm_extraction_job_document_job_index_unique"'
                ),
            ):
                session.commit()

    def _seed_for_job_doc_check(self, session: Any) -> None:
        self._seed_chain(session)
        session.add(_job(start_datetime_utc=_NOW))
        session.flush()
        session.add(
            schema.LLMExtractionBatchJobMetadata(
                state_code=_STATE,
                job_id="job1",
                batch_index=0,
                external_job_id="x1",
                external_job_provider="VERTEX_AI_BATCH",
                gcs_input_uri="gs://b/in.jsonl",
                gcs_output_uri=None,
                row_creation_datetime_utc=_NOW,
            )
        )
        session.flush()

    def test_job_documents_result_datetime_and_type_consistent(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._seed_for_job_doc_check(session)
            # result_datetime set but result_type null
            session.add(
                schema.LLMExtractionJobDocument(
                    state_code=_STATE,
                    job_id="job1",
                    document_contents_id="doc1",
                    batch_index=0,
                    job_index=0,
                    result_datetime_utc=_LATER,
                    result_type=None,
                    is_relevant=None,
                    input_token_count=1,
                    output_token_count=1,
                    cached_input_token_count=0,
                    thinking_token_count=0,
                )
            )
            with self._assert_check_violation(
                session,
                "llm_extraction_job_document_result_datetime_and_type_consistent",
            ):
                session.commit()

    def test_job_documents_error_message_only_for_failure(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._seed_for_job_doc_check(session)
            session.add(
                schema.LLMExtractionJobDocument(
                    state_code=_STATE,
                    job_id="job1",
                    document_contents_id="doc1",
                    batch_index=0,
                    job_index=0,
                    result_datetime_utc=_LATER,
                    result_type=LLMExtractionJobDocumentResultType.SUCCESS.value,
                    is_relevant=True,
                    error_message="boom",
                    input_token_count=1,
                    output_token_count=1,
                    cached_input_token_count=0,
                    thinking_token_count=0,
                )
            )
            with self._assert_check_violation(
                session, "llm_extraction_job_document_error_message_only_for_failure"
            ):
                session.commit()

    def test_job_documents_is_relevant_iff_success(self) -> None:
        # SUCCESS with is_relevant null -> violation
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._seed_for_job_doc_check(session)
            session.add(
                schema.LLMExtractionJobDocument(
                    state_code=_STATE,
                    job_id="job1",
                    document_contents_id="doc1",
                    batch_index=0,
                    job_index=0,
                    result_datetime_utc=_LATER,
                    result_type=LLMExtractionJobDocumentResultType.SUCCESS.value,
                    is_relevant=None,
                    input_token_count=1,
                    output_token_count=1,
                    cached_input_token_count=0,
                    thinking_token_count=0,
                )
            )
            with self._assert_check_violation(
                session, "llm_extraction_job_document_is_relevant_iff_success"
            ):
                session.commit()

        # Non-SUCCESS with is_relevant set -> violation
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._seed_for_job_doc_check(session)
            session.add(
                schema.LLMExtractionJobDocument(
                    state_code=_STATE,
                    job_id="job1",
                    document_contents_id="doc1",
                    batch_index=0,
                    job_index=0,
                    result_datetime_utc=_LATER,
                    result_type=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT.value,
                    is_relevant=True,
                    error_message="boom",
                    input_token_count=1,
                    output_token_count=1,
                    cached_input_token_count=0,
                    thinking_token_count=0,
                )
            )
            with self._assert_check_violation(
                session, "llm_extraction_job_document_is_relevant_iff_success"
            ):
                session.commit()

        # Open doc (result_type null) with is_relevant set -> violation.
        # Guards against three-valued-logic slipping through the strict iff.
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._seed_for_job_doc_check(session)
            session.add(
                schema.LLMExtractionJobDocument(
                    state_code=_STATE,
                    job_id="job1",
                    document_contents_id="doc1",
                    batch_index=0,
                    job_index=0,
                    result_datetime_utc=None,
                    result_type=None,
                    is_relevant=True,
                )
            )
            with self._assert_check_violation(
                session, "llm_extraction_job_document_is_relevant_iff_success"
            ):
                session.commit()

    def test_job_documents_nonnegative_job_index(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._seed_for_job_doc_check(session)
            session.add(
                schema.LLMExtractionJobDocument(
                    state_code=_STATE,
                    job_id="job1",
                    document_contents_id="doc1",
                    batch_index=0,
                    job_index=-1,
                )
            )
            with self._assert_check_violation(
                session, "llm_extraction_job_document_nonnegative_job_index"
            ):
                session.commit()

    def test_job_documents_token_counts_iff_result_datetime(self) -> None:
        # result_datetime set but a token count is null
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._seed_for_job_doc_check(session)
            session.add(
                schema.LLMExtractionJobDocument(
                    state_code=_STATE,
                    job_id="job1",
                    document_contents_id="doc1",
                    batch_index=0,
                    job_index=0,
                    result_datetime_utc=_LATER,
                    result_type=LLMExtractionJobDocumentResultType.SUCCESS.value,
                    is_relevant=True,
                    input_token_count=1,
                    output_token_count=None,
                    cached_input_token_count=0,
                    thinking_token_count=0,
                )
            )
            with self._assert_check_violation(
                session,
                "llm_extraction_job_document_token_counts_iff_result_datetime",
            ):
                session.commit()

        # result_datetime null but a token count is set
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._seed_for_job_doc_check(session)
            session.add(
                schema.LLMExtractionJobDocument(
                    state_code=_STATE,
                    job_id="job1",
                    document_contents_id="doc1",
                    batch_index=0,
                    job_index=0,
                    input_token_count=1,
                    output_token_count=1,
                    cached_input_token_count=0,
                    thinking_token_count=0,
                )
            )
            with self._assert_check_violation(
                session,
                "llm_extraction_job_document_token_counts_iff_result_datetime",
            ):
                session.commit()

    def test_cap_override_extractor_version_fk(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(
                schema.LLMExtractionCapOverride(
                    state_code=_STATE,
                    extractor_version_id="ev_missing",
                    document_filter_id="f1",
                    override_document_count_cap=1,
                    expires_datetime_utc=_LATER,
                    row_creation_datetime_utc=_NOW,
                )
            )
            with self._assert_fk_violation(session):
                session.commit()

    def _assert_pk_violation(self) -> Any:
        return self.assertRaisesRegex(
            IntegrityError,
            r"\(psycopg2\.errors\.UniqueViolation\).*duplicate key value violates",
        )

    def test_primary_key_uniqueness(self) -> None:
        # Duplicate (collection_name, collection_version_id) on llm_extractor_collection
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(_collection())
            session.flush()
            session.add(_collection())
            with self._assert_pk_violation():
                session.commit()

        # Duplicate (state_code, extractor_id) on llm_extractor
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(_extractor())
            session.flush()
            session.add(_extractor())
            with self._assert_pk_violation():
                session.commit()

        # Duplicate (state_code, extractor_version_id) on llm_extractor_version
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._seed_chain(session)
            session.add(_extractor_version())
            with self._assert_pk_violation():
                session.commit()
