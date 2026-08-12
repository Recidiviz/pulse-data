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
"""Tests for LLMExtractionJobGenerator against a real test Postgres, with the
eligible-document BQ query mocked."""

import datetime
import unittest
from unittest.mock import MagicMock

import attr
import pytest
import pytz
from google.cloud import bigquery

from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMDocumentExtractionErrorType,
    LLMExtractionJobDocumentResultType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_client.types import (
    LLMClientDocumentExtractionResult,
    LLMDocumentExtractionTokenCounts,
    LLMRequestErrorType,
)
from recidiviz.documents.extraction.llm_extraction_eligible_document_query_builder import (
    LLMExtractionEligibleDocumentQueryBuilder,
)
from recidiviz.documents.extraction.llm_extraction_job_generator import (
    LLMExtractionJobGenerator,
)
from recidiviz.documents.extraction.llm_extraction_job_manager import (
    LLMExtractionJobManager,
    LLMJobDocumentExtractionResult,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.llm_extractor_metadata_manager import (
    LLMExtractorMetadataManager,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.documents.extraction.validation.llm_document_validation_result import (
    LLMDocumentValidationResult,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    DOCUMENT_LENGTH_BYTES_COLUMN_NAME,
    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
)
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.documents import fake_config
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
_NOW = datetime.datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)


@pytest.mark.uses_db
class LLMExtractionJobGeneratorTest(unittest.TestCase):
    """Tests for LLMExtractionJobGenerator."""

    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    def setUp(self) -> None:
        self.job_manager = LLMExtractionJobManager()
        self.database_key = SQLAlchemyDatabaseKey.for_schema(
            self.job_manager.database_key.schema_type
        )
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.database_key
        )

        self.config = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        )
        # Seed the collection → extractor → version parent rows the job tables'
        # foreign keys require, and capture the active version the generator
        # selects work for.
        self.active_version = (
            LLMExtractorMetadataManager().set_active_extractor_version(
                config=self.config
            )
        )
        self.extractor_version_id = self.active_version.extractor_version_id

        self.bq_client = MagicMock()
        self.bq_client.project_id = "recidiviz-testing"

        self.generator = LLMExtractionJobGenerator(
            eligible_documents_query_builder=LLMExtractionEligibleDocumentQueryBuilder(
                document_filter=self.config.document_filter,
                input_document_collection=self.config.input_document_collection,
            ),
            job_manager=self.job_manager,
            bq_client=self.bq_client,
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

    def _set_eligible_doc_query_results(self, document_contents_ids: list[str]) -> None:
        query_job = MagicMock()
        query_job.result.return_value = [
            bigquery.table.Row(
                [document_contents_id, 100, _NOW],
                {
                    DOCUMENT_CONTENTS_ID_COLUMN_NAME: 0,
                    DOCUMENT_LENGTH_BYTES_COLUMN_NAME: 1,
                    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME: 2,
                },
            )
            for document_contents_id in document_contents_ids
        ]
        self.bq_client.run_query_async.return_value = query_job

    def _pending_document_ids(self, job_id: str) -> set[str]:
        return {
            document.document_contents_id
            for document in self.job_manager.get_pending_job_documents(
                state_code=_STATE_CODE, job_id=job_id
            )
        }

    def _mark_document_result(
        self,
        *,
        job_id: str,
        document_contents_id: str,
        result_type: LLMExtractionJobDocumentResultType,
    ) -> None:
        self.job_manager.set_job_document_results(
            state_code=_STATE_CODE,
            results=[
                self._document_result(
                    job_id=job_id,
                    document_contents_id=document_contents_id,
                    result_type=result_type,
                )
            ],
        )

    def _document_result(
        self,
        *,
        job_id: str,
        document_contents_id: str,
        result_type: LLMExtractionJobDocumentResultType,
    ) -> LLMJobDocumentExtractionResult:
        """Builds a SUCCESS result with validated output, or a failure result
        with a raw error and no validation, matching |result_type|."""
        token_counts = LLMDocumentExtractionTokenCounts(
            input_token_count=10,
            output_token_count=5,
            cached_input_token_count=2,
            thinking_token_count=1,
        )
        if result_type is LLMExtractionJobDocumentResultType.SUCCESS:
            return LLMJobDocumentExtractionResult(
                job_id=job_id,
                document_contents_id=document_contents_id,
                result_datetime_utc=_NOW,
                raw_result=LLMClientDocumentExtractionResult.from_success(
                    document_contents_id=document_contents_id,
                    result_json={"is_relevant": True},
                    token_counts=token_counts,
                ),
                result_type=result_type,
                is_relevant=True,
                error_type=None,
                error_message=None,
                validation_results=LLMDocumentValidationResult(
                    validated_output=LLMRequestOutputValues(
                        output_schema=self.config.extractor_collection.output_schema,
                        output_json={"is_relevant": True},
                    ),
                    audit_issues=[],
                    result_type_override=None,
                    validation_config_version_id="vc1",
                    validation_datetime_utc=_NOW,
                ),
            )
        return LLMJobDocumentExtractionResult(
            job_id=job_id,
            document_contents_id=document_contents_id,
            result_datetime_utc=_NOW,
            raw_result=LLMClientDocumentExtractionResult.from_error(
                document_contents_id=document_contents_id,
                error_type=LLMRequestErrorType.TIMEOUT,
                error_message=f"error for {document_contents_id}",
                token_counts=token_counts,
            ),
            result_type=result_type,
            is_relevant=None,
            error_type=LLMDocumentExtractionErrorType.LLM_REQUEST_TIMEOUT,
            error_message=f"error for {document_contents_id}",
            validation_results=None,
        )

    def test_generate_job_selects_new_documents(self) -> None:
        self._set_eligible_doc_query_results(["doc1", "doc2"])
        job = self.generator.generate_job(
            config=self.config, active_version=self.active_version
        )

        assert job is not None
        self.assertEqual(
            {
                "state_code": _STATE_CODE.value,
                "extractor_version_id": self.extractor_version_id,
                "start_datetime_utc": None,
                "completion_datetime_utc": None,
                "result_type": None,
                "error_message": None,
            },
            {k: v for k, v in attr.asdict(job).items() if k != "job_id"},
        )
        self.assertEqual(
            {"doc1", "doc2"},
            self._pending_document_ids(job.job_id),
        )

    def test_generate_job_returns_none_when_no_documents(self) -> None:
        self._set_eligible_doc_query_results([])
        self.assertIsNone(
            self.generator.generate_job(
                config=self.config, active_version=self.active_version
            )
        )

    def test_generate_job_returns_none_when_all_documents_already_terminal(
        self,
    ) -> None:
        # First run selects the document and processes it to a terminal SUCCESS.
        self._set_eligible_doc_query_results(["doc1"])
        first_job = self.generator.generate_job(
            config=self.config, active_version=self.active_version
        )
        assert first_job is not None
        self._mark_document_result(
            job_id=first_job.job_id,
            document_contents_id="doc1",
            result_type=LLMExtractionJobDocumentResultType.SUCCESS,
        )
        self.job_manager.mark_job_started(
            state_code=_STATE_CODE, job_id=first_job.job_id
        )
        self.job_manager.mark_job_completed(
            state_code=_STATE_CODE, job_id=first_job.job_id
        )

        # The query still returns doc1, but it now has a terminal result, so the
        # anti-join leaves nothing needing processing and no new job is created.
        self._set_eligible_doc_query_results(["doc1"])
        self.assertIsNone(
            self.generator.generate_job(
                config=self.config, active_version=self.active_version
            )
        )

    def test_generate_job_skips_succeeded_reselects_transient_failure(self) -> None:
        # First run selects both documents.
        self._set_eligible_doc_query_results(["doc1", "doc2"])
        first_job = self.generator.generate_job(
            config=self.config, active_version=self.active_version
        )
        assert first_job is not None

        # doc1 permanently succeeds; doc2 hits a transient failure (non-terminal).
        self._mark_document_result(
            job_id=first_job.job_id,
            document_contents_id="doc1",
            result_type=LLMExtractionJobDocumentResultType.SUCCESS,
        )
        self._mark_document_result(
            job_id=first_job.job_id,
            document_contents_id="doc2",
            result_type=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
        )
        # Close the first job so a second one can be created. A completed job
        # must have been started (the completion-requires-start DB constraint).
        self.job_manager.mark_job_started(
            state_code=_STATE_CODE, job_id=first_job.job_id
        )
        self.job_manager.mark_job_completed(
            state_code=_STATE_CODE, job_id=first_job.job_id
        )

        # Second run re-runs selection: doc1 is skipped (terminal SUCCESS), doc2
        # is re-selected (transient), doc3 is newly eligible.
        self._set_eligible_doc_query_results(["doc1", "doc2", "doc3"])
        second_job = self.generator.generate_job(
            config=self.config, active_version=self.active_version
        )
        assert second_job is not None
        self.assertNotEqual(first_job.job_id, second_job.job_id)
        self.assertEqual(
            {"doc2", "doc3"},
            self._pending_document_ids(second_job.job_id),
        )

    def test_generate_job_resumes_open_job_without_reselecting(self) -> None:
        self._set_eligible_doc_query_results(["doc1", "doc2"])
        first_job = self.generator.generate_job(
            config=self.config, active_version=self.active_version
        )
        assert first_job is not None

        # The first job is left open (a crashed run). The next generate_job must
        # return that same job without running selection again — the query is
        # never issued and no second job is created.
        self.bq_client.run_query_async.reset_mock()
        resumed_job = self.generator.generate_job(
            config=self.config, active_version=self.active_version
        )
        assert resumed_job is not None
        self.assertEqual(first_job.job_id, resumed_job.job_id)
        self.bq_client.run_query_async.assert_not_called()
