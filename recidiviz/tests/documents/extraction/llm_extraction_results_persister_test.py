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
"""Tests for LLMExtractionResultsPersister against the BigQuery emulator."""

import datetime

import pytz

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMExtractionJobDocumentResultType,
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
    LLMJobDocumentExtractionResult,
)
from recidiviz.documents.extraction.llm_extraction_results_persister import (
    LLMExtractionResultsPersister,
)
from recidiviz.documents.extraction.llm_extraction_results_tables import (
    ExtractionRawResultsBQTable,
    ExtractionValidatedResultsBQTable,
    ExtractionValidationAuditBQTable,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.source_tables.extraction_results_source_table_collection import (
    collect_extraction_results_source_table_collections,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.documents import fake_config

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
_JOB_ID = "job1"
_RESULT_DATETIME = datetime.datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
_VALIDATION_DATETIME = datetime.datetime(2026, 1, 1, 12, 5, tzinfo=pytz.UTC)
_RAW_RESULT_JSON = {"is_relevant": True, "location": {"value": "here"}}
_OUTPUT_SCHEMA = get_first_order_llm_extractor_config(
    _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
).extractor_collection.output_schema


def _success_result(
    *,
    document_contents_id: str,
    is_relevant: bool,
    validated_content: dict | None,
    audit_issues: list[ValidationIssue],
    result_type_override: LLMExtractionJobDocumentResultType | None,
) -> LLMJobDocumentExtractionResult:
    return LLMJobDocumentExtractionResult(
        job_id=_JOB_ID,
        document_contents_id=document_contents_id,
        result_datetime_utc=_RESULT_DATETIME,
        raw_result=LLMClientDocumentExtractionResult.from_success(
            document_contents_id=document_contents_id,
            result_json=_RAW_RESULT_JSON,
            token_counts=LLMDocumentExtractionTokenCounts.empty(),
        ),
        result_type=LLMExtractionJobDocumentResultType.SUCCESS,
        is_relevant=is_relevant,
        error_type=None,
        error_message=None,
        validation_results=LLMDocumentValidationResult(
            validated_content=LLMRequestOutputValues(
                output_schema=_OUTPUT_SCHEMA, output_json=validated_content
            ),
            audit_issues=audit_issues,
            result_type_override=result_type_override,
            validation_config_version_id="vc1",
            validation_datetime_utc=_VALIDATION_DATETIME,
        ),
    )


def _failure_result(document_contents_id: str) -> LLMJobDocumentExtractionResult:
    return LLMJobDocumentExtractionResult(
        job_id=_JOB_ID,
        document_contents_id=document_contents_id,
        result_datetime_utc=_RESULT_DATETIME,
        raw_result=LLMClientDocumentExtractionResult.from_error(
            document_contents_id=document_contents_id,
            error_type=LLMRequestErrorType.CONTENT_FILTERED,
            error_message="filtered",
        ),
        result_type=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT,
        is_relevant=None,
        error_type=None,
        error_message="filtered",
        validation_results=None,
    )


class LLMExtractionResultsPersisterTest(BigQueryEmulatorTestCase):
    """Tests that a batch of results round-trips into the raw, validated, and
    audit tables."""

    # The result tables are created once in setUpClass; keep them across tests
    # and only clear their rows between tests.
    wipe_emulator_data_on_teardown = False

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        return collect_extraction_results_source_table_collections(
            configs={
                _STATE_CODE: {
                    _COLLECTION_NAME: get_first_order_llm_extractor_config(
                        _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
                    )
                }
            }
        )

    def setUp(self) -> None:
        super().setUp()
        self.config = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        )
        self.persister = LLMExtractionResultsPersister(bq_client=self.bq_client)

    def tearDown(self) -> None:
        self._clear_emulator_table_data()
        super().tearDown()

    @property
    def _raw_results_table_address(self) -> BigQueryAddress:
        return ExtractionRawResultsBQTable.address(
            state_code=_STATE_CODE,
            collection_name=_COLLECTION_NAME,
        )

    @property
    def _validated_results_table_address(self) -> BigQueryAddress:
        return ExtractionValidatedResultsBQTable.address(
            state_code=_STATE_CODE,
            collection_name=_COLLECTION_NAME,
        )

    @property
    def _validation_audit_table_address(self) -> BigQueryAddress:
        return ExtractionValidationAuditBQTable.address(
            state_code=_STATE_CODE,
            collection_name=_COLLECTION_NAME,
        )

    def test_batch_round_trips_into_all_three_tables(self) -> None:
        issue = ValidationIssue(
            check_type=ValidationCheckType.SCHEMA_CONFORMANCE,
            field_name="location",
            detail="missing required field",
        )
        results = [
            # Relevant success: raw + validated + audit (clean).
            _success_result(
                document_contents_id="doc_relevant",
                is_relevant=True,
                validated_content={"is_relevant": True, "location": "here"},
                audit_issues=[],
                result_type_override=None,
            ),
            # Irrelevant success: raw + validated ({"is_relevant": false}) + audit.
            _success_result(
                document_contents_id="doc_irrelevant",
                is_relevant=False,
                validated_content={"is_relevant": False},
                audit_issues=[],
                result_type_override=None,
            ),
            # Success with a non-fatal audit issue: still writes all three tables
            # (validated content passes through), and the audit row records the
            # issue.
            _success_result(
                document_contents_id="doc_with_issue",
                is_relevant=True,
                validated_content={"is_relevant": True, "location": "here"},
                audit_issues=[issue],
                result_type_override=None,
            ),
            # Hard failure: no raw JSON, no validation — writes nothing.
            _failure_result("doc_failed"),
        ]

        self.persister.persist_results(config=self.config, results=results)

        def raw_row(document_contents_id: str) -> dict:
            return ExtractionRawResultsBQTable.to_row(
                state_code_str=_STATE_CODE.value,
                job_id=_JOB_ID,
                extractor_id=self.config.extractor_id,
                extractor_version_id=self.config.extractor_version_id,
                document_contents_id=document_contents_id,
                result_datetime_utc=_RESULT_DATETIME,
                result_json=_RAW_RESULT_JSON,
            )

        self.compare_table_to_results(
            self._raw_results_table_address,
            [
                raw_row(doc)
                for doc in ("doc_relevant", "doc_irrelevant", "doc_with_issue")
            ],
        )

        def validated_row(
            document_contents_id: str, *, is_relevant: bool, validated_content: dict
        ) -> dict:
            return ExtractionValidatedResultsBQTable.to_row(
                state_code_str=_STATE_CODE.value,
                document_contents_id=document_contents_id,
                job_id=_JOB_ID,
                extractor_version_id=self.config.extractor_version_id,
                validation_config_version_id="vc1",
                validation_datetime_utc=_VALIDATION_DATETIME,
                is_relevant=is_relevant,
                validated_content=validated_content,
            )

        self.compare_table_to_results(
            self._validated_results_table_address,
            [
                validated_row(
                    "doc_relevant",
                    is_relevant=True,
                    validated_content={"is_relevant": True, "location": "here"},
                ),
                validated_row(
                    "doc_irrelevant",
                    is_relevant=False,
                    validated_content={"is_relevant": False},
                ),
                validated_row(
                    "doc_with_issue",
                    is_relevant=True,
                    validated_content={"is_relevant": True, "location": "here"},
                ),
            ],
        )

        def audit_row(
            document_contents_id: str,
            *,
            is_relevant: bool,
            audit_issues: list[ValidationIssue],
        ) -> dict:
            return ExtractionValidationAuditBQTable.to_row(
                state_code_str=_STATE_CODE.value,
                document_contents_id=document_contents_id,
                job_id=_JOB_ID,
                extractor_version_id=self.config.extractor_version_id,
                validation_config_version_id="vc1",
                validation_datetime_utc=_VALIDATION_DATETIME,
                passed_validation=True,
                will_retry=False,
                is_relevant=is_relevant,
                audit_issues_json=[i.to_dict() for i in audit_issues],
            )

        self.compare_table_to_results(
            self._validation_audit_table_address,
            [
                audit_row("doc_relevant", is_relevant=True, audit_issues=[]),
                audit_row("doc_irrelevant", is_relevant=False, audit_issues=[]),
                audit_row("doc_with_issue", is_relevant=True, audit_issues=[issue]),
            ],
        )

    def test_empty_batch_writes_nothing(self) -> None:
        self.persister.persist_results(config=self.config, results=[])

        self.compare_table_to_results(self._raw_results_table_address, [])
        self.compare_table_to_results(self._validated_results_table_address, [])
        self.compare_table_to_results(self._validation_audit_table_address, [])

    def test_at_least_once_re_persist_appends_duplicate_rows(self) -> None:
        result = _success_result(
            document_contents_id="doc_relevant",
            is_relevant=True,
            validated_content={"is_relevant": True},
            audit_issues=[],
            result_type_override=None,
        )
        self.persister.persist_results(config=self.config, results=[result])
        self.persister.persist_results(config=self.config, results=[result])

        raw_row = ExtractionRawResultsBQTable.to_row(
            state_code_str=_STATE_CODE.value,
            job_id=_JOB_ID,
            extractor_id=self.config.extractor_id,
            extractor_version_id=self.config.extractor_version_id,
            document_contents_id="doc_relevant",
            result_datetime_utc=_RESULT_DATETIME,
            result_json=_RAW_RESULT_JSON,
        )
        # The multiset comparison keeps the intentional duplicate rather than
        # collapsing it.
        self.compare_table_to_results(
            self._raw_results_table_address,
            [raw_row, raw_row],
        )
