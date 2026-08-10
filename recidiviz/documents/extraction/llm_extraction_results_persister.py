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
"""Persists a batch of LLMJobDocumentExtractionResults to the BQ result tables."""

from typing import Sequence

import attr

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.common import attr_validators
from recidiviz.documents.extraction.llm_document_validation_result import (
    LLMDocumentValidationResult,
)
from recidiviz.documents.extraction.llm_extraction_job_manager import (
    LLMJobDocumentExtractionResult,
)
from recidiviz.documents.extraction.llm_extraction_results_tables import (
    ExtractionRawResultsBQTable,
    ExtractionValidatedResultsBQTable,
    ExtractionValidationAuditBQTable,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.utils.types import assert_type


@attr.define(frozen=True, kw_only=True)
class LLMExtractionResultsPersister:
    """Persists a batch of LLMJobDocumentExtractionResults to the BQ result
    tables.

    Writes are at-least-once: re-persisting a document that already has BQ rows
    just appends duplicate rows (the parsed views dedup to latest per
    document_contents_id), so the persister never checks for or cleans up prior
    rows.
    """

    _sandbox_prefix: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_non_empty_str
    )
    """The sandbox prefix to use for the BQ result tables, or None to write to the production tables.
    The persister does not create or delete tables, so the caller must ensure the tables exist before calling persist_results."""
    _bq_client: BigQueryClient = attr.ib()
    """Client used to write the result rows to BigQuery."""

    def persist_results(
        self,
        *,
        config: LLMExtractorConfig,
        results: Sequence[LLMJobDocumentExtractionResult],
    ) -> None:
        """Writes every result's content to the three BQ result tables for the
        extractor: the raw JSON to the raw table, any validated content to the
        validated table, and each validation outcome to the audit table.

        Each table is written in a single batch; a table with no rows for this
        batch is skipped.
        """
        collection_name = config.extractor_collection.name
        state_code = config.state_code
        state_code_str = state_code.value

        raw_rows = [
            ExtractionRawResultsBQTable.to_row(
                state_code_str=state_code_str,
                job_id=result.job_id,
                extractor_id=config.extractor_id,
                extractor_version_id=config.extractor_version_id,
                document_contents_id=result.document_contents_id,
                result_datetime_utc=result.result_datetime_utc,
                result_json=result.raw_result.result_json,
            )
            for result in results
            if result.raw_result.result_json is not None
        ]
        validated_rows = [
            ExtractionValidatedResultsBQTable.to_row(
                state_code_str=state_code_str,
                document_contents_id=result.document_contents_id,
                job_id=result.job_id,
                extractor_version_id=config.extractor_version_id,
                validation_config_version_id=assert_type(
                    result.validation_results, LLMDocumentValidationResult
                ).validation_config_version_id,
                validation_datetime_utc=assert_type(
                    result.validation_results, LLMDocumentValidationResult
                ).validation_datetime_utc,
                is_relevant=assert_type(result.is_relevant, bool),
                validated_content=assert_type(
                    assert_type(
                        result.validation_results, LLMDocumentValidationResult
                    ).validated_content.output_json,
                    dict,
                ),
            )
            for result in results
            if result.is_validated_result
        ]
        audit_rows = [
            ExtractionValidationAuditBQTable.to_row(
                state_code_str=state_code_str,
                document_contents_id=result.document_contents_id,
                job_id=result.job_id,
                extractor_version_id=config.extractor_version_id,
                validation_config_version_id=result.validation_results.validation_config_version_id,
                validation_datetime_utc=result.validation_results.validation_datetime_utc,
                passed_validation=result.validation_results.passed_validation,
                will_retry=result.validation_results.will_retry,
                is_relevant=result.is_relevant,
                audit_issues_json=[
                    issue.to_dict() for issue in result.validation_results.audit_issues
                ],
            )
            for result in results
            if result.validation_results is not None
        ]

        # TODO(OBT-41801): Switch these writes from streaming inserts to BQ load jobs
        # (self._bq_client.load_into_table_async, awaiting each returned LoadJob
        # before returning so the persist-then-mark ordering still holds). Load
        # jobs are the right path for large backfills: streaming inserts here go
        # through tabledata.insertAll, which BigQuery rejects above a 10 MB
        # payload per request (a 413), bills per GB, and lands rows in a streaming
        # buffer that can take minutes (rarely up to 90) to become visible. Load
        # jobs have no 10 MB request cap (up to 15 TB/job), are free (shared slot
        # pool), and commit atomically. Deferred for now because the BigQuery
        # emulator this persister is tested against does not support load jobs
        # (load_table_from_json returns "400 unspecified job configuration"), so
        # the change would force the round-trip emulator test down to a mocked
        # client. Load jobs also require the row dicts to be JSON-serializable
        # (the to_row builders would need to emit timestamps as ISO strings, since
        # json.dumps can't serialize datetime), and their 1,500-loads-per-table-
        # per-day quota means the persist chunk size must stay large.
        if raw_rows:
            self._bq_client.stream_into_table(
                address=ExtractionRawResultsBQTable.address(
                    state_code=state_code,
                    collection_name=collection_name,
                    sandbox_prefix=self._sandbox_prefix,
                ),
                rows=raw_rows,
            )
        if validated_rows:
            self._bq_client.stream_into_table(
                address=ExtractionValidatedResultsBQTable.address(
                    state_code=state_code,
                    collection_name=collection_name,
                    sandbox_prefix=self._sandbox_prefix,
                ),
                rows=validated_rows,
            )
        if audit_rows:
            self._bq_client.stream_into_table(
                address=ExtractionValidationAuditBQTable.address(
                    state_code=state_code,
                    collection_name=collection_name,
                    sandbox_prefix=self._sandbox_prefix,
                ),
                rows=audit_rows,
            )
