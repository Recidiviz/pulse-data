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
"""Samples the results of one document-extraction run, returning one row per document.

Each row carries the extraction result as the extractor wrote it, plus the text of the
document it was read from.

    state_code  document_contents_id  extractor_version_id  result_json      document_text
    US_CO       a1b2c3                9f8e7d                {"result": ...}  Client started
                                                                             at Walmart...
    US_CO       d4e5f6                9f8e7d                {"result": ...}  Still looking
                                                                             for work.

The result JSON comes back whole rather than parsed into columns, because
LLMRequestOutputValues already reads one against its output schema, unwrapping the result
envelope and each INFERRED field and reporting the companion metadata. Pulling the fields
apart in SQL would implement that a second time, in a language with worse tools for it.

The query narrows to a single extraction run, dedupes to that run's latest result per
document, and keeps only documents the model judged relevant and whose text still exists.
"""
from recidiviz.big_query.big_query_query_builder import BigQueryQueryBuilder
from recidiviz.documents.extraction.extraction_results_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    EXTRACTION_JOB_ID_COLUMN_NAME,
    EXTRACTOR_VERSION_ID_COLUMN_NAME,
    IS_RELEVANT_COLUMN_NAME,
    RESULT_JSON_COLUMN_NAME,
    STATE_CODE_COLUMN_NAME,
    VALIDATION_DATETIME_UTC_COLUMN_NAME,
)
from recidiviz.documents.extraction.extraction_results_narrowing import (
    ExtractionResultsNarrowing,
)
from recidiviz.documents.extraction.llm_extraction_results_tables import (
    ExtractionValidatedResultsBQTable,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.store.document_store_columns import DOCUMENT_TEXT_COLUMN_NAME
from recidiviz.utils.string_formatting import fix_indent


class DocumentExtractionResultSampleQueryBuilder:
    """Samples the results of one document-extraction run, returning one row per document
    carrying the extraction result as the extractor wrote it and the text it was read from.
    """

    def __init__(
        self,
        *,
        # The extractor whose results to sample. Supplies the state, the collection whose
        # results table is read, and the input document collection the text comes from.
        extractor_config: LLMExtractorConfig,
        # Restricts the sample to a single extraction run's results.
        results_narrowing: ExtractionResultsNarrowing,
        # Maximum number of documents to sample.
        sample_size: int,
        # Sandbox prefix of the extraction results dataset to read, or None to read
        # production results.
        input_results_sandbox_dataset_prefix: str | None,
        # Sandbox prefix of the document store to read the document text from, or None to
        # read production documents. Separate from the results prefix because the two move
        # independently: a sandbox extraction run writes its results to a sandbox dataset
        # while usually still reading the production document store.
        input_documents_sandbox_dataset_prefix: str | None,
    ) -> None:
        self.extractor_config = extractor_config
        self.results_narrowing = results_narrowing
        self.sample_size = sample_size
        self.input_results_sandbox_dataset_prefix = input_results_sandbox_dataset_prefix
        self.input_documents_sandbox_dataset_prefix = (
            input_documents_sandbox_dataset_prefix
        )
        self._query_builder = BigQueryQueryBuilder(
            parent_address_overrides=None, parent_address_formatter_provider=None
        )

    def build_query(self, *, project_id: str) -> str:
        """Returns the sample query, ready to run against the given project. Read each row's
        result with LLMRequestOutputValues.
        """
        return self._query_builder.build_query(
            project_id=project_id,
            query_template=self._build_query_template(),
            query_format_kwargs={},
        )

    def _build_query_template(self) -> str:
        """Returns the sample query as a template, leaving the addresses it reads as
        "{project_id}"-qualified references for build_query to fill in.
        """
        results_table = ExtractionValidatedResultsBQTable.address(
            state_code=self.extractor_config.state_code,
            collection_name=self.extractor_config.extractor_collection.name,
            sandbox_prefix=self.input_results_sandbox_dataset_prefix,
        ).format_address_for_query_template()
        document_contents_table = self.extractor_config.input_document_collection.document_contents_table_address(
            sandbox_dataset_prefix=self.input_documents_sandbox_dataset_prefix
        ).format_address_for_query_template()
        query = f"""
WITH deduped_results AS (
    SELECT
        {STATE_CODE_COLUMN_NAME},
        {DOCUMENT_CONTENTS_ID_COLUMN_NAME},
        {EXTRACTOR_VERSION_ID_COLUMN_NAME},
        {RESULT_JSON_COLUMN_NAME},
        {IS_RELEVANT_COLUMN_NAME}
    FROM `{results_table}`
{fix_indent(self.results_narrowing.build_where_clause_sql(), indent_level=4)}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {DOCUMENT_CONTENTS_ID_COLUMN_NAME}
        ORDER BY
            {VALIDATION_DATETIME_UTC_COLUMN_NAME} DESC,
            {EXTRACTOR_VERSION_ID_COLUMN_NAME} DESC,
            {EXTRACTION_JOB_ID_COLUMN_NAME} DESC
    ) = 1
),
sampled_results AS (
    SELECT * EXCEPT ({IS_RELEVANT_COLUMN_NAME})
    FROM deduped_results
    -- Applied after the dedup, so an older relevant result cannot resurrect a document
    -- whose latest result says it is irrelevant. This matches the parsed results views.
    WHERE {IS_RELEVANT_COLUMN_NAME}
    -- Deterministic on document id, so re-running the same sample size picks the same
    -- documents.
    ORDER BY FARM_FINGERPRINT({DOCUMENT_CONTENTS_ID_COLUMN_NAME})
    LIMIT {self.sample_size}
)
SELECT
    sampled_results.*,
    doc_contents.{DOCUMENT_TEXT_COLUMN_NAME}
FROM sampled_results
JOIN `{document_contents_table}` doc_contents
    USING ({DOCUMENT_CONTENTS_ID_COLUMN_NAME})
-- A document whose text was deleted or scrubbed in source data has nothing to annotate.
WHERE doc_contents.{DOCUMENT_TEXT_COLUMN_NAME} IS NOT NULL
"""
        return fix_indent(query, indent_level=0)
