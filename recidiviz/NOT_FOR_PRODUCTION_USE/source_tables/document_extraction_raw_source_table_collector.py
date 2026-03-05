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
"""Collects raw source table definitions for document extraction results.

All raw extraction results live in a single dataset
(document_extraction_results__raw) with one table per extractor
({state_code}_{collection_name}).
"""
from google.cloud.bigquery import SchemaField

from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.document_extractor_configs import (
    collect_extractors,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_extraction_result_metadata import (
    DocumentExtractionResultMetadata,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
)

_RAW_RESULT_SCHEMA: list[SchemaField] = [
    SchemaField(name="job_id", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="document_id", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="extractor_id", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="extractor_version_id", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="extraction_datetime", field_type="TIMESTAMP", mode="REQUIRED"),
    SchemaField(name="status", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="result_json", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="error_message", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="error_type", field_type="STRING", mode="NULLABLE"),
]


def collect_document_extraction_raw_source_table_collection() -> SourceTableCollection:
    """Creates a single SourceTableCollection for all raw extraction result tables.

    Dataset: document_extraction_results__raw
    Table naming: {state_code}_{collection_name}
    """
    extractors = collect_extractors()

    collection = SourceTableCollection(
        dataset_id=DocumentExtractionResultMetadata.RAW_DATASET_ID,
        update_config=SourceTableCollectionUpdateConfig.protected(),
        description="Raw extraction results. One table per state-specific extractor.",
        labels=[],
    )

    for extractor in sorted(extractors.values(), key=lambda e: e.extractor_id):
        table_id = DocumentExtractionResultMetadata.raw_table_id(
            extractor.state_code, extractor.collection_name
        )
        collection.add_source_table(
            table_id=table_id,
            schema_fields=_RAW_RESULT_SCHEMA,
            description=(
                f"Raw extraction results for extractor {extractor.extractor_id}."
            ),
        )

    return collection
