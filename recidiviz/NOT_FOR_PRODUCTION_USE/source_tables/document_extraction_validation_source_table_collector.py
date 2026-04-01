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
"""Collects source table definitions for exclusion and validated extraction
result tables.

Exclusions live in document_extraction_results__exclusions with one table
per extractor ({state_code}_{collection_name}).

Validated results live in document_extraction_results__validated with one table
per extractor ({state_code}_{collection_name}).
"""
from google.cloud.bigquery import SchemaField

from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.document_extractor_configs import (
    collect_extractors,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_result_exclusion_metadata import (
    DocumentResultExclusionMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.validated_extraction_result_metadata import (
    ValidatedExtractionResultMetadata,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
)

_EXCLUSION_SCHEMA: list[SchemaField] = [
    SchemaField(name="job_id", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="document_id", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="extractor_id", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="extractor_version_id", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="extraction_datetime", field_type="TIMESTAMP", mode="REQUIRED"),
    SchemaField(name="exclusion_type", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="exclusion_details_json", field_type="STRING", mode="NULLABLE"),
]

_VALIDATED_OUTPUT_SCHEMA: list[SchemaField] = [
    SchemaField(name="job_id", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="document_id", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="extractor_id", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="extractor_version_id", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="extraction_datetime", field_type="TIMESTAMP", mode="REQUIRED"),
    SchemaField(name="state_code", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="result_json", field_type="STRING", mode="REQUIRED"),
]


def collect_document_extraction_failures_source_table_collection() -> (
    SourceTableCollection
):
    """Creates a SourceTableCollection for exclusion tables.

    Dataset: document_extraction_results__exclusions
    Table naming: {state_code}_{collection_name}
    """
    extractors = collect_extractors()

    collection = SourceTableCollection(
        dataset_id=DocumentResultExclusionMetadata.EXCLUSIONS_DATASET_ID,
        update_config=SourceTableCollectionUpdateConfig.protected(),
        description=(
            "Excluded documents (validation failures and LLM errors). One table per "
            "state-specific extractor."
        ),
        labels=[],
    )

    for extractor in sorted(extractors.values(), key=lambda e: e.extractor_id):
        table_id = DocumentResultExclusionMetadata.table_id(
            extractor.state_code, extractor.collection_name
        )
        collection.add_source_table(
            table_id=table_id,
            schema_fields=_EXCLUSION_SCHEMA,
            description=(f"Excluded documents for extractor {extractor.extractor_id}."),
        )

    return collection


def collect_document_extraction_validated_source_table_collection() -> (
    SourceTableCollection
):
    """Creates a SourceTableCollection for validated extraction result tables.

    Dataset: document_extraction_results__validated
    Table naming: {state_code}_{collection_name}
    """
    extractors = collect_extractors()

    collection = SourceTableCollection(
        dataset_id=ValidatedExtractionResultMetadata.VALIDATED_DATASET_ID,
        update_config=SourceTableCollectionUpdateConfig.protected(),
        description=(
            "Validated extraction results. One table per " "state-specific extractor."
        ),
        labels=[],
    )

    for extractor in sorted(extractors.values(), key=lambda e: e.extractor_id):
        table_id = ValidatedExtractionResultMetadata.table_id(
            extractor.state_code, extractor.collection_name
        )
        collection.add_source_table(
            table_id=table_id,
            schema_fields=_VALIDATED_OUTPUT_SCHEMA,
            description=(
                f"Validated extraction results for extractor "
                f"{extractor.extractor_id}."
            ),
        )

    return collection
