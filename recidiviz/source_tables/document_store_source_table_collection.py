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
"""Contains source table definitions for document store metadata and contents tables."""

from types import ModuleType

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.dataset_config import (
    document_contents_dataset_for_region,
    document_store_metadata_dataset_for_region,
    document_store_temp_dataset_for_region,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    EntityResolutionDocumentCollectionConfig,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_entry_source_map_table import (
    EntityResolutionEntrySourceMapBQTable,
)
from recidiviz.documents.store.document_collection_config import (
    get_states_with_document_collections,
)
from recidiviz.documents.store.document_collection_config_collectors import (
    collect_all_document_collection_configs,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
)
from recidiviz.documents.store.document_upload_status_table import (
    DocumentUploadStatusTable,
)
from recidiviz.source_tables.source_table_config import (
    DocumentStoreSourceTableLabel,
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    StateSpecificSourceTableLabel,
)

TWO_WEEK_MS = 14 * 24 * 60 * 60 * 1000


def collect_document_store_source_tables(
    config_module: ModuleType | None = None,
) -> list[SourceTableCollection]:
    """Collects source table definitions for the document store metadata and
    contents tables of every document collection — first-order and generated
    entity-resolution — in |config_module| (the production config package by
    default). Each entity-resolution collection additionally gets its
    entry→source map table.
    """
    collections: list[SourceTableCollection] = []

    for state_code in get_states_with_document_collections(config_module):
        configs = collect_all_document_collection_configs(state_code, config_module)
        if not configs:
            continue

        labels = [
            StateSpecificSourceTableLabel(state_code=state_code),
            DocumentStoreSourceTableLabel(state_code=state_code),
        ]

        metadata_collection = SourceTableCollection(
            dataset_id=document_store_metadata_dataset_for_region(state_code),
            labels=labels,
            update_config=SourceTableCollectionUpdateConfig.protected(),
            description=f"Document store metadata tables for {StateCode.get_state(state_code)}",
        )

        contents_collection = SourceTableCollection(
            dataset_id=document_contents_dataset_for_region(state_code),
            labels=labels,
            update_config=SourceTableCollectionUpdateConfig.protected(),
            description=f"Document contents tables for {StateCode.get_state(state_code)}",
        )

        for config in configs.values():
            metadata_collection.add_source_table(
                table_id=config.metadata_table_address(
                    sandbox_dataset_prefix=None
                ).table_id,
                description=config.description,
                schema_fields=config.build_bq_metadata_schema(),
            )
            contents_collection.add_source_table(
                table_id=config.document_contents_table_address(
                    sandbox_dataset_prefix=None
                ).table_id,
                description=(
                    f"Document contents table for collection [{config.name}]. "
                    "One row per distinct document_contents_id successfully uploaded to GCS."
                ),
                schema_fields=config.build_bq_document_contents_schema(),
                clustering_fields=[DOCUMENT_CONTENTS_ID_COLUMN_NAME],
            )
            if isinstance(config, EntityResolutionDocumentCollectionConfig):
                # An entity-resolution collection's composite documents carry an
                # entry→source map, which lands in its own metadata table.
                # Clustered on the root entity ID: the key the map is written and
                # replaced by, and its consumers' leading join key.
                metadata_collection.add_source_table(
                    table_id=config.entry_source_map_table_address.table_id,
                    description=config.entry_source_map_table_description,
                    schema_fields=EntityResolutionEntrySourceMapBQTable.schema(
                        root_entity_id_type=config.root_entity_id_type
                    ),
                    clustering_fields=[config.root_entity_id_column_name],
                )

        metadata_collection.add_source_table(
            table_id=DocumentUploadStatusTable.table_id,
            description=DocumentUploadStatusTable.description,
            schema_fields=DocumentUploadStatusTable.schema(),
        )

        collections.append(metadata_collection)
        collections.append(contents_collection)

        collections.append(
            SourceTableCollection(
                dataset_id=document_store_temp_dataset_for_region(state_code),
                labels=labels,
                update_config=SourceTableCollectionUpdateConfig.regenerable(),
                # temporary tables should be created/deleted by the document store processing code,
                # but we set a two week expiration just in case to prevent orphaned tables from accumulating.
                default_table_expiration_ms=TWO_WEEK_MS,
                description=(
                    f"Temporary tables used during document store processing "
                    f"for {StateCode.get_state(state_code)}"
                ),
            )
        )

    return collections
