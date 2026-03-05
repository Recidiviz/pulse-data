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
"""Collects source table definitions for document store metadata tables."""
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.store.document_collection_config import (
    DOCUMENT_STORE_METADATA_DATASET_ID,
    collect_document_collection_configs,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
)


def collect_document_store_source_table_collections() -> list[SourceTableCollection]:
    """Creates a SourceTableCollection for the document_store_metadata dataset
    containing one table per document collection config."""
    configs = collect_document_collection_configs()

    collection = SourceTableCollection(
        dataset_id=DOCUMENT_STORE_METADATA_DATASET_ID,
        update_config=SourceTableCollectionUpdateConfig.protected(),
        description=(
            "Document store metadata tables. One table per document collection, "
            "tracking metadata for each stored document."
        ),
        labels=[],
    )

    for config_name, config in sorted(configs.items()):
        address = config.metadata_table_address()
        collection.add_source_table(
            table_id=address.table_id,
            schema_fields=config.metadata_table_schema(),
            description=f"Metadata for document collection {config_name}.",
        )

    return [collection]
