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
"""Contains source table definitions for document store metadata tables."""
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_collection_config import (
    collect_document_collection_configs,
)
from recidiviz.ingest.direct.dataset_config import (
    document_store_metadata_dataset_for_region,
    document_store_temp_dataset_for_region,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.source_tables.source_table_config import (
    DocumentStoreSourceTableLabel,
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    StateSpecificSourceTableLabel,
)

TWO_WEEK_MS = 14 * 24 * 60 * 60 * 1000


def collect_document_store_source_tables() -> list[SourceTableCollection]:
    """Collects source table definitions for all document store metadata tables."""
    collections: list[SourceTableCollection] = []

    for state_code in get_direct_ingest_states_existing_in_env():
        configs = collect_document_collection_configs(state_code)
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

        for config in configs.values():
            metadata_collection.add_source_table(
                table_id=config.name,
                description=config.description,
                schema_fields=config.build_bq_metadata_schema(),
            )

        collections.append(metadata_collection)

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
