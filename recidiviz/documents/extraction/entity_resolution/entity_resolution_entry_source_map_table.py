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
"""The entry→source map: the BigQuery table that maps each numbered entry in an
entity-resolution composite document back to the first-order mention occurrence
it was rendered from.

Pure framework bookkeeping (no model output) — the traceability bridge that lets
an ER extractor's clustering output, which references entry numbers, be stitched
back onto the original first-order mention rows.

The map is produced as the nested `entry_source_map` generation-output column of
the composite-document collection (see
`entity_resolution_composite_document_query_builder.py`). On every discovery run
the document store materializes the collection's full generation output — every
root entity's composite, not just changed ones — and this table is rebuilt from
that snapshot, independent of document-change detection and of upload success.
The table is exactly that struct `UNNEST`ed with the root entity and the
composite document's `document_contents_id` attached, so its schema is derived
from the struct's sub-fields rather than restated — the two cannot drift apart.

Rows are keyed by **root entity**, not by the composite's `document_contents_id`,
and hold exactly one current row set per (root entity, entry) — replaced in place
whenever the map changes. Two things make the composite hash the wrong key:

  - Two root entities can share a composite `document_contents_id` and still need
    different map rows. The composite text renders only the *date* of each source
    occurrence, while the map records the full `source_document_update_datetime`,
    so identically-worded notes written at different times of the same day
    produce one hash and two distinct valid maps.
  - The map can change while the composite text, and therefore its hash, does
    not: an entry renders no array index, so a re-extraction that stops emitting
    an all-null array element shifts a mention's `source_array_index` without
    changing a byte of the rendered document.

Because exactly one current version is stored per root entity, consumers (the ER
validator, the enriched parsed views) need no generation filtering.
"""
from google.cloud import bigquery
from google.cloud.bigquery.enums import SqlTypeNames

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import BigQueryFieldMode
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.dataset_config import (
    document_store_metadata_dataset_for_region,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_composite_document_query_builder import (
    entry_source_map_schema_field,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentRootEntityIdType,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    get_document_store_column_schema,
)

# Suffix appended to the first-order extractor collection name and the entity
# group name to form the map table's id.
ENTRY_SOURCE_MAP_TABLE_ID_SUFFIX = "entry_source_map"


class EntityResolutionEntrySourceMapBQTable:
    """The entry→source map table for one entity-resolution composite-document
    collection: one row per composite-document entry, mapping it back to the
    first-order mention occurrence it was rendered from. See the module docstring.

    Named for, and addressable from, the (first-order extractor collection,
    entity group) pair alone — consumers that start from the first-order side
    never need to resolve the ER collection to find it.
    """

    @staticmethod
    def table_id(
        *, first_order_extractor_collection_name: str, entity_group_name: str
    ) -> str:
        """Returns the map table's id for one (first-order extractor collection,
        entity group) pair, e.g.
        `case_note_employment_info_employer_entry_source_map`.
        """
        return (
            f"{first_order_extractor_collection_name}_{entity_group_name}"
            f"_{ENTRY_SOURCE_MAP_TABLE_ID_SUFFIX}"
        ).lower()

    @classmethod
    def address(
        cls,
        *,
        state_code: StateCode,
        first_order_extractor_collection_name: str,
        entity_group_name: str,
    ) -> BigQueryAddress:
        """Returns the project-agnostic address of the map table, which lives in
        the state's document store metadata dataset alongside the composite
        documents' own metadata table.
        """
        return BigQueryAddress(
            dataset_id=document_store_metadata_dataset_for_region(state_code),
            table_id=cls.table_id(
                first_order_extractor_collection_name=first_order_extractor_collection_name,
                entity_group_name=entity_group_name,
            ),
        )

    @staticmethod
    def description(
        *,
        state_code: StateCode,
        first_order_extractor_collection_name: str,
        entity_group_name: str,
    ) -> str:
        """Returns the map table's description."""
        return (
            f"Entry→source map for the [{entity_group_name}] entity group of the "
            f"[{first_order_extractor_collection_name}] collection in "
            f"{StateCode.get_state(state_code)}. One row per entity-resolution "
            f"composite-document entry, mapping it back to the first-order mention "
            f"occurrence it was rendered from. Framework-built and deterministic "
            f"(no model output). Holds exactly one current row set per root entity, "
            f"replaced whenever that entity's map changes."
        )

    @staticmethod
    def schema(
        *, root_entity_id_type: DocumentRootEntityIdType
    ) -> list[bigquery.SchemaField]:
        """Returns the map table's schema: the root entity whose composite document
        the entry belongs to, that composite's contents id, then the flattened
        sub-fields of the `entry_source_map` struct the composite-document
        generation query emits — so the table is exactly one row per `UNNEST`ed
        entry.

        |root_entity_id_type| is the ER collection's root entity ID type, always an
        internal (`person_id` / `staff_id`) type. It is the key rows are written
        and replaced by, and the leg that keeps two root entities sharing a
        composite from crossing their mention rows.
        """
        if root_entity_id_type is not root_entity_id_type.internal_id_type:
            raise ValueError(
                f"Entry→source map tables are keyed by the internal root entity ID, "
                f"but got external-id type [{root_entity_id_type}]. Expected "
                f"[{root_entity_id_type.internal_id_type}]."
            )
        return [
            get_document_store_column_schema(root_entity_id_type.id_column_name),
            bigquery.SchemaField(
                name=DOCUMENT_CONTENTS_ID_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode=BigQueryFieldMode.REQUIRED.value,
                description="document_contents_id of the entity-resolution composite "
                "document this entry belongs to.",
            ),
            *entry_source_map_schema_field().fields,
        ]
