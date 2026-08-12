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
"""Builds the SQL template that generates an entity-resolution extractor's
composite documents.

An entity-resolution (ER) extractor resolves one first-order collection's
differently-worded mentions of the same real-world entity (an employer, a
residence, ...) into canonical entities. It does this by running a per-document
LLM pass over a *composite document* — one per root entity, holding all of that
entity's first-order mentions rendered into a single numbered text blob.

Rather than a bespoke composite-document builder, we reuse the document store:
the composite documents are an ordinary document collection (see
`EntityResolutionDocumentCollectionConfig`, whose
`build_document_generation_query_template` builds its generation query from this
module), so the standard document upload flow generates, content-addresses, and
uploads them like any other collection.

The composite renders the person's mentions as a *timeline*: one block per
source-note occurrence — a `(source document, update datetime)` pair — so an
identically-worded note appearing on two dates renders as two dated blocks.
(Two notes with identical text share a `document_contents_id` in the
content-addressed store; their distinct occurrences are distinguished by
`document_update_datetime`.)

The generated template reads the first-order "__pre_resolution" parsed views, and
relies on those views producing exactly one row per (root entity, source document,
document_update_datetime[, array element]), with exact duplicates deduplicated and
null-datetime / deleted documents excluded upstream.

It numbers each included per-entry row once (`ROW_NUMBER` over the framework-fixed
temporal sort with a deterministic total-order tiebreaker), then produces both
outputs from those same rows in one `GROUP BY` per root entity: the composite
`document_text` (an ordered `STRING_AGG` of per-occurrence blocks) and an
`entry_source_map` column (an ordered `ARRAY_AGG` of `STRUCT<entry_num,
source_document_contents_id, source_document_update_datetime,
source_array_index>`). Because both come out of a single execution, the rendered
text and the map cannot disagree. The map is declared via the collection's
`other_document_generation_output_columns`; the entry→source map table is rebuilt
on every discovery run from the run's materialized generation output — every root
entity's row, not just changed ones — because a mention's `source_array_index`
can shift without changing a byte of the rendered text (see
`entity_resolution_entry_source_map_table.py`).
"""
from google.cloud import bigquery
from google.cloud.bigquery.enums import SqlTypeNames

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import BigQueryFieldMode
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.documents.extraction.extraction_results_columns import (
    SOURCE_ARRAY_INDEX_COLUMN_NAME,
)
from recidiviz.documents.extraction.models.llm_extractor_collection_config import (
    EntityGroupConfig,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentRootEntityIdType,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    DOCUMENT_TEXT_COLUMN_NAME,
    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
)
from recidiviz.utils.string_formatting import fix_indent

# The generation-output-only `entry_source_map` column and its struct sub-fields.
ENTRY_SOURCE_MAP_COLUMN_NAME = "entry_source_map"
ENTRY_NUM_FIELD_NAME = "entry_num"
SOURCE_DOCUMENT_CONTENTS_ID_FIELD_NAME = "source_document_contents_id"
SOURCE_DOCUMENT_UPDATE_DATETIME_FIELD_NAME = "source_document_update_datetime"
SOURCE_ARRAY_INDEX_FIELD_NAME = "source_array_index"

# Rendered into the composite document text: the label prefixing each source
# document's text, and the placeholder shown for an included entry's null entity
# field. The label coincides with the document_text column name but is
# prompt-facing text, kept as its own constant so a column rename never silently
# rewords the composite documents.
SOURCE_DOCUMENT_TEXT_LABEL = "document_text"
NULL_ENTITY_FIELD_PLACEHOLDER = "[not provided]"


def entry_source_map_schema_field() -> bigquery.SchemaField:
    """Returns the schema of the generation-output-only `entry_source_map`
    column: a REPEATED RECORD, one struct per composite-document entry, mapping
    that entry back to the first-order mention occurrence it renders.
    """
    return bigquery.SchemaField(
        name=ENTRY_SOURCE_MAP_COLUMN_NAME,
        field_type=SqlTypeNames.RECORD.value,
        mode=BigQueryFieldMode.REPEATED.value,
        fields=[
            bigquery.SchemaField(
                name=ENTRY_NUM_FIELD_NAME,
                field_type=SqlTypeNames.INT64.value,
                mode=BigQueryFieldMode.REQUIRED.value,
                description="1-based entry number within the composite document.",
            ),
            bigquery.SchemaField(
                name=SOURCE_DOCUMENT_CONTENTS_ID_FIELD_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode=BigQueryFieldMode.REQUIRED.value,
                description="document_contents_id of the first-order source document "
                "this entry was rendered from.",
            ),
            bigquery.SchemaField(
                name=SOURCE_DOCUMENT_UPDATE_DATETIME_FIELD_NAME,
                field_type=SqlTypeNames.TIMESTAMP.value,
                mode=BigQueryFieldMode.REQUIRED.value,
                description="document_update_datetime of the source-note occurrence "
                "this entry was rendered from. Distinguishes occurrences of "
                "identically-worded notes, which share a document_contents_id.",
            ),
            bigquery.SchemaField(
                name=SOURCE_ARRAY_INDEX_FIELD_NAME,
                field_type=SqlTypeNames.INT64.value,
                mode=BigQueryFieldMode.NULLABLE.value,
                description="0-based element position within the source document's "
                "array; null for a top-level entity group.",
            ),
        ],
    )


class EntityResolutionCompositeDocumentQueryTemplateBuilder:
    """Builds the composite-document generation SQL template for one
    entity-resolution collection. See the module docstring.

    Takes the values the template needs rather than the ER collection config,
    since that config derives its generation query from this builder.
    """

    def __init__(
        self,
        *,
        # The internal root entity ID type (person_id / staff_id) the composite
        # documents are partitioned and keyed by.
        root_entity_id_type: DocumentRootEntityIdType,
        # The entity group whose mentions the composite documents render.
        entity_group: EntityGroupConfig,
        # The materialized address of the first-order `__pre_resolution` parsed view
        # the mentions are read from: the array-level view for an array entity group,
        # the doc-level view for a top-level one.
        pre_resolution_view_materialized_address: BigQueryAddress,
        # The first-order collection's document_contents table, which holds the
        # source note text keyed by document_contents_id.
        source_document_contents_address: BigQueryAddress,
    ) -> None:
        self.root_entity_id_type = root_entity_id_type
        self.entity_group = entity_group
        self.pre_resolution_view_materialized_address = (
            pre_resolution_view_materialized_address
        )
        self.source_document_contents_address = source_document_contents_address

    @property
    def _root_entity_id_column_name(self) -> str:
        """The internal root entity ID column (person_id / staff_id) the composite
        documents are partitioned and keyed by.
        """
        return self.root_entity_id_type.id_column_name

    @property
    def _entity_field_names(self) -> list[str]:
        return [field.name for field in self.entity_group.entity_fields]

    def _source_array_index_select(self) -> str:
        """Returns the `source_array_index` select expression for the mentions
        CTE: the array element index for an array group, a typed NULL for a
        top-level group (which has one entry per source document).
        """
        if self.entity_group.source_array_field is not None:
            return f"pre.{SOURCE_ARRAY_INDEX_COLUMN_NAME}"
        return "CAST(NULL AS INT64)"

    def _mention_inclusion_filter(self) -> str:
        """Returns the WHERE clause including a mention iff any of its entity
        fields is non-null.
        """
        return " OR ".join(
            f"pre.{field_name} IS NOT NULL" for field_name in self._entity_field_names
        )

    def _entry_block_concat(self) -> str:
        """Returns the CONCAT expression rendering a single entry's block: the
        `[Entry N]` header followed by one `field: value` line per entity field
        (null fields shown as the placeholder). For an entity group whose only
        entity field is `employer_name`, entry 3 renders as:

            [Entry 3]
            employer_name: McDonalds

        with `employer_name: [not provided]` in place of the value when it is null.
        """
        concat_args = [
            "'[Entry '",
            f"CAST({ENTRY_NUM_FIELD_NAME} AS STRING)",
            "']'",
        ]
        for field_name in self._entity_field_names:
            # Each field is one newline-prefixed "field: value" line, with the
            # placeholder substituted when the value is null.
            concat_args.append(
                f"'\\n{field_name}: ', "
                f"COALESCE(CAST({field_name} AS STRING), "
                f"'{NULL_ENTITY_FIELD_PLACEHOLDER}')"
            )
        return f"CONCAT({', '.join(concat_args)})"

    def _source_document_block_concat(self) -> str:
        """Returns the CONCAT expression rendering one source-note occurrence's
        block: the dated header, the source note text shown once, then all of
        that occurrence's entry blocks nested underneath. The grouping key is
        (source document, update datetime), so `document_update_datetime` is
        referenced directly and an identically-worded note on two dates renders
        as two separately-dated blocks.
        """
        return fix_indent(
            f"""
            CONCAT(
                '=== Source document — ',
                CAST(CAST({DOCUMENT_UPDATE_DATETIME_COLUMN_NAME} AS DATE) AS STRING),
                ' ===\\n',
                '{SOURCE_DOCUMENT_TEXT_LABEL}: ', ANY_VALUE(source_document_text), '\\n\\n',
                STRING_AGG(entry_block, '\\n\\n' ORDER BY {ENTRY_NUM_FIELD_NAME})
            )
            """,
            indent_level=0,
        )

    def build_query_template(self) -> str:
        """Returns the composite-document generation SQL template. Its output is
        exactly `{root_id, document_text, document_update_datetime,
        entry_source_map}` — the document store wrapper adds document_contents_id.

        The mentions CTE excludes rows with a null source note text or update
        datetime (a deleted/scrubbed source document): each block is rendered
        with a single CONCAT, which nulls out entirely if any input is null, and
        STRING_AGG then silently drops the block from the text while the map —
        aggregated upstream of the CONCAT — would still list its entries.
        Filtering before both aggregations keeps the text and the map derived
        from the same row set no matter what. The pre-resolution view contract
        (OBT-32175) forbids such rows; this filter is the structural backstop.
        """
        root_id = self._root_entity_id_column_name
        entity_field_select = list_to_query_string(
            self._entity_field_names, table_prefix="pre"
        )

        # The template is indented to sit under `query =`; fix_indent dedents it
        # to a clean left margin. The one multi-line embedded fragment
        # (_source_document_block_concat) is fix_indent'd to its column so its
        # continuation lines nest cleanly — hence its flush-left placeholder line.
        query = f"""
        WITH mentions AS (
            SELECT
                pre.{root_id} AS {root_id},
                pre.{DOCUMENT_CONTENTS_ID_COLUMN_NAME} AS {SOURCE_DOCUMENT_CONTENTS_ID_FIELD_NAME},
                pre.{DOCUMENT_UPDATE_DATETIME_COLUMN_NAME},
                {self._source_array_index_select()} AS {SOURCE_ARRAY_INDEX_FIELD_NAME},
                source_docs.{DOCUMENT_TEXT_COLUMN_NAME} AS source_document_text,
                {entity_field_select}
            FROM `{self.pre_resolution_view_materialized_address.format_address_for_query_template()}` pre
            JOIN `{self.source_document_contents_address.format_address_for_query_template()}` source_docs
                ON pre.{DOCUMENT_CONTENTS_ID_COLUMN_NAME} = source_docs.{DOCUMENT_CONTENTS_ID_COLUMN_NAME}
            WHERE ({self._mention_inclusion_filter()})
                AND source_docs.{DOCUMENT_TEXT_COLUMN_NAME} IS NOT NULL
                AND pre.{DOCUMENT_UPDATE_DATETIME_COLUMN_NAME} IS NOT NULL
        ),
        numbered_entries AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY {root_id}
                    ORDER BY
                        {DOCUMENT_UPDATE_DATETIME_COLUMN_NAME},
                        {SOURCE_DOCUMENT_CONTENTS_ID_FIELD_NAME},
                        {SOURCE_ARRAY_INDEX_FIELD_NAME}
                ) AS {ENTRY_NUM_FIELD_NAME}
            FROM mentions
        ),
        entry_blocks AS (
            SELECT
                {root_id},
                {SOURCE_DOCUMENT_CONTENTS_ID_FIELD_NAME},
                {SOURCE_ARRAY_INDEX_FIELD_NAME},
                {DOCUMENT_UPDATE_DATETIME_COLUMN_NAME},
                source_document_text,
                {ENTRY_NUM_FIELD_NAME},
                {self._entry_block_concat()} AS entry_block,
                STRUCT(
                    {ENTRY_NUM_FIELD_NAME} AS {ENTRY_NUM_FIELD_NAME},
                    {SOURCE_DOCUMENT_CONTENTS_ID_FIELD_NAME} AS {SOURCE_DOCUMENT_CONTENTS_ID_FIELD_NAME},
                    {DOCUMENT_UPDATE_DATETIME_COLUMN_NAME} AS {SOURCE_DOCUMENT_UPDATE_DATETIME_FIELD_NAME},
                    {SOURCE_ARRAY_INDEX_FIELD_NAME} AS {SOURCE_ARRAY_INDEX_FIELD_NAME}
                ) AS entry_source
            FROM numbered_entries
        ),
        source_document_blocks AS (
            SELECT
                {root_id},
                MIN({ENTRY_NUM_FIELD_NAME}) AS first_entry_num,
                {DOCUMENT_UPDATE_DATETIME_COLUMN_NAME},
{fix_indent(self._source_document_block_concat(), indent_level=16)} AS source_document_block
            FROM entry_blocks
            GROUP BY {root_id}, {SOURCE_DOCUMENT_CONTENTS_ID_FIELD_NAME}, {DOCUMENT_UPDATE_DATETIME_COLUMN_NAME}
        ),
        composite_document_text AS (
            SELECT
                {root_id},
                STRING_AGG(source_document_block, '\\n\\n' ORDER BY first_entry_num) AS {DOCUMENT_TEXT_COLUMN_NAME},
                MAX({DOCUMENT_UPDATE_DATETIME_COLUMN_NAME}) AS {DOCUMENT_UPDATE_DATETIME_COLUMN_NAME}
            FROM source_document_blocks
            GROUP BY {root_id}
        ),
        composite_entry_source_map AS (
            SELECT
                {root_id},
                ARRAY_AGG(entry_source ORDER BY {ENTRY_NUM_FIELD_NAME}) AS {ENTRY_SOURCE_MAP_COLUMN_NAME}
            FROM entry_blocks
            GROUP BY {root_id}
        )
        SELECT
            composite_document_text.{root_id} AS {root_id},
            composite_document_text.{DOCUMENT_TEXT_COLUMN_NAME} AS {DOCUMENT_TEXT_COLUMN_NAME},
            composite_document_text.{DOCUMENT_UPDATE_DATETIME_COLUMN_NAME} AS {DOCUMENT_UPDATE_DATETIME_COLUMN_NAME},
            composite_entry_source_map.{ENTRY_SOURCE_MAP_COLUMN_NAME} AS {ENTRY_SOURCE_MAP_COLUMN_NAME}
        FROM composite_document_text
        JOIN composite_entry_source_map USING ({root_id})
        """
        return fix_indent(query, indent_level=0)
