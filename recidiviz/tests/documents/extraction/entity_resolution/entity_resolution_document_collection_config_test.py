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
"""Tests for entity_resolution_document_collection_config.py."""
import datetime
import unittest
from typing import Any

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.common.descriptor import Descriptor
from recidiviz.documents.extraction.entity_resolution.entity_resolution_composite_document_query_builder import (
    ENTRY_NUM_FIELD_NAME,
    ENTRY_SOURCE_MAP_COLUMN_NAME,
    PRE_RESOLUTION_DOCUMENT_ID_COLUMN_NAME,
    PRE_RESOLUTION_SOURCE_ARRAY_INDEX_COLUMN_NAME,
    SOURCE_ARRAY_INDEX_FIELD_NAME,
    SOURCE_DOCUMENT_CONTENTS_ID_FIELD_NAME,
    SOURCE_DOCUMENT_UPDATE_DATETIME_FIELD_NAME,
    entry_source_map_schema_field,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    EntityResolutionDocumentCollectionConfig,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    load_first_order_llm_extractor_configs,
)
from recidiviz.documents.extraction.models.llm_extractor_collection_config import (
    EntityGroupConfig,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    PrimitiveScalarLLMRequestOutputSchemaField,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentRootEntityIdType,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    DOCUMENT_LENGTH_BYTES_COLUMN_NAME,
    DOCUMENT_TEXT_COLUMN_NAME,
    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
    PERSON_ID_COLUMN_NAME,
    ROW_CREATE_DATETIME_COLUMN_NAME,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.big_query.sqlglot_helpers import (
    check_query_is_not_ordered_outside_of_windows,
    check_query_selects_output_columns,
)
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    FAKE_ASSIGNMENT_ER_COLLECTION_NAME,
    FAKE_COLLECTION_NAME,
    FAKE_LOCATION_ER_COLLECTION_NAME,
    fake_entity_resolution_document_collection_config,
    fake_first_order_extractor_config,
    get_entity_group_by_name,
)
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.types import assert_type

# A fixed value for the required document_contents columns the composite-document
# query never reads (document_length_bytes / row_create_datetime).
_ROW_CREATE_DATETIME = datetime.datetime(2026, 7, 1, tzinfo=datetime.timezone.utc)


class EntityResolutionDocumentCollectionConfigTest(unittest.TestCase):
    """Tests for EntityResolutionDocumentCollectionConfig: the fields it derives
    from its (first-order config, entity group) pair, including the generated SQL
    text (without executing the query)."""

    def _assert_derived_collection_fields(
        self,
        config: EntityResolutionDocumentCollectionConfig,
        *,
        expected_name: str,
        expected_group_name: str,
        expected_query_template: str,
    ) -> None:
        """Asserts every DocumentCollectionConfig field the ER collection derives
        from its (first-order config, entity group) pair.
        """
        self.assertEqual(StateCode.US_XX, config.state_code)
        self.assertEqual(expected_name, config.name)
        self.assertEqual(
            f"Auto-generated entity-resolution composite documents for the "
            f"[{expected_group_name}] entity group of the "
            f"[FAKE_EXTRACTOR_COLLECTION] collection in [US_XX]. One composite "
            f"document per root entity, aggregating that entity's first-order "
            f"mentions for the entity resolution extractor to resolve.",
            config.description,
        )
        self.assertEqual(DocumentRootEntityIdType.PERSON_ID, config.root_entity_id_type)
        self.assertEqual([], config.document_primary_key_columns)
        self.assertEqual([], config.other_metadata_columns)
        self.assertEqual(
            [entry_source_map_schema_field()],
            config.other_document_generation_output_columns,
        )
        self.assertEqual(
            expected_query_template, config.document_generation_query_template
        )
        # An ER collection's documents are composite documents, so it derives this
        # fixed descriptor rather than reading one from YAML.
        self.assertEqual(
            Descriptor(singular="composite document", plural="composite documents"),
            config.document_descriptor,
        )

    def test_derives_expected_fields_for_array_group(self) -> None:

        expected_document_generation_query_template = r"""WITH mentions AS (
    SELECT
        pre.person_id AS person_id,
        pre.document_id AS source_document_contents_id,
        pre.document_update_datetime,
        pre.source_array_index AS source_array_index,
        source_docs.document_text AS source_document_text,
        pre.assignment_name, pre.assignment_type
    FROM `{project_id}.us_xx_document_extraction_results__pre_resolution.fake_extractor_collection_assignments` pre
    JOIN `{project_id}.us_xx_document_contents.fake_input_notes_document_contents` source_docs
        ON pre.document_id = source_docs.document_contents_id
    WHERE (pre.assignment_name IS NOT NULL OR pre.assignment_type IS NOT NULL)
        AND source_docs.document_text IS NOT NULL
        AND pre.document_update_datetime IS NOT NULL
),
numbered_entries AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY person_id
            ORDER BY
                document_update_datetime,
                source_document_contents_id,
                source_array_index
        ) AS entry_num
    FROM mentions
),
entry_blocks AS (
    SELECT
        person_id,
        source_document_contents_id,
        source_array_index,
        document_update_datetime,
        source_document_text,
        entry_num,
        CONCAT('[Entry ', CAST(entry_num AS STRING), ']', '\nassignment_name: ', COALESCE(CAST(assignment_name AS STRING), '[not provided]'), '\nassignment_type: ', COALESCE(CAST(assignment_type AS STRING), '[not provided]')) AS entry_block,
        STRUCT(
            entry_num AS entry_num,
            source_document_contents_id AS source_document_contents_id,
            document_update_datetime AS source_document_update_datetime,
            source_array_index AS source_array_index
        ) AS entry_source
    FROM numbered_entries
),
source_document_blocks AS (
    SELECT
        person_id,
        MIN(entry_num) AS first_entry_num,
        document_update_datetime,
        CONCAT(
            '=== Source document — ',
            CAST(CAST(document_update_datetime AS DATE) AS STRING),
            ' ===\n',
            'document_text: ', ANY_VALUE(source_document_text), '\n\n',
            STRING_AGG(entry_block, '\n\n' ORDER BY entry_num)
        ) AS source_document_block
    FROM entry_blocks
    GROUP BY person_id, source_document_contents_id, document_update_datetime
),
composite_document_text AS (
    SELECT
        person_id,
        STRING_AGG(source_document_block, '\n\n' ORDER BY first_entry_num) AS document_text,
        MAX(document_update_datetime) AS document_update_datetime
    FROM source_document_blocks
    GROUP BY person_id
),
composite_entry_source_map AS (
    SELECT
        person_id,
        ARRAY_AGG(entry_source ORDER BY entry_num) AS entry_source_map
    FROM entry_blocks
    GROUP BY person_id
)
SELECT
    composite_document_text.person_id AS person_id,
    composite_document_text.document_text AS document_text,
    composite_document_text.document_update_datetime AS document_update_datetime,
    composite_entry_source_map.entry_source_map AS entry_source_map
FROM composite_document_text
JOIN composite_entry_source_map USING (person_id)"""

        self._assert_derived_collection_fields(
            fake_entity_resolution_document_collection_config("assignment"),
            expected_name=FAKE_ASSIGNMENT_ER_COLLECTION_NAME,
            expected_group_name="assignment",
            expected_query_template=expected_document_generation_query_template,
        )

    def test_derives_expected_fields_for_top_level_group(self) -> None:

        expected_document_generation_query_template = r"""WITH mentions AS (
    SELECT
        pre.person_id AS person_id,
        pre.document_id AS source_document_contents_id,
        pre.document_update_datetime,
        CAST(NULL AS INT64) AS source_array_index,
        source_docs.document_text AS source_document_text,
        pre.location
    FROM `{project_id}.us_xx_document_extraction_results__pre_resolution.fake_extractor_collection` pre
    JOIN `{project_id}.us_xx_document_contents.fake_input_notes_document_contents` source_docs
        ON pre.document_id = source_docs.document_contents_id
    WHERE (pre.location IS NOT NULL)
        AND source_docs.document_text IS NOT NULL
        AND pre.document_update_datetime IS NOT NULL
),
numbered_entries AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY person_id
            ORDER BY
                document_update_datetime,
                source_document_contents_id,
                source_array_index
        ) AS entry_num
    FROM mentions
),
entry_blocks AS (
    SELECT
        person_id,
        source_document_contents_id,
        source_array_index,
        document_update_datetime,
        source_document_text,
        entry_num,
        CONCAT('[Entry ', CAST(entry_num AS STRING), ']', '\nlocation: ', COALESCE(CAST(location AS STRING), '[not provided]')) AS entry_block,
        STRUCT(
            entry_num AS entry_num,
            source_document_contents_id AS source_document_contents_id,
            document_update_datetime AS source_document_update_datetime,
            source_array_index AS source_array_index
        ) AS entry_source
    FROM numbered_entries
),
source_document_blocks AS (
    SELECT
        person_id,
        MIN(entry_num) AS first_entry_num,
        document_update_datetime,
        CONCAT(
            '=== Source document — ',
            CAST(CAST(document_update_datetime AS DATE) AS STRING),
            ' ===\n',
            'document_text: ', ANY_VALUE(source_document_text), '\n\n',
            STRING_AGG(entry_block, '\n\n' ORDER BY entry_num)
        ) AS source_document_block
    FROM entry_blocks
    GROUP BY person_id, source_document_contents_id, document_update_datetime
),
composite_document_text AS (
    SELECT
        person_id,
        STRING_AGG(source_document_block, '\n\n' ORDER BY first_entry_num) AS document_text,
        MAX(document_update_datetime) AS document_update_datetime
    FROM source_document_blocks
    GROUP BY person_id
),
composite_entry_source_map AS (
    SELECT
        person_id,
        ARRAY_AGG(entry_source ORDER BY entry_num) AS entry_source_map
    FROM entry_blocks
    GROUP BY person_id
)
SELECT
    composite_document_text.person_id AS person_id,
    composite_document_text.document_text AS document_text,
    composite_document_text.document_update_datetime AS document_update_datetime,
    composite_entry_source_map.entry_source_map AS entry_source_map
FROM composite_document_text
JOIN composite_entry_source_map USING (person_id)"""

        self._assert_derived_collection_fields(
            fake_entity_resolution_document_collection_config("location"),
            expected_name=FAKE_LOCATION_ER_COLLECTION_NAME,
            expected_group_name="location",
            expected_query_template=expected_document_generation_query_template,
        )

    def test_generated_query_selects_exactly_the_expected_columns(self) -> None:
        config = fake_first_order_extractor_config()
        for entity_group in config.extractor_collection.entity_groups:
            with self.subTest(group=entity_group.name):
                built = EntityResolutionDocumentCollectionConfig(
                    first_order_config=config, entity_group=entity_group
                )
                # The generation query outputs the temp-updates schema minus the
                # framework-computed document_contents_id.
                expected_columns = {
                    field.name
                    for field in built.build_bq_temp_document_metadata_updates_schema()
                } - {DOCUMENT_CONTENTS_ID_COLUMN_NAME}
                query = StrictStringFormatter().format(
                    built.document_generation_query_template,
                    project_id="recidiviz-test",
                )
                check_query_selects_output_columns(query, expected_columns)

    def test_generated_query_only_orders_within_windows_and_aggregations(self) -> None:
        config = fake_first_order_extractor_config()
        for entity_group in config.extractor_collection.entity_groups:
            with self.subTest(group=entity_group.name):
                query = StrictStringFormatter().format(
                    EntityResolutionDocumentCollectionConfig(
                        first_order_config=config, entity_group=entity_group
                    ).document_generation_query_template,
                    project_id="recidiviz-test",
                )
                check_query_is_not_ordered_outside_of_windows(query)

    def test_entry_source_map_table_address(self) -> None:
        for group_name, expected_address in [
            (
                "assignment",
                "us_xx_document_store_metadata."
                "fake_extractor_collection_assignment_entry_source_map",
            ),
            (
                "location",
                "us_xx_document_store_metadata."
                "fake_extractor_collection_location_entry_source_map",
            ),
        ]:
            with self.subTest(group=group_name):
                self.assertEqual(
                    BigQueryAddress.from_str(expected_address),
                    fake_entity_resolution_document_collection_config(
                        group_name
                    ).entry_source_map_table_address,
                )

    def test_entry_source_map_table_description(self) -> None:
        description = fake_entity_resolution_document_collection_config(
            "assignment"
        ).entry_source_map_table_description
        self.assertIn("[assignment]", description)
        self.assertIn(f"[{FAKE_COLLECTION_NAME}]", description)
        self.assertIn(str(StateCode.get_state(StateCode.US_XX)), description)

    def test_raises_for_group_not_declared_on_collection(self) -> None:
        config = fake_first_order_extractor_config()
        undeclared_group = EntityGroupConfig(
            name="undeclared",
            entity_descriptor=Descriptor(
                singular="undeclared thing", plural="undeclared things"
            ),
            entity_fields=[
                assert_type(
                    config.extractor_collection.output_schema.get_field("location"),
                    PrimitiveScalarLLMRequestOutputSchemaField,
                )
            ],
            source_array_field=None,
            grouping_instructions=None,
        )
        with self.assertRaisesRegex(
            ValueError, "is not declared on first-order collection"
        ):
            EntityResolutionDocumentCollectionConfig(
                first_order_config=config, entity_group=undeclared_group
            )


class BuildAllRealEntityResolutionDocumentCollectionsTest(unittest.TestCase):
    """Guards that an ER composite-document collection builds and validates for
    every entity group declared on a real first-order extractor."""

    def test_all_real_entity_groups(self) -> None:
        configs_by_state = load_first_order_llm_extractor_configs()
        self.assertTrue(configs_by_state)

        for state_configs in configs_by_state.values():
            for config in state_configs.values():
                for group in config.extractor_collection.entity_groups:
                    with self.subTest(
                        collection=config.extractor_collection.name,
                        state=config.state_code.value,
                        entity_group=group.name,
                    ):
                        # Raises if the ER composite-document collection for this
                        # (state, collection, group) fails to build or validate.
                        EntityResolutionDocumentCollectionConfig(
                            first_order_config=config, entity_group=group
                        )


def _contents_row(
    document_contents_id: str, document_text: str | None
) -> dict[str, Any]:
    """A document_contents row; a None |document_text| models a document whose
    text has been scrubbed after a source-data deletion."""
    return {
        DOCUMENT_CONTENTS_ID_COLUMN_NAME: document_contents_id,
        DOCUMENT_TEXT_COLUMN_NAME: document_text,
        # Required document_contents columns the generation query never reads.
        DOCUMENT_LENGTH_BYTES_COLUMN_NAME: (
            len(document_text.encode("utf-8")) if document_text is not None else 0
        ),
        ROW_CREATE_DATETIME_COLUMN_NAME: _ROW_CREATE_DATETIME,
    }


# TODO(OBT-32175): Remove this and derive the schema from the
#  LLMExtractorPreResolutionResultsViewBuilder instead, once it exists.
def _pre_resolution_schema(
    entity_group: EntityGroupConfig,
) -> list[bigquery.SchemaField]:
    """The subset of the first-order __pre_resolution parsed view columns the
    composite-document query reads, derived from the entity group."""
    schema = [
        bigquery.SchemaField(PERSON_ID_COLUMN_NAME, "INT64"),
        bigquery.SchemaField(PRE_RESOLUTION_DOCUMENT_ID_COLUMN_NAME, "STRING"),
        bigquery.SchemaField(DOCUMENT_UPDATE_DATETIME_COLUMN_NAME, "TIMESTAMP"),
    ]
    if entity_group.source_array_field is not None:
        schema.append(
            bigquery.SchemaField(PRE_RESOLUTION_SOURCE_ARRAY_INDEX_COLUMN_NAME, "INT64")
        )
    schema.extend(
        bigquery.SchemaField(field.name, "STRING")
        for field in entity_group.entity_fields
    )
    return schema


_OMIT_SOURCE_ARRAY_INDEX = object()


def _mention_row(
    *,
    person_id: int,
    document_id: str,
    document_update_date: str | None,
    source_array_index: Any = _OMIT_SOURCE_ARRAY_INDEX,
    **entity_field_values: Any,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        PERSON_ID_COLUMN_NAME: person_id,
        PRE_RESOLUTION_DOCUMENT_ID_COLUMN_NAME: document_id,
        DOCUMENT_UPDATE_DATETIME_COLUMN_NAME: (
            datetime.datetime.fromisoformat(document_update_date)
            if document_update_date is not None
            else None
        ),
        **entity_field_values,
    }
    if source_array_index is not _OMIT_SOURCE_ARRAY_INDEX:
        row[PRE_RESOLUTION_SOURCE_ARRAY_INDEX_COLUMN_NAME] = source_array_index
    return row


def _entry_source(
    entry_num: int,
    source_document_contents_id: str,
    source_document_update_date: str,
    source_array_index: int | None,
) -> dict[str, Any]:
    return {
        ENTRY_NUM_FIELD_NAME: entry_num,
        SOURCE_DOCUMENT_CONTENTS_ID_FIELD_NAME: source_document_contents_id,
        SOURCE_DOCUMENT_UPDATE_DATETIME_FIELD_NAME: datetime.datetime.fromisoformat(
            source_document_update_date
        ).replace(tzinfo=datetime.timezone.utc),
        SOURCE_ARRAY_INDEX_FIELD_NAME: source_array_index,
    }


class EntityResolutionCompositeDocumentQueryTest(BigQueryEmulatorTestCase):
    """Black-box tests: seed the first-order __pre_resolution view and
    document_contents table with data, run the generated composite-document
    query, and assert the resulting rows (the rendered composite documents and
    entry->source maps)."""

    def _seed_contents(self, rows: list[dict[str, Any]]) -> None:
        input_collection = fake_first_order_extractor_config().input_document_collection
        address = input_collection.document_contents_table_address
        self.create_mock_table(
            address, schema=input_collection.build_bq_document_contents_schema()
        )
        self.load_rows_into_table(address, rows)

    def _seed_pre_resolution(
        self,
        collection: EntityResolutionDocumentCollectionConfig,
        entity_group: EntityGroupConfig,
        rows: list[dict[str, Any]],
    ) -> None:
        self.create_mock_table(
            collection.pre_resolution_view_address,
            schema=_pre_resolution_schema(entity_group),
        )
        self.load_rows_into_table(collection.pre_resolution_view_address, rows)

    def _composite_documents_query(
        self, collection: EntityResolutionDocumentCollectionConfig
    ) -> str:
        """The generation query, formatted and wrapped in a deterministic ordering
        (the generation query itself has no top-level ORDER BY — one row per root
        entity) so the result rows can be compared exactly."""
        query = StrictStringFormatter().format(
            collection.document_generation_query_template,
            project_id=self.project_id,
        )
        return f"SELECT * FROM ({query}) ORDER BY {PERSON_ID_COLUMN_NAME}"

    def test_array_group_composite_documents(self) -> None:
        config = fake_first_order_extractor_config()
        entity_group = get_entity_group_by_name(
            config.extractor_collection, "assignment"
        )
        collection = EntityResolutionDocumentCollectionConfig(
            first_order_config=config, entity_group=entity_group
        )
        self._seed_contents(
            [
                _contents_row("doc_a", "Assigned to Kitchen crew."),
                _contents_row("doc_b", "Still on kitchen duty."),
                _contents_row("doc_c", "Moved to Laundry; picked up Library shift."),
                _contents_row("doc_d", "Assigned to Grounds."),
            ]
        )
        self._seed_pre_resolution(
            collection,
            entity_group,
            [
                # person 111 — mentions across three source documents. The same
                # entity is worded differently across notes ("Kitchen" vs "the
                # kitchen") — exactly what entity resolution exists to reconcile.
                _mention_row(
                    person_id=111,
                    document_id="doc_a",
                    document_update_date="2026-01-15",
                    source_array_index=0,
                    assignment_name="Kitchen",
                    assignment_type="internal",
                ),
                # Excluded: every entity field is null.
                _mention_row(
                    person_id=111,
                    document_id="doc_a",
                    document_update_date="2026-01-15",
                    source_array_index=1,
                    assignment_name=None,
                    assignment_type=None,
                ),
                _mention_row(
                    person_id=111,
                    document_id="doc_b",
                    document_update_date="2026-02-20",
                    source_array_index=0,
                    assignment_name="the kitchen",
                    assignment_type="internal",
                ),
                # assignment_type null -> rendered as the placeholder.
                _mention_row(
                    person_id=111,
                    document_id="doc_c",
                    document_update_date="2026-03-10",
                    source_array_index=0,
                    assignment_name="Laundry",
                    assignment_type=None,
                ),
                _mention_row(
                    person_id=111,
                    document_id="doc_c",
                    document_update_date="2026-03-10",
                    source_array_index=1,
                    assignment_name="Library",
                    assignment_type="external",
                ),
                # person 222 — a single mention; entry numbering restarts.
                _mention_row(
                    person_id=222,
                    document_id="doc_d",
                    document_update_date="2026-02-01",
                    source_array_index=0,
                    assignment_name="Grounds",
                    assignment_type="external",
                ),
            ],
        )

        person_111_composite = """=== Source document — 2026-01-15 ===
document_text: Assigned to Kitchen crew.

[Entry 1]
assignment_name: Kitchen
assignment_type: internal

=== Source document — 2026-02-20 ===
document_text: Still on kitchen duty.

[Entry 2]
assignment_name: the kitchen
assignment_type: internal

=== Source document — 2026-03-10 ===
document_text: Moved to Laundry; picked up Library shift.

[Entry 3]
assignment_name: Laundry
assignment_type: [not provided]

[Entry 4]
assignment_name: Library
assignment_type: external"""

        person_222_composite = """=== Source document — 2026-02-01 ===
document_text: Assigned to Grounds.

[Entry 1]
assignment_name: Grounds
assignment_type: external"""

        self.run_query_test(
            self._composite_documents_query(collection),
            expected_result=[
                {
                    PERSON_ID_COLUMN_NAME: 111,
                    DOCUMENT_TEXT_COLUMN_NAME: person_111_composite,
                    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME: datetime.datetime(
                        2026, 3, 10, tzinfo=datetime.timezone.utc
                    ),
                    ENTRY_SOURCE_MAP_COLUMN_NAME: [
                        _entry_source(1, "doc_a", "2026-01-15", 0),
                        _entry_source(2, "doc_b", "2026-02-20", 0),
                        _entry_source(3, "doc_c", "2026-03-10", 0),
                        _entry_source(4, "doc_c", "2026-03-10", 1),
                    ],
                },
                {
                    PERSON_ID_COLUMN_NAME: 222,
                    DOCUMENT_TEXT_COLUMN_NAME: person_222_composite,
                    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME: datetime.datetime(
                        2026, 2, 1, tzinfo=datetime.timezone.utc
                    ),
                    ENTRY_SOURCE_MAP_COLUMN_NAME: [
                        _entry_source(1, "doc_d", "2026-02-01", 0)
                    ],
                },
            ],
        )

    def test_top_level_group_composite_documents(self) -> None:
        config = fake_first_order_extractor_config()
        entity_group = get_entity_group_by_name(config.extractor_collection, "location")
        collection = EntityResolutionDocumentCollectionConfig(
            first_order_config=config, entity_group=entity_group
        )
        self._seed_contents(
            [
                _contents_row("doc_x", "Living at Building A."),
                _contents_row("doc_w", "Moved to Building B."),
                _contents_row("doc_z", "No location mentioned."),
            ]
        )
        self._seed_pre_resolution(
            collection,
            entity_group,
            [
                _mention_row(
                    person_id=333,
                    document_id="doc_x",
                    document_update_date="2026-01-10",
                    location="Building A",
                ),
                _mention_row(
                    person_id=333,
                    document_id="doc_w",
                    document_update_date="2026-02-05",
                    location="Building B",
                ),
                # Excluded: the only entity field is null.
                _mention_row(
                    person_id=333,
                    document_id="doc_z",
                    document_update_date="2026-03-10",
                    location=None,
                ),
            ],
        )

        person_333_composite = """=== Source document — 2026-01-10 ===
document_text: Living at Building A.

[Entry 1]
location: Building A

=== Source document — 2026-02-05 ===
document_text: Moved to Building B.

[Entry 2]
location: Building B"""

        self.run_query_test(
            self._composite_documents_query(collection),
            expected_result=[
                {
                    PERSON_ID_COLUMN_NAME: 333,
                    DOCUMENT_TEXT_COLUMN_NAME: person_333_composite,
                    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME: datetime.datetime(
                        2026, 2, 5, tzinfo=datetime.timezone.utc
                    ),
                    # source_array_index is null for a top-level entity group.
                    ENTRY_SOURCE_MAP_COLUMN_NAME: [
                        _entry_source(1, "doc_x", "2026-01-10", None),
                        _entry_source(2, "doc_w", "2026-02-05", None),
                    ],
                },
            ],
        )

    def test_sort_tiebreaker_orders_by_source_document_contents_id(self) -> None:
        config = fake_first_order_extractor_config()
        entity_group = get_entity_group_by_name(
            config.extractor_collection, "assignment"
        )
        collection = EntityResolutionDocumentCollectionConfig(
            first_order_config=config, entity_group=entity_group
        )
        self._seed_contents(
            [
                _contents_row("doc_1", "First note."),
                _contents_row("doc_2", "Second note."),
            ]
        )
        # Same datetime, inserted out of order — the query must order by
        # source_document_contents_id, identically in the text and the map.
        self._seed_pre_resolution(
            collection,
            entity_group,
            [
                _mention_row(
                    person_id=444,
                    document_id="doc_2",
                    document_update_date="2026-05-01",
                    source_array_index=0,
                    assignment_name="Beta",
                    assignment_type="internal",
                ),
                _mention_row(
                    person_id=444,
                    document_id="doc_1",
                    document_update_date="2026-05-01",
                    source_array_index=0,
                    assignment_name="Alpha",
                    assignment_type="internal",
                ),
            ],
        )

        tiebreaker_composite = """=== Source document — 2026-05-01 ===
document_text: First note.

[Entry 1]
assignment_name: Alpha
assignment_type: internal

=== Source document — 2026-05-01 ===
document_text: Second note.

[Entry 2]
assignment_name: Beta
assignment_type: internal"""

        self.run_query_test(
            self._composite_documents_query(collection),
            expected_result=[
                {
                    PERSON_ID_COLUMN_NAME: 444,
                    DOCUMENT_TEXT_COLUMN_NAME: tiebreaker_composite,
                    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME: datetime.datetime(
                        2026, 5, 1, tzinfo=datetime.timezone.utc
                    ),
                    ENTRY_SOURCE_MAP_COLUMN_NAME: [
                        _entry_source(1, "doc_1", "2026-05-01", 0),
                        _entry_source(2, "doc_2", "2026-05-01", 0),
                    ],
                },
            ],
        )

    def test_duplicate_text_on_different_dates_renders_separate_blocks(self) -> None:
        """Two identically-worded notes on different dates share a
        document_contents_id (contents are content-addressed) but are distinct
        timeline occurrences: each renders as its own dated block, in
        chronological order, and each map entry carries its occurrence's
        datetime."""
        config = fake_first_order_extractor_config()
        entity_group = get_entity_group_by_name(
            config.extractor_collection, "assignment"
        )
        collection = EntityResolutionDocumentCollectionConfig(
            first_order_config=config, entity_group=entity_group
        )
        self._seed_contents(
            [
                _contents_row("doc_dup", "Still on Kitchen duty."),
                _contents_row("doc_mid", "Likes their job in the kitchen"),
            ]
        )
        # The parse for the duplicated text exists once (per contents id), so
        # both of its occurrence rows carry identical entity field values.
        self._seed_pre_resolution(
            collection,
            entity_group,
            [
                _mention_row(
                    person_id=555,
                    document_id="doc_dup",
                    document_update_date="2026-06-01",
                    source_array_index=0,
                    assignment_name="Kitchen duty",
                    assignment_type="internal",
                ),
                _mention_row(
                    person_id=555,
                    document_id="doc_mid",
                    document_update_date="2026-06-02",
                    source_array_index=0,
                    assignment_name="the kitchen",
                    assignment_type="internal",
                ),
                _mention_row(
                    person_id=555,
                    document_id="doc_dup",
                    document_update_date="2026-06-03",
                    source_array_index=0,
                    assignment_name="Kitchen duty",
                    assignment_type="internal",
                ),
            ],
        )

        timeline_composite = """=== Source document — 2026-06-01 ===
document_text: Still on Kitchen duty.

[Entry 1]
assignment_name: Kitchen duty
assignment_type: internal

=== Source document — 2026-06-02 ===
document_text: Likes their job in the kitchen

[Entry 2]
assignment_name: the kitchen
assignment_type: internal

=== Source document — 2026-06-03 ===
document_text: Still on Kitchen duty.

[Entry 3]
assignment_name: Kitchen duty
assignment_type: internal"""

        self.run_query_test(
            self._composite_documents_query(collection),
            expected_result=[
                {
                    PERSON_ID_COLUMN_NAME: 555,
                    DOCUMENT_TEXT_COLUMN_NAME: timeline_composite,
                    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME: datetime.datetime(
                        2026, 6, 3, tzinfo=datetime.timezone.utc
                    ),
                    ENTRY_SOURCE_MAP_COLUMN_NAME: [
                        _entry_source(1, "doc_dup", "2026-06-01", 0),
                        _entry_source(2, "doc_mid", "2026-06-02", 0),
                        _entry_source(3, "doc_dup", "2026-06-03", 0),
                    ],
                },
            ],
        )

    def test_scrubbed_source_text_excluded_from_text_and_map(self) -> None:
        """A source document whose contents text has been scrubbed to null
        (deleted in source data after extraction ran) is excluded from BOTH the
        composite text and the entry_source_map, with the remaining entries
        renumbered compactly — the text and the map never fall out of sync.

        NOTE: This is not something we expect to happen in a normal flow at the time of
        writing this test, but it is technically possible to have null document_text
        because the schema allows it.
        """
        config = fake_first_order_extractor_config()
        entity_group = get_entity_group_by_name(config.extractor_collection, "location")
        collection = EntityResolutionDocumentCollectionConfig(
            first_order_config=config, entity_group=entity_group
        )
        self._seed_contents(
            [
                _contents_row("doc_live_1", "Living at Building A."),
                _contents_row("doc_scrubbed", None),
                _contents_row("doc_live_2", "Moved to Building B."),
            ]
        )
        # The scrubbed document's parsed mention still exists in the
        # pre-resolution results (extraction ran before the deletion).
        self._seed_pre_resolution(
            collection,
            entity_group,
            [
                _mention_row(
                    person_id=666,
                    document_id="doc_live_1",
                    document_update_date="2026-06-01",
                    location="Building A",
                ),
                _mention_row(
                    person_id=666,
                    document_id="doc_scrubbed",
                    document_update_date="2026-06-02",
                    location="Building C",
                ),
                _mention_row(
                    person_id=666,
                    document_id="doc_live_2",
                    document_update_date="2026-06-03",
                    location="Building B",
                ),
            ],
        )

        scrubbed_composite = """=== Source document — 2026-06-01 ===
document_text: Living at Building A.

[Entry 1]
location: Building A

=== Source document — 2026-06-03 ===
document_text: Moved to Building B.

[Entry 2]
location: Building B"""

        self.run_query_test(
            self._composite_documents_query(collection),
            expected_result=[
                {
                    PERSON_ID_COLUMN_NAME: 666,
                    DOCUMENT_TEXT_COLUMN_NAME: scrubbed_composite,
                    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME: datetime.datetime(
                        2026, 6, 3, tzinfo=datetime.timezone.utc
                    ),
                    ENTRY_SOURCE_MAP_COLUMN_NAME: [
                        _entry_source(1, "doc_live_1", "2026-06-01", None),
                        _entry_source(2, "doc_live_2", "2026-06-03", None),
                    ],
                },
            ],
        )

    def test_null_update_datetime_excluded_from_text_and_map(self) -> None:
        """A pre-resolution row with a null document_update_datetime (a deleted
        document leaking through the pre-resolution view) is excluded from BOTH
        the composite text and the entry_source_map."""
        config = fake_first_order_extractor_config()
        entity_group = get_entity_group_by_name(config.extractor_collection, "location")
        collection = EntityResolutionDocumentCollectionConfig(
            first_order_config=config, entity_group=entity_group
        )
        self._seed_contents(
            [
                _contents_row("doc_dated", "Living at Building A."),
                _contents_row("doc_undated", "Moved to Building B."),
            ]
        )
        self._seed_pre_resolution(
            collection,
            entity_group,
            [
                _mention_row(
                    person_id=777,
                    document_id="doc_dated",
                    document_update_date="2026-06-01",
                    location="Building A",
                ),
                _mention_row(
                    person_id=777,
                    document_id="doc_undated",
                    document_update_date=None,
                    location="Building B",
                ),
            ],
        )

        dated_only_composite = """=== Source document — 2026-06-01 ===
document_text: Living at Building A.

[Entry 1]
location: Building A"""

        self.run_query_test(
            self._composite_documents_query(collection),
            expected_result=[
                {
                    PERSON_ID_COLUMN_NAME: 777,
                    DOCUMENT_TEXT_COLUMN_NAME: dated_only_composite,
                    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME: datetime.datetime(
                        2026, 6, 1, tzinfo=datetime.timezone.utc
                    ),
                    ENTRY_SOURCE_MAP_COLUMN_NAME: [
                        _entry_source(1, "doc_dated", "2026-06-01", None),
                    ],
                },
            ],
        )
