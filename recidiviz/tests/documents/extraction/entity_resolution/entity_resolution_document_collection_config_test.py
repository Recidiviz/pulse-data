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

import unittest

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.common.descriptor import Descriptor
from recidiviz.documents.extraction.entity_resolution.entity_resolution_composite_document_query_builder import (
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
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    FAKE_ASSIGNMENT_ER_COLLECTION_NAME,
    FAKE_COLLECTION_NAME,
    FAKE_LOCATION_ER_COLLECTION_NAME,
    fake_entity_resolution_document_collection_config,
    fake_first_order_extractor_config,
)
from recidiviz.utils.types import assert_type


class EntityResolutionDocumentCollectionConfigTest(unittest.TestCase):
    """Tests for EntityResolutionDocumentCollectionConfig: the fields it derives
    from its (first-order config, entity group) pair."""

    def _assert_derived_collection_fields(
        self,
        config: EntityResolutionDocumentCollectionConfig,
        *,
        expected_name: str,
        expected_group_name: str,
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
        # An ER collection's documents are composite documents, so it derives this
        # fixed descriptor rather than reading one from YAML.
        self.assertEqual(
            Descriptor(singular="composite document", plural="composite documents"),
            config.document_descriptor,
        )

    def test_derives_expected_fields_for_array_group(self) -> None:
        self._assert_derived_collection_fields(
            fake_entity_resolution_document_collection_config("assignment"),
            expected_name=FAKE_ASSIGNMENT_ER_COLLECTION_NAME,
            expected_group_name="assignment",
        )

    def test_derives_expected_fields_for_top_level_group(self) -> None:
        self._assert_derived_collection_fields(
            fake_entity_resolution_document_collection_config("location"),
            expected_name=FAKE_LOCATION_ER_COLLECTION_NAME,
            expected_group_name="location",
        )

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
                    ).entry_source_map_table_address(sandbox_dataset_prefix=None),
                )

    def test_sandbox_prefix_scopes_addresses(self) -> None:
        config = fake_entity_resolution_document_collection_config("assignment")

        # Passing a prefix scopes each address's dataset; omitting it (production)
        # leaves them un-prefixed.
        self.assertTrue(
            config.metadata_table_address(
                sandbox_dataset_prefix="sb"
            ).dataset_id.startswith("sb_us_xx_document_store_metadata")
        )
        self.assertTrue(
            config.document_contents_table_address(
                sandbox_dataset_prefix="sb"
            ).dataset_id.startswith("sb_us_xx_document_contents")
        )
        self.assertTrue(
            config.entry_source_map_table_address(
                sandbox_dataset_prefix="sb"
            ).dataset_id.startswith("sb_us_xx_document_store_metadata")
        )
        self.assertFalse(
            config.metadata_table_address(
                sandbox_dataset_prefix=None
            ).dataset_id.startswith("sb_")
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
