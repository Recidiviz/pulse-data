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
"""Tests for entity_resolution_extractor_collection_config_builder.py.

Builds the ER extractor collection from the fake first-order collection's entity
groups — a top-level group (`location`) and an array group (`assignment`) — and
asserts the synthesized `LLMExtractorCollectionConfig` and the parent → ER
`collection_version_id` cascade.
"""
import re
from unittest import TestCase

import attr

from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    entity_resolution_collection_name,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_extractor_collection_config_builder import (
    ENTITY_RESOLUTION_DEFAULT_MODEL_CONFIG_NAME,
    build_entity_resolution_extractor_collection_config,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_output_schema_builder import (
    build_entity_resolution_output_schema,
)
from recidiviz.documents.extraction.models.llm_extractor_collection_config import (
    LLMExtractorCollectionConfig,
    load_llm_extractor_collection_configs,
)
from recidiviz.documents.extraction.models.llm_model_registry import (
    load_llm_model_registry,
)
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    FAKE_ASSIGNMENT_ER_COLLECTION_NAME,
    FAKE_LOCATION_ER_COLLECTION_NAME,
    fake_first_order_collection,
    get_entity_group_by_name,
)


class BuildEntityResolutionExtractorCollectionConfigTest(TestCase):
    """Tests for build_entity_resolution_extractor_collection_config."""

    def test_build_produces_expected_config_for_top_level_group(self) -> None:
        parent = fake_first_order_collection()
        group = get_entity_group_by_name(parent, "location")

        expected = LLMExtractorCollectionConfig(
            name=FAKE_LOCATION_ER_COLLECTION_NAME,
            description=(
                "Auto-generated entity-resolution extractor collection for the "
                "[location] entity group of the [FAKE_EXTRACTOR_COLLECTION] "
                "collection. Clusters that group's first-order mentions into "
                "canonical entities, emitting one resolved record per entity."
            ),
            relevance_criteria=None,
            prompt_template="<FAKE PROMPT>",
            default_model_config_name=ENTITY_RESOLUTION_DEFAULT_MODEL_CONFIG_NAME,
            minimum_confidence_level=None,
            output_schema=build_entity_resolution_output_schema(entity_group=group),
            reference_data_config=parent.reference_data_config,
            entity_groups=[],
        )
        self.assertEqual(
            expected,
            build_entity_resolution_extractor_collection_config(
                parent_collection=parent, entity_group=group
            ),
        )

    def test_build_produces_expected_config_for_array_group(self) -> None:
        parent = fake_first_order_collection()
        group = get_entity_group_by_name(parent, "assignment")

        expected = LLMExtractorCollectionConfig(
            name=FAKE_ASSIGNMENT_ER_COLLECTION_NAME,
            description=(
                "Auto-generated entity-resolution extractor collection for the "
                "[assignment] entity group of the [FAKE_EXTRACTOR_COLLECTION] "
                "collection. Clusters that group's first-order mentions into "
                "canonical entities, emitting one resolved record per entity."
            ),
            relevance_criteria=None,
            prompt_template="<FAKE PROMPT>",
            default_model_config_name=ENTITY_RESOLUTION_DEFAULT_MODEL_CONFIG_NAME,
            minimum_confidence_level=None,
            output_schema=build_entity_resolution_output_schema(entity_group=group),
            reference_data_config=parent.reference_data_config,
            entity_groups=[],
        )
        self.assertEqual(
            expected,
            build_entity_resolution_extractor_collection_config(
                parent_collection=parent, entity_group=group
            ),
        )

    def test_collection_name_matches_composite_document_collection(self) -> None:
        # The ER extractor collection and its composite-document collection name
        # the same (collection, group), so a state ER extractor binds one as its
        # input and the other as its extractor collection.
        parent = fake_first_order_collection()
        group = get_entity_group_by_name(parent, "assignment")
        config = build_entity_resolution_extractor_collection_config(
            parent_collection=parent, entity_group=group
        )
        self.assertEqual(
            entity_resolution_collection_name(
                first_order_extractor_collection_name=parent.name,
                entity_group_name=group.name,
            ),
            config.name,
        )

    def test_default_model_is_thinking_enabled(self) -> None:
        # The framework default is barred for first-order extractors, so it must
        # resolve in the production registry to a thinking-enabled config.
        model_config = load_llm_model_registry().get_model_config(
            ENTITY_RESOLUTION_DEFAULT_MODEL_CONFIG_NAME
        )
        self.assertTrue(model_config.enables_thinking)

    def test_entity_group_not_on_parent_raises(self) -> None:
        parent = fake_first_order_collection()
        undeclared_group = attr.evolve(
            get_entity_group_by_name(parent, "location"), name="not_a_declared_group"
        )
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Entity group [not_a_declared_group] is not declared on collection "
                "[FAKE_EXTRACTOR_COLLECTION]."
            ),
        ):
            build_entity_resolution_extractor_collection_config(
                parent_collection=parent, entity_group=undeclared_group
            )

    def test_collection_version_id_cascades_from_parent_entity_field(self) -> None:
        # A change to a parent entity field's definition reshapes the synthesized
        # output schema, so the ER collection_version_id changes with no special
        # cascade rule.
        parent = fake_first_order_collection()
        group = get_entity_group_by_name(parent, "assignment")

        original_field = group.entity_fields[0]
        mutated_field = attr.evolve(
            original_field, description=original_field.description + " (changed)"
        )
        mutated_group = attr.evolve(
            group, entity_fields=[mutated_field, *group.entity_fields[1:]]
        )
        mutated_parent = attr.evolve(
            parent,
            entity_groups=[
                mutated_group if g.name == group.name else g
                for g in parent.entity_groups
            ],
        )

        self.assertNotEqual(
            build_entity_resolution_extractor_collection_config(
                parent_collection=parent, entity_group=group
            ).collection_version_id,
            build_entity_resolution_extractor_collection_config(
                parent_collection=mutated_parent, entity_group=mutated_group
            ).collection_version_id,
        )

    def test_collection_version_id_stable_across_equal_inputs(self) -> None:
        parent = fake_first_order_collection()
        group = get_entity_group_by_name(parent, "assignment")
        self.assertEqual(
            build_entity_resolution_extractor_collection_config(
                parent_collection=parent, entity_group=group
            ).collection_version_id,
            build_entity_resolution_extractor_collection_config(
                parent_collection=parent, entity_group=group
            ).collection_version_id,
        )


class BuildAllRealEntityResolutionCollectionsTest(TestCase):
    """Guards that an ER extractor collection builds and validates for every
    entity group declared on a real first-order collection.
    """

    def test_build_for_all_real_entity_groups(self) -> None:
        collections = load_llm_extractor_collection_configs()
        self.assertTrue(collections)

        for collection_name, collection in collections.items():
            for group in collection.entity_groups:
                with self.subTest(collection=collection_name, entity_group=group.name):
                    # Raises if the ER extractor collection for this (collection,
                    # group) fails to build or validate.
                    build_entity_resolution_extractor_collection_config(
                        parent_collection=collection, entity_group=group
                    )
