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
"""Tests for entity_resolution_extractor_generator.py.

Generates the ER `LLMExtractorConfig` from the fake first-order US_XX config for
both entity-group shapes — a top-level group (`location`) and an array group
(`assignment`) — asserting the fully-resolved config, the thinking model flowing
through from the ER collection, the stub prompt round-tripping through prompt
assembly, and the parent entity-field → `extractor_version_id` cascade.
"""
from unittest import TestCase
from unittest.mock import patch

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.config_defaults import (
    DEFAULT_ER_SINGLE_JOB_DOCUMENT_COUNT_BATCH_THRESHOLD,
    DEFAULT_ER_TOTAL_PENDING_DOCUMENT_COUNT_HARD_CAP,
    DEFAULT_MAX_TRANSIENT_RETRY_COUNT,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    EntityResolutionDocumentCollectionConfig,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_extractor_collection_config_builder import (
    build_entity_resolution_extractor_collection_config,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_extractor_generator import (
    EntityResolutionExtractorGenerator,
)
from recidiviz.documents.extraction.models.llm_extractor_collection_config import (
    EntityGroupConfig,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
    LLMExtractorDocumentFilterConfig,
)
from recidiviz.documents.extraction.models.llm_model_registry import (
    load_llm_model_registry,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    fake_entity_resolution_document_collection_config,
    fake_first_order_extractor_config,
    get_entity_group_by_name,
)
from recidiviz.utils.types import assert_type

# The framework-fixed ENTITY_RESOLUTION_DEFAULT_MODEL_CONFIG_NAME only exists in
# the production model registry. These tests generate ER configs against the
# fake first-order config's fake registry, so each test patches the builder's
# default model config name to this fake-registry config instead.
_FAKE_ENTITY_RESOLUTION_MODEL_CONFIG_NAME = "ACME_LARGE_FIXED_THINKING"


class EntityResolutionExtractorGeneratorTest(TestCase):
    """Tests for EntityResolutionExtractorGenerator.generate."""

    def setUp(self) -> None:
        self.enterContext(
            patch(
                "recidiviz.documents.extraction.entity_resolution."
                "entity_resolution_extractor_collection_config_builder."
                "ENTITY_RESOLUTION_DEFAULT_MODEL_CONFIG_NAME",
                _FAKE_ENTITY_RESOLUTION_MODEL_CONFIG_NAME,
            )
        )

    def _generate_fake_er_config_for_group(self, group_name: str) -> LLMExtractorConfig:
        first_order_config = fake_first_order_extractor_config()
        entity_group = get_entity_group_by_name(
            first_order_config.extractor_collection, group_name
        )
        entity_resolution_collection = (
            build_entity_resolution_extractor_collection_config(
                parent_collection=first_order_config.extractor_collection,
                entity_group=entity_group,
            )
        )
        return EntityResolutionExtractorGenerator().generate(
            entity_resolution_collection=entity_resolution_collection,
            model_registry=load_llm_model_registry(config_module=fake_config),
            entity_resolution_document_collection=(
                fake_entity_resolution_document_collection_config(group_name)
            ),
        )

    def test_generate_top_level_group(self) -> None:
        generated_er_config = self._generate_fake_er_config_for_group("location")
        entity_group = assert_type(generated_er_config.entity_group, EntityGroupConfig)
        composite_document_collection = generated_er_config.input_document_collection

        expected = LLMExtractorConfig(
            state_code=StateCode.US_XX,
            input_document_collection=composite_document_collection,
            document_filter=LLMExtractorDocumentFilterConfig(
                document_metadata_filter_query_template=(
                    f"SELECT {DOCUMENT_CONTENTS_ID_COLUMN_NAME}\n"
                    f"FROM `{composite_document_collection.metadata_table_address.format_address_for_query_template()}`\n"
                    f"WHERE {DOCUMENT_CONTENTS_ID_COLUMN_NAME} IS NOT NULL"
                ),
                is_sandbox_config=False,
                document_limit=None,
                root_entity_ids=None,
            ),
            prompt_vars={},
            # Retry is inherited from the parent (which declares no override, so
            # the shared default); the document-count caps are the ER-specific
            # framework defaults, not the parent's first-order caps.
            max_transient_retry_count=DEFAULT_MAX_TRANSIENT_RETRY_COUNT,
            total_pending_document_count_hard_cap=DEFAULT_ER_TOTAL_PENDING_DOCUMENT_COUNT_HARD_CAP,
            single_job_document_count_batch_threshold=DEFAULT_ER_SINGLE_JOB_DOCUMENT_COUNT_BATCH_THRESHOLD,
            extractor_collection=generated_er_config.extractor_collection,
            model_config=load_llm_model_registry(
                config_module=fake_config
            ).get_model_config(_FAKE_ENTITY_RESOLUTION_MODEL_CONFIG_NAME),
            # Resolved reference data is complex to rebuild; the generator inherits
            # it from the parent verbatim, so assert against the parent's.
            reference_data=fake_first_order_extractor_config().reference_data,
            entity_group=entity_group,
        )
        # instructions_prompt is eq=False, so full equality does not cover the stub
        # prompt; test_stub_prompt_round_trips_through_prompt_assembly does.
        self.assertEqual(expected, generated_er_config)

    def test_generate_array_group(self) -> None:
        generated_er_config = self._generate_fake_er_config_for_group("assignment")
        entity_group = assert_type(generated_er_config.entity_group, EntityGroupConfig)
        composite_document_collection = generated_er_config.input_document_collection

        expected = LLMExtractorConfig(
            state_code=StateCode.US_XX,
            input_document_collection=composite_document_collection,
            document_filter=LLMExtractorDocumentFilterConfig(
                document_metadata_filter_query_template=(
                    f"SELECT {DOCUMENT_CONTENTS_ID_COLUMN_NAME}\n"
                    f"FROM `{composite_document_collection.metadata_table_address.format_address_for_query_template()}`\n"
                    f"WHERE {DOCUMENT_CONTENTS_ID_COLUMN_NAME} IS NOT NULL"
                ),
                is_sandbox_config=False,
                document_limit=None,
                root_entity_ids=None,
            ),
            prompt_vars={},
            # Retry is inherited from the parent (which declares no override, so
            # the shared default); the document-count caps are the ER-specific
            # framework defaults, not the parent's first-order caps.
            max_transient_retry_count=DEFAULT_MAX_TRANSIENT_RETRY_COUNT,
            total_pending_document_count_hard_cap=DEFAULT_ER_TOTAL_PENDING_DOCUMENT_COUNT_HARD_CAP,
            single_job_document_count_batch_threshold=DEFAULT_ER_SINGLE_JOB_DOCUMENT_COUNT_BATCH_THRESHOLD,
            extractor_collection=generated_er_config.extractor_collection,
            model_config=load_llm_model_registry(
                config_module=fake_config
            ).get_model_config(_FAKE_ENTITY_RESOLUTION_MODEL_CONFIG_NAME),
            # Resolved reference data is complex to rebuild; the generator inherits
            # it from the parent verbatim, so assert against the parent's.
            reference_data=fake_first_order_extractor_config().reference_data,
            entity_group=entity_group,
        )
        # instructions_prompt is eq=False, so full equality does not cover the stub
        # prompt; test_stub_prompt_round_trips_through_prompt_assembly does.
        self.assertEqual(expected, generated_er_config)

    def test_model_is_thinking_enabled(self) -> None:
        # The thinking-enabled default flows through from the ER collection; it is
        # only permitted because entity_group is set (the first-order ban does not
        # apply).
        generated_er_config = self._generate_fake_er_config_for_group("location")
        self.assertTrue(generated_er_config.model_config.enables_thinking)

    def test_stub_prompt_round_trips_through_prompt_assembly(self) -> None:
        # The stub ER prompt references none of the reserved template variables, so
        # _build_instructions_prompt must return it verbatim rather than crash when
        # StrictStringFormatter rejects the unused reserved kwargs.
        generated_er_config = self._generate_fake_er_config_for_group("location")
        self.assertEqual(
            generated_er_config.extractor_collection.prompt_template,
            generated_er_config.instructions_prompt,
        )

    def test_version_id_cascades_from_parent_entity_field(self) -> None:
        first_order_config = fake_first_order_extractor_config()
        # Changing a parent entity field's definition reshapes the synthesized ER
        # output schema, which flows through collection_version_id into the ER
        # extractor_version_id (a @property, so not covered by full equality).
        generated_er_config = self._generate_fake_er_config_for_group("assignment")

        entity_group = assert_type(generated_er_config.entity_group, EntityGroupConfig)
        original_field = entity_group.entity_fields[0]
        mutated_group = attr.evolve(
            entity_group,
            entity_fields=[
                attr.evolve(
                    original_field,
                    description=original_field.description + " (changed)",
                ),
                *entity_group.entity_fields[1:],
            ],
        )
        mutated_parent_collection = attr.evolve(
            first_order_config.extractor_collection,
            entity_groups=[
                mutated_group if g.name == entity_group.name else g
                for g in first_order_config.extractor_collection.entity_groups
            ],
        )
        mutated_entity_resolution_collection = (
            build_entity_resolution_extractor_collection_config(
                parent_collection=mutated_parent_collection, entity_group=mutated_group
            )
        )
        mutated_generated = EntityResolutionExtractorGenerator().generate(
            entity_resolution_collection=mutated_entity_resolution_collection,
            model_registry=load_llm_model_registry(config_module=fake_config),
            entity_resolution_document_collection=assert_type(
                generated_er_config.input_document_collection,
                EntityResolutionDocumentCollectionConfig,
            ),
        )

        self.assertNotEqual(
            generated_er_config.extractor_version_id,
            mutated_generated.extractor_version_id,
        )
