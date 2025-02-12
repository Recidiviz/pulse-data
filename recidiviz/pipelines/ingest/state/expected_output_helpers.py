# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Helpers for determining the set of expected output entities / tables from a given
ingest pipeline.
"""
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_entity_class,
)
from recidiviz.persistence.entity.entity_utils import (
    entities_have_direct_relationship,
    get_association_table_id,
    get_entity_class_in_module_with_table_id,
    is_many_to_many_relationship,
)
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.pipelines.ingest.state.normalization.state_specific_normalization_delegate import (
    StateSpecificNormalizationDelegate,
)
from recidiviz.utils.types import assert_subclass_list


def get_expected_output_pre_normalization_entity_classes(
    ingest_manifest_collector: IngestViewManifestCollector,
    ingest_views_to_run: list[str],
) -> set[type[Entity]]:
    """Returns the list of entity types from the pre-normalization schema that we expect
    to see produced by this ingest pipeline.

    Args:
        ingest_manifest_collector: The IngestViewManifestCollector for this pipeline's
            state.
        ingest_views_to_run: The set of ingest views that will be run as part of this
            pipeline.
    """
    return {
        entity_cls
        for ingest_view in ingest_views_to_run
        for entity_cls in ingest_manifest_collector.ingest_view_to_manifest[
            ingest_view
        ].hydrated_entity_classes()
    }


def get_expected_output_normalized_entity_classes(
    expected_output_pre_normalization_entity_classes: set[type[Entity]],
    delegate: StateSpecificNormalizationDelegate,
) -> set[type[Entity]]:
    """Returns the list of entity types from the normalized entities schema that we
    expect to see produced by this ingest pipeline.
    """
    expected_normalized_entity_classes = {
        get_entity_class_in_module_with_table_id(
            normalized_entities, entity.get_table_id()
        )
        for entity in expected_output_pre_normalization_entity_classes
    }

    expected_normalized_entity_classes.update(
        assert_subclass_list(
            delegate.extra_entities_generated_via_normalization(
                expected_output_pre_normalization_entity_classes
            ),
            Entity,
        )
    )

    if (
        normalized_entities.NormalizedStateSentence
        in expected_normalized_entity_classes
        and normalized_entities.NormalizedStateSentenceStatusSnapshot
        in expected_normalized_entity_classes
    ):
        expected_normalized_entity_classes.add(
            normalized_entities.NormalizedStateSentenceInferredGroup
        )
        expected_normalized_entity_classes.add(
            normalized_entities.NormalizedStateSentenceImposedGroup
        )

    return expected_normalized_entity_classes


def get_pipeline_output_tables(
    expected_output_entity_classes: set[type[Entity]],
) -> set[str]:
    """Returns the set of tables that the pipeline will output to given a list of
    expected entity types that will be produced.
    """
    expected_output_tables = {
        entity_cls.get_table_id() for entity_cls in expected_output_entity_classes
    }

    sorted_entity_classes = sorted(
        expected_output_entity_classes,
        key=lambda entity_cls: entity_cls.get_table_id(),
    )

    for i, entity_class_a in enumerate(sorted_entity_classes):
        entities_module_context = entities_module_context_for_entity_class(
            entity_class_a
        )
        for entity_class_b in sorted_entity_classes[i + 1 :]:
            if not entities_have_direct_relationship(entity_class_a, entity_class_b):
                continue

            if not is_many_to_many_relationship(entity_class_a, entity_class_b):
                continue

            expected_output_tables.add(
                get_association_table_id(
                    entity_class_a, entity_class_b, entities_module_context
                )
            )
    return expected_output_tables
