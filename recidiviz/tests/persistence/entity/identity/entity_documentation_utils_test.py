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
"""Tests for identity entity_documentation_utils."""
from types import ModuleType

import pytest

from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_documentation_utils import (
    FIELD_KEY,
    get_field_descriptions_from_yaml,
)
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.identity import (
    identity_cluster_entities,
    identity_fragment_entities,
)
from recidiviz.persistence.entity.identity.entity_documentation_utils import (
    ENTITY_DESCRIPTIONS_YAML_PATH,
    description_for_field,
)
from recidiviz.persistence.entity.identity.identity_cluster_entities_module_context import (
    IdentityClusterEntitiesModuleContext,
)


def _flat_fields(entity_cls: type[Entity]) -> set[str]:
    """Returns all flat (non-reference) attr fields on the entity class.

    Every flat field must resolve to a description (via YAML, the `tenant` root
    branch, the `external_id` auto-gen, or the primary-key auto-gen).
    """
    field_ref = attribute_field_type_reference_for_class(entity_cls)
    return {
        name
        for name, info in field_ref.field_to_attribute_info.items()
        if not info.referenced_cls_name
    }


@pytest.mark.parametrize(
    "module",
    [identity_fragment_entities, identity_cluster_entities],
    ids=["fragment", "cluster"],
)
def test_every_flat_field_resolves_to_a_description(module: ModuleType) -> None:
    """Every flat field on every identity entity must have a non-empty
    description, either via the YAML, the `tenant` root field, the
    `external_id` auto-gen, or the primary-key auto-gen.
    """
    for entity_cls in get_all_entity_classes_in_module(module):
        for field in _flat_fields(entity_cls):
            description = description_for_field(entity_cls, field)
            assert (
                description
            ), f"Empty description returned for {entity_cls.__name__}.{field}"


def test_no_stale_yaml_entries() -> None:
    """Every documented field in the YAML corresponds to a real attr field on
    the entity it documents. Each top-level YAML table_id must match an entity
    class in either the fragment or cluster module.
    """
    yaml_data = get_field_descriptions_from_yaml(ENTITY_DESCRIPTIONS_YAML_PATH)
    entities_by_table = {
        e.get_table_id(): e
        for e in (
            list(get_all_entity_classes_in_module(identity_fragment_entities))
            + list(get_all_entity_classes_in_module(identity_cluster_entities))
        )
    }

    for table_id, sections in yaml_data.items():
        entity_cls = entities_by_table.get(table_id)
        if entity_cls is None:
            raise ValueError(
                f"YAML table_id [{table_id}] doesn't match any identity entity"
            )
        entity_fields = set(attribute_field_type_reference_for_class(entity_cls).fields)
        for documented_field in sections.get(FIELD_KEY, {}):
            if documented_field not in entity_fields:
                raise ValueError(
                    f"YAML documents nonexistent field [{documented_field}] on "
                    f"[{entity_cls.__name__}] (table_id [{table_id}])"
                )


def test_tenant_returns_root_description() -> None:
    assert (
        description_for_field(identity_fragment_entities.IdentityFragment, "tenant")
        == "The tenant (e.g. state or partner) that provided the source data."
    )
    assert (
        description_for_field(identity_cluster_entities.IdentityCluster, "tenant")
        == "The tenant (e.g. state or partner) that provided the source data."
    )


def test_reference_field_returns_reference_description() -> None:
    assert (
        description_for_field(
            identity_fragment_entities.IdentityFragment, "external_ids"
        )
        == "Reference to IdentityExternalId"
    )
    assert (
        description_for_field(identity_fragment_entities.IdentityFragment, "attributes")
        == "Reference to IdentityAttributes"
    )


def test_external_id_auto_generated() -> None:
    description = description_for_field(
        identity_fragment_entities.IdentityExternalId, "external_id"
    )
    assert "IdentityExternalId" in description
    assert "unique identifier" in description


def test_cluster_entity_reuses_fragment_yaml() -> None:
    """Cluster entities (other than the cluster root) share their fragment
    equivalents' descriptions via YAML anchors/aliases in the descriptions
    file.
    """
    assert description_for_field(
        identity_cluster_entities.IdentityClusterName, "given_name"
    ) == description_for_field(identity_fragment_entities.IdentityName, "given_name")
    assert description_for_field(
        identity_cluster_entities.IdentityClusterRace, "race_raw_text"
    ) == description_for_field(identity_fragment_entities.IdentityRace, "race_raw_text")


def test_cluster_root_reuses_attributes_yaml_for_shared_fields() -> None:
    """`IdentityCluster` shares `person_type`/`birthdate` descriptions with
    `identity_attributes` via a YAML merge key (since the cluster tree flattens
    `IdentityFragment` + `IdentityAttributes` into a single root).
    """
    assert description_for_field(
        identity_cluster_entities.IdentityCluster, "person_type"
    ) == description_for_field(
        identity_fragment_entities.IdentityAttributes, "person_type"
    )
    assert description_for_field(
        identity_cluster_entities.IdentityCluster, "birthdate"
    ) == description_for_field(
        identity_fragment_entities.IdentityAttributes, "birthdate"
    )


def test_identity_cluster_id_yaml_overrides_primary_key_auto_gen() -> None:
    """`identity_cluster_id` is both an attr field and `IdentityCluster`'s
    primary-key column name. The YAML description (the SHA-256 explanation)
    must take precedence over the generic auto-generated PK description.
    """
    assert (
        identity_cluster_entities.IdentityCluster.get_primary_key_column_name()
        == "identity_cluster_id"
    )
    description = description_for_field(
        identity_cluster_entities.IdentityCluster, "identity_cluster_id"
    )
    assert "SHA-256" in description
    assert "automatically by the Recidiviz system" not in description


def test_cluster_hash_resolves_via_identity_cluster_section() -> None:
    description = description_for_field(
        identity_cluster_entities.IdentityCluster, "cluster_hash"
    )
    assert "SHA-256" in description


def test_unknown_field_raises() -> None:
    with pytest.raises(ValueError, match="Unexpected field for class"):
        description_for_field(
            identity_cluster_entities.IdentityCluster, "this_field_does_not_exist"
        )


def test_module_context_wires_field_description_for_cluster() -> None:
    context = entities_module_context_for_module(identity_cluster_entities)
    assert isinstance(context, IdentityClusterEntitiesModuleContext)
    assert (
        context.field_description(
            identity_cluster_entities.IdentityCluster, "cluster_hash"
        )
        is not None
    )


def test_module_context_wires_field_description_for_fragment() -> None:
    context = entities_module_context_for_module(identity_fragment_entities)
    assert (
        context.field_description(identity_fragment_entities.IdentityName, "given_name")
        is not None
    )
