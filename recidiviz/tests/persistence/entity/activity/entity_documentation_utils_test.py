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
"""Tests for activity entity_documentation_utils."""
from typing import Type

import pytest

from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.persistence.entity.activity import entities as state_entities
from recidiviz.persistence.entity.activity import normalized_entities
from recidiviz.persistence.entity.activity.entity_documentation_utils import (
    ENTITY_DESCRIPTIONS_YAML_PATH,
    NORMALIZATION_FIELD_KEY,
    STATE_DATASET_ONLY_FIELD_KEY,
    description_for_field,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_documentation_utils import (
    FIELD_KEY,
    get_field_descriptions_from_yaml,
)
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module


def _entity_fields_to_document(entity: Type[Entity]) -> set[str]:
    """
    We do not check the yaml for descriptions of:
        - state_code
        - external_id
        - the primary key field of the given entity
        - references to other state entities
    """
    fields = attribute_field_type_reference_for_class(entity).field_to_attribute_info
    return {
        field_name
        for (field_name, info) in fields.items()
        if field_name not in {"state_code", "external_id"}
        and not info.referenced_cls_name
        and field_name != entity.get_primary_key_column_name()
    }


def test_all_state_entities_have_field_descriptions() -> None:
    """
    This tests that all entities in state/entities.py
    have documented fields in entity_field_descriptions.yaml.

    We expect all fields to be documented except:
        - state_code
        - external_id
        - the primary key for each entity
        - references to other state entities
    """
    entities: set[Type[Entity]] = get_all_entity_classes_in_module(state_entities)
    yaml_data = get_field_descriptions_from_yaml(ENTITY_DESCRIPTIONS_YAML_PATH)
    for entity in entities:
        table_name = entity.get_table_id()
        table_description_data = yaml_data[table_name]
        documented_fields = set(
            table_description_data[FIELD_KEY]
            | table_description_data.get(STATE_DATASET_ONLY_FIELD_KEY, {})
        )
        defined_fields = _entity_fields_to_document(entity)
        if undocumented_fields := defined_fields - documented_fields:
            raise ValueError(
                f"{table_name} has undocumented fields: {', '.join(undocumented_fields)}"
            )
        if nonexistent_fields := documented_fields - defined_fields:
            raise ValueError(
                f"{table_name} has documented fields that don't exist or should not be manually documented: {', '.join(nonexistent_fields)}"
            )


def test_all_normalized_state_entities_have_field_descriptions() -> None:
    """
    This tests that all entities in state/entities.py
    have documented fields in entity_field_descriptions.yaml.

    We expect all fields to be documented except:
        - state_code
        - external_id
        - the primary key for each entity
        - references to other state entities
    """
    entities: set[Type[Entity]] = get_all_entity_classes_in_module(normalized_entities)
    yaml_data = get_field_descriptions_from_yaml(ENTITY_DESCRIPTIONS_YAML_PATH)
    for entity in entities:
        table_name = entity.get_table_id()
        descriptions: dict[str, dict[str, str]] = yaml_data[table_name]
        documented_fields: set[str] = set(descriptions[FIELD_KEY])
        if NORMALIZATION_FIELD_KEY in descriptions:
            documented_fields |= set(descriptions[NORMALIZATION_FIELD_KEY])
        defined_fields = _entity_fields_to_document(entity)
        if undocumented_fields := defined_fields - documented_fields:
            raise ValueError(
                f"{table_name} has undocumented fields: {', '.join(undocumented_fields)}"
            )
        if nonexistent_fields := documented_fields - defined_fields:
            raise ValueError(
                f"{table_name} has documented fields that don't exist or should not be manually documented: {', '.join(nonexistent_fields)}"
            )


def test_description_for_field() -> None:
    assert description_for_field(state_entities.StatePerson, "state_code") == (
        "The U.S. state or region that provided the source data."
    )
    assert description_for_field(state_entities.StatePerson, "person_id") == (
        "Unique identifier for the StatePerson entity generated automatically by the Recidiviz system."
    )
    with pytest.raises(ValueError, match="Unexpected field for class"):
        description_for_field(state_entities.StatePerson, "external_id")
