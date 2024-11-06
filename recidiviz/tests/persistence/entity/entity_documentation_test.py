"""
This test checks that all fields in entities.py are documented!

# TODO(#32863) Handle normalized entities too.
"""
import os
from typing import Type

import yaml

from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.state import entities as state_entities


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
    state_dir = os.path.dirname(state_entities.__file__)
    with open(
        f"{state_dir}/entity_field_descriptions.yaml", "r", encoding="utf-8"
    ) as f:
        field_definitions_by_entity = yaml.safe_load(f.read())
    entities: set[Type[Entity]] = get_all_entity_classes_in_module(state_entities)
    for entity in entities:
        table_name = entity.get_table_id()
        documented_fields = set(field_definitions_by_entity[table_name]["fields"])
        defined_fields = _entity_fields_to_document(entity)
        if undocumented_fields := defined_fields - documented_fields:
            raise ValueError(
                f"{table_name} has undocumented fields: {', '.join(undocumented_fields)}"
            )
        if nonexistent_fields := documented_fields - defined_fields:
            raise ValueError(
                f"{table_name} has documented fields that don't exist or should not be manually documented: {', '.join(nonexistent_fields)}"
            )
