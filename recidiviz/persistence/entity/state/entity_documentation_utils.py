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
"""Helper functions to handle entity documentation and descriptions."""
import os
from functools import cache

import yaml

from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.persistence.entity.base_entity import CoreEntity
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.persistence.entity.state.state_entity_mixins import StateEntityMixin

FIELD_KEY = "fields"
STATE_DATASET_ONLY_FIELD_KEY = "state_dataset_only_fields"
NORMALIZATION_FIELD_KEY = "normalization_only_fields"
_ENTITY_DESCRIPTIONS_YAML_PATH = (
    f"{os.path.dirname(__file__)}/entity_field_descriptions.yaml"
)


@cache
def get_entity_field_descriptions() -> dict[str, dict[str, dict[str, str]]]:
    with open(_ENTITY_DESCRIPTIONS_YAML_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f.read())


def description_for_field(entity_cls: type[StateEntityMixin], field_name: str) -> str:
    """Returns a description for the given class and field to be used in BQ Schemas and documentation."""
    if not issubclass(entity_cls, CoreEntity):
        raise ValueError(
            "StateEntityMixin must be used with a class that inherits from CoreEntity"
        )

    field_ref = attribute_field_type_reference_for_class(entity_cls)
    if field_name not in field_ref.fields:
        raise ValueError(f"Unexpected field for class [{entity_cls}]: {field_name}")

    # StateCode, external_id, primary keys, and referenced classes aren't in the YAML
    if ref_entity := field_ref.get_field_info(field_name).referenced_cls_name:
        return f"Reference to {ref_entity}"

    if field_name == "state_code":
        return "The U.S. state or region that provided the source data."

    if field_name == "external_id":
        return (
            f"The unique identifier for {entity_cls.__name__}, unique within the scope of "
            "the source data system."
        )

    if field_name == entity_cls.get_primary_key_column_name():
        return (
            f"Unique identifier for the {entity_cls.__name__} entity generated "
            "automatically by the Recidiviz system."
        )

    table_descriptions = get_entity_field_descriptions()[entity_cls.get_table_id()]
    # Make a copy so we don't mutate the cache reference
    descriptions = table_descriptions[FIELD_KEY].copy()
    if issubclass(entity_cls, NormalizedStateEntity):
        if norm_only_fields := table_descriptions.get(NORMALIZATION_FIELD_KEY):
            descriptions |= norm_only_fields
    else:
        if state_only_fields := table_descriptions.get(STATE_DATASET_ONLY_FIELD_KEY):
            descriptions |= state_only_fields
    if field_name not in descriptions:
        raise ValueError(
            f"Didn't find description for field [{field_name}] in class "
            f"[{entity_cls.__name__}] in {_ENTITY_DESCRIPTIONS_YAML_PATH}"
        )
    return descriptions[field_name]
