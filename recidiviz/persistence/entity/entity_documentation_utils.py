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
"""Shared helpers for resolving documentation strings for entity fields.

Module-specific wrappers (`activity/entity_documentation_utils.py`,
`identity/entity_documentation_utils.py`) supply their entities package (which
locates the package's `entity_field_descriptions.yaml`), the module-wide root
field (`state_code` / `tenant`), and the YAML sections to consult for each
table.
"""
import os
from functools import cache
from types import ModuleType

import attr
import yaml

from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.common.attr_validators import is_str
from recidiviz.persistence.entity.core_entity import CoreEntity
from recidiviz.utils.types import assert_type

FIELD_KEY = "fields"

_ENTITY_FIELD_DESCRIPTIONS_YAML_NAME = "entity_field_descriptions.yaml"


def entity_field_descriptions_yaml_path(entities_package: ModuleType) -> str:
    """Returns the path to the entity_field_descriptions.yaml documenting the
    entities modules defined in the given package (e.g.
    `recidiviz.persistence.entity.identity`).
    """
    return os.path.join(
        os.path.dirname(assert_type(entities_package.__file__, str)),
        _ENTITY_FIELD_DESCRIPTIONS_YAML_NAME,
    )


@attr.s(frozen=True, kw_only=True)
class RootFieldDescription:
    """The field present on every entity in an entities module (e.g.
    `state_code` for activity entities, `tenant` for identity entities), paired
    with its description.
    """

    field_name: str = attr.ib(validator=is_str)
    description: str = attr.ib(validator=is_str)


@cache
def get_field_descriptions_from_yaml(
    yaml_path: str,
) -> dict[str, dict[str, dict[str, str]]]:
    """Loads (and caches) a field-descriptions YAML. The top-level keys are
    table IDs; each table maps to a dict of section name → field-to-description
    mapping (e.g. {"fields": {...}, "normalization_only_fields": {...}}).

    Tables that share descriptions may do so via YAML anchors/aliases and merge
    keys (e.g. identity cluster tables aliasing their fragment equivalents);
    these are resolved at load time, so each table's sections are fully
    populated in the returned dict.
    """
    with open(yaml_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f.read())


def resolve_field_description(
    entity_cls: type[CoreEntity],
    field_name: str,
    *,
    yaml_path: str,
    root_field: RootFieldDescription,
    sections: tuple[str, ...],
) -> str:
    """Returns a description for `field_name` on `entity_cls`, suitable for use
    in BQ schemas and entity documentation.

    `sections` lists the sections of the entity's own YAML table to consult, in
    order; the first match wins. Most callers pass just `("fields",)`; activity
    adds a dataset-specific section (`normalization_only_fields` /
    `state_dataset_only_fields`).

    Resolution order:
      1. Forward-edge references → `"Reference to {ClassName}"`.
      2. The module-wide root field (`state_code` / `tenant`) →
         `root_field.description`.
      3. YAML, via `sections`. YAML takes precedence over the auto-generated
         `external_id` / primary-key descriptions so callers can override them
         when needed (e.g. `IdentityCluster.identity_cluster_id` is the primary
         key but has a content-specific description).
      4. `external_id` → auto-generated description.
      5. Primary-key column → auto-generated description.

    Raises ValueError if `field_name` isn't an attr field on `entity_cls`, or
    if no description can be resolved.
    """
    field_ref = attribute_field_type_reference_for_class(entity_cls)
    if field_name not in field_ref.fields:
        raise ValueError(f"Unexpected field for class [{entity_cls}]: {field_name}")

    if ref_entity := field_ref.get_field_info(field_name).referenced_cls_name:
        return f"Reference to {ref_entity}"

    if field_name == root_field.field_name:
        return root_field.description

    table_descriptions = get_field_descriptions_from_yaml(yaml_path).get(
        entity_cls.get_table_id(), {}
    )
    for section in sections:
        description = table_descriptions.get(section, {}).get(field_name)
        if description is not None:
            return description

    if field_name == "external_id":
        return (
            f"The unique identifier for {entity_cls.__name__}, unique within the "
            "scope of the source data system."
        )

    if field_name == entity_cls.get_primary_key_column_name():
        return (
            f"Unique identifier for the {entity_cls.__name__} entity generated "
            "automatically by the Recidiviz system."
        )

    raise ValueError(
        f"Didn't find description for field [{field_name}] in class "
        f"[{entity_cls.__name__}] in {yaml_path}"
    )
