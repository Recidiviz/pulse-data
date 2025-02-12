# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Utils to help with entity merging."""
from typing import Set

from more_itertools import one

from recidiviz.common.attr_mixins import (
    BuildableAttrFieldType,
    attr_field_type_for_field_name,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest import (
    _raw_text_field_name,
)
from recidiviz.persistence.entity.base_entity import (
    EnumEntity,
    ExternalIdEntity,
    HasMultipleExternalIdsEntity,
)
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_entity,
)
from recidiviz.persistence.entity.entity_field_index import EntityFieldType


def root_entity_external_id_keys(root_entity: HasMultipleExternalIdsEntity) -> Set[str]:
    """Generates a set of unique string keys for a root entity's external id objects."""
    return {external_id_key(e) for e in root_entity.get_external_ids()}


def external_id_key(external_id_entity: ExternalIdEntity) -> str:
    """Generates a unique string key for an ExternalIdEntity. If two root
    entities each have an ExternaIdEntity with the same key, the root entities
    should be merged into one.
    """
    e = external_id_entity
    return f"{type(e).__name__}##{e.id_type}|{e.external_id}"


def enum_entity_key(enum_entity: EnumEntity) -> str:
    """Generates a unique string key for an EnumEntity. All EnumEntity with the same
    entity key that are attached to the same parent should be considered duplicates and
    merged into one.
    """
    entities_module_context = entities_module_context_for_entity(enum_entity)
    field_index = entities_module_context.field_index()
    fields = field_index.get_all_entity_fields(
        type(enum_entity), EntityFieldType.FLAT_FIELD
    )
    enum_field_name = one(
        f
        for f in fields
        if attr_field_type_for_field_name(type(enum_entity), f)
        is BuildableAttrFieldType.ENUM
    )
    raw_text_field_name = _raw_text_field_name(enum_field_name)

    enum_value = enum_entity.get_field(enum_field_name)
    raw_text_value = enum_entity.get_field(raw_text_field_name)
    return f"{type(enum_entity).__name__}##{enum_value}|{raw_text_value}"
