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
"""Helper functions to handle entity documentation and descriptions for the
identity entities modules (both fragment and cluster).
"""
from recidiviz.persistence.entity import identity
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_documentation_utils import (
    FIELD_KEY,
    RootFieldDescription,
    entity_field_descriptions_yaml_path,
    resolve_field_description,
)

ENTITY_DESCRIPTIONS_YAML_PATH = entity_field_descriptions_yaml_path(identity)
_TENANT_ROOT_FIELD = RootFieldDescription(
    field_name="tenant",
    description="The tenant (e.g. state or partner) that provided the source data.",
)


def description_for_field(entity_cls: type[Entity], field_name: str) -> str:
    """Returns a description for the given identity entity class and field to
    be used in BQ Schemas and documentation.

    Every entity resolves against its own table_id section in the YAML; cluster
    tables share their fragment equivalents' descriptions via YAML
    anchors/aliases declared in the descriptions file itself.
    """
    return resolve_field_description(
        entity_cls,
        field_name,
        yaml_path=ENTITY_DESCRIPTIONS_YAML_PATH,
        root_field=_TENANT_ROOT_FIELD,
        sections=(FIELD_KEY,),
    )
