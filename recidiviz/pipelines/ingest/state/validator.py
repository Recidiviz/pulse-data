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
"""Utility classes for validating state entities and entity trees."""
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.persistence_utils import RootEntityT


def validate_root_entity(root_entity: RootEntityT) -> RootEntityT:
    """The assumed input is a root entity with hydrated children entities attached to it.
    This function checks if the root entity does not violate any entity tree specific
    constraints. If the root entity constraints are not met, an exception should be thrown.

    TODO(#21564): Check that the root entities do not violate any entity tree constraints.
    This function should replicate the database_invariant_validator checks."""
    return root_entity


def validate_entity(entity: Entity) -> Entity:
    """The assumed input is a child entity of a root entity. This function checks if the
    entity does not violate any entity-specific constraints. If the constraints are not
    met, an exception should be thrown.

    TODO(#21564): Check that the entity does not violate any entity-level constraints.
    This function should replicate the CheckConstraints in the entity schema."""
    return entity
