# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Code for building LookML explores for state_person and state_staff entities."""
from typing import List

from recidiviz.looker.lookml_explore import LookMLExplore
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.tools.looker.entity.entity_explore_builder import (
    EntityLookMLExploreBuilder,
)


def generate_state_staff_lookml_explore() -> List[LookMLExplore]:
    """Generates LookML explore for state_staff and its related entities."""
    return EntityLookMLExploreBuilder(
        module_context=entities_module_context_for_module(state_entities),
        root_entity_cls=state_entities.StateStaff,
    ).build()


def generate_state_person_lookml_explore() -> List[LookMLExplore]:
    """Generates LookML explore for state_person and its related entities."""
    return EntityLookMLExploreBuilder(
        module_context=entities_module_context_for_module(state_entities),
        root_entity_cls=state_entities.StatePerson,
    ).build()
