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
"""Code for building the state person and state staff LookML dashboards"""
from recidiviz.looker.lookml_dashboard import LookMLDashboard
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.tools.looker.entity.entity_dashboard_builder import (
    EntityLookMLDashboardBuilder,
)


def generate_state_person_dashboard(views: list[LookMLView]) -> LookMLDashboard:
    """Generates the state person lookml dashboard."""
    return EntityLookMLDashboardBuilder(
        module_context=entities_module_context_for_module(state_entities),
        root_entity_cls=state_entities.StatePerson,
        views=views,
    ).build_and_validate()


def generate_state_staff_dashboard(views: list[LookMLView]) -> LookMLDashboard:
    """Generates the state staff lookml dashboard."""
    return EntityLookMLDashboardBuilder(
        module_context=entities_module_context_for_module(state_entities),
        root_entity_cls=state_entities.StateStaff,
        views=views,
    ).build_and_validate()
