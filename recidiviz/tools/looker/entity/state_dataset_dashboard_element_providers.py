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
"""Provider for custom LookML dashboard elements for entities."""
from typing import Type

import attr

from recidiviz.looker.lookml_dashboard_builder import (
    LookMLDashboardElementMetadata,
    LookMLDashboardElementsProvider,
)
from recidiviz.looker.lookml_dashboard_element import (
    LookMLDashboardElement,
    LookMLListen,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.tools.looker.entity.custom_views.person_periods import (
    person_periods_view_name_for_dataset,
)
from recidiviz.tools.looker.entity.entity_dashboard_element_factory import (
    EntityDashboardElementFactory,
)


@attr.define
class StatePersonLookMLDashboardElementsProvider(LookMLDashboardElementsProvider):
    """Class to provide LookML dashboard elements for StatePerson entity."""

    dataset_id: str

    def _build_custom_elements(
        self, explore_name: str, all_filters_listen: LookMLListen, model: str
    ) -> list[LookMLDashboardElement]:
        return [
            EntityDashboardElementFactory.info_element(),
            # TODO(#51203) Remove once multiparent entities are supported or fully deprecated
            EntityDashboardElementFactory.multiparent_disclaimer_element(),
            EntityDashboardElementFactory.actions_element(
                explore_name, all_filters_listen, model
            ),
            EntityDashboardElementFactory.person_periods_timeline_element(
                explore=explore_name,
                person_periods_view_name=person_periods_view_name_for_dataset(
                    self.dataset_id
                ),
                listen=all_filters_listen,
                model=model,
            ),
        ]

    def build_dashboard_elements(
        self, explore_name: str, all_filters_listen: LookMLListen, model: str
    ) -> list[LookMLDashboardElement]:
        """Builds LookML dashboard elements for StatePerson."""
        return self._build_custom_elements(explore_name, all_filters_listen, model) + [
            LookMLDashboardElement.for_table_chart(
                name=table_metadata.name,
                explore=explore_name,
                model=model,
                listen=all_filters_listen,
                fields=sorted(table_metadata.fields),
                sorts=table_metadata.sort_fields,
            )
            for table_metadata in sorted(
                self.table_element_metadata, key=lambda x: x.name
            )
        ]


@attr.define
class StateStaffLookMLDashboardElementsProvider(LookMLDashboardElementsProvider):
    """Class to provide LookML dashboard elements for StateStaff entity."""

    def build_dashboard_elements(
        self, explore_name: str, all_filters_listen: LookMLListen, model: str
    ) -> list[LookMLDashboardElement]:
        """Builds LookML dashboard elements for StateStaff."""
        return [
            LookMLDashboardElement.for_table_chart(
                name=table_metadata.name,
                explore=explore_name,
                model=model,
                listen=all_filters_listen,
                fields=sorted(table_metadata.fields),
                sorts=table_metadata.sort_fields,
            )
            for table_metadata in sorted(
                self.table_element_metadata, key=lambda x: x.name
            )
        ]


def get_elements_provider(
    root_entity_cls: Type[Entity],
    dataset_id: str,
    table_element_metadata: list[LookMLDashboardElementMetadata],
) -> LookMLDashboardElementsProvider:
    """Returns the provider for the dashboard elements based on the root entity."""
    if root_entity_cls in [
        state_entities.StateStaff,
        normalized_entities.NormalizedStateStaff,
    ]:
        return StateStaffLookMLDashboardElementsProvider(table_element_metadata)
    if root_entity_cls in [
        state_entities.StatePerson,
        normalized_entities.NormalizedStatePerson,
    ]:
        return StatePersonLookMLDashboardElementsProvider(
            dataset_id=dataset_id, table_element_metadata=table_element_metadata
        )

    raise ValueError(f"Unsupported entity {root_entity_cls}.")
