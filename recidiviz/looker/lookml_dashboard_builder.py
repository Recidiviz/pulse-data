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
"""Builder for a LookML dashboard."""
import abc

import attr

from recidiviz.common import attr_validators
from recidiviz.common.str_field_utils import snake_to_title
from recidiviz.looker.lookml_dashboard import LookMLDashboard
from recidiviz.looker.lookml_dashboard_element import (
    LookMLDashboardElement,
    LookMLListen,
    LookMLSort,
    build_dashboard_grid,
)
from recidiviz.looker.lookml_dashboard_filter import (
    LookMLDashboardFilter,
    LookMLFilterUIConfig,
    LookMLFilterUIDisplay,
    LookMLFilterUIType,
)
from recidiviz.looker.lookml_dashboard_parameter import (
    LookMLDashboardLayoutType,
    LookMLDashboardParameter,
)
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_field import TimeDimensionGroupLookMLViewField


@attr.define
class LookMLDashboardElementMetadata:
    """Metadata used to create a table dashboard element.

    Attributes:
        name: The name of the element.
        fields: List of fully-scoped field names in the format `view_name.field_name`.
        sort_fields: List of LookMLSort objects representing the fields to sort the element by.
    """

    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    fields: list[str] = attr.ib(validator=attr_validators.is_list_of(str))
    sort_fields: list[LookMLSort] = attr.ib(
        validator=attr_validators.is_list_of(LookMLSort)
    )

    @classmethod
    def from_view(cls, view: LookMLView) -> "LookMLDashboardElementMetadata":
        """
        Builds a dashboard element metadata from a LookML view.
        Includes dimensions and the date dimension created by time-based dimension groups as fields.
        Includes the date dimension created by time-based dimension groups in the sort fields,
        sorting each field in descending order.
        """
        date_field_names = [
            view.qualified_name_for_field(dimension_group.date_dimension_name)
            for dimension_group in view.dimension_group_fields
            if isinstance(dimension_group, TimeDimensionGroupLookMLViewField)
        ]
        return cls(
            name=snake_to_title(view.view_name),
            fields=view.qualified_dimension_names() + date_field_names,
            sort_fields=[
                LookMLSort(field_name, desc=True) for field_name in date_field_names
            ],
        )


@attr.define
class LookMLDashboardElementsProvider:
    """Provider for LookML dashboard elements."""

    table_element_metadata: list[LookMLDashboardElementMetadata] = attr.ib(
        validator=attr_validators.is_list_of(LookMLDashboardElementMetadata)
    )

    @abc.abstractmethod
    def build_dashboard_elements(
        self, explore_name: str, all_filters_listen: LookMLListen, model: str
    ) -> list[LookMLDashboardElement]:
        """Builds the dashboard elements."""


@attr.define
class SingleExploreLookMLDashboardBuilder:
    """Builder for a LookML dashboard that pulls all data from a single explore and a single model.

    Attributes:
        dashboard_name: The name of the dashboard.
        dashboard_title: The title of the dashboard.
        explore_name: The name of the explore used by the dashboard elements.
        model_name: The name of the model used by the dashboard elements.
        filter_fields: List of fully-scoped field names in the format `view_name.field_name`.
        element_provider: provider used to build the dashboard elements.
    """

    dashboard_name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    dashboard_title: str = attr.ib(validator=attr_validators.is_non_empty_str)
    explore_name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    model_name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    filter_fields: list[str] = attr.ib(validator=attr_validators.is_list_of(str))
    element_provider: LookMLDashboardElementsProvider

    @property
    def lookml_listen(self) -> LookMLListen:
        """
        The listen parameter for the dashboard elements.
        This builder class assumes that all elements listen to all filter fields.
        """
        return LookMLListen.from_fields(self.filter_fields)

    @property
    def dashboard_parameters(self) -> list[LookMLDashboardParameter]:
        """Top-level parameters for the dashboard."""
        return [
            LookMLDashboardParameter.layout(LookMLDashboardLayoutType.NEWSPAPER),
            LookMLDashboardParameter.title(self.dashboard_title),
        ]

    def _build_filter(self, field: str) -> LookMLDashboardFilter:
        return LookMLDashboardFilter.for_field(
            field=field,
            allow_multiple_values=True,
            required=False,
            explore=self.explore_name,
            model=self.model_name,
            ui_config=LookMLFilterUIConfig(
                type=LookMLFilterUIType.ADVANCED, display=LookMLFilterUIDisplay.POPOVER
            ),
        )

    def _build_elements(self) -> list[LookMLDashboardElement]:
        """
        Builds the elements for the dashboard and arranges them in a grid layout.
        """
        elements = self.element_provider.build_dashboard_elements(
            self.explore_name, self.lookml_listen, self.model_name
        )
        build_dashboard_grid(elements)

        return elements

    def build(self) -> LookMLDashboard:
        """Builds the dashboard."""
        return LookMLDashboard(
            dashboard_name=self.dashboard_name,
            parameters=self.dashboard_parameters,
            filters=[self._build_filter(field) for field in self.filter_fields],
            elements=self._build_elements(),
            load_configuration_wait=True,
        )
