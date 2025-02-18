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
"""Defines a class representing LookML dashboard files, which provide
   settings for a data dashboard."""
from typing import List, Optional

import attr

from recidiviz.looker.lookml_dashboard_element import LookMLDashboardElement
from recidiviz.looker.lookml_dashboard_filter import LookMLDashboardFilter
from recidiviz.looker.lookml_dashboard_parameter import LookMLDashboardParameter
from recidiviz.looker.lookml_utils import write_lookml_file
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.utils.string import StrictStringFormatter

DASHBOARD_TEMPLATE = """- dashboard: {dashboard_name}
  {parameters}

  filters:
  {filters}

  elements:
  {elements}
"""


@attr.define
class LookMLDashboard:
    """
    Produces LookML dashboard text that satisfies the syntax described in
    https://cloud.google.com/looker/docs/reference/param-lookml-dashboard
    """

    dashboard_name: str
    parameters: List[LookMLDashboardParameter]
    filters: List[LookMLDashboardFilter] = attr.field(
        validator=attr.validators.min_len(1)
    )
    elements: List[LookMLDashboardElement] = attr.field(
        validator=attr.validators.min_len(1)
    )
    load_configuration_wait: bool = attr.field(default=False)
    extended_dashboard: Optional[str] = attr.field(default=None)
    extension_required: bool = attr.field(default=False)

    def __attrs_post_init__(self) -> None:
        self._validate_listens_correspond_to_filters()

    def _validate_listens_correspond_to_filters(self) -> None:
        """Each element's listens must correspond to one of the dashboard's filters."""
        for element in self.elements:
            if not element.listens:
                continue
            for name, field in element.listens.items():
                if not any((name, field) == (f.name, f.field) for f in self.filters):
                    raise ValueError(
                        f"LookML element [{element.name}] is listening to filter [{name}: {field}] that does not exist."
                        f" Valid filters: [{self.filters}]"
                    )

    def validate_referenced_fields_exist_in_views(
        self, views: list[LookMLView]
    ) -> None:
        """Ensures all referenced fields exist in the provided LookML views."""
        for element in self.elements:
            element.validate_referenced_fields_exist_in_views(views)
        for filter_ in self.filters:
            filter_.validate_referenced_fields_exist_in_views(views)

    def build(self) -> str:
        all_parameters = [param.build() for param in self.parameters]
        if self.load_configuration_wait:
            all_parameters.append("load_configuration: wait")
        if self.extended_dashboard:
            all_parameters.append(f"extends: {self.extended_dashboard}")
        if self.extension_required:
            all_parameters.append("extension: required")
        parameters_clause = "\n  ".join(all_parameters)

        filters_clause = "\n\n  ".join(f.build() for f in self.filters)

        elements_clause = "\n\n  ".join(e.build() for e in self.elements)

        return StrictStringFormatter().format(
            DASHBOARD_TEMPLATE,
            dashboard_name=self.dashboard_name,
            parameters=parameters_clause,
            filters=filters_clause,
            elements=elements_clause,
        )

    def write(self, output_directory: str, source_script_path: str) -> None:
        """
        Writes LookML dashboard file into the specified output directory with a
        header indicating the date and script source of the auto-generated dashboard.
        """
        file_name = f"{self.dashboard_name}.dashboard.lookml"
        write_lookml_file(
            output_directory=output_directory,
            file_name=file_name,
            source_script_path=source_script_path,
            file_body=self.build(),
        )
