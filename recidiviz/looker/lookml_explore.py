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
"""Defines a class representing LookML explore files, which reference
   and join together views."""

from typing import List, Optional

import attr

from recidiviz.looker.lookml_explore_parameter import LookMLExploreParameter
from recidiviz.looker.lookml_utils import write_lookml_file
from recidiviz.utils.string import StrictStringFormatter

EXPLORE_TEMPLATE = """explore: {explore_name} {{
{extension_required}{extends_clause}{view_name_clause}{parameters}
}}"""


@attr.define
class LookMLExplore:
    """Produces LookML Explore text that satisfies the syntax described in
    https://cloud.google.com/looker/docs/reference/param-explore. Not all syntax
    is supported.
    """

    explore_name: str = attr.field(validator=attr.validators.min_len(1))
    parameters: List[LookMLExploreParameter] = attr.field(
        validator=attr.validators.instance_of(List)
    )
    extension_required: bool = attr.field(default=False)
    view_name: Optional[str] = attr.field(default=None)
    view_label: Optional[str] = attr.field(default=None)
    extended_explores: List[str] = attr.field(factory=list)

    @property
    def non_template_name(self) -> str:
        """Returns this LookMLExplore's name with `_template` removed
        if the name ends with that string, otherwise the name itself."""
        suffix = "_template"
        if self.explore_name.endswith(suffix):
            return self.explore_name[: -len(suffix)]
        return self.explore_name

    def build(self) -> str:
        """Returns string containing contents of this LookML explore file"""
        extends_clause = ""
        if self.extended_explores:
            extends_str = ",\n".join(
                [f"    {explore_name}" for explore_name in self.extended_explores]
            )
            extends_clause = f"  extends: [\n{extends_str}\n  ]\n"

        extension_required_clause = ""
        if self.extension_required:
            extension_required_clause = "  extension: required\n"

        view_name_clause = ""
        if self.view_name:
            label = self.view_label or self.view_name
            view_name_clause = (
                f'\n  view_name: {self.view_name}\n  view_label: "{label}"\n'
            )

        parameters = "".join(f"\n  {param.build()}" for param in self.parameters)

        return StrictStringFormatter().format(
            EXPLORE_TEMPLATE,
            explore_name=self.explore_name,
            extension_required=extension_required_clause,
            extends_clause=extends_clause,
            view_name_clause=view_name_clause,
            parameters=parameters,
        )

    def write(self, output_directory: str, source_script_path: str) -> None:
        """
        Writes LookML explore file into the specified output directory with a
        header indicating the date and script source of the auto-generated explore.
        """
        file_name = f"{self.explore_name}.explore.lkml"
        write_lookml_file(
            output_directory=output_directory,
            file_name=file_name,
            source_script_path=source_script_path,
            file_body=self.build(),
        )


def write_explores_to_file(
    explores: List[LookMLExplore],
    top_level_explore_name: str,
    output_directory: str,
    source_script_path: str,
) -> None:
    """Writes multiple LookML explores to a single file in the specified output directory,
    with a header indicating the date and script source of the auto-generated explores.
    """
    file_name = f"{top_level_explore_name}.explore.lkml"
    write_lookml_file(
        output_directory=output_directory,
        file_name=file_name,
        source_script_path=source_script_path,
        file_body="\n".join([explore.build() for explore in explores]),
    )
