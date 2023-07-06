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
"""Creates a LookMLView object and associated functions"""
import os
from datetime import date
from typing import List, Optional

import attr

import recidiviz
from recidiviz.looker.lookml_view_field import LookMLViewField
from recidiviz.looker.lookml_view_source_table import LookMLViewSourceTable
from recidiviz.utils.string import StrictStringFormatter

VIEW_TEMPLATE = """{include_clause}view: {view_name} {{
{extension_required}{extends_clause}{table_clause}{field_declarations}
}}"""

VIEW_FILE_TEMPLATE = """# This file was automatically generated using a pulse-data script on {date_str}.
# To regenerate, see `{script_repo_relative_path}`.

{view_body}
"""


@attr.define
class LookMLView:
    """Produces LookML view text that satisfies the syntax described in
    https://cloud.google.com/looker/docs/reference/param-view. Not all view syntax
    is supported.
    """

    view_name: str
    table: Optional[LookMLViewSourceTable] = None
    fields: List[LookMLViewField] = attr.ib(factory=list)
    included_paths: List[str] = attr.ib(factory=list)
    extended_views: List[str] = attr.ib(factory=list)
    extension_required: bool = attr.ib(default=False)

    def __attrs_post_init__(self) -> None:
        field_names = [field.field_name for field in self.fields]
        if len(set(field_names)) != len(field_names):
            raise ValueError(f"Duplicate field names found in {field_names}")

    def build(self) -> str:
        """Builds string defining a standalone LookML view file"""
        field_declarations = "".join([f"\n{field.build()}" for field in self.fields])
        include_clause = ""
        if self.included_paths:
            include_clause = "".join(
                [f'include: "{path}"\n' for path in self.included_paths]
            )

        extension_required_clause = ""
        if self.extension_required:
            extension_required_clause = "  extension: required\n"

        extends_clause = ""
        if self.extended_views:
            extends_str = ",\n".join(
                [f"    {view_name}" for view_name in self.extended_views]
            )
            extends_clause = f"extends: [\n{extends_str}\n]"

        return StrictStringFormatter().format(
            VIEW_TEMPLATE,
            include_clause=include_clause,
            extension_required=extension_required_clause,
            extends_clause=extends_clause,
            view_name=self.view_name,
            table_clause=self.table.build() if self.table else "",
            field_declarations=field_declarations,
        )

    def write(self, output_directory: str, source_script_path: str) -> None:
        """
        Writes LookML view string into the specified output directory with a header indicating the
        date and script source of the auto-generated view.
        """
        date_str = date.today().isoformat()
        script_repo_relative_path = os.path.join(
            "recidiviz",
            os.path.relpath(source_script_path, os.path.dirname(recidiviz.__file__)),
        )
        # if directory doesn't already exist, create
        os.makedirs(output_directory, exist_ok=True)

        with open(
            os.path.join(output_directory, f"{self.view_name}.view.lkml"),
            "w",
            encoding="UTF-8",
        ) as view_file:
            file_text = StrictStringFormatter().format(
                VIEW_FILE_TEMPLATE,
                date_str=date_str,
                script_repo_relative_path=script_repo_relative_path,
                view_body=self.build(),
            )

            view_file.write(file_text)
