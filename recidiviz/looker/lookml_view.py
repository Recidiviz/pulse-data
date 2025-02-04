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
from typing import List, Optional

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.looker.lookml_utils import write_lookml_file
from recidiviz.looker.lookml_view_field import DimensionLookMLViewField, LookMLViewField
from recidiviz.looker.lookml_view_source_table import LookMLViewSourceTable
from recidiviz.utils.string import StrictStringFormatter

VIEW_TEMPLATE = """{include_clause}view: {view_name} {{
{extension_required}{extends_clause}{table_clause}{field_declarations}
}}"""


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

    @classmethod
    def for_big_query_table(
        cls, dataset_id: str, table_id: str, fields: List[LookMLViewField]
    ) -> "LookMLView":
        """Create a LookMLView object for a BigQuery table"""
        return cls(
            view_name=table_id,
            table=LookMLViewSourceTable.sql_table_address(
                BigQueryAddress(dataset_id=dataset_id, table_id=table_id)
            ),
            fields=fields,
        )

    def qualified_name_for_field(self, field_name: str) -> str:
        """Return a string with the format view_name.field_name
        or raises an error if the field is not in this view"""
        if not any(field.field_name == field_name for field in self.fields):
            raise ValueError(
                f"Field name {field_name} does not exist in {self.view_name}"
            )

        return f"{self.view_name}.{field_name}"

    def qualified_dimension_names(self) -> List[str]:
        """Return a list of qualified names for all dimensions in this view
        -- not including dimension groups"""
        return [
            self.qualified_name_for_field(field.field_name)
            for field in self.fields
            if isinstance(
                field,
                DimensionLookMLViewField,
            )
        ]

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
            extends_clause = f"  extends: [\n{extends_str}\n  ]\n"

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
        Writes LookML view file into the specified output directory with a
        header indicating the date and script source of the auto-generated view.
        """
        file_name = f"{self.view_name}.view.lkml"
        write_lookml_file(
            output_directory=output_directory,
            file_name=file_name,
            source_script_path=source_script_path,
            file_body=self.build(),
        )
