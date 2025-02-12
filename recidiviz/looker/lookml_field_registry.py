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
"""Registry for LookML fields."""
from typing import Dict, List

import attr

from recidiviz.looker.lookml_view_field import LookMLViewField


@attr.define
class LookMLFieldRegistry:
    """Registry for LookML fields. This is useful if you are autogenerating LookML views
    and want to include custom fields for some tables.

    Attributes:
        _table_fields: A dictionary mapping table ids to a list of LookML fields.
        default_fields: A list of default LookML fields to return for every table.
    """

    _table_fields: Dict[str, List[LookMLViewField]] = attr.field(factory=dict)
    default_fields: List[LookMLViewField] = attr.field(factory=list)

    def register(self, table_id: str, fields: List[LookMLViewField]) -> None:
        """Registers a list of LookML fields for the given table id. Will overwrite any existing fields."""
        self._table_fields[table_id] = fields

    def get(self, table_id: str) -> List[LookMLViewField]:
        """Returns a list of LookML fields for the given table id.
        Will return default fields for any table id, even if it is not present in the registry.
        If a table has any fields registed with the same name as a default field, the default field will be ignored.
        """
        fields = self._table_fields.get(table_id, [])
        existing_field_names = {field.field_name for field in fields}
        missing_defaults = [
            default_field
            for default_field in self.default_fields
            if default_field.field_name not in existing_field_names
        ]
        return fields + missing_defaults
