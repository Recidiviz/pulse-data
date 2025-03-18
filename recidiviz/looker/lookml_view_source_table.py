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
"""Creates a LookMLSourceTable object and associated functions"""
import abc
from typing import Union

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.looker.parameterized_value import ParameterizedValue


@attr.define
class LookMLViewSourceTable:
    """Produces a clause that specifies the source table for the view, which can be either a table address
    from the databse, or a derived table from a sql query."""

    @abc.abstractmethod
    def build(self) -> str:
        pass

    @classmethod
    def sql_table_address(
        cls, address: Union[BigQueryAddress, ParameterizedValue]
    ) -> "LookMLViewSourceTable":
        return SqlTableAddress(address)

    @classmethod
    def derived_table(cls, sql: str) -> "LookMLViewSourceTable":
        return DerivedTable(sql)

    @property
    @abc.abstractmethod
    def is_derived_table(self) -> bool:
        """Returns True if the source table is a derived table, False otherwise."""


@attr.define
class SqlTableAddress(LookMLViewSourceTable):
    """
    Constructs the `sql_table_name` clause of a view file
    (see https://cloud.google.com/looker/docs/reference/param-view-sql-table-name)
    """

    address: Union[BigQueryAddress, ParameterizedValue]

    def build(self) -> str:
        if isinstance(self.address, ParameterizedValue):
            sql_table_name = self.address.build_liquid_template()
        else:
            sql_table_name = self.address.to_str()
        return f"  sql_table_name: {sql_table_name} ;;"

    @property
    def is_derived_table(self) -> bool:
        return False


@attr.define
class DerivedTable(LookMLViewSourceTable):
    """
    Constructs the `derived_table` clause of a view file. Not all syntax is supported.
    (see https://cloud.google.com/looker/docs/reference/param-view-derived-table)
    """

    sql: str

    def build(self) -> str:
        return f"""  derived_table: {{
    sql: {self.sql} ;;
  }}"""

    @property
    def is_derived_table(self) -> bool:
        return True
