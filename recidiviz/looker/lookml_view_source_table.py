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

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress


@attr.define
class LookMLViewSourceTable:
    """Produces a clause that specifies the source table for the view, which can be either a table address
    from the databse, or a derived table from a sql query."""

    @abc.abstractmethod
    def build(self) -> str:
        pass

    @classmethod
    def sql_table_address(cls, address: BigQueryAddress) -> "LookMLViewSourceTable":
        return SqlTableAddress(address)

    @classmethod
    def derived_table(cls, sql: str) -> "LookMLViewSourceTable":
        return DerivedTable(sql)


@attr.define
class SqlTableAddress(LookMLViewSourceTable):
    """
    Constructs the `sql_table_name` clause of a view file
    (see https://cloud.google.com/looker/docs/reference/param-view-sql-table-name)
    """

    address: BigQueryAddress

    def build(self) -> str:
        sql_table_name = f"{self.address.dataset_id}.{self.address.table_id}"
        return f"  sql_table_name: {sql_table_name} ;;"


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
