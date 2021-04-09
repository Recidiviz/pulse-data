# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

from typing import List, Optional, Union

from sqlalchemy import Column
from sqlalchemy.engine import Connection
from sqlalchemy.types import TypeEngine
from typing_extensions import Literal

ConstraintType = Union[
    Literal["foreignkey"],
    Literal["primarykey"],
    Literal["unique"],
    Literal["check"],
]

def add_column(table_name: str, column: str) -> None: ...
def alter_column(
    table_name: str,
    column_name: str,
    type_: Optional[TypeEngine] = None,
    postgresql_using: Optional[str] = None,
) -> None: ...
def create_check_constraint(
    cosntraint_name: str, table_name: str, condition: str
) -> None: ...
def create_index(
    index_name: str, table_name: str, columns: List[str], unique: bool = False
) -> None: ...
def create_table(table_name: str, *columns: List[Column]) -> None: ...
def create_unique_constraint(
    constraint_name: str, table_name: str, columns: List[str]
) -> None: ...
def drop_column(table_name: str, column_name: str) -> None: ...
def drop_constraint(
    constraint_name: str, table_name: str, type_: Optional[ConstraintType] = None
) -> None: ...
def drop_index(index_name: str, table_name: Optional[str] = None) -> None: ...
def drop_table(table_name: str) -> None: ...
def execute(sqltext: str) -> None: ...
def f(name: str) -> str: ...
def get_bind() -> Connection: ...
