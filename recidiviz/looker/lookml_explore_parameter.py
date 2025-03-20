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
"""Defines a class representing a parameter to be added to a 
   LookML explore."""

import abc
from enum import Enum
from typing import List

import attr


class JoinType(Enum):
    LEFT_OUTER = "left_outer"
    FULL_OUTER = "full_outer"
    INNER = "inner"
    CROSS = "cross"


class JoinCardinality(Enum):
    ONE_TO_ONE = "one_to_one"
    MANY_TO_ONE = "many_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_MANY = "many_to_many"


@attr.define
class LookMLExploreParameter:
    """Defines a LookML field parameter, including the parameter key
    and value string and attributes specific to the subclass"""

    @property
    @abc.abstractmethod
    def key(self) -> str:
        pass

    @property
    @abc.abstractmethod
    def value_text(self) -> str:
        pass

    def build(self) -> str:
        return f"{self.key}: {self.value_text}"

    @classmethod
    def description(cls, description: str) -> "LookMLExploreParameter":
        return ExploreParameterDescription(text=description)

    @classmethod
    def group_label(cls, group_label: str) -> "LookMLExploreParameter":
        return ExploreParameterGroupLabel(text=group_label)

    @classmethod
    def label(cls, label: str) -> "LookMLExploreParameter":
        return ExploreParameterLabel(text=label)

    @classmethod
    def join(
        cls, join_name: str, join_params: List["LookMLJoinParameter"]
    ) -> "LookMLExploreParameter":
        return ExploreParameterJoin(join_name, join_params)


# DISPLAY PARAMETERS
@attr.define
class ExploreParameterDescription(LookMLExploreParameter):
    """Generates a `description` field parameter
    (see https://cloud.google.com/looker/docs/reference/param-explore-description).
    """

    text: str

    @property
    def key(self) -> str:
        return "description"

    @property
    def value_text(self) -> str:
        return f'"{self.text}"'


@attr.define
class ExploreParameterGroupLabel(LookMLExploreParameter):
    """Generates a `group_label` field parameter
    (see https://cloud.google.com/looker/docs/reference/param-explore-group-label).
    """

    text: str

    @property
    def key(self) -> str:
        return "group_label"

    @property
    def value_text(self) -> str:
        return f'"{self.text}"'


@attr.define
class ExploreParameterLabel(LookMLExploreParameter):
    """Generates a `label` field parameter
    (see https://cloud.google.com/looker/docs/reference/param-explore-label).
    """

    text: str

    @property
    def key(self) -> str:
        return "label"

    @property
    def value_text(self) -> str:
        return f'"{self.text}"'


# JOIN PARAMETERS
@attr.define
class LookMLJoinParameter(LookMLExploreParameter):
    """
    Abstract class representing an inner parameter for a Join within an explore
    """

    @property
    @abc.abstractmethod
    def key(self) -> str:
        pass

    @property
    @abc.abstractmethod
    def value_text(self) -> str:
        pass

    @classmethod
    def from_parameter(cls, table: str) -> "LookMLJoinParameter":
        return JoinParameterFrom(table)

    @classmethod
    def relationship(cls, relationship_type: JoinCardinality) -> "LookMLJoinParameter":
        return JoinParameterRelationship(relationship_type)

    @classmethod
    def type(cls, join_type: JoinType) -> "LookMLJoinParameter":
        return JoinParameterType(join_type)

    @classmethod
    def sql_on(cls, sql_text: str) -> "LookMLJoinParameter":
        return JoinParameterSqlOn(sql_text)

    @classmethod
    def view_label(cls, label: str) -> "LookMLJoinParameter":
        return JoinParameterViewLabel(label)


@attr.define
class ExploreParameterJoin(LookMLExploreParameter):
    """Generates a `join` parameter for explores
    (see https://cloud.google.com/looker/docs/reference/param-explore-join).
    """

    view_name: str
    join_params: List[LookMLJoinParameter]

    @property
    def key(self) -> str:
        return "join"

    @property
    def value_text(self) -> str:
        params = "\n    ".join(param.build() for param in self.join_params)
        return f"{self.view_name} {{\n    {params}\n  }}\n"

    @classmethod
    def build_join_parameter(
        cls,
        parent_view: str,
        child_view: str,
        join_field: str,
        join_cardinality: JoinCardinality,
    ) -> "ExploreParameterJoin":
        sql_on_text = (
            f"${{{parent_view}.{join_field}}} = ${{{child_view}.{join_field}}}"
        )
        sql_on = LookMLJoinParameter.sql_on(sql_on_text)

        return cls(
            view_name=child_view,
            join_params=[sql_on, LookMLJoinParameter.relationship(join_cardinality)],
        )


@attr.define
class JoinParameterFrom(LookMLJoinParameter):
    """
    Generates a `from` parameter inside a `join` parameter in an explore
    (see the section on `from` in https://cloud.google.com/looker/docs/reference/param-explore-join)
    """

    table: str

    @property
    def key(self) -> str:
        return "from"

    @property
    def value_text(self) -> str:
        return self.table


@attr.define
class JoinParameterRelationship(LookMLJoinParameter):
    """
    Generates a `relationship` parameter inside a `join` parameter in an explore
    (see https://cloud.google.com/looker/docs/reference/param-explore-join#relationship
    and https://cloud.google.com/looker/docs/reference/param-explore-join-relationship)
    """

    relationship_type: JoinCardinality

    @property
    def key(self) -> str:
        return "relationship"

    @property
    def value_text(self) -> str:
        return self.relationship_type.value


@attr.define
class JoinParameterType(LookMLJoinParameter):
    """
    Generates a `type` parameter inside a `join` parameter in an explore
    (see https://cloud.google.com/looker/docs/reference/param-explore-join#type)
    """

    join_type: JoinType

    @property
    def key(self) -> str:
        return "type"

    @property
    def value_text(self) -> str:
        return self.join_type.value


@attr.define
class JoinParameterViewLabel(LookMLJoinParameter):
    """
    Generates a `view_label` parameter inside a `join` parameter in an explore
    (see https://cloud.google.com/looker/docs/reference/param-explore-join-view-label)
    """

    label_text: str

    @property
    def key(self) -> str:
        return "view_label"

    @property
    def value_text(self) -> str:
        return f'"{self.label_text}"'


@attr.define
class JoinParameterSqlOn(LookMLJoinParameter):
    """
    Generates a `sql_on` parameter inside a `join` parameter in an explore
    (see https://cloud.google.com/looker/docs/reference/param-explore-join-sql-on)
    """

    sql_field: str

    @property
    def key(self) -> str:
        return "sql_on"

    @property
    def value_text(self) -> str:
        return f"{self.sql_field};;"
