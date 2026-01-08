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
"""Defines a set of classes for modeling the join relationships between state raw data
tables.
"""
import abc
import re
from enum import Enum
from typing import Any, List, Optional, Tuple

import attr

from recidiviz.common import attr_validators
from recidiviz.utils.yaml_dict import YAMLDict


class RawDataJoinCardinality(Enum):
    """Defines the cardinality of the join between two raw data tables."""

    MANY_TO_MANY = "MANY_TO_MANY"
    MANY_TO_ONE = "MANY_TO_ONE"
    ONE_TO_MANY = "ONE_TO_MANY"
    ONE_TO_ONE = "ONE_TO_ONE"

    def invert(self) -> "RawDataJoinCardinality":
        """Returns the cardinality value that is inverted relative to this value (e.g.
        ONE_TO_MANY -> MANY_TO_ONE).
        """

        if self in (
            RawDataJoinCardinality.MANY_TO_MANY,
            RawDataJoinCardinality.ONE_TO_ONE,
        ):
            return self

        if self is RawDataJoinCardinality.ONE_TO_MANY:
            return RawDataJoinCardinality.MANY_TO_ONE

        if self is RawDataJoinCardinality.MANY_TO_ONE:
            return RawDataJoinCardinality.ONE_TO_MANY

        raise ValueError(f"Unexpected value [{self}]")


@attr.define(kw_only=True, frozen=True)
class JoinColumn:
    """Defines a raw table column that is involved in a join clause."""

    RAW_TABLE_COLUMN_REGEX = re.compile(r"^(?P<file_tag>\w+)\.(?P<column>\w+)$")

    file_tag: str = attr.ib(validator=attr_validators.is_non_empty_str)
    column: str = attr.ib(validator=attr_validators.is_non_empty_str)

    def __str__(self) -> str:
        return f"{self.file_tag}.{self.column}"

    @classmethod
    def parse_from_string(cls, column_string: str) -> Optional["JoinColumn"]:
        """Parses the join column from a string like 'my_table.my_column'."""
        if not (match := re.match(cls.RAW_TABLE_COLUMN_REGEX, column_string)):
            return None
        return JoinColumn(
            file_tag=match.group("file_tag"), column=match.group("column")
        )


@attr.define(frozen=True)
class JoinTransform:
    """Defines a way to transform a raw table column that is involved in a join clause."""

    column: JoinColumn = attr.ib()
    transformation: str = attr.ib()

    def __attrs_post_init__(self) -> None:
        if "{col_name}" not in self.transformation:
            raise ValueError(
                f"Transformation {self.transformation} does not include "
                f"the string {{col_name}}"
            )


@attr.define(frozen=True)
class JoinBooleanClause:
    """An abstract interface which represents one boolean clause that makes up part of
    a join condition between two raw data tables. Multiple boolean clauses can be joined
    with AND to create the overall join condition.
    """

    CLAUSE_SPLITTING_REGEX = re.compile(
        r"^(?P<left_side>[^=<>!]+)(?P<operator>=|!=|<=|>=|==| IN )(?P<right_side>[^=<>!]+)$"
    )

    @abc.abstractmethod
    def to_sql(self) -> str:
        """Converts this JoinBooleanClause to a string SQL boolean clause, e.g.
        "my_table.col_1 = my_table2.col_2".
        """

    @abc.abstractmethod
    def get_referenced_columns(self) -> List[JoinColumn]:
        """Returns a sorted, non-empty list of columns referenced in this clause."""

    @classmethod
    def parse_from_string(cls, join_clause_str: str) -> "JoinBooleanClause":
        if not (match := re.match(cls.CLAUSE_SPLITTING_REGEX, join_clause_str)):
            raise ValueError(f"Unable to parse join clause [{join_clause_str}]")

        operator = match.group("operator").strip()
        if operator != "=":
            raise ValueError(
                f"Found unsupported operator in join clause [{join_clause_str}]: "
                f"[{operator}]. Operator must be =."
            )

        left_side = match.group("left_side").strip()
        left_side_column = JoinColumn.parse_from_string(left_side)
        if not left_side_column:
            raise ValueError(
                f"Left side of equals sign in the join clause must always be a column. "
                f"Found left side [{left_side}] in join clause [{join_clause_str}]."
            )
        right_side = match.group("right_side").strip()
        if right_side_column := JoinColumn.parse_from_string(right_side):
            return ColumnEqualityJoinBooleanClause(
                column_1=left_side_column, column_2=right_side_column
            )

        return ColumnFilterJoinBooleanClause(
            column=left_side_column, filter_value=right_side
        )


# Set eq=False to allow us to define a custom equality operator
@attr.define(kw_only=True, eq=False)
class ColumnEqualityJoinBooleanClause(JoinBooleanClause):
    """Represents a boolean clause expressing equality between two raw data table
    columns.
    """

    column_1: JoinColumn
    column_2: JoinColumn

    def __attrs_post_init__(self) -> None:
        if self.column_1 == self.column_2:
            raise ValueError(
                f"Table relationship join clause found between the same column. "
                f"Column 1: [{self.column_1}]. Column 2: [{self.column_2}]. "
                f"Join relationships must show equality between two different columns. "
            )

    def to_sql(self) -> str:
        """Converts this JoinBooleanClause to a string SQL boolean clause, e.g.
        "my_table.col_1 = my_table2.col_2".
        """
        return f"{self.column_1} = {self.column_2}"

    def get_referenced_columns(self) -> List[JoinColumn]:
        return sorted(
            [self.column_1, self.column_2], key=lambda c: (c.file_tag, c.column)
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ColumnEqualityJoinBooleanClause):
            return False
        return self.get_referenced_columns() == other.get_referenced_columns()

    def __hash__(self) -> int:
        return hash(tuple(self.get_referenced_columns()))


@attr.define
class ColumnFilterJoinBooleanClause(JoinBooleanClause):
    """Represents a boolean clause expressing equality between a single raw data table
    column and a constant value. Can be used in a join expression alongside a
    ColumnEqualityJoinBooleanClause to produce a more nuanced filter.

    For example, allows you to express the second half of join logic like this:
        my_table.my_col = my_other_table.my_col AND my_other_table.my_col = "ABC"
    """

    column: JoinColumn

    # Filter value string which must contain an integer or a quoted string.
    filter_value: str = attr.ib(validator=attr_validators.is_non_empty_str)

    def __attrs_post_init__(self) -> None:
        if not self._is_valid_string_filter_value(
            self.filter_value
        ) and not self._is_valid_int_filter_value(self.filter_value):
            raise ValueError(
                f"Found invalid filter value [{self.filter_value}] in a join clause. "
                f"Join filters must either be an integer or a quoted string."
            )

    @staticmethod
    def _is_valid_string_filter_value(filter_value: str) -> bool:
        if len(filter_value) <= 1:
            return False

        double_quote = '"'
        single_quote = "'"
        return (
            filter_value.startswith(double_quote)
            and filter_value.endswith(double_quote)
        ) or (
            filter_value.startswith(single_quote)
            and filter_value.endswith(single_quote)
        )

    @staticmethod
    def _is_valid_int_filter_value(filter_value: str) -> bool:
        try:
            int(filter_value)
            return True
        except ValueError:
            return False

    def to_sql(self) -> str:
        """Converts this JoinBooleanClause to a string SQL boolean clause, e.g.
        "my_table.col_1 = 'SOME_VALUE'".
        """
        return f"{self.column} = {self.filter_value}"

    def get_referenced_columns(self) -> List[JoinColumn]:
        return [self.column]


# Set eq=False to allow us to define a custom equality operator
@attr.define(eq=False)
class RawTableRelationshipInfo:
    """Describes one way to join a table to another table (or itself)."""

    file_tag: str = attr.ib(validator=attr_validators.is_non_empty_str)
    foreign_table: str = attr.ib(validator=attr_validators.is_non_empty_str)

    join_clauses: List[JoinBooleanClause]
    cardinality: RawDataJoinCardinality = attr.ib()
    transforms: List[JoinTransform]

    def __attrs_post_init__(self) -> None:
        for join_clause in self.join_clauses:
            join_file_tags = {c.file_tag for c in join_clause.get_referenced_columns()}
            if disallowed_file_tags := join_file_tags - set(
                self.get_referenced_tables()
            ):
                raise ValueError(
                    f"Found join clause [{join_clause.to_sql()}] which involves "
                    f"table(s) [{disallowed_file_tags}] which are not defined as part "
                    f"of the relationship. Allowed tables: {self.get_referenced_tables()}."
                )

        if not any(
            jc
            for jc in self.join_clauses
            if isinstance(jc, ColumnEqualityJoinBooleanClause)
        ):
            raise ValueError(
                f"Found table relationship defined for table(s) "
                f"{self.get_referenced_tables()} which does not have any join_logic "
                f"expressing equality between two different columns."
            )

        cols_being_transformed = [transform.column for transform in self.transforms]
        if len(cols_being_transformed) != len(set(cols_being_transformed)):
            raise ValueError(
                f"Found table relationship defined for table(s) "
                f"{self.get_referenced_tables()} which has multiple transforms "
                f"defined for one column."
            )
        for col in cols_being_transformed:
            if not any(
                col in join_clause.get_referenced_columns()
                for join_clause in self.join_clauses
            ):
                raise ValueError(
                    f"Found transformation on {col} defined for table(s) "
                    f"{self.get_referenced_tables()} which does not reference "
                    f"that column."
                )

    def get_referenced_tables(self) -> Tuple[str, ...]:
        """Returns a sorted tuple of the file tags for the tables involved in this
        table relationship.
        """
        return tuple(sorted({self.file_tag, self.foreign_table}))

    def join_sql(self) -> str:
        """Builds a composite SQL clause that can be used in the ON clause of a JOIN,
        using AND to combine all individual boolean join clauses.
        """
        return " AND ".join([j.to_sql() for j in self.join_clauses])

    def invert(self) -> "RawTableRelationshipInfo":
        return RawTableRelationshipInfo(
            file_tag=self.foreign_table,
            foreign_table=self.file_tag,
            cardinality=self.cardinality.invert(),
            join_clauses=self.join_clauses,
            transforms=self.transforms,
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, RawTableRelationshipInfo):
            return False

        # Order of join clauses doesn't matter as all clauses are joined with AND.
        if set(self.join_clauses) != set(other.join_clauses):
            return False
        # Order of transforms also doesn't matter
        if set(self.transforms) != set(other.transforms):
            return False

        if (
            self.file_tag == other.file_tag
            and self.foreign_table == other.foreign_table
            and self.cardinality == other.cardinality
        ):
            return True

        other_inverted = other.invert()

        return (
            self.file_tag == other_inverted.file_tag
            and self.foreign_table == other_inverted.foreign_table
            and self.cardinality == other_inverted.cardinality
        )

    @classmethod
    def build_from_table_relationship_yaml(
        cls, file_tag: str, table_relationship: YAMLDict
    ) -> "RawTableRelationshipInfo":
        """
        Builds a RawTableRelationshipInfo object for the given file tag
        from a YAMLDict with required keys `foreign_table` and `join_logic`
        and optional keys `cardinality` and `transforms`
        """
        foreign_table = table_relationship.pop("foreign_table", str)

        cardinality_str = table_relationship.pop_optional("cardinality", str)
        cardinality = (
            RawDataJoinCardinality(cardinality_str)
            if cardinality_str
            else RawDataJoinCardinality.MANY_TO_MANY
        )

        join_clauses = [
            JoinBooleanClause.parse_from_string(join_clause_str)
            for join_clause_str in table_relationship.pop_list("join_logic", str)
        ]

        transforms_list = table_relationship.pop_dicts_optional("transforms")
        transforms = []
        if transforms_list:
            for join_transform in transforms_list:
                col = JoinColumn.parse_from_string(join_transform.pop("column", str))
                if col is None:
                    raise ValueError(
                        f"Could not parse column in transform for raw file"
                        f"[{file_tag}]: {col}"
                    )
                transforms.append(
                    JoinTransform(
                        column=col, transformation=join_transform.pop("transform", str)
                    )
                )

        if len(table_relationship) > 0:
            raise ValueError(
                f"Found unexpected related_tables config values for raw file"
                f"[{file_tag}]: {repr(table_relationship.get())}"
            )

        return RawTableRelationshipInfo(
            file_tag=file_tag,
            foreign_table=foreign_table,
            join_clauses=join_clauses,
            cardinality=cardinality,
            transforms=transforms,
        )
