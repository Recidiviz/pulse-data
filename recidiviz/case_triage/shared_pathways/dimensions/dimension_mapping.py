# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
# ============================================================================
""" Dimension mapping module"""
import enum
from typing import List, Literal, Mapping, Union, overload

import attr
from sqlalchemy import Column, and_, false, not_, or_, tuple_
from sqlalchemy.sql.elements import ColumnElement

from recidiviz.case_triage.shared_pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.shared_pathways.exceptions import MetricMappingError
from recidiviz.common.constants.pathways import PATHWAYS_COMPOUND_FILTER_VALUE_DELIMITER


class DimensionOperation(enum.IntFlag):
    """Flags for if the dimension supports a given operation"""

    FILTER = 1
    GROUP = 2
    ALL = FILTER | GROUP


@attr.s(auto_attribs=True)
class DimensionMappingCollection:
    """Contains functionality useful for enumerating mappings"""

    dimension_mappings: List["DimensionMapping"]

    @property
    def operable_map(
        self,
    ) -> Mapping[DimensionOperation, Mapping[Dimension, "DimensionMapping"]]:
        return {
            dimension_operation: {
                dimension_mapping.dimension: dimension_mapping
                for dimension_mapping in self.dimension_mappings
                if dimension_operation in dimension_mapping.operations
            }
            for dimension_operation in DimensionOperation
        }

    @overload
    def columns_for_dimension_operation(
        self,
        dimension_operation: Literal[DimensionOperation.FILTER],
        dimension: Dimension,
    ) -> Column:
        ...

    @overload
    def columns_for_dimension_operation(
        self,
        dimension_operation: Literal[DimensionOperation.GROUP],
        dimension: Dimension,
    ) -> List[Column]:
        ...

    def columns_for_dimension_operation(
        self, dimension_operation: DimensionOperation, dimension: Dimension
    ) -> Union[Column, List[Column]]:
        dimension_mapping = self.mapping_for_dimension_operation(
            dimension_operation, dimension
        )
        if dimension_operation == DimensionOperation.FILTER:
            column, *rest = dimension_mapping.columns

            if rest:
                raise MetricMappingError(
                    "Composite dimensions do not directly map to a single column and cannot be filtered"
                )
            return column

        return dimension_mapping.columns

    def mapping_for_dimension_operation(
        self, dimension_operation: DimensionOperation, dimension: Dimension
    ) -> "DimensionMapping":
        """Returns the mapping that supports the given operation on the given
        dimension."""
        try:
            return self.operable_map[dimension_operation][dimension]
        except KeyError as e:
            raise MetricMappingError(
                f"Dimension {dimension.value} is not allowed for {dimension_operation} {self}"
            ) from e


@attr.s(auto_attribs=True)
class DimensionMapping:
    """Maps a dimension onto the columns an operation on it reads."""

    dimension: Dimension
    operations: DimensionOperation
    # For a FILTER mapping with more than one column, each filter value names
    # one value per column, joined by PATHWAYS_COMPOUND_FILTER_VALUE_DELIMITER.
    columns: List[Column] = attr.ib(factory=list)
    # Conditions that select the rows this dimension describes, e.g. an
    # admission type describes admission rows and not release rows. Grouping by
    # the dimension counts only those rows. Filtering on it narrows only those
    # rows, and keeps every other row.
    applicable_row_conditions: List[ColumnElement] = attr.ib(factory=list)

    def build_filter_condition(self, values: List[str]) -> ColumnElement:
        """Returns the condition that keeps the rows matching any of the given
        filter values.

        For example, with columns (custody_type, movement_type), applicable to
        movement_category = 'admission', the value
        "incarcerated_parolee|other" keeps parolee admissions of type other,
        and every release.

        An empty value names no option. A query string cannot carry an empty
        list, so a reader who unchecks every option sends one empty value, and
        the filter matches none of the rows it applies to.
        """
        named_values = [value for value in values if value]
        if not named_values:
            matches_value = false()
        elif len(self.columns) == 1:
            matches_value = self.columns[0].in_(named_values)
        else:
            matches_value = tuple_(*self.columns).in_(
                [self._split_compound_value(value) for value in named_values]
            )

        if not self.applicable_row_conditions:
            return matches_value
        return or_(not_(and_(*self.applicable_row_conditions)), matches_value)

    def _split_compound_value(self, value: str) -> tuple[str, ...]:
        parts = tuple(value.split(PATHWAYS_COMPOUND_FILTER_VALUE_DELIMITER))
        if len(parts) != len(self.columns):
            raise MetricMappingError(
                f"Filter value [{value}] for dimension [{self.dimension.value}] "
                f"must have {len(self.columns)} parts joined by "
                f"[{PATHWAYS_COMPOUND_FILTER_VALUE_DELIMITER}], one each for "
                f"{[column.name for column in self.columns]}."
            )
        return parts
