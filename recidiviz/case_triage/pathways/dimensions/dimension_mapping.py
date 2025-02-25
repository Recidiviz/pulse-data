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
from sqlalchemy import Column

from recidiviz.case_triage.pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.pathways.exceptions import MetricMappingError


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
        try:
            dimension_mapping = self.operable_map[dimension_operation][dimension]
            if dimension_operation == DimensionOperation.FILTER:
                column, *rest = dimension_mapping.columns

                if rest:
                    raise MetricMappingError(
                        "Composite dimensions do not directly map to a single column and cannot be filtered"
                    )
                return column

            return dimension_mapping.columns
        except KeyError as e:
            raise MetricMappingError(
                f"Dimension {dimension.value} is not allowed for {dimension_operation} {self}"
            ) from e


@attr.s(auto_attribs=True)
class DimensionMapping:
    dimension: Dimension
    operations: DimensionOperation
    columns: List[Column] = attr.ib(factory=list)
