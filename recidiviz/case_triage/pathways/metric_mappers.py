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
""" Contains functionality to map metrics to our relational models inside the query builders"""
import enum
from typing import List, Union

import attr
from sqlalchemy import Column

from recidiviz.case_triage.pathways.dimension import Dimension
from recidiviz.persistence.database.schema.pathways.schema import (
    LibertyToPrisonTransitions,
    PathwaysBase,
    PrisonToSupervisionTransitions,
)


@attr.s(auto_attribs=True)
class MetricMappingError(ValueError):
    message: str


class DimensionOperation(enum.IntFlag):
    """Flags for if the dimension supports a given operation"""

    FILTER = 1
    GROUP = 2
    ALL = FILTER | GROUP


@attr.s(auto_attribs=True)
class DimensionMapping:
    dimension: Dimension
    operations: DimensionOperation
    columns: List[Column] = attr.ib(factory=list)

    @property
    def is_composite(self) -> bool:
        return len(self.columns) > 1


@attr.s(auto_attribs=True)
class CountByDimensionMetricMapper:
    """Contains mappings for count by dimension metrics"""

    name: str
    model: PathwaysBase
    timestamp_column: Column
    dimension_mappings: List[DimensionMapping]

    def __attrs_post_init__(self) -> None:
        self.filter_dimensions = {
            dimension_mapping.dimension: dimension_mapping.columns
            for dimension_mapping in self.dimension_mappings
            if DimensionOperation.FILTER in dimension_mapping.operations
        }

        self.grouping_dimensions = {
            dimension_mapping.dimension: dimension_mapping.columns
            for dimension_mapping in self.dimension_mappings
            if DimensionOperation.GROUP in dimension_mapping.operations
        }

    def column_for_dimension_filter(self, dimension: Dimension) -> Column:
        try:
            column, *rest = self.filter_dimensions[dimension]

            if rest:
                raise MetricMappingError(
                    "Composite dimensions do not directly map to a single column and cannot be filtered"
                )
            return column
        except KeyError as e:
            raise MetricMappingError(
                f"Dimension {dimension.value} is not allowed for {self}"
            ) from e

    def columns_for_dimension_grouping(
        self, dimension: Dimension
    ) -> Union[Column, List[Column]]:
        try:
            return self.grouping_dimensions[dimension]
        except KeyError as e:
            raise MetricMappingError(
                f"Dimension {dimension.value} is not allowed for {self}"
            ) from e


LibertyToPrisonTransitionsCount = CountByDimensionMetricMapper(
    name="LibertyToPrisonTransitionsCount",
    model=LibertyToPrisonTransitions,
    timestamp_column=LibertyToPrisonTransitions.transition_date,
    dimension_mappings=[
        DimensionMapping(
            dimension=Dimension.YEAR_MONTH,
            operations=DimensionOperation.GROUP,
            columns=[LibertyToPrisonTransitions.year, LibertyToPrisonTransitions.month],
        ),
        DimensionMapping(
            dimension=Dimension.JUDICIAL_DISTRICT,
            operations=DimensionOperation.ALL,
            columns=[LibertyToPrisonTransitions.judicial_district],
        ),
        DimensionMapping(
            dimension=Dimension.PRIOR_LENGTH_OF_INCARCERATION,
            operations=DimensionOperation.ALL,
            columns=[LibertyToPrisonTransitions.prior_length_of_incarceration],
        ),
        DimensionMapping(
            dimension=Dimension.GENDER,
            operations=DimensionOperation.ALL,
            columns=[LibertyToPrisonTransitions.gender],
        ),
        DimensionMapping(
            dimension=Dimension.AGE_GROUP,
            operations=DimensionOperation.ALL,
            columns=[LibertyToPrisonTransitions.age_group],
        ),
        DimensionMapping(
            dimension=Dimension.RACE,
            operations=DimensionOperation.ALL,
            columns=[LibertyToPrisonTransitions.race],
        ),
    ],
)

PrisonToSupervisionTransitionsCount = CountByDimensionMetricMapper(
    name="PrisonToSupervisionTransitionsCount",
    model=PrisonToSupervisionTransitions,
    timestamp_column=PrisonToSupervisionTransitions.transition_date,
    dimension_mappings=[
        DimensionMapping(
            dimension=Dimension.YEAR_MONTH,
            operations=DimensionOperation.GROUP,
            columns=[
                PrisonToSupervisionTransitions.year,
                PrisonToSupervisionTransitions.month,
            ],
        ),
        DimensionMapping(
            dimension=Dimension.AGE_GROUP,
            operations=DimensionOperation.ALL,
            columns=[PrisonToSupervisionTransitions.age_group],
        ),
        DimensionMapping(
            dimension=Dimension.GENDER,
            operations=DimensionOperation.ALL,
            columns=[PrisonToSupervisionTransitions.gender],
        ),
        DimensionMapping(
            dimension=Dimension.FACILITY,
            operations=DimensionOperation.ALL,
            columns=[PrisonToSupervisionTransitions.facility],
        ),
    ],
)
