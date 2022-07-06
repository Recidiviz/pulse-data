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
import abc
import enum
from typing import Dict, Generic, List, TypeVar, Union

import attr
from attrs import validators
from sqlalchemy import Column, String, cast, func, literal_column
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.orm import Query

from recidiviz.case_triage.pathways.dimension import Dimension
from recidiviz.persistence.database.schema.pathways.schema import (
    LibertyToPrisonTransitions,
    PathwaysBase,
    PrisonToSupervisionTransitions,
    SupervisionToLibertyTransitions,
    SupervisionToPrisonTransitions,
)


class TimePeriod(enum.Enum):
    MONTHS_0_6 = "months_0_6"
    MONTHS_7_12 = "months_7_12"
    MONTHS_13_24 = "months_13_24"
    MONTHS_25_60 = "months_25_60"

    @classmethod
    def month_map(cls) -> Dict[str, int]:
        return {member.value: int(member.value.split("_")[1]) for member in cls}

    @classmethod
    def period_range(cls, time_period: "TimePeriod") -> List[str]:
        month_map = cls.month_map()

        return [
            member.value
            for member in TimePeriod
            if month_map[member.value] <= month_map[time_period.value]
        ]


@attr.s(auto_attribs=True)
class FetchMetricParams:
    time_period: TimePeriod = attr.field(
        default=TimePeriod.MONTHS_25_60,
        converter=TimePeriod,
        validator=validators.in_(TimePeriod),
    )
    filters: Dict[Dimension, Union[str, List[str]]] = attr.field(factory=dict)


@attr.s(auto_attribs=True)
class CountByDimensionMetricParams(FetchMetricParams):
    group: Dimension = attr.attrib(default=Dimension.YEAR_MONTH)


@attr.s(auto_attribs=True)
class MetricMappingError(ValueError):
    message: str


@attr.s(auto_attribs=True)
class MetricQueryError(ValueError):
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


ParamsType = TypeVar("ParamsType", bound=FetchMetricParams)


@attr.s(auto_attribs=True)
class MetricQueryBuilder(Generic[ParamsType]):
    """Builds Postgres queries for Pathways metrics"""

    name: str
    model: PathwaysBase
    dimension_mappings: List[DimensionMapping]

    def __attrs_post_init__(self) -> None:
        self.filter_dimensions = {
            dimension_mapping.dimension: dimension_mapping.columns
            for dimension_mapping in self.dimension_mappings
            if DimensionOperation.FILTER in dimension_mapping.operations
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

    @abc.abstractmethod
    def build_query(self, params: ParamsType) -> Query:
        ...

    @abc.abstractmethod
    def build_params(self, schema: Dict) -> ParamsType:
        ...


@attr.s(auto_attribs=True)
class CountByDimensionMetricQueryBuilder(
    MetricQueryBuilder[CountByDimensionMetricParams]
):
    """Builder for Pathways postgres queries that return the count of entries matching a filter and grouped by a dimension."""

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()
        self.grouping_dimensions = {
            dimension_mapping.dimension: dimension_mapping.columns
            for dimension_mapping in self.dimension_mappings
            if DimensionOperation.GROUP in dimension_mapping.operations
        }

    def columns_for_dimension_grouping(
        self, dimension: Dimension
    ) -> Union[Column, List[Column]]:
        try:
            return self.grouping_dimensions[dimension]
        except KeyError as e:
            raise MetricMappingError(
                f"Dimension {dimension.value} is not allowed for {self}"
            ) from e

    def build_query(self, params: CountByDimensionMetricParams) -> Query:
        grouped_columns = self.columns_for_dimension_grouping(params.group)
        conditions = [
            self.column_for_dimension_filter(dimension).in_(value)
            for dimension, value in params.filters.items()
        ]

        if params.time_period:
            if not self.model.time_period:
                raise MetricQueryError(
                    f"Querying 'time_period' is not allowed for {self}"
                )

            time_periods = TimePeriod.period_range(params.time_period)
            conditions.append(self.model.time_period.in_(time_periods))

        return (
            Query([*grouped_columns, func.count(self.model.time_period)])
            .filter(*conditions)
            .group_by(*grouped_columns)
            .order_by(*grouped_columns)
        )

    def build_params(self, schema: Dict) -> CountByDimensionMetricParams:
        return CountByDimensionMetricParams(**schema)


@attr.s(auto_attribs=True)
class PersonLevelMetricQueryBuilder(MetricQueryBuilder[FetchMetricParams]):
    """Builder for Pathways postgres queries that return individual rows, potentially filtered by a condition."""

    non_aggregate_columns: List[Column]
    aggregate_columns: List[Column]

    def build_query(self, params: FetchMetricParams) -> Query:
        conditions = [
            self.column_for_dimension_filter(dimension).in_(value)
            for dimension, value in params.filters.items()
        ]

        if params.time_period:
            if not self.model.time_period:
                raise MetricQueryError(
                    f"Querying 'time_period' is not allowed for {self}"
                )
            time_periods = TimePeriod.period_range(params.time_period)
            conditions.append(self.model.time_period.in_(time_periods))

        grouped_columns = [
            column
            for mapping in self.dimension_mappings
            for column in mapping.columns
            if column not in self.aggregate_columns
        ] + self.non_aggregate_columns

        # The frontend displays a single row per person, with multiple ages/facilities separated
        # by ", ". Do that logic here to open the possibility of paginating later.
        aggregate_columns = [
            func.string_agg(
                cast(column, String),
                # Sort the results alphabetically, which is likely to end up being the way they're
                # ordered in the old backend since we have ORDER BY in our views.
                aggregate_order_by(literal_column("', '"), column),
            ).label(column.name)
            for column in self.aggregate_columns
        ]
        return (
            Query([*grouped_columns, *aggregate_columns])
            .filter(*conditions)
            .group_by(*grouped_columns)
            .order_by(*grouped_columns)
        )

    def build_params(self, schema: Dict) -> FetchMetricParams:
        return FetchMetricParams(**schema)


LibertyToPrisonTransitionsCount = CountByDimensionMetricQueryBuilder(
    name="LibertyToPrisonTransitionsCount",
    model=LibertyToPrisonTransitions,
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

PrisonToSupervisionTransitionsCount = CountByDimensionMetricQueryBuilder(
    name="PrisonToSupervisionTransitionsCount",
    model=PrisonToSupervisionTransitions,
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
            dimension=Dimension.RACE,
            operations=DimensionOperation.ALL,
            columns=[PrisonToSupervisionTransitions.race],
        ),
        DimensionMapping(
            dimension=Dimension.FACILITY,
            operations=DimensionOperation.ALL,
            columns=[PrisonToSupervisionTransitions.facility],
        ),
    ],
)

PrisonToSupervisionTransitionsPersonLevel = PersonLevelMetricQueryBuilder(
    name="PrisonToSupervisionTransitionsPersonLevel",
    model=PrisonToSupervisionTransitions,
    dimension_mappings=[
        DimensionMapping(
            dimension=Dimension.AGE_GROUP,
            operations=DimensionOperation.FILTER,
            columns=[PrisonToSupervisionTransitions.age_group],
        ),
        DimensionMapping(
            dimension=Dimension.GENDER,
            operations=DimensionOperation.FILTER,
            columns=[PrisonToSupervisionTransitions.gender],
        ),
        DimensionMapping(
            dimension=Dimension.RACE,
            operations=DimensionOperation.FILTER,
            columns=[PrisonToSupervisionTransitions.race],
        ),
        DimensionMapping(
            dimension=Dimension.FACILITY,
            operations=DimensionOperation.FILTER,
            columns=[PrisonToSupervisionTransitions.facility],
        ),
        DimensionMapping(
            dimension=Dimension.TIME_PERIOD,
            operations=DimensionOperation.FILTER,
            columns=[PrisonToSupervisionTransitions.time_period],
        ),
    ],
    non_aggregate_columns=[
        PrisonToSupervisionTransitions.full_name,
        PrisonToSupervisionTransitions.state_id,
    ],
    aggregate_columns=[
        PrisonToSupervisionTransitions.age,
        PrisonToSupervisionTransitions.facility,
    ],
)

SupervisionToLibertyTransitionsCount = CountByDimensionMetricQueryBuilder(
    name="SupervisionToLibertyTransitionsCount",
    model=SupervisionToLibertyTransitions,
    dimension_mappings=[
        DimensionMapping(
            dimension=Dimension.YEAR_MONTH,
            operations=DimensionOperation.GROUP,
            columns=[
                SupervisionToLibertyTransitions.year,
                SupervisionToLibertyTransitions.month,
            ],
        ),
        DimensionMapping(
            dimension=Dimension.AGE_GROUP,
            operations=DimensionOperation.ALL,
            columns=[SupervisionToLibertyTransitions.age_group],
        ),
        DimensionMapping(
            dimension=Dimension.GENDER,
            operations=DimensionOperation.ALL,
            columns=[SupervisionToLibertyTransitions.gender],
        ),
        DimensionMapping(
            dimension=Dimension.LENGTH_OF_STAY,
            operations=DimensionOperation.ALL,
            columns=[SupervisionToLibertyTransitions.length_of_stay],
        ),
        DimensionMapping(
            dimension=Dimension.RACE,
            operations=DimensionOperation.ALL,
            columns=[SupervisionToLibertyTransitions.race],
        ),
        DimensionMapping(
            dimension=Dimension.SUPERVISION_TYPE,
            operations=DimensionOperation.ALL,
            columns=[SupervisionToLibertyTransitions.supervision_type],
        ),
        DimensionMapping(
            dimension=Dimension.SUPERVISION_LEVEL,
            operations=DimensionOperation.ALL,
            columns=[SupervisionToLibertyTransitions.supervision_level],
        ),
        DimensionMapping(
            dimension=Dimension.SUPERVISION_DISTRICT,
            operations=DimensionOperation.ALL,
            columns=[SupervisionToLibertyTransitions.supervision_district],
        ),
        # TODO(#13552): Remove this once FE uses supervision_district
        DimensionMapping(
            dimension=Dimension.DISTRICT,
            operations=DimensionOperation.ALL,
            columns=[
                SupervisionToLibertyTransitions.supervision_district.label("district")
            ],
        ),
        DimensionMapping(
            dimension=Dimension.SUPERVISING_OFFICER,
            operations=DimensionOperation.ALL,
            columns=[SupervisionToLibertyTransitions.supervising_officer],
        ),
    ],
)

SupervisionToPrisonTransitionsCount = CountByDimensionMetricQueryBuilder(
    name="SupervisionToPrisonTransitionsCount",
    model=SupervisionToPrisonTransitions,
    dimension_mappings=[
        DimensionMapping(
            dimension=Dimension.YEAR_MONTH,
            operations=DimensionOperation.GROUP,
            columns=[
                SupervisionToPrisonTransitions.year,
                SupervisionToPrisonTransitions.month,
            ],
        ),
        DimensionMapping(
            dimension=Dimension.AGE_GROUP,
            operations=DimensionOperation.ALL,
            columns=[SupervisionToPrisonTransitions.age_group],
        ),
        DimensionMapping(
            dimension=Dimension.GENDER,
            operations=DimensionOperation.ALL,
            columns=[SupervisionToPrisonTransitions.gender],
        ),
        DimensionMapping(
            dimension=Dimension.RACE,
            operations=DimensionOperation.ALL,
            columns=[SupervisionToPrisonTransitions.race],
        ),
        DimensionMapping(
            dimension=Dimension.SUPERVISION_TYPE,
            operations=DimensionOperation.ALL,
            columns=[SupervisionToPrisonTransitions.supervision_type],
        ),
        DimensionMapping(
            dimension=Dimension.SUPERVISION_LEVEL,
            operations=DimensionOperation.ALL,
            columns=[SupervisionToPrisonTransitions.supervision_level],
        ),
        DimensionMapping(
            dimension=Dimension.SUPERVISION_DISTRICT,
            operations=DimensionOperation.ALL,
            columns=[SupervisionToPrisonTransitions.supervision_district],
        ),
        # TODO(#13552): Remove this once FE uses supervision_district
        DimensionMapping(
            dimension=Dimension.DISTRICT,
            operations=DimensionOperation.ALL,
            columns=[
                SupervisionToPrisonTransitions.supervision_district.label("district")
            ],
        ),
        DimensionMapping(
            dimension=Dimension.SUPERVISING_OFFICER,
            operations=DimensionOperation.ALL,
            columns=[SupervisionToPrisonTransitions.supervising_officer],
        ),
        DimensionMapping(
            dimension=Dimension.LENGTH_OF_STAY,
            operations=DimensionOperation.ALL,
            columns=[SupervisionToPrisonTransitions.length_of_stay],
        ),
    ],
)
