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
from typing import Dict, Generic, List, Tuple, TypeVar, Union

import attr
from sqlalchemy import Column, String, cast, distinct, func, literal_column
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.orm import Query
from sqlalchemy.sql.ddl import DDLElement

from recidiviz.case_triage.pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.pathways.dimensions.dimension_mapping import (
    DimensionMapping,
    DimensionMappingCollection,
    DimensionOperation,
)
from recidiviz.persistence.database.schema.pathways.schema import (
    LibertyToPrisonTransitions,
    PathwaysBase,
    PrisonPopulationByDimension,
    PrisonPopulationOverTime,
    PrisonPopulationPersonLevel,
    PrisonPopulationProjection,
    PrisonToSupervisionTransitions,
    SupervisionPopulationByDimension,
    SupervisionPopulationOverTime,
    SupervisionPopulationProjection,
    SupervisionToLibertyTransitions,
    SupervisionToPrisonTransitions,
)


@attr.s(auto_attribs=True)
class FetchMetricParams:
    filters: Dict[Dimension, Union[str, List[str]]] = attr.field(factory=dict)

    @property
    def sorted_filters(self) -> List[Tuple[str, List[str]]]:
        return sorted(
            (dimension.value, sorted(values))
            for dimension, values in self.filters.items()
        )

    @property
    def cache_fragment(self) -> str:
        return f"filters={repr(self.sorted_filters)}"


@attr.s(auto_attribs=True)
class CountByDimensionMetricParams(FetchMetricParams):
    group: Dimension = attr.attrib(default=Dimension.YEAR_MONTH)

    @property
    def cache_fragment(self) -> str:
        return f"{super().cache_fragment} group={repr(self.group.value)}"


ParamsType = TypeVar("ParamsType", bound=FetchMetricParams)


@attr.s(auto_attribs=True)
class MetricQueryBuilder(Generic[ParamsType]):
    """Builds Postgres queries for Pathways metrics"""

    name: str
    model: PathwaysBase
    dimension_mappings: List[DimensionMapping]

    def __attrs_post_init__(self) -> None:
        self.dimension_mapping_collection = DimensionMappingCollection(
            self.dimension_mappings
        )

    def build_filter_conditions(self, params: ParamsType) -> List[str]:
        conditions = [
            self.dimension_mapping_collection.columns_for_dimension_operation(
                DimensionOperation.FILTER,
                dimension,
            ).in_(value)
            for dimension, value in params.filters.items()
        ]

        return conditions

    @property
    def cache_fragment(self) -> str:
        return self.name

    @abc.abstractmethod
    def build_query(self, params: ParamsType) -> Query:
        ...

    @classmethod
    @abc.abstractmethod
    def build_params(cls, schema: Dict) -> ParamsType:
        ...


@attr.s(auto_attribs=True)
class CountByDimensionMetricQueryBuilder(
    MetricQueryBuilder[CountByDimensionMetricParams]
):
    """Builder for Pathways postgres queries that return the count of entries matching a filter and grouped by a
    dimension."""

    counting_function: Union[DDLElement, Column] = attr.field(default=func.count())

    def build_query(self, params: CountByDimensionMetricParams) -> Query:
        grouped_columns = list(
            self.dimension_mapping_collection.columns_for_dimension_operation(
                DimensionOperation.GROUP,
                params.group,
            )
        )

        return (
            Query([*grouped_columns, self.counting_function])
            .filter(*self.build_filter_conditions(params))
            .group_by(*grouped_columns)
            .order_by(*grouped_columns)
        )

    @classmethod
    def build_params(cls, schema: Dict) -> CountByDimensionMetricParams:
        return CountByDimensionMetricParams(**schema)


@attr.s(auto_attribs=True)
class PopulationProjectionMetricQueryBuilder(MetricQueryBuilder[FetchMetricParams]):
    """Builder for Pathways postgres queries that return population projection data matching a filter"""

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()

        required_attributes = [
            "year",
            "month",
            "simulation_tag",
            "gender",
            "admission_reason",
            "total_population",
            "total_population_min",
            "total_population_max",
        ]

        self.base_columns = [
            getattr(self.model, attribute)
            for attribute in required_attributes
            if hasattr(self.model, attribute)
        ]

        if len(self.base_columns) != len(required_attributes):
            raise ValueError(
                "ProjectionCountMetricQueryBuilder model must have required attributes"
            )

    def build_query(self, params: FetchMetricParams) -> Query:
        return (
            Query([*self.base_columns])
            .filter(*self.build_filter_conditions(params))
            .order_by(*self.base_columns)
        )

    @classmethod
    def build_params(cls, schema: Dict) -> FetchMetricParams:
        return FetchMetricParams(**schema)


@attr.s(auto_attribs=True)
class PersonLevelMetricQueryBuilder(MetricQueryBuilder[FetchMetricParams]):
    """Builder for Pathways postgres queries that return individual rows, potentially filtered by a condition."""

    non_aggregate_columns: List[Column]
    aggregate_columns: List[Column]

    def build_query(self, params: FetchMetricParams) -> Query:
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
            .filter(*self.build_filter_conditions(params))
            .group_by(*grouped_columns)
            .order_by(*grouped_columns)
        )

    @classmethod
    def build_params(cls, schema: Dict) -> FetchMetricParams:
        return FetchMetricParams(**schema)


LibertyToPrisonTransitionsCount = CountByDimensionMetricQueryBuilder(
    name="LibertyToPrisonTransitionsCount",
    model=LibertyToPrisonTransitions,
    dimension_mappings=[
        DimensionMapping(
            dimension=Dimension.YEAR_MONTH,
            operations=DimensionOperation.GROUP,
            columns=[
                LibertyToPrisonTransitions.year,
                LibertyToPrisonTransitions.month,
            ],
        ),
        DimensionMapping(
            dimension=Dimension.TIME_PERIOD,
            operations=DimensionOperation.FILTER,
            columns=[LibertyToPrisonTransitions.time_period],
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


PrisonPopulationOverTimeCount = CountByDimensionMetricQueryBuilder(
    name="PrisonPopulationOverTimeCount",
    model=PrisonPopulationOverTime,
    counting_function=func.count(distinct(PrisonPopulationOverTime.person_id)),
    dimension_mappings=[
        DimensionMapping(
            dimension=Dimension.YEAR_MONTH,
            operations=DimensionOperation.GROUP,
            columns=[
                PrisonPopulationOverTime.year,
                PrisonPopulationOverTime.month,
            ],
        ),
        DimensionMapping(
            dimension=Dimension.AGE_GROUP,
            operations=DimensionOperation.FILTER,
            columns=[PrisonPopulationOverTime.age_group],
        ),
        DimensionMapping(
            dimension=Dimension.FACILITY,
            operations=DimensionOperation.FILTER,
            columns=[PrisonPopulationOverTime.facility],
        ),
        DimensionMapping(
            dimension=Dimension.GENDER,
            operations=DimensionOperation.FILTER,
            columns=[PrisonPopulationOverTime.gender],
        ),
        DimensionMapping(
            dimension=Dimension.ADMISSION_REASON,
            operations=DimensionOperation.FILTER,
            columns=[PrisonPopulationOverTime.admission_reason],
        ),
        DimensionMapping(
            dimension=Dimension.RACE,
            operations=DimensionOperation.FILTER,
            columns=[PrisonPopulationOverTime.race],
        ),
        DimensionMapping(
            dimension=Dimension.TIME_PERIOD,
            operations=DimensionOperation.FILTER,
            columns=[PrisonPopulationOverTime.time_period],
        ),
    ],
)

PrisonPopulationByDimensionCount = CountByDimensionMetricQueryBuilder(
    name="PrisonPopulationByDimensionCount",
    model=SupervisionPopulationByDimension,
    counting_function=func.count(distinct(PrisonPopulationByDimension.person_id)),
    dimension_mappings=[
        DimensionMapping(
            dimension=Dimension.AGE_GROUP,
            operations=DimensionOperation.ALL,
            columns=[PrisonPopulationByDimension.age_group],
        ),
        DimensionMapping(
            dimension=Dimension.FACILITY,
            operations=DimensionOperation.ALL,
            columns=[PrisonPopulationByDimension.facility],
        ),
        DimensionMapping(
            dimension=Dimension.GENDER,
            operations=DimensionOperation.ALL,
            columns=[PrisonPopulationByDimension.gender],
        ),
        DimensionMapping(
            dimension=Dimension.ADMISSION_REASON,
            operations=DimensionOperation.ALL,
            columns=[PrisonPopulationByDimension.admission_reason],
        ),
        DimensionMapping(
            dimension=Dimension.RACE,
            operations=DimensionOperation.ALL,
            columns=[PrisonPopulationByDimension.race],
        ),
        DimensionMapping(
            dimension=Dimension.LENGTH_OF_STAY,
            operations=DimensionOperation.ALL,
            columns=[PrisonPopulationByDimension.length_of_stay],
        ),
    ],
)

PrisonPopulationPersonLevelMetric = PersonLevelMetricQueryBuilder(
    name="PrisonPopulationPersonLevel",
    model=PrisonPopulationPersonLevel,
    dimension_mappings=[
        DimensionMapping(
            dimension=Dimension.AGE_GROUP,
            operations=DimensionOperation.FILTER,
            columns=[PrisonPopulationPersonLevel.age_group],
        ),
        DimensionMapping(
            dimension=Dimension.GENDER,
            operations=DimensionOperation.FILTER,
            columns=[PrisonPopulationPersonLevel.gender],
        ),
        DimensionMapping(
            dimension=Dimension.ADMISSION_REASON,
            operations=DimensionOperation.FILTER,
            columns=[PrisonPopulationPersonLevel.admission_reason],
        ),
        DimensionMapping(
            dimension=Dimension.RACE,
            operations=DimensionOperation.FILTER,
            columns=[PrisonPopulationPersonLevel.race],
        ),
        DimensionMapping(
            dimension=Dimension.FACILITY,
            operations=DimensionOperation.FILTER,
            columns=[PrisonPopulationPersonLevel.facility],
        ),
    ],
    non_aggregate_columns=[
        PrisonPopulationPersonLevel.full_name,
        PrisonPopulationPersonLevel.state_id,
    ],
    aggregate_columns=[
        PrisonPopulationPersonLevel.age,
        PrisonPopulationPersonLevel.facility,
    ],
)


PrisonPopulationProjectionMetric = PopulationProjectionMetricQueryBuilder(
    name="PrisonPopulationProjection",
    model=PrisonPopulationProjection,
    dimension_mappings=[
        DimensionMapping(
            dimension=Dimension.GENDER,
            operations=DimensionOperation.FILTER,
            columns=[PrisonPopulationProjection.gender],
        ),
        DimensionMapping(
            dimension=Dimension.ADMISSION_REASON,
            operations=DimensionOperation.FILTER,
            columns=[PrisonPopulationProjection.admission_reason],
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
            dimension=Dimension.TIME_PERIOD,
            operations=DimensionOperation.FILTER,
            columns=[PrisonToSupervisionTransitions.time_period],
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
            dimension=Dimension.TIME_PERIOD,
            operations=DimensionOperation.FILTER,
            columns=[PrisonToSupervisionTransitions.time_period],
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

SupervisionPopulationOverTimeCount = CountByDimensionMetricQueryBuilder(
    name="SupervisionPopulationOverTimeCount",
    model=SupervisionPopulationOverTime,
    counting_function=func.count(distinct(SupervisionPopulationOverTime.person_id)),
    dimension_mappings=[
        DimensionMapping(
            dimension=Dimension.YEAR_MONTH,
            operations=DimensionOperation.GROUP,
            columns=[
                SupervisionPopulationOverTime.year,
                SupervisionPopulationOverTime.month,
            ],
        ),
        DimensionMapping(
            dimension=Dimension.TIME_PERIOD,
            operations=DimensionOperation.FILTER,
            columns=[SupervisionPopulationOverTime.time_period],
        ),
        # TODO(#13552): Remove this once FE uses supervision_district
        DimensionMapping(
            dimension=Dimension.DISTRICT,
            operations=DimensionOperation.FILTER,
            columns=[
                SupervisionPopulationOverTime.supervision_district.label("district")
            ],
        ),
        DimensionMapping(
            dimension=Dimension.SUPERVISION_DISTRICT,
            operations=DimensionOperation.FILTER,
            columns=[SupervisionPopulationOverTime.supervision_district],
        ),
        DimensionMapping(
            dimension=Dimension.SUPERVISION_LEVEL,
            operations=DimensionOperation.FILTER,
            columns=[SupervisionPopulationOverTime.supervision_level],
        ),
        DimensionMapping(
            dimension=Dimension.RACE,
            operations=DimensionOperation.FILTER,
            columns=[SupervisionPopulationOverTime.race],
        ),
        DimensionMapping(
            dimension=Dimension.TIME_PERIOD,
            operations=DimensionOperation.FILTER,
            columns=[SupervisionPopulationOverTime.time_period],
        ),
    ],
)

SupervisionPopulationByDimensionCount = CountByDimensionMetricQueryBuilder(
    name="SupervisionPopulationByDimensionCount",
    model=SupervisionPopulationByDimension,
    counting_function=func.count(distinct(SupervisionPopulationByDimension.person_id)),
    dimension_mappings=[
        # TODO(#13552): Remove this once FE uses supervision_district
        DimensionMapping(
            dimension=Dimension.DISTRICT,
            operations=DimensionOperation.ALL,
            columns=[
                SupervisionPopulationByDimension.supervision_district.label("district")
            ],
        ),
        DimensionMapping(
            dimension=Dimension.SUPERVISION_DISTRICT,
            operations=DimensionOperation.ALL,
            columns=[SupervisionPopulationByDimension.supervision_district],
        ),
        DimensionMapping(
            dimension=Dimension.SUPERVISION_LEVEL,
            operations=DimensionOperation.ALL,
            columns=[SupervisionPopulationByDimension.supervision_level],
        ),
        DimensionMapping(
            dimension=Dimension.RACE,
            operations=DimensionOperation.ALL,
            columns=[SupervisionPopulationByDimension.race],
        ),
    ],
)

SupervisionPopulationProjectionMetric = PopulationProjectionMetricQueryBuilder(
    name="SupervisionPopulationProjection",
    model=SupervisionPopulationProjection,
    dimension_mappings=[
        DimensionMapping(
            dimension=Dimension.GENDER,
            operations=DimensionOperation.FILTER,
            columns=[SupervisionPopulationProjection.gender],
        ),
        DimensionMapping(
            dimension=Dimension.ADMISSION_REASON,
            operations=DimensionOperation.FILTER,
            columns=[SupervisionPopulationProjection.admission_reason],
        ),
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
            dimension=Dimension.TIME_PERIOD,
            operations=DimensionOperation.FILTER,
            columns=[SupervisionToLibertyTransitions.time_period],
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
            dimension=Dimension.TIME_PERIOD,
            operations=DimensionOperation.FILTER,
            columns=[SupervisionToPrisonTransitions.time_period],
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
