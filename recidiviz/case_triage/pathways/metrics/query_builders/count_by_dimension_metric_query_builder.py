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
# =============================================================================
""" Metric query builder for count by dimension metrics"""
from typing import Dict, Type, Union

import attr
from sqlalchemy import Column, distinct, func
from sqlalchemy.orm import Query
from sqlalchemy.sql.ddl import DDLElement

from recidiviz.case_triage.pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.pathways.dimensions.dimension_mapping import (
    DimensionOperation,
)
from recidiviz.case_triage.pathways.metrics.query_builders.metric_query_builder import (
    FetchMetricParams,
    MetricConfigOptionsType,
    MetricQueryBuilder,
)
from recidiviz.persistence.database.schema.pathways.schema import PathwaysBase


@attr.s(auto_attribs=True)
class CountByDimensionMetricParams(FetchMetricParams):
    group: Dimension = attr.attrib(default=Dimension.YEAR_MONTH)

    @property
    def cache_fragment(self) -> str:
        return f"{super().cache_fragment} group={repr(self.group.value)}"


@attr.s(auto_attribs=True)
class CountByDimensionMetricQueryBuilder(MetricQueryBuilder):
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
    def get_params_class(cls) -> Type[CountByDimensionMetricParams]:
        return CountByDimensionMetricParams

    @classmethod
    def adapt_config_options(
        cls, model: PathwaysBase, options: MetricConfigOptionsType
    ) -> Dict[str, DDLElement]:
        adapted_options = {}

        if cls.has_valid_option(options, "counting_column", instance=str):
            counting_column = str(options["counting_column"])
            adapted_options["counting_function"] = func.count(
                distinct(getattr(model, counting_column))
            )
        return adapted_options
