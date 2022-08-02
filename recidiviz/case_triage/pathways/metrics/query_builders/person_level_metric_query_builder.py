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
""" MetricQueryBuilder for person level metrics """

from typing import Dict, List

import attr
from sqlalchemy import Column, String, cast, func, literal_column
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.orm import Query

from recidiviz.case_triage.pathways.metrics.query_builders.metric_query_builder import (
    FetchMetricParams,
    MetricConfigOptionsType,
    MetricQueryBuilder,
)
from recidiviz.persistence.database.schema.pathways.schema import PathwaysBase


@attr.s(auto_attribs=True)
class PersonLevelMetricQueryBuilder(MetricQueryBuilder):
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
    def adapt_config_options(
        cls,
        model: PathwaysBase,
        options: MetricConfigOptionsType,
    ) -> Dict[str, List[Column]]:
        adapted_options = {}
        if cls.has_valid_option(options, "non_aggregate_columns", instance=list):
            adapted_options["non_aggregate_columns"] = [
                getattr(model, column) for column in options["non_aggregate_columns"]
            ]

        if cls.has_valid_option(options, "aggregate_columns", instance=list):
            adapted_options["aggregate_columns"] = [
                getattr(model, column) for column in options["aggregate_columns"]
            ]

        return adapted_options
