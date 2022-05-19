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
""" Implements Pathways metrics """
import enum
from http import HTTPStatus
from typing import Dict, List, Mapping, Optional, Type, Union

import attr
from sqlalchemy import Column, func

from recidiviz.case_triage.pathways.pathways_database_manager import (
    PathwaysDatabaseManager,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.pathways.schema import (
    LibertyToPrisonTransitions,
    PathwaysBase,
)
from recidiviz.utils.flask_exception import FlaskException


class Dimension(enum.Enum):
    YEAR_MONTH = "year_month"
    TIME_PERIOD = "time_period"
    GENDER = "gender"
    AGE_GROUP = "age_group"
    RACE = "race"
    JUDICIAL_DISTRICT = "judicial_district"
    PRIOR_LENGTH_OF_INCARCERATION = "prior_length_of_incarceration"


COMPOSITE_DIMENSIONS = [Dimension.YEAR_MONTH]


@attr.s(auto_attribs=True)
class FetchMetricParams:
    group: Dimension
    since: Optional[str] = attr.attrib(default=None)
    filters: Dict[Dimension, Union[str, List[str]]] = attr.attrib(factory=dict)


class MetricNotEnabledException(FlaskException):
    pass


@attr.s(auto_attribs=True)
class MetricFactory:
    state_code: StateCode

    def get_metric_class(self, metric_name: str) -> Type["PathwaysMetric"]:
        try:
            return ENABLED_METRICS_BY_STATE[self.state_code][metric_name]
        except KeyError as e:
            raise MetricNotEnabledException(
                code="metric_not_enabled",
                description=f"{metric_name} is not enabled for {self.state_code.value}",
                status_code=HTTPStatus.BAD_REQUEST,
            ) from e

    def build_metric(self, metric_name: str) -> "PathwaysMetric":
        metric_class = self.get_metric_class(metric_name)
        return metric_class(state_code=self.state_code)


class PathwaysMetric:
    model: PathwaysBase
    timestamp_column: Column
    dimensions: List[Dimension]

    def __init__(self, state_code: StateCode) -> None:
        self.database_manager = PathwaysDatabaseManager()
        self.database_session = self.database_manager.get_pathways_session(state_code)
        self.state_code = state_code

    def column_for_dimension(self, dimension: Dimension) -> Column:
        return getattr(self.model, dimension.value)

    def grouping_for_dimension(self, dimension: Dimension) -> List[Column]:
        if dimension == Dimension.YEAR_MONTH:
            return [self.model.year, self.model.month]

        return [self.column_for_dimension(dimension)]

    def fetch(self, params: FetchMetricParams) -> List[Mapping[str, Union[str, int]]]:
        raise NotImplementedError()


class CountByDimensionMetric(PathwaysMetric):
    def fetch(self, params: FetchMetricParams) -> List[Mapping[str, Union[str, int]]]:
        conditions = [
            self.column_for_dimension(dimension).in_(value)
            for dimension, value in params.filters.items()
        ]

        grouped_columns = self.grouping_for_dimension(params.group)

        with self.database_session() as session:
            query = (
                session.query(
                    *grouped_columns,
                    func.count(self.model.transition_date),
                )
                .filter(*conditions)
                .group_by(*grouped_columns)
                .order_by(*grouped_columns)
            )

            if params.since:
                query = query.filter(
                    getattr(self.model, self.timestamp_column) >= params.since
                )

            results = query.all()

        return [
            {
                column: result[index]
                for index, column in enumerate(query.statement.columns.keys())
            }
            for result in results
        ]


class LibertyToPrisonTransitionsCount(CountByDimensionMetric):
    model = LibertyToPrisonTransitions

    timestamp_column = "transition_date"

    dimensions = [
        Dimension.YEAR_MONTH,
        Dimension.JUDICIAL_DISTRICT,
        Dimension.PRIOR_LENGTH_OF_INCARCERATION,
        Dimension.GENDER,
        Dimension.AGE_GROUP,
        Dimension.RACE,
    ]


ENABLED_METRICS_BY_STATE = {
    StateCode.US_TN: {
        "LibertyToPrisonTransitionsCount": LibertyToPrisonTransitionsCount,
    }
}
