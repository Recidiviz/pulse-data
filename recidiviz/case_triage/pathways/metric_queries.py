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
""" Contains query builders for the various types of metrics we serve """
from typing import Dict, List, Optional, Union

import attr
from sqlalchemy import func
from sqlalchemy.orm import Query

from recidiviz.case_triage.pathways.dimension import Dimension
from recidiviz.case_triage.pathways.metric_mappers import CountByDimensionMetricMapper


@attr.s(auto_attribs=True)
class FetchMetricParams:
    group: Dimension
    since: Optional[str] = attr.attrib(default=None)
    filters: Dict[Dimension, Union[str, List[str]]] = attr.attrib(factory=dict)


@attr.s(auto_attribs=True)
class PathwaysCountByDimensionQueryBuilder:
    mapper: CountByDimensionMetricMapper

    def build(self, params: FetchMetricParams) -> Query:
        grouped_columns = self.mapper.columns_for_dimension_grouping(params.group)
        conditions = [
            self.mapper.column_for_dimension_filter(dimension).in_(value)
            for dimension, value in params.filters.items()
        ]

        if params.since:
            conditions.append(self.mapper.timestamp_column >= params.since)

        return (
            Query([*grouped_columns, func.count(self.mapper.timestamp_column)])
            .filter(*conditions)
            .group_by(*grouped_columns)
            .order_by(*grouped_columns)
        )
