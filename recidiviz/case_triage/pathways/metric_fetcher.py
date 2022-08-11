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
""" Interface for fetching metrics from Pathways CloudSQL """
from functools import cached_property
from typing import List, Mapping, Union

import attr
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from recidiviz.case_triage.pathways.metrics.query_builders.metric_query_builder import (
    FetchMetricParams,
    MetricQueryBuilder,
)
from recidiviz.case_triage.pathways.pathways_database_manager import (
    PathwaysDatabaseManager,
)
from recidiviz.common.constants.states import _FakeStateCode
from recidiviz.common.str_field_utils import snake_to_camel


@attr.s(auto_attribs=True)
class PathwaysMetricFetcher:
    """Interface for fetching metrics from Cloud SQL"""

    # TODO(#13950): Replace with StateCode
    state_code: _FakeStateCode
    database_manager: PathwaysDatabaseManager = attr.ib(factory=PathwaysDatabaseManager)

    @cached_property
    def database_session(self) -> sessionmaker:
        return self.database_manager.get_pathways_session(self.state_code)

    def fetch(
        self, mapper: MetricQueryBuilder, params: FetchMetricParams
    ) -> Mapping[
        str, Union[List[Mapping[str, Union[str, int]]], Mapping[str, Union[str, int]]]
    ]:
        with self.database_session() as session:
            data_query = mapper.build_query(params).with_session(session)
            data: List[Mapping[str, Union[str, int]]] = [
                {
                    snake_to_camel(column): result[index]
                    for index, column in enumerate(
                        data_query.statement.selected_columns.keys()
                    )
                }
                for result in data_query.all()
            ]

            try:
                metadata_query = mapper.build_metadata_query().with_session(session)
                metadata = metadata_query.one().to_json()
                # We don't need to return the value of metric
                metadata.pop("metric", None)
            except NoResultFound:
                metadata = {}

            return {
                "data": data,
                "metadata": metadata,
            }
