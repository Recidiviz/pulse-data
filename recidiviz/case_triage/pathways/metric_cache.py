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
""" Interface for fetching metrics from Pathways Cloud Memorystore, falling back to Cloud SQL """
from typing import List, Mapping, Union

import attr
from redis import Redis

from recidiviz.case_triage.pathways.metric_fetcher import PathwaysMetricFetcher
from recidiviz.case_triage.pathways.metric_queries import (
    DimensionOperation,
    FetchMetricParams,
    MetricQueryBuilder,
    TimePeriod,
)
from recidiviz.case_triage.util import get_pathways_metric_redis
from recidiviz.cloud_memorystore import utils as cloud_memorystore_utils
from recidiviz.common.constants.states import _FakeStateCode


@attr.s(auto_attribs=True)
class PathwaysMetricCache:
    """Contains functionality for fetching metrics from cache"""

    # TODO(#13950): Replace with StateCode
    state_code: _FakeStateCode
    metric_fetcher: PathwaysMetricFetcher
    redis: Redis

    def fetch(
        self, mapper: MetricQueryBuilder, params: FetchMetricParams
    ) -> List[Mapping[str, Union[str, int]]]:
        return cloud_memorystore_utils.get_or_set_json(
            self.redis,
            self.cache_key_for(mapper, params),
            lambda: self.metric_fetcher.fetch(mapper, params),
        )

    def cache_key_for(
        self, mapper: MetricQueryBuilder, params: FetchMetricParams
    ) -> str:
        return (
            f"{self.state_code.value} {mapper.cache_fragment} {params.cache_fragment}"
        )

    def purge_cache_for_mapper(self, mapper: MetricQueryBuilder) -> None:
        cache_key_pattern = f"{self.state_code.value} {mapper.cache_fragment}*"
        pipe = self.redis.pipeline()

        for key in self.redis.scan_iter(cache_key_pattern):
            pipe.delete(key)

        pipe.execute()

    def reset_cache(self, mapper: MetricQueryBuilder) -> None:
        self.purge_cache_for_mapper(mapper)
        self.initialize_cache(mapper)

    def initialize_cache(self, mapper: MetricQueryBuilder) -> None:
        for time_period in TimePeriod:
            params = mapper.build_params({"time_period": time_period})

            self.fetch(mapper=mapper, params=params)

            for dimension_mapping in mapper.dimension_mappings:
                if DimensionOperation.GROUP in dimension_mapping.operations:
                    self.fetch(
                        mapper=mapper,
                        params=attr.evolve(params, group=dimension_mapping.dimension),
                    )

    @classmethod
    def build(cls, state_code: _FakeStateCode) -> "PathwaysMetricCache":
        return PathwaysMetricCache(
            state_code=state_code,
            metric_fetcher=PathwaysMetricFetcher(state_code=state_code),
            redis=get_pathways_metric_redis(),
        )
