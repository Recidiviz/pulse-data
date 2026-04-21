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
"""Implements tests for Pathways metric cache."""
import json
from unittest import TestCase
from unittest.mock import MagicMock, patch

from fakeredis import FakeRedis

from recidiviz.case_triage.pathways.metrics.metric_query_builders import (
    ALL_PATHWAYS_METRICS_BY_NAME,
)
from recidiviz.case_triage.shared_pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.shared_pathways.dimensions.dimension_mapping import (
    DimensionMapping,
    DimensionMappingCollection,
    DimensionOperation,
)
from recidiviz.case_triage.shared_pathways.dimensions.time_period import TimePeriod
from recidiviz.case_triage.shared_pathways.metric_cache import PathwaysMetricCache
from recidiviz.case_triage.shared_pathways.metric_fetcher import PathwaysMetricFetcher
from recidiviz.case_triage.shared_pathways.query_builders.count_by_dimension_metric_query_builder import (
    CountByDimensionMetricParams,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.pathways.schema import (
    MetricMetadata as PathwaysMetricMetadata,
)
from recidiviz.persistence.database.schema_type import SchemaType


class PathwaysMetricCacheTest(TestCase):
    """Tests for pathways metric cache"""

    def setUp(self) -> None:
        self.redis = FakeRedis()
        self.query_builder = ALL_PATHWAYS_METRICS_BY_NAME[
            "LibertyToPrisonTransitionsCount"
        ]

        self.metric_cache = PathwaysMetricCache(
            state_code=StateCode.US_XX,
            schema_type=SchemaType.PATHWAYS,
            metric_fetcher=PathwaysMetricFetcher(
                state_code=StateCode.US_XX,
                enabled_states=["US_XX"],
                schema_type=SchemaType.PATHWAYS,
                metric_metadata=PathwaysMetricMetadata,
            ),
            redis=self.redis,
        )

    def test_cache_key_for(self) -> None:
        self.assertEqual(
            "US_XX LibertyToPrisonTransitionsCount filters=[('time_period', ['months_0_6', 'months_7_12'])] group='gender'",
            self.metric_cache.cache_key_for(
                self.query_builder,
                self.query_builder.build_params(
                    {
                        "group": Dimension.GENDER,
                        "filters": {
                            Dimension.TIME_PERIOD: TimePeriod.period_range(
                                TimePeriod.MONTHS_7_12.value
                            ),
                        },
                    }
                ),
            ),
        )

        # Filters are sorted deterministically
        self.assertEqual(
            self.metric_cache.cache_key_for(
                self.query_builder,
                self.query_builder.build_params(
                    {
                        "filters": {
                            Dimension.GENDER: ["MALE", "FEMALE"],
                            Dimension.RACE: ["BLACK", "WHITE"],
                        }
                    }
                ),
            ),
            self.metric_cache.cache_key_for(
                self.query_builder,
                self.query_builder.build_params(
                    {
                        "filters": {
                            Dimension.RACE: ["WHITE", "BLACK"],
                            Dimension.GENDER: ["FEMALE", "MALE"],
                        }
                    }
                ),
            ),
        )

    def test_fetch_with_cold_cache(self) -> None:
        """Metric is first fetched from the database, then fetched from redis"""
        cached_value = [{"foo": "bar"}]
        with patch.object(self.metric_cache, "metric_fetcher") as mock_metric_fetcher:
            self.assertEqual(0, len(self.redis.keys()))

            mock_metric_fetcher.fetch.return_value = cached_value
            self.metric_cache.fetch(
                self.query_builder, self.query_builder.build_params({})
            )

            mock_metric_fetcher.fetch.assert_called()
            mock_metric_fetcher.fetch.reset_mock()

            result = self.metric_cache.fetch(
                self.query_builder, self.query_builder.build_params({})
            )

            # Value is grabbed from cache
            mock_metric_fetcher.fetch.assert_not_called()
            self.assertEqual(cached_value, result)
            self.assertEqual(
                1, len(self.redis.keys("US_XX LibertyToPrisonTransitionsCount*"))
            )

    def test_fetch_with_initialized_cache(self) -> None:
        """When the cache is initialized we don't make a database call"""
        mock_metric_fetcher = MagicMock()
        with patch.object(self.metric_cache, "metric_fetcher") as mock_metric_fetcher:
            self.assertEqual(0, len(self.redis.keys()))

            cached_value = [{"foo": "bar"}]
            params = self.query_builder.build_params({})
            cache_key = self.metric_cache.cache_key_for(self.query_builder, params)
            self.redis.set(cache_key, json.dumps(cached_value))

            self.assertEqual(
                cached_value, self.metric_cache.fetch(self.query_builder, params)
            )
            mock_metric_fetcher.fetch.assert_not_called()

    def test_reset_cache_logs(self) -> None:
        with patch.object(self.metric_cache, "metric_fetcher") as mock_metric_fetcher:
            mock_metric_fetcher.fetch.return_value = {}

            mock_mapper = MagicMock(
                build_params=lambda kwargs: CountByDimensionMetricParams(**kwargs),
                cache_fragment="MockMetric",
                dimension_mapping_collection=DimensionMappingCollection(
                    [
                        DimensionMapping(
                            dimension=Dimension.GENDER,
                            operations=DimensionOperation.GROUP,
                        ),
                    ]
                ),
            )
            mock_mapper.configure_mock(name="MockMetric")

            # Pre-populate a key so purge has something to delete
            self.redis.set("US_XX MockMetric some_key", "value")

            with self.assertLogs(level="INFO") as log:
                self.metric_cache.reset_cache(mock_mapper)

            self.assertTrue(
                any(
                    "Resetting cache for PATHWAYS/US_XX/MockMetric" in msg
                    for msg in log.output
                )
            )
            self.assertTrue(
                any(
                    "Purged 1 cache keys for PATHWAYS/US_XX/MockMetric" in msg
                    for msg in log.output
                )
            )
            self.assertTrue(
                any(
                    "Cache warming complete for PATHWAYS/US_XX/MockMetric" in msg
                    for msg in log.output
                )
            )

    def test_initialize_cache(self) -> None:
        mock_metric_fetcher = MagicMock()
        with patch.object(self.metric_cache, "metric_fetcher") as mock_metric_fetcher:
            mock_metric_fetcher.fetch.return_value = {}

            mock_mapper = MagicMock(
                build_params=lambda kwargs: CountByDimensionMetricParams(**kwargs),
                cache_fragment="MockMapper",
                dimension_mapping_collection=DimensionMappingCollection(
                    [
                        DimensionMapping(
                            dimension=Dimension.GENDER,
                            operations=DimensionOperation.GROUP,
                        ),
                        DimensionMapping(
                            dimension=Dimension.TIME_PERIOD,
                            operations=DimensionOperation.FILTER,
                        ),
                    ]
                ),
            )

            self.metric_cache.initialize_cache(mock_mapper)

            self.assertEqual(
                [
                    b"US_XX MockMapper filters=[] group='gender'",
                    b"US_XX MockMapper filters=[('time_period', ['months_0_6'])] group='gender'",
                    b"US_XX MockMapper filters=[('time_period', ['months_0_6', 'months_7_12'])] group='gender'",
                    b"US_XX MockMapper filters=[('time_period', ['months_0_6', 'months_13_24', 'months_7_12'])] group='gender'",
                    b"US_XX MockMapper filters=[('time_period', ['months_0_6', 'months_13_24', 'months_25_60', 'months_7_12'])] group='gender'",
                ],
                self.metric_cache.redis.keys(),
            )
