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
"""This class implements tests for Pathways population projection metrics."""
import abc
from typing import Dict, List, Union
from unittest import TestCase

from recidiviz.case_triage.pathways.metric_fetcher import PathwaysMetricFetcher
from recidiviz.case_triage.pathways.metrics.metric_query_builders import (
    ALL_METRICS_BY_NAME,
)
from recidiviz.case_triage.pathways.metrics.query_builders.metric_query_builder import (
    FetchMetricParams,
    MetricQueryBuilder,
)
from recidiviz.common.constants.states import _FakeStateCode
from recidiviz.tests.case_triage.pathways.metrics.base_metrics_test import (
    PathwaysMetricTestBase,
)


class PathwaysPopulationProjectionMetricTestBase(PathwaysMetricTestBase):
    @property
    @abc.abstractmethod
    def all_expected_rows(self) -> List[Dict[str, Union[str, int, float]]]:
        ...

    def test_metrics_base(self) -> None:
        metric_fetcher = PathwaysMetricFetcher(_FakeStateCode.US_TN)
        results = metric_fetcher.fetch(self.query_builder, FetchMetricParams())

        self.test.assertEqual(
            self.all_expected_rows,
            results,
        )


class TestPrisonPopulationProjectionMetric(
    PathwaysPopulationProjectionMetricTestBase, TestCase
):
    """Test for PrisonPopulationProjection metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return ALL_METRICS_BY_NAME["PrisonPopulationProjection"]

    @property
    def all_expected_rows(
        self,
    ) -> List[Dict[str, Union[str, int, float]]]:
        return [
            {
                "gender": "FEMALE",
                "admission_reason": "NEW_ADMISSION",
                "month": 1,
                "simulation_tag": "HISTORICAL",
                "total_population": 1,
                "total_population_max": 1.25,
                "total_population_min": 0.25,
                "year": 2022,
            },
            {
                "gender": "MALE",
                "admission_reason": "NEW_ADMISSION",
                "month": 1,
                "simulation_tag": "HISTORICAL",
                "total_population": 1,
                "total_population_max": 1.5,
                "total_population_min": 0.5,
                "year": 2022,
            },
            {
                "gender": "FEMALE",
                "admission_reason": "NEW_ADMISSION",
                "month": 2,
                "simulation_tag": "BASELINE",
                "total_population": 1,
                "total_population_max": 1.25,
                "total_population_min": 0.25,
                "year": 2022,
            },
            {
                "gender": "MALE",
                "admission_reason": "NEW_ADMISSION",
                "month": 2,
                "simulation_tag": "BASELINE",
                "total_population": 2,
                "total_population_max": 1.5,
                "total_population_min": 0.5,
                "year": 2022,
            },
            {
                "gender": "FEMALE",
                "admission_reason": "NEW_ADMISSION",
                "month": 3,
                "simulation_tag": "BASELINE",
                "total_population": 1,
                "total_population_max": 1.25,
                "total_population_min": 0.25,
                "year": 2022,
            },
            {
                "gender": "MALE",
                "admission_reason": "NEW_ADMISSION",
                "month": 3,
                "simulation_tag": "BASELINE",
                "total_population": 3,
                "total_population_max": 1.5,
                "total_population_min": 0.5,
                "year": 2022,
            },
        ]


class TestSupervisionPopulationProjectionMetric(
    PathwaysPopulationProjectionMetricTestBase, TestCase
):
    """Test for SupervisionPopulationProjection metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return ALL_METRICS_BY_NAME["SupervisionPopulationProjection"]

    @property
    def all_expected_rows(
        self,
    ) -> List[Dict[str, Union[str, int, float]]]:
        return [
            {
                "gender": "FEMALE",
                "admission_reason": "NEW_ADMISSION",
                "month": 1,
                "simulation_tag": "HISTORICAL",
                "total_population": 1,
                "total_population_max": 1.25,
                "total_population_min": 0.25,
                "year": 2022,
            },
            {
                "gender": "MALE",
                "admission_reason": "NEW_ADMISSION",
                "month": 1,
                "simulation_tag": "HISTORICAL",
                "total_population": 1,
                "total_population_max": 1.5,
                "total_population_min": 0.5,
                "year": 2022,
            },
            {
                "gender": "FEMALE",
                "admission_reason": "NEW_ADMISSION",
                "month": 2,
                "simulation_tag": "BASELINE",
                "total_population": 1,
                "total_population_max": 1.25,
                "total_population_min": 0.25,
                "year": 2022,
            },
            {
                "gender": "MALE",
                "admission_reason": "NEW_ADMISSION",
                "month": 2,
                "simulation_tag": "BASELINE",
                "total_population": 2,
                "total_population_max": 1.5,
                "total_population_min": 0.5,
                "year": 2022,
            },
        ]
