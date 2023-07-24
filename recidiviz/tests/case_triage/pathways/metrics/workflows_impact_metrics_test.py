# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""This class implements tests for Workflows impact metrics."""

import abc
from typing import Any, Dict, List, Union
from unittest import TestCase

from sqlalchemy import Date

from recidiviz.case_triage.pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.pathways.metric_fetcher import PathwaysMetricFetcher
from recidiviz.case_triage.pathways.metrics.metric_query_builders import (
    ALL_METRICS_BY_NAME,
)
from recidiviz.case_triage.pathways.metrics.query_builders.metric_query_builder import (
    FetchMetricParams,
    MetricQueryBuilder,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.tests.case_triage.pathways.metrics.base_metrics_test import (
    PathwaysMetricTestBase,
)


class WorkflowsImpactMetricTestBase(PathwaysMetricTestBase):
    @property
    @abc.abstractmethod
    def all_expected_rows(self) -> List[Dict[str, Union[str, int, float, Date]]]:
        ...

    @property
    def expected_metadata(self) -> Dict[str, Any]:
        return {}

    def test_metrics_base(self) -> None:
        metric_fetcher = PathwaysMetricFetcher(StateCode.US_TN)
        results = metric_fetcher.fetch(self.query_builder, FetchMetricParams())
        self.test.assertEqual(
            {"data": self.all_expected_rows, "metadata": self.expected_metadata},
            results,
        )


class TestUsTnCompliantReportingWorkflowsImpactMetric(
    WorkflowsImpactMetricTestBase, TestCase
):
    """Test for WorkflowsImpact metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return ALL_METRICS_BY_NAME["UsTnCompliantReportingWorkflowsImpact"]

    @property
    def all_expected_rows(
        self,
    ) -> List[Dict[str, Union[str, int, float, Date]]]:
        return [
            {
                "stateCode": "US_TN",
                "supervisionDistrict": "10",
                "districtName": "Test 10",
                "variantId": "WORKFLOWS_LAUNCH",
                "variantDate": "2022-04-11",
                "startDate": "2022-04-01",
                "endDate": "2022-05-01",
                "monthsSinceTreatment": 0,
                "avgDailyPopulation": 3927.9999999999995,
                "avgPopulationLimitedSupervisionLevel": 106.69999999999999,
            },
            {
                "stateCode": "US_TN",
                "supervisionDistrict": "20",
                "districtName": "Test 20",
                "variantId": "WORKFLOWS_LAUNCH",
                "variantDate": "2022-04-11",
                "startDate": "2022-04-01",
                "endDate": "2022-05-01",
                "monthsSinceTreatment": 0,
                "avgDailyPopulation": 2512.400000000001,
                "avgPopulationLimitedSupervisionLevel": 131.86666666666667,
            },
            {
                "stateCode": "US_TN",
                "supervisionDistrict": "30",
                "districtName": "Test 30",
                "variantId": "WORKFLOWS_LAUNCH",
                "variantDate": "2022-04-11",
                "startDate": "2022-04-01",
                "endDate": "2022-05-01",
                "monthsSinceTreatment": 0,
                "avgDailyPopulation": 2837.8666666666659,
                "avgPopulationLimitedSupervisionLevel": 100.26666666666667,
            },
            {
                "stateCode": "US_TN",
                "supervisionDistrict": "40",
                "districtName": "Test 40",
                "variantId": "WORKFLOWS_LAUNCH",
                "variantDate": "2022-04-11",
                "startDate": "2022-04-01",
                "endDate": "2022-05-01",
                "monthsSinceTreatment": 0,
                "avgDailyPopulation": 2408.7,
                "avgPopulationLimitedSupervisionLevel": 133.1,
            },
            {
                "stateCode": "US_TN",
                "supervisionDistrict": "50",
                "districtName": "Test 50",
                "variantId": "WORKFLOWS_LAUNCH",
                "variantDate": "2022-04-11",
                "startDate": "2022-04-01",
                "endDate": "2022-05-01",
                "monthsSinceTreatment": 0,
                "avgDailyPopulation": 2302.5333333333333,
                "avgPopulationLimitedSupervisionLevel": 216.86666666666667,
            },
            {
                "stateCode": "US_TN",
                "supervisionDistrict": "60",
                "districtName": "Test 60",
                "variantId": "WORKFLOWS_LAUNCH",
                "variantDate": "2022-04-11",
                "startDate": "2022-04-01",
                "endDate": "2022-05-01",
                "monthsSinceTreatment": 0,
                "avgDailyPopulation": 4027.9333333333329,
                "avgPopulationLimitedSupervisionLevel": 175.13333333333333,
            },
            {
                "stateCode": "US_TN",
                "supervisionDistrict": "70",
                "districtName": "Test 70",
                "variantId": "WORKFLOWS_LAUNCH",
                "variantDate": "2022-04-11",
                "startDate": "2022-04-01",
                "endDate": "2022-05-01",
                "monthsSinceTreatment": 0,
                "avgDailyPopulation": 3779.333333333333,
                "avgPopulationLimitedSupervisionLevel": 59.933333333333337,
            },
            {
                "stateCode": "US_TN",
                "supervisionDistrict": "80",
                "districtName": "Test 80",
                "variantId": "WORKFLOWS_LAUNCH",
                "variantDate": "2022-04-11",
                "startDate": "2022-04-01",
                "endDate": "2022-05-01",
                "monthsSinceTreatment": 0,
                "avgDailyPopulation": 5434.1666666666624,
                "avgPopulationLimitedSupervisionLevel": 234.0,
            },
            {
                "stateCode": "US_TN",
                "supervisionDistrict": "90",
                "districtName": "Test 90",
                "variantId": "WORKFLOWS_LAUNCH",
                "variantDate": "2022-04-11",
                "startDate": "2022-04-01",
                "endDate": "2022-05-01",
                "monthsSinceTreatment": 0,
                "avgDailyPopulation": 4957.666666666667,
                "avgPopulationLimitedSupervisionLevel": 225.16666666666669,
            },
            {
                "stateCode": "US_TN",
                "supervisionDistrict": "NOT_APPLICABLE",
                "districtName": "NOT_APPLICABLE",
                "variantId": "WORKFLOWS_LAUNCH",
                "variantDate": "2022-04-11",
                "startDate": "2022-04-01",
                "endDate": "2022-05-01",
                "monthsSinceTreatment": 0,
                "avgDailyPopulation": 4647.3666666666686,
                "avgPopulationLimitedSupervisionLevel": 3.4666666666666668,
            },
        ]

    def test_supervision_district_filter(self) -> None:
        """Tests that the supervision district filter works as expected"""
        results = PathwaysMetricFetcher(state_code=StateCode.US_TN).fetch(
            self.query_builder,
            FetchMetricParams(
                filters={
                    Dimension.SUPERVISION_DISTRICT: ["10", "20", "30", "40", "50"],
                },
            ),
        )
        self.test.maxDiff = None
        self.test.assertCountEqual(
            [
                {
                    "stateCode": "US_TN",
                    "supervisionDistrict": "10",
                    "districtName": "Test 10",
                    "variantId": "WORKFLOWS_LAUNCH",
                    "variantDate": "2022-04-11",
                    "startDate": "2022-04-01",
                    "endDate": "2022-05-01",
                    "monthsSinceTreatment": 0,
                    "avgDailyPopulation": 3927.9999999999995,
                    "avgPopulationLimitedSupervisionLevel": 106.69999999999999,
                },
                {
                    "stateCode": "US_TN",
                    "supervisionDistrict": "20",
                    "districtName": "Test 20",
                    "variantId": "WORKFLOWS_LAUNCH",
                    "variantDate": "2022-04-11",
                    "startDate": "2022-04-01",
                    "endDate": "2022-05-01",
                    "monthsSinceTreatment": 0,
                    "avgDailyPopulation": 2512.400000000001,
                    "avgPopulationLimitedSupervisionLevel": 131.86666666666667,
                },
                {
                    "stateCode": "US_TN",
                    "supervisionDistrict": "30",
                    "districtName": "Test 30",
                    "variantId": "WORKFLOWS_LAUNCH",
                    "variantDate": "2022-04-11",
                    "startDate": "2022-04-01",
                    "endDate": "2022-05-01",
                    "monthsSinceTreatment": 0,
                    "avgDailyPopulation": 2837.8666666666659,
                    "avgPopulationLimitedSupervisionLevel": 100.26666666666667,
                },
                {
                    "stateCode": "US_TN",
                    "supervisionDistrict": "40",
                    "districtName": "Test 40",
                    "variantId": "WORKFLOWS_LAUNCH",
                    "variantDate": "2022-04-11",
                    "startDate": "2022-04-01",
                    "endDate": "2022-05-01",
                    "monthsSinceTreatment": 0,
                    "avgDailyPopulation": 2408.7,
                    "avgPopulationLimitedSupervisionLevel": 133.1,
                },
                {
                    "stateCode": "US_TN",
                    "supervisionDistrict": "50",
                    "districtName": "Test 50",
                    "variantId": "WORKFLOWS_LAUNCH",
                    "variantDate": "2022-04-11",
                    "startDate": "2022-04-01",
                    "endDate": "2022-05-01",
                    "monthsSinceTreatment": 0,
                    "avgDailyPopulation": 2302.5333333333333,
                    "avgPopulationLimitedSupervisionLevel": 216.86666666666667,
                },
            ],
            results["data"],
        )
