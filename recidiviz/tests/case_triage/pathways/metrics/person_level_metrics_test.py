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
"""This class implements tests for Pathways person level metrics."""
import abc
from datetime import date
from typing import Dict, List, Union
from unittest import TestCase

from recidiviz.case_triage.pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.pathways.dimensions.time_period import TimePeriod
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


class PathwaysPersonLevelMetricTestBase(PathwaysMetricTestBase):
    @property
    @abc.abstractmethod
    def all_expected_rows(self) -> List[Dict[str, Union[str, int, date]]]:
        ...

    def test_metrics_base(self) -> None:
        metric_fetcher = PathwaysMetricFetcher(_FakeStateCode.US_TN)
        results = metric_fetcher.fetch(self.query_builder, FetchMetricParams())

        self.test.assertEqual(
            self.all_expected_rows,
            results,
        )


class TestPrisonPopulationPersonLevel(PathwaysPersonLevelMetricTestBase, TestCase):
    """Test for PrisonPopulationPersonLevel metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return ALL_METRICS_BY_NAME["PrisonPopulationPersonLevel"]

    @property
    def all_expected_rows(self) -> List[Dict[str, Union[str, int, date]]]:
        return [
            {
                "age": "25",
                "age_group": "25-29",
                "facility": "F1",
                "full_name": "Example,Person",
                "gender": "MALE",
                "admission_reason": "NEW_ADMISSION",
                "race": "WHITE",
                "state_id": "1",
            },
            {
                "age": "65",
                "age_group": "60+",
                "facility": "F2",
                "full_name": "Fake,Person",
                "gender": "FEMALE",
                "admission_reason": "RETURN_FROM_TEMPORARY_RELEASE",
                "race": "BLACK",
                "state_id": "2",
            },
        ]


class TestPrisonToSupervisionTransitionsPersonLevel(
    PathwaysPersonLevelMetricTestBase, TestCase
):
    """Test for PrisonToSupervisionTransitionsPersonLevel metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return ALL_METRICS_BY_NAME["PrisonToSupervisionTransitionsPersonLevel"]

    @property
    def all_expected_rows(self) -> List[Dict[str, Union[str, int, date]]]:
        return [
            {
                "age_group": "20-25",
                "age": "22, 23",
                "gender": "MALE",
                "race": "WHITE",
                "facility": "ABC, DEF",
                "full_name": "TEST, PERSON",
                "time_period": "months_0_6",
                "state_id": "0001",
            },
            {
                "age_group": "60+",
                "age": "62",
                "gender": "FEMALE",
                "race": "BLACK",
                "facility": "ABC",
                "full_name": "FAKE, USER",
                "time_period": "months_0_6",
                "state_id": "0003",
            },
            {
                "age_group": "60+",
                "age": "64",
                "gender": "MALE",
                "race": "ASIAN",
                "facility": "ABC",
                "full_name": "EXAMPLE, INDIVIDUAL",
                "time_period": "months_0_6",
                "state_id": "0005",
            },
            {
                "age_group": "60+",
                "age": "63",
                "gender": "MALE",
                "race": "BLACK",
                "facility": "DEF",
                "full_name": "FAKE2, USER2",
                "time_period": "months_0_6",
                "state_id": "0004",
            },
            {
                "age_group": "60+",
                "age": "61",
                "gender": "MALE",
                "race": "WHITE",
                "facility": "DEF",
                "full_name": "TEST, PERSON2",
                "time_period": "months_0_6",
                "state_id": "0002",
            },
            {
                "age": "65",
                "age_group": "60+",
                "facility": "GHI",
                "full_name": "EXAMPLE, TIME",
                "gender": "MALE",
                "race": "WHITE",
                "state_id": "0006",
                "time_period": "months_25_60",
            },
        ]

    def test_metrics_filter(self) -> None:
        results = PathwaysMetricFetcher(state_code=_FakeStateCode.US_TN).fetch(
            self.query_builder,
            FetchMetricParams(
                filters={
                    Dimension.FACILITY: ["ABC"],
                },
            ),
        )

        self.test.assertEqual(
            [
                {
                    "age_group": "20-25",
                    "age": "22",
                    "gender": "MALE",
                    "race": "WHITE",
                    "facility": "ABC",
                    "full_name": "TEST, PERSON",
                    "time_period": "months_0_6",
                    "state_id": "0001",
                },
                {
                    "age_group": "60+",
                    "age": "62",
                    "gender": "FEMALE",
                    "race": "BLACK",
                    "facility": "ABC",
                    "full_name": "FAKE, USER",
                    "time_period": "months_0_6",
                    "state_id": "0003",
                },
                {
                    "age_group": "60+",
                    "age": "64",
                    "gender": "MALE",
                    "race": "ASIAN",
                    "facility": "ABC",
                    "full_name": "EXAMPLE, INDIVIDUAL",
                    "time_period": "months_0_6",
                    "state_id": "0005",
                },
            ],
            results,
        )

    def test_filter_time_period(self) -> None:
        """Tests that person id 6 is not included in the response"""
        results = PathwaysMetricFetcher(_FakeStateCode.US_TN).fetch(
            self.query_builder,
            FetchMetricParams(
                filters={Dimension.TIME_PERIOD: [TimePeriod.MONTHS_0_6.value]},
            ),
        )

        self.test.assertEqual(
            [
                {
                    "age": "22, 23",
                    "age_group": "20-25",
                    "facility": "ABC, DEF",
                    "full_name": "TEST, PERSON",
                    "gender": "MALE",
                    "race": "WHITE",
                    "state_id": "0001",
                    "time_period": "months_0_6",
                },
                {
                    "age": "62",
                    "age_group": "60+",
                    "facility": "ABC",
                    "full_name": "FAKE, USER",
                    "gender": "FEMALE",
                    "race": "BLACK",
                    "state_id": "0003",
                    "time_period": "months_0_6",
                },
                {
                    "age": "64",
                    "age_group": "60+",
                    "facility": "ABC",
                    "full_name": "EXAMPLE, INDIVIDUAL",
                    "gender": "MALE",
                    "race": "ASIAN",
                    "state_id": "0005",
                    "time_period": "months_0_6",
                },
                {
                    "age": "63",
                    "age_group": "60+",
                    "facility": "DEF",
                    "full_name": "FAKE2, USER2",
                    "gender": "MALE",
                    "race": "BLACK",
                    "state_id": "0004",
                    "time_period": "months_0_6",
                },
                {
                    "age": "61",
                    "age_group": "60+",
                    "facility": "DEF",
                    "full_name": "TEST, PERSON2",
                    "gender": "MALE",
                    "race": "WHITE",
                    "state_id": "0002",
                    "time_period": "months_0_6",
                },
            ],
            results,
        )
