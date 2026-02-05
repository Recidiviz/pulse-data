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
from typing import Any, Dict, List, Union
from unittest import TestCase

from recidiviz.case_triage.pathways.metrics.metric_query_builders import (
    ALL_METRICS_BY_NAME,
)
from recidiviz.case_triage.pathways.metrics.query_builders.metric_query_builder import (
    FetchMetricParams,
    MetricQueryBuilder,
)
from recidiviz.case_triage.shared_pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.shared_pathways.dimensions.time_period import TimePeriod
from recidiviz.case_triage.shared_pathways.metric_fetcher import PathwaysMetricFetcher
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.tests.case_triage.pathways.metrics.base_metrics_test import (
    PathwaysMetricTestBase,
)


class PathwaysPersonLevelMetricTestBase(PathwaysMetricTestBase):
    @property
    @abc.abstractmethod
    def all_expected_rows(self) -> List[Dict[str, Union[str, int, date]]]:
        ...

    @property
    @abc.abstractmethod
    def expected_metadata(self) -> Dict[str, Any]:
        ...

    def test_metrics_base(self) -> None:
        metric_fetcher = PathwaysMetricFetcher(
            StateCode.US_TN, schema_type=SchemaType.PATHWAYS
        )
        results = metric_fetcher.fetch(self.query_builder, FetchMetricParams())

        self.test.assertEqual(list(results.keys()), ["data", "metadata"])
        # Person-level metrics data are not returned in any particular order, so assert unordered.
        self.test.assertCountEqual(results["data"], self.all_expected_rows)
        self.test.assertEqual(results["metadata"], self.expected_metadata)


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
                "facility": "F1",
                "fullName": "Example,Person",
                "sex": "MALE",
                "admissionReason": "NEW_ADMISSION",
                "race": "WHITE",
                "stateId": "1",
            },
            {
                "age": "65",
                "facility": "F2",
                "fullName": "Fake,Person",
                "sex": "FEMALE",
                "admissionReason": "RETURN_FROM_TEMPORARY_RELEASE",
                "race": "BLACK",
                "stateId": "2",
            },
        ]

    @property
    def expected_metadata(self) -> Dict[str, Any]:
        return {"lastUpdated": "2022-08-04"}


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
                "age": "22, 23",
                "sex": "MALE",
                "race": "WHITE",
                "facility": "ABC, DEF",
                "fullName": "TEST, PERSON",
                "stateId": "0001",
            },
            {
                "age": "62",
                "sex": "FEMALE",
                "race": "BLACK",
                "facility": "ABC",
                "fullName": "FAKE, USER",
                "stateId": "0003",
            },
            {
                "age": "64",
                "sex": "MALE",
                "race": "ASIAN",
                "facility": "ABC",
                "fullName": "EXAMPLE, INDIVIDUAL",
                "stateId": "0005",
            },
            {
                "age": "63",
                "sex": "MALE",
                "race": "BLACK",
                "facility": "DEF",
                "fullName": "FAKE2, USER2",
                "stateId": "0004",
            },
            {
                "age": "61, 61",
                "sex": "MALE",
                "race": "WHITE",
                "facility": "ABC, DEF",
                "fullName": "TEST, PERSON2",
                "stateId": "0002",
            },
            {
                "age": "65",
                "facility": "GHI",
                "fullName": "EXAMPLE, TIME",
                "sex": "MALE",
                "race": "WHITE",
                "stateId": "0006",
            },
            {
                "age": "39, 40",
                "facility": "DEF, GHI",
                "fullName": "EXAMPLE, TIME",
                "sex": "MALE",
                "race": "WHITE",
                "stateId": "0007",
            },
        ]

    @property
    def expected_metadata(self) -> Dict[str, Any]:
        return {"lastUpdated": "2022-08-05"}

    def test_metrics_filter(self) -> None:
        results = PathwaysMetricFetcher(
            state_code=StateCode.US_TN, schema_type=SchemaType.PATHWAYS
        ).fetch(
            self.query_builder,
            FetchMetricParams(
                filters={
                    Dimension.FACILITY: ["ABC"],
                },
            ),
        )

        self.test.assertCountEqual(
            [
                {
                    "age": "22",
                    "sex": "MALE",
                    "race": "WHITE",
                    "facility": "ABC",
                    "fullName": "TEST, PERSON",
                    "stateId": "0001",
                },
                {
                    "age": "62",
                    "sex": "FEMALE",
                    "race": "BLACK",
                    "facility": "ABC",
                    "fullName": "FAKE, USER",
                    "stateId": "0003",
                },
                {
                    "age": "64",
                    "sex": "MALE",
                    "race": "ASIAN",
                    "facility": "ABC",
                    "fullName": "EXAMPLE, INDIVIDUAL",
                    "stateId": "0005",
                },
                {
                    "age": "61",
                    "sex": "MALE",
                    "race": "WHITE",
                    "facility": "ABC",
                    "fullName": "TEST, PERSON2",
                    "stateId": "0002",
                },
            ],
            results["data"],
        )

    def test_filter_timePeriod(self) -> None:
        """Tests that person id 6 is not included in the response"""
        results = PathwaysMetricFetcher(
            StateCode.US_TN, schema_type=SchemaType.PATHWAYS
        ).fetch(
            self.query_builder,
            FetchMetricParams(
                filters={Dimension.TIME_PERIOD: [TimePeriod.MONTHS_0_6.value]},
            ),
        )

        self.test.assertCountEqual(
            [
                {
                    "age": "22, 23",
                    "facility": "ABC, DEF",
                    "fullName": "TEST, PERSON",
                    "sex": "MALE",
                    "race": "WHITE",
                    "stateId": "0001",
                },
                {
                    "age": "62",
                    "facility": "ABC",
                    "fullName": "FAKE, USER",
                    "sex": "FEMALE",
                    "race": "BLACK",
                    "stateId": "0003",
                },
                {
                    "age": "64",
                    "facility": "ABC",
                    "fullName": "EXAMPLE, INDIVIDUAL",
                    "sex": "MALE",
                    "race": "ASIAN",
                    "stateId": "0005",
                },
                {
                    "age": "63",
                    "facility": "DEF",
                    "fullName": "FAKE2, USER2",
                    "sex": "MALE",
                    "race": "BLACK",
                    "stateId": "0004",
                },
                {
                    "age": "61, 61",
                    "facility": "ABC, DEF",
                    "fullName": "TEST, PERSON2",
                    "sex": "MALE",
                    "race": "WHITE",
                    "stateId": "0002",
                },
                {
                    "age": "40",
                    "facility": "DEF",
                    "fullName": "EXAMPLE, TIME",
                    "sex": "MALE",
                    "race": "WHITE",
                    "stateId": "0007",
                },
            ],
            results["data"],
        )
