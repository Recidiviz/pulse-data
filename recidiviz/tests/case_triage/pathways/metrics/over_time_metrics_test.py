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
"""This class implements tests for Pathways over time metrics."""
import abc
from typing import Dict, List
from unittest.case import TestCase

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
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.case_triage.pathways.metrics.base_metrics_test import (
    PathwaysMetricTestBase,
)


class OverTimeMetricTestBase(PathwaysMetricTestBase):
    @property
    @abc.abstractmethod
    def all_expected_counts(self) -> List[Dict[str, int]]:
        ...

    def test_metrics_base(self) -> None:
        # TODO(#13950): Replace with StateCode
        metric_fetcher = PathwaysMetricFetcher(_FakeStateCode.US_TN)
        results = metric_fetcher.fetch(
            self.query_builder,
            self.query_builder.build_params(
                {"filters": {Dimension.TIME_PERIOD: [TimePeriod.MONTHS_0_6.value]}}
            ),
        )
        self.test.assertEqual(self.all_expected_counts, results)


class TestSupervisionPopulationOverTime(OverTimeMetricTestBase, TestCase):
    """Test for SupervisionPopulation metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return ALL_METRICS_BY_NAME["SupervisionPopulationOverTime"]

    @property
    def all_expected_counts(
        self,
    ) -> List[Dict[str, int]]:
        return [{"avg90day": 2, "count": 2, "month": 2, "year": 2022}]

    def test_filter_time_period(self) -> None:
        """When filtering for a time period that doesn't exist in the data,
        the earliest date is selected from the list of time periods in range"""
        # TODO(#13950): Replace with StateCode
        results = PathwaysMetricFetcher(_FakeStateCode.US_TN).fetch(
            self.query_builder,
            FetchMetricParams(
                filters={Dimension.TIME_PERIOD: [TimePeriod.MONTHS_25_60.value]},
            ),
        )

        self.test.assertEqual(
            [
                {"avg90day": 2, "count": 2, "month": 1, "year": 2022},
                {"avg90day": 2, "count": 2, "month": 2, "year": 2022},
            ],
            results,
        )


class TestPrisonPopulationOverTime(OverTimeMetricTestBase, TestCase):
    """Test for PrisonPopulationOverTime metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return ALL_METRICS_BY_NAME["PrisonPopulationOverTime"]

    @property
    def all_expected_counts(
        self,
    ) -> List[Dict[str, int]]:
        return [
            {"avg90day": 2, "count": 2, "month": 1, "year": 2022},
            {"avg90day": 2, "count": 2, "month": 2, "year": 2022},
            {"avg90day": 2, "count": 2, "month": 3, "year": 2022},
        ]


class TestLibertyToPrisonTransitionsOverTime(OverTimeMetricTestBase, TestCase):
    """Test for PrisonPopulationOverTimeCount metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return ALL_METRICS_BY_NAME["LibertyToPrisonTransitionsOverTime"]

    @property
    def all_expected_counts(
        self,
    ) -> List[Dict[str, int]]:
        return [
            {"avg90day": 1, "count": 1, "month": 1, "year": 2022},
            {"avg90day": 1, "count": 1, "month": 2, "year": 2022},
            {"avg90day": 2, "count": 3, "month": 3, "year": 2022},
        ]


class TestPrisonToSupervisionTransitionsOverTime(OverTimeMetricTestBase, TestCase):
    """Test for PrisonToSupervisionTransitionsOverTime metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return ALL_METRICS_BY_NAME["PrisonToSupervisionTransitionsOverTime"]

    @property
    def all_expected_counts(
        self,
    ) -> List[Dict[str, int]]:
        return [
            {"avg90day": 1, "count": 1, "month": 1, "year": 2022},
            {"avg90day": 2, "count": 2, "month": 2, "year": 2022},
            {"avg90day": 2, "count": 3, "month": 3, "year": 2022},
        ]


class TestSupervisionToLibertyTransitionsOverTime(OverTimeMetricTestBase, TestCase):
    """Test for SupervisionToLibertyTransitionsOverTime metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return ALL_METRICS_BY_NAME["SupervisionToLibertyTransitionsOverTime"]

    @property
    def all_expected_counts(
        self,
    ) -> List[Dict[str, int]]:
        return [
            {"avg90day": 1, "count": 1, "month": 1, "year": 2022},
            {"avg90day": 2, "count": 2, "month": 2, "year": 2022},
            {"avg90day": 1, "count": 1, "month": 3, "year": 2022},
        ]


class TestSupervisionToPrisonTransitionsOverTime(OverTimeMetricTestBase, TestCase):
    """Test for SupervisionToPrisonTransitionsOverTime metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return ALL_METRICS_BY_NAME["SupervisionToPrisonTransitionsOverTime"]

    @property
    def all_expected_counts(
        self,
    ) -> List[Dict[str, int]]:
        return [
            {"avg90day": 1, "count": 1, "month": 1, "year": 2022},
            {"avg90day": 2, "count": 2, "month": 2, "year": 2022},
            {"avg90day": 1, "count": 1, "month": 3, "year": 2022},
        ]

    def test_month_backtrack(self) -> None:
        """If the `time_period` switches in the middle of the month,
        transitions in the earlier part of the month are still counted"""
        with SessionFactory.using_database(self.database_key) as session:
            session.execute(f"TRUNCATE TABLE {self.schema.__tablename__}")
            session.commit()

            session.add(
                self.schema(
                    state_code="US_XX",
                    person_id=1,
                    year=2022,
                    month=1,
                    transition_date="2022-01-01",
                    time_period=TimePeriod.MONTHS_7_12.value,
                    supervision_type="PAROLE",
                    supervision_level="MEDIUM",
                    age="22",
                    age_group="20 - 25",
                    gender="MALE",
                    race="WHITE",
                    supervising_officer="7890",
                    supervision_district="DISTRICT_10",
                    length_of_stay="months_0_3",
                )
            )
            session.add(
                self.schema(
                    state_code="US_XX",
                    person_id=2,
                    year=2022,
                    month=1,
                    transition_date="2022-01-15",
                    time_period=TimePeriod.MONTHS_0_6.value,
                    supervision_type="PAROLE",
                    supervision_level="MEDIUM",
                    age="22",
                    age_group="20 - 25",
                    gender="MALE",
                    race="WHITE",
                    supervising_officer="7890",
                    supervision_district="DISTRICT_10",
                    length_of_stay="months_0_3",
                )
            )

        # TODO(#13950): Replace with StateCode
        metric_fetcher = PathwaysMetricFetcher(_FakeStateCode.US_TN)
        results = metric_fetcher.fetch(
            self.query_builder,
            self.query_builder.build_params(
                {"filters": {Dimension.TIME_PERIOD: [TimePeriod.MONTHS_0_6.value]}}
            ),
        )

        self.assertEqual(
            [
                {"avg90day": 2, "count": 2, "month": 1, "year": 2022},
            ],
            results,
        )
