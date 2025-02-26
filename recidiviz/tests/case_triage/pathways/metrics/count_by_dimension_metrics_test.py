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
"""This class implements tests for Pathways count by dimension metrics."""
import abc
from typing import Any, Dict, List, Union
from unittest.case import TestCase

from recidiviz.case_triage.pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.pathways.dimensions.dimension_mapping import (
    DimensionOperation,
)
from recidiviz.case_triage.pathways.dimensions.time_period import TimePeriod
from recidiviz.case_triage.pathways.metric_fetcher import PathwaysMetricFetcher
from recidiviz.case_triage.pathways.metrics.metric_query_builders import (
    ALL_METRICS_BY_NAME,
)
from recidiviz.case_triage.pathways.metrics.query_builders.count_by_dimension_metric_query_builder import (
    CountByDimensionMetricParams,
)
from recidiviz.case_triage.pathways.metrics.query_builders.metric_query_builder import (
    MetricQueryBuilder,
)
from recidiviz.common.constants.states import _FakeStateCode
from recidiviz.tests.case_triage.pathways.metrics.base_metrics_test import (
    PathwaysMetricTestBase,
)


class PathwaysCountByMetricTestBase(PathwaysMetricTestBase):
    @property
    @abc.abstractmethod
    def all_expected_counts(self) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
        ...

    @property
    @abc.abstractmethod
    def expected_metadata(self) -> Dict[str, Any]:
        ...

    def test_metrics_base(self) -> None:
        results = {}
        # TODO(#13950): Replace with StateCode
        metric_fetcher = PathwaysMetricFetcher(_FakeStateCode.US_TN)
        for dimension_mapping in self.query_builder.dimension_mappings:
            if DimensionOperation.GROUP in dimension_mapping.operations:
                results[dimension_mapping.dimension] = metric_fetcher.fetch(
                    self.query_builder,
                    self.query_builder.build_params(
                        {"group": dimension_mapping.dimension}
                    ),
                )

        for dimension, expected_counts in self.all_expected_counts.items():
            self.test.assertEqual(
                {"data": expected_counts, "metadata": self.expected_metadata},
                results.get(dimension),
            )

        # self.test.assertEqual(self.all_expected_counts, results)


class TestLibertyToPrisonTransitionsCount(PathwaysCountByMetricTestBase, TestCase):
    """Test for LibertyToPrisonTransitionsCount metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return ALL_METRICS_BY_NAME["LibertyToPrisonTransitionsCount"]

    @property
    def all_expected_counts(self) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
        return {
            Dimension.GENDER: [
                {"gender": "FEMALE", "count": 1},
                {"gender": "MALE", "count": 6},
                {"gender": "NON_BINARY", "count": 1},
            ],
            Dimension.AGE_GROUP: [
                {"ageGroup": "20-25", "count": 3},
                {"ageGroup": "30-34", "count": 1},
                {"ageGroup": "60+", "count": 4},
            ],
            Dimension.RACE: [
                {"race": "ASIAN", "count": 2},
                {"race": "BLACK", "count": 2},
                {"race": "WHITE", "count": 4},
            ],
            Dimension.JUDICIAL_DISTRICT: [
                {"judicialDistrict": "D1", "count": 5},
                {"judicialDistrict": "D2", "count": 2},
                {"judicialDistrict": "D3", "count": 1},
            ],
            Dimension.PRIOR_LENGTH_OF_INCARCERATION: [
                {
                    "priorLengthOfIncarceration": "months_0_3",
                    "count": 6,
                },
                {
                    "priorLengthOfIncarceration": "months_3_6",
                    "count": 2,
                },
            ],
        }

    @property
    def expected_metadata(self) -> Dict[str, Any]:
        return {"lastUpdated": "2022-08-01"}

    def test_metrics_filter(self) -> None:
        results = PathwaysMetricFetcher(state_code=_FakeStateCode.US_TN).fetch(
            self.query_builder,
            CountByDimensionMetricParams(
                group=Dimension.GENDER,
                filters={
                    Dimension.RACE: ["WHITE"],
                },
            ),
        )

        self.test.assertEqual([{"gender": "MALE", "count": 4}], results["data"])

    def test_filter_time_period(self) -> None:
        """Asserts that person id 6 is dropped from the counts"""
        results = PathwaysMetricFetcher(_FakeStateCode.US_TN).fetch(
            self.query_builder,
            CountByDimensionMetricParams(
                group=Dimension.GENDER,
                filters={Dimension.TIME_PERIOD: [TimePeriod.MONTHS_0_6.value]},
            ),
        )

        self.test.assertEqual(
            [
                {"gender": "FEMALE", "count": 1},
                {"gender": "MALE", "count": 5},
                {"gender": "NON_BINARY", "count": 1},
            ],
            results["data"],
        )


class TestPrisonToSupervisionTransitionsCount(PathwaysCountByMetricTestBase, TestCase):
    """Test for PrisonToSupervisionTransitionsCount metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return ALL_METRICS_BY_NAME["PrisonToSupervisionTransitionsCount"]

    @property
    def all_expected_counts(self) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
        return {
            Dimension.GENDER: [
                {"gender": "FEMALE", "count": 1},
                {"gender": "MALE", "count": 9},
            ],
            Dimension.AGE_GROUP: [
                {"ageGroup": "20-25", "count": 2},
                {"ageGroup": "35-39", "count": 1},
                {"ageGroup": "40-44", "count": 1},
                {"ageGroup": "60+", "count": 6},
            ],
            Dimension.FACILITY: [
                {"facility": "ABC", "count": 4},
                {"facility": "DEF", "count": 4},
                {"facility": "GHI", "count": 2},
            ],
            Dimension.RACE: [
                {"race": "ASIAN", "count": 1},
                {"race": "BLACK", "count": 2},
                {"race": "WHITE", "count": 7},
            ],
        }

    @property
    def expected_metadata(self) -> Dict[str, Any]:
        return {"lastUpdated": "2022-08-05"}

    def test_metrics_filter(self) -> None:
        results = PathwaysMetricFetcher(state_code=_FakeStateCode.US_TN).fetch(
            self.query_builder,
            CountByDimensionMetricParams(
                group=Dimension.GENDER,
                filters={
                    Dimension.FACILITY: ["ABC"],
                },
            ),
        )

        self.test.assertEqual(
            [{"gender": "FEMALE", "count": 1}, {"gender": "MALE", "count": 3}],
            results["data"],
        )

    def test_filter_time_period(self) -> None:
        """Asserts that person id 6 is filtered out of the 6 month count"""
        results = PathwaysMetricFetcher(_FakeStateCode.US_TN).fetch(
            self.query_builder,
            CountByDimensionMetricParams(
                group=Dimension.FACILITY,
                filters={Dimension.TIME_PERIOD: [TimePeriod.MONTHS_0_6.value]},
            ),
        )

        self.test.assertEqual(
            [{"facility": "ABC", "count": 4}, {"facility": "DEF", "count": 4}],
            results["data"],
        )


class TestSupervisionToPrisonTransitionsCount(PathwaysCountByMetricTestBase, TestCase):
    """Test for SupervisionToPrisonTransitionsCount metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return ALL_METRICS_BY_NAME["SupervisionToPrisonTransitionsCount"]

    @property
    def all_expected_counts(self) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
        return {
            Dimension.GENDER: [
                {"gender": "FEMALE", "count": 2},
                {"gender": "MALE", "count": 2},
                {"gender": "NON_BINARY", "count": 1},
            ],
            Dimension.AGE_GROUP: [
                {"ageGroup": "20-25", "count": 1},
                {"ageGroup": "26-35", "count": 3},
                {"ageGroup": "60+", "count": 1},
            ],
            Dimension.RACE: [
                {"race": "ASIAN", "count": 1},
                {"race": "BLACK", "count": 1},
                {"race": "WHITE", "count": 3},
            ],
            Dimension.SUPERVISION_TYPE: [
                {"supervisionType": "PAROLE", "count": 2},
                {"supervisionType": "PROBATION", "count": 3},
            ],
            Dimension.SUPERVISION_LEVEL: [
                {"supervisionLevel": "MAXIMUM", "count": 1},
                {"supervisionLevel": "MEDIUM", "count": 2},
                {"supervisionLevel": "MINIMUM", "count": 2},
            ],
            Dimension.SUPERVISION_DISTRICT: [
                {"supervisionDistrict": "DISTRICT_10", "count": 2},
                {"supervisionDistrict": "DISTRICT_18", "count": 3},
            ],
            Dimension.DISTRICT: [
                {"district": "DISTRICT_10", "count": 2},
                {"district": "DISTRICT_18", "count": 3},
            ],
            Dimension.SUPERVISING_OFFICER: [
                {"supervisingOfficer": "3456", "count": 1},
                {"supervisingOfficer": "4567", "count": 1},
                {"supervisingOfficer": "7890", "count": 2},
                {"supervisingOfficer": "9999", "count": 1},
            ],
            Dimension.LENGTH_OF_STAY: [
                {"lengthOfStay": "months_0_3", "count": 1},
                {"lengthOfStay": "months_25_60", "count": 1},
                {"lengthOfStay": "months_3_6", "count": 1},
                {"lengthOfStay": "months_6_9", "count": 2},
            ],
        }

    @property
    def expected_metadata(self) -> Dict[str, Any]:
        return {"lastUpdated": "2022-08-09"}

    def test_metrics_filter(self) -> None:
        results = PathwaysMetricFetcher(state_code=_FakeStateCode.US_TN).fetch(
            self.query_builder,
            CountByDimensionMetricParams(
                group=Dimension.DISTRICT,
                filters={
                    Dimension.RACE: ["WHITE"],
                },
            ),
        )

        self.test.assertEqual(
            [
                {"district": "DISTRICT_10", "count": 1},
                {"district": "DISTRICT_18", "count": 2},
            ],
            results["data"],
        )

    def test_filter_time_period(self) -> None:
        """Tests that person 1 and 5 are not included, as the transition occurred more than 12 months ago"""
        results = PathwaysMetricFetcher(_FakeStateCode.US_TN).fetch(
            self.query_builder,
            CountByDimensionMetricParams(
                group=Dimension.GENDER,
                filters={
                    Dimension.TIME_PERIOD: [
                        TimePeriod.MONTHS_0_6.value,
                        TimePeriod.MONTHS_7_12.value,
                    ]
                },
            ),
        )

        self.test.assertEqual(
            [
                {"gender": "FEMALE", "count": 2},
                {"gender": "NON_BINARY", "count": 1},
            ],
            results["data"],
        )


class TestSupervisionToLibertyTransitionsCount(PathwaysCountByMetricTestBase, TestCase):
    """Test for SupervisionToLibertyTransitionsCount metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return ALL_METRICS_BY_NAME["SupervisionToLibertyTransitionsCount"]

    @property
    def all_expected_counts(self) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
        return {
            Dimension.AGE_GROUP: [
                {"ageGroup": "20-25", "count": 1},
                {"ageGroup": "26-35", "count": 3},
                {"ageGroup": "60+", "count": 1},
            ],
            Dimension.GENDER: [
                {"gender": "FEMALE", "count": 2},
                {"gender": "MALE", "count": 2},
                {"gender": "NON_BINARY", "count": 1},
            ],
            Dimension.LENGTH_OF_STAY: [
                {"lengthOfStay": "months_0_3", "count": 1},
                {"lengthOfStay": "months_24_36", "count": 1},
                {"lengthOfStay": "months_3_6", "count": 1},
                {"lengthOfStay": "months_6_9", "count": 2},
            ],
            Dimension.RACE: [
                {"race": "ASIAN", "count": 1},
                {"race": "BLACK", "count": 1},
                {"race": "WHITE", "count": 3},
            ],
            Dimension.SUPERVISION_TYPE: [
                {"supervisionType": "PAROLE", "count": 2},
                {"supervisionType": "PROBATION", "count": 3},
            ],
            Dimension.SUPERVISION_LEVEL: [
                {"supervisionLevel": "MAXIMUM", "count": 1},
                {"supervisionLevel": "MEDIUM", "count": 2},
                {"supervisionLevel": "MINIMUM", "count": 2},
            ],
            Dimension.SUPERVISION_DISTRICT: [
                {"supervisionDistrict": "DISTRICT_10", "count": 2},
                {"supervisionDistrict": "DISTRICT_18", "count": 2},
                {"supervisionDistrict": "DISTRICT_20", "count": 1},
            ],
            # TODO(#13552): Remove this once FE uses supervisionDistrict
            Dimension.DISTRICT: [
                {"district": "DISTRICT_10", "count": 2},
                {"district": "DISTRICT_18", "count": 2},
                {"district": "DISTRICT_20", "count": 1},
            ],
            Dimension.SUPERVISING_OFFICER: [
                {"supervisingOfficer": "3456", "count": 1},
                {"supervisingOfficer": "4567", "count": 1},
                {"supervisingOfficer": "7890", "count": 2},
                {"supervisingOfficer": "9999", "count": 1},
            ],
        }

    @property
    def expected_metadata(self) -> Dict[str, Any]:
        return {"lastUpdated": "2022-08-08"}

    def test_metrics_filter(self) -> None:
        results = PathwaysMetricFetcher(state_code=_FakeStateCode.US_TN).fetch(
            self.query_builder,
            CountByDimensionMetricParams(
                group=Dimension.GENDER,
                filters={
                    Dimension.RACE: ["WHITE"],
                },
            ),
        )

        self.test.assertEqual(
            [
                {"gender": "FEMALE", "count": 1},
                {"gender": "MALE", "count": 1},
                {"gender": "NON_BINARY", "count": 1},
            ],
            results["data"],
        )

    def test_filter_time_period(self) -> None:
        """Asserts that person id 5 is not included in the counts"""
        results = PathwaysMetricFetcher(_FakeStateCode.US_TN).fetch(
            self.query_builder,
            CountByDimensionMetricParams(
                group=Dimension.SUPERVISION_DISTRICT,
                filters={Dimension.TIME_PERIOD: [TimePeriod.MONTHS_0_6.value]},
            ),
        )

        self.test.assertEqual(
            [
                {"supervisionDistrict": "DISTRICT_10", "count": 2},
                {"supervisionDistrict": "DISTRICT_18", "count": 2},
            ],
            results["data"],
        )


class TestSupervisionPopulationByDimensionCount(
    PathwaysCountByMetricTestBase, TestCase
):
    """Test for SupervisionPopulationByDimensionCount metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return ALL_METRICS_BY_NAME["SupervisionPopulationByDimensionCount"]

    @property
    def all_expected_counts(
        self,
    ) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
        return {
            Dimension.SUPERVISION_LEVEL: [
                {"supervisionLevel": "HIGH", "count": 1},
                {"supervisionLevel": "MINIMUM", "count": 1},
            ],
            Dimension.DISTRICT: [
                {"district": "District 1", "count": 1},
                {"district": "District 2", "count": 1},
                {"district": "OTHER", "count": 1},
            ],
            Dimension.SUPERVISION_DISTRICT: [
                {"supervisionDistrict": "District 1", "count": 1},
                {"supervisionDistrict": "District 2", "count": 1},
                {"supervisionDistrict": "OTHER", "count": 1},
            ],
            Dimension.RACE: [
                {"race": "HISPANIC", "count": 1},
                {"race": "WHITE", "count": 1},
            ],
        }

    @property
    def expected_metadata(self) -> Dict[str, Any]:
        return {"lastUpdated": "2022-08-06"}

    def test_metrics_filter(self) -> None:
        # TODO(#13950): Replace with StateCode
        results = PathwaysMetricFetcher(state_code=_FakeStateCode.US_TN).fetch(
            self.query_builder,
            CountByDimensionMetricParams(
                group=Dimension.RACE,
                filters={
                    Dimension.SUPERVISION_DISTRICT: ["OTHER"],
                },
            ),
        )

        self.test.assertEqual(
            [
                {"race": "WHITE", "count": 1},
            ],
            results["data"],
        )


class TestPrisonPopulationByDimensionCount(PathwaysCountByMetricTestBase, TestCase):
    """Test for PrisonPopulationByDimensionCount metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return ALL_METRICS_BY_NAME["PrisonPopulationByDimensionCount"]

    @property
    def all_expected_counts(
        self,
    ) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
        return {
            Dimension.AGE_GROUP: [
                {"ageGroup": "25-29", "count": 1},
                {"ageGroup": "60+", "count": 3},
            ],
            Dimension.FACILITY: [
                {"facility": "F1", "count": 4},
            ],
            Dimension.GENDER: [
                {"gender": "FEMALE", "count": 2},
                {"gender": "MALE", "count": 2},
            ],
            Dimension.ADMISSION_REASON: [
                {"admissionReason": "NEW_ADMISSION", "count": 1},
                {"admissionReason": "REVOCATION", "count": 2},
                {"admissionReason": "UNKNOWN", "count": 1},
            ],
            Dimension.RACE: [
                {"race": "BLACK", "count": 2},
                {"race": "WHITE", "count": 2},
            ],
            Dimension.LENGTH_OF_STAY: [{"count": 4, "lengthOfStay": "months_0_3"}],
        }

    @property
    def expected_metadata(self) -> Dict[str, Any]:
        return {"lastUpdated": "2022-08-02"}
