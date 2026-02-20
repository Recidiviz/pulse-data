# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""This class implements tests for Public Pathways over time metrics."""
import abc
from datetime import date
from typing import Any, Dict, List
from unittest.case import TestCase
from unittest.mock import patch

from recidiviz.case_triage.shared_pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.shared_pathways.dimensions.time_period import TimePeriod
from recidiviz.case_triage.shared_pathways.metric_fetcher import PathwaysMetricFetcher
from recidiviz.case_triage.shared_pathways.query_builders.metric_query_builder import (
    MetricQueryBuilder,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.public_pathways.schema import MetricMetadata
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.public_pathways.metrics.metric_query_builders import (
    ALL_PUBLIC_PATHWAYS_METRICS_BY_NAME,
)
from recidiviz.tests.public_pathways.metrics.base_metrics_test import (
    PublicPathwaysMetricTestBase,
)


class PublicPathwaysOverTimeMetricTestBase(PublicPathwaysMetricTestBase):
    """Test class for Public Pathways over time metrics."""

    @property
    @abc.abstractmethod
    def all_expected_counts(self) -> List[Dict[str, int]]:
        ...

    @property
    @abc.abstractmethod
    def expected_metadata(self) -> Dict[str, Any]:
        ...

    def setUp(self) -> None:
        super().setUp()
        self.current_date_patcher = patch(
            "recidiviz.case_triage.shared_pathways.query_builders.over_time_metric_query_builder.func.current_date",
            return_value=date(2022, 3, 3),
        )
        self.current_date_patcher.start()

    def tearDown(self) -> None:
        self.current_date_patcher.stop()
        super().tearDown()

    def test_metrics_base(self) -> None:
        metric_fetcher = PathwaysMetricFetcher(
            StateCode.US_NY,
            enabled_states=["US_NY"],
            metric_metadata=MetricMetadata,
            schema_type=SchemaType.PUBLIC_PATHWAYS,
        )
        results = metric_fetcher.fetch(
            self.query_builder,
            self.query_builder.build_params(
                {"filters": {Dimension.TIME_PERIOD: [TimePeriod.MONTHS_0_6.value]}}
            ),
        )
        self.test.assertEqual(
            {"data": self.all_expected_counts, "metadata": self.expected_metadata},
            results,
        )


class TestPrisonPopulationOverTime(PublicPathwaysOverTimeMetricTestBase, TestCase):
    """Test for PrisonPopulationOverTime metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return ALL_PUBLIC_PATHWAYS_METRICS_BY_NAME["PrisonPopulationOverTime"]

    @property
    def all_expected_counts(
        self,
    ) -> List[Dict[str, int]]:
        return [
            {"avg90day": 1, "count": 2, "month": 11, "year": 2021},
            {"avg90day": 1, "count": 2, "month": 12, "year": 2021},
            {"avg90day": 2, "count": 2, "month": 1, "year": 2022},
            {"avg90day": 1, "count": 0, "month": 2, "year": 2022},
            {"avg90day": 1, "count": 0, "month": 3, "year": 2022},
        ]

    @property
    def expected_metadata(self) -> Dict[str, Any]:
        return {
            "lastUpdated": "2022-08-03",
            "facilityIdNameMap": '[{"value": "1", "label": "Facility 1"}, {"value": "2", "label": "Facility 2"}]',
            "genderIdNameMap": '[{"value": "MALE", "label": "Male"}, {"value": "FEMALE", "label": "Female"}, {"value": "NON_BINARY", "label": "Non-binary"}]',
            "dynamicFilterOptions": '{"gender_id_name_map": [{"value": "MALE", "label": "Male"}, {"value": "FEMALE", "label": "Female"}, {"value": "NON_BINARY", "label": "Non-binary"}]}',
        }

    def test_demo(self) -> None:
        metric_fetcher = PathwaysMetricFetcher(
            StateCode.US_NY,
            enabled_states=["US_NY"],
            metric_metadata=MetricMetadata,
            schema_type=SchemaType.PUBLIC_PATHWAYS,
        )
        results = metric_fetcher.fetch(
            self.query_builder,
            self.query_builder.build_params(
                {
                    "filters": {Dimension.TIME_PERIOD: [TimePeriod.MONTHS_0_6.value]},
                    "demo": True,
                }
            ),
        )
        self.test.assertEqual(
            [
                {"avg90day": 1, "count": 2, "month": 11, "year": 2021},
                {"avg90day": 1, "count": 2, "month": 12, "year": 2021},
            ],
            results["data"],
        )
