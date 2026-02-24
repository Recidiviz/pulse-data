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
"""This class implements tests for Public Pathways count by dimension metrics."""
import abc
from typing import Any, Dict, List, Union
from unittest.case import TestCase

from recidiviz.case_triage.shared_pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.shared_pathways.dimensions.dimension_mapping import (
    DimensionOperation,
)
from recidiviz.case_triage.shared_pathways.metric_fetcher import PathwaysMetricFetcher
from recidiviz.case_triage.shared_pathways.query_builders.count_by_dimension_metric_query_builder import (
    CountByDimensionMetricParams,
)
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


class PublicPathwaysCountByMetricTestBase(PublicPathwaysMetricTestBase):
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
        metric_fetcher = PathwaysMetricFetcher(
            StateCode.US_NY,
            enabled_states=["US_NY"],
            metric_metadata=MetricMetadata,
            schema_type=SchemaType.PUBLIC_PATHWAYS,
        )
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


class TestPrisonPopulationByDimensionCount(
    PublicPathwaysCountByMetricTestBase, TestCase
):
    """Test for PrisonPopulationByDimensionCount metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return ALL_PUBLIC_PATHWAYS_METRICS_BY_NAME["PrisonPopulationByDimensionCount"]

    @property
    def all_expected_counts(
        self,
    ) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
        return {
            Dimension.AGE_GROUP: [
                {"ageGroup": "25-29", "count": 2},
                {"ageGroup": "60+", "count": 6},
            ],
            Dimension.FACILITY: [
                {"facility": "F1", "count": 8},
            ],
            Dimension.GENDER: [
                {"gender": "FEMALE", "count": 2},
                {"gender": "MALE", "count": 4},
                {"gender": "NON_BINARY", "count": 2},
            ],
            Dimension.SEX: [
                {"sex": "FEMALE", "count": 2},
                {"sex": "MALE", "count": 6},
            ],
            Dimension.RACE: [
                {"race": "BLACK", "count": 4},
                {"race": "WHITE", "count": 4},
            ],
            Dimension.ETHNICITY: [
                {"ethnicity": "HISPANIC", "count": 2},
                {"ethnicity": "NOT_HISPANIC", "count": 6},
            ],
            Dimension.CHARGE_COUNTY_CODE: [
                {"chargeCountyCode": "COUNTY_1", "count": 4},
                {"chargeCountyCode": "COUNTY_2", "count": 4},
            ],
            Dimension.OFFENSE_TYPE: [
                {"offenseType": "DRUG OFFENSES", "count": 2},
                {"offenseType": "PROPERTY AND OTHER OFFENSES", "count": 2},
                {"offenseType": "VIOLENT FELONY", "count": 4},
            ],
        }

    @property
    def expected_metadata(self) -> Dict[str, Any]:
        return {
            "lastUpdated": "2022-08-02",
            "facilityIdNameMap": '[{"value": "1", "label": "Facility 1"}, {"value": "2", "label": "Facility 2"}]',
            "genderIdNameMap": '[{"value": "MALE", "label": "Male"}, {"value": "FEMALE", "label": "Female"}, {"value": "NON_BINARY", "label": "Non-binary"}]',
            "dynamicFilterOptions": '{"gender_id_name_map": [{"value": "MALE", "label": "Male"}, {"value": "FEMALE", "label": "Female"}, {"value": "NON_BINARY", "label": "Non-binary"}]}',
        }

    def test_metrics_filter(self) -> None:
        results = PathwaysMetricFetcher(
            state_code=StateCode.US_NY,
            enabled_states=["US_NY"],
            metric_metadata=MetricMetadata,
            schema_type=SchemaType.PUBLIC_PATHWAYS,
        ).fetch(
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
                {"gender": "FEMALE", "count": 2},
                {"gender": "MALE", "count": 2},
            ],
            results["data"],
        )
