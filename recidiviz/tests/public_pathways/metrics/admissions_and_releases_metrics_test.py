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
"""Tests for the Public Pathways admissions and releases metrics."""
from typing import Any, Dict, List, Union
from unittest.case import TestCase

from recidiviz.case_triage.shared_pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.shared_pathways.dimensions.dimension_mapping import (
    DimensionOperation,
)
from recidiviz.case_triage.shared_pathways.metric_fetcher import PathwaysMetricFetcher
from recidiviz.case_triage.shared_pathways.metric_query_builder_config_parser import (
    MetricQueryBuilderConfigParser,
)
from recidiviz.case_triage.shared_pathways.query_builders.count_by_dimension_metric_query_builder import (
    CountByDimensionMetricParams,
)
from recidiviz.case_triage.shared_pathways.query_builders.metric_query_builder import (
    MetricQueryBuilder,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.public_pathways.schema import (
    MetricMetadata,
    PublicPrisonAdmissionsAndReleases,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.public_pathways.metrics.metric_query_builders import (
    ALL_PUBLIC_PATHWAYS_METRICS_BY_NAME,
    config_path,
)
from recidiviz.tests.public_pathways.metrics.count_by_dimension_metrics_test import (
    PublicPathwaysCountByMetricTestBase,
)

EXPECTED_METADATA = {
    "lastUpdated": "2026-09-30",
    "dynamicFilterOptions": (
        '{"custody_type_id_name_map": ['
        '{"value": "incarcerated_individual", "label": "Incarcerated Individual"}, '
        '{"value": "incarcerated_parolee", "label": "Incarcerated Parolee"}]}'
    ),
}


def fetch(
    query_builder: MetricQueryBuilder, params: CountByDimensionMetricParams
) -> Dict[str, Any]:
    return dict(
        PathwaysMetricFetcher(
            state_code=StateCode.US_NY,
            enabled_states=["US_NY"],
            metric_metadata=MetricMetadata,
            schema_type=SchemaType.PUBLIC_PATHWAYS,
        ).fetch(query_builder, params)
    )


class TestAdmissionsAndReleasesOverTime(PublicPathwaysCountByMetricTestBase, TestCase):
    """Tests for the AdmissionsAndReleasesOverTime metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return ALL_PUBLIC_PATHWAYS_METRICS_BY_NAME["AdmissionsAndReleasesOverTime"]

    @property
    def all_expected_counts(
        self,
    ) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
        return {
            Dimension.MOVEMENT_DATE_EVENT_TYPE: [
                {"movementDate": "2023-01-01", "eventType": "admission", "count": 3},
                {"movementDate": "2024-01-01", "eventType": "admission", "count": 1},
                {"movementDate": "2024-01-01", "eventType": "release", "count": 1},
                {"movementDate": "2024-02-01", "eventType": "admission", "count": 1},
                {"movementDate": "2024-02-01", "eventType": "release", "count": 2},
            ]
        }

    @property
    def expected_metadata(self) -> Dict[str, Any]:
        return EXPECTED_METADATA

    def test_custody_status_filter_narrows_both_series(self) -> None:
        results = fetch(
            self.query_builder,
            CountByDimensionMetricParams(
                group=Dimension.MOVEMENT_DATE_EVENT_TYPE,
                filters={Dimension.CUSTODY_STATUS: ["incarcerated_parolee"]},
            ),
        )

        self.assertEqual(
            [
                {"movementDate": "2023-01-01", "eventType": "admission", "count": 1},
                {"movementDate": "2024-02-01", "eventType": "admission", "count": 1},
                {"movementDate": "2024-02-01", "eventType": "release", "count": 1},
            ],
            results["data"],
        )


class TestAdmissionsAndReleasesByDimensionCount(
    PublicPathwaysCountByMetricTestBase, TestCase
):
    """Tests for the AdmissionsAndReleasesByDimensionCount metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return ALL_PUBLIC_PATHWAYS_METRICS_BY_NAME[
            "AdmissionsAndReleasesByDimensionCount"
        ]

    @property
    def all_expected_counts(
        self,
    ) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
        # Admission type and release type both read movement_type, and the
        # fixture has a release that shares drug_treatment_judicially_sanctioned
        # with an admission. Each grouping still counts only its own event type.
        return {
            Dimension.CUSTODY_STATUS: [
                {
                    "eventType": "admission",
                    "custodyStatus": "incarcerated_individual",
                    "count": 3,
                },
                {
                    "eventType": "admission",
                    "custodyStatus": "incarcerated_parolee",
                    "count": 2,
                },
                {
                    "eventType": "release",
                    "custodyStatus": "incarcerated_individual",
                    "count": 2,
                },
                {
                    "eventType": "release",
                    "custodyStatus": "incarcerated_parolee",
                    "count": 1,
                },
            ],
            Dimension.ADMISSION_TYPE: [
                {
                    "eventType": "admission",
                    "custodyStatus": "incarcerated_individual",
                    "admissionType": "court_commitment",
                    "count": 2,
                },
                {
                    "eventType": "admission",
                    "custodyStatus": "incarcerated_individual",
                    "admissionType": "returned_parole_violator",
                    "count": 1,
                },
                {
                    "eventType": "admission",
                    "custodyStatus": "incarcerated_parolee",
                    "admissionType": "drug_treatment_judicially_sanctioned",
                    "count": 1,
                },
                {
                    "eventType": "admission",
                    "custodyStatus": "incarcerated_parolee",
                    "admissionType": "drug_treatment_parole_violation",
                    "count": 1,
                },
            ],
            Dimension.RELEASE_TYPE: [
                {
                    "eventType": "release",
                    "custodyStatus": "incarcerated_individual",
                    "releaseType": "max_expiration",
                    "count": 1,
                },
                {
                    "eventType": "release",
                    "custodyStatus": "incarcerated_individual",
                    "releaseType": "parole",
                    "count": 1,
                },
                {
                    "eventType": "release",
                    "custodyStatus": "incarcerated_parolee",
                    "releaseType": "drug_treatment_judicially_sanctioned",
                    "count": 1,
                },
            ],
            Dimension.COMMUNITY_SUPERVISION: [
                {
                    "eventType": "release",
                    "communitySupervision": "not_applicable",
                    "count": 1,
                },
                {
                    "eventType": "release",
                    "communitySupervision": "not_released_to_community_supervision",
                    "count": 1,
                },
                {
                    "eventType": "release",
                    "communitySupervision": "released_to_community_supervision",
                    "count": 1,
                },
            ],
        }

    @property
    def expected_metadata(self) -> Dict[str, Any]:
        return EXPECTED_METADATA

    def test_custody_status_filter_narrows_a_grouping(self) -> None:
        results = fetch(
            self.query_builder,
            CountByDimensionMetricParams(
                group=Dimension.ADMISSION_TYPE,
                filters={Dimension.CUSTODY_STATUS: ["incarcerated_parolee"]},
            ),
        )

        self.assertEqual(
            [
                {
                    "eventType": "admission",
                    "custodyStatus": "incarcerated_parolee",
                    "admissionType": "drug_treatment_judicially_sanctioned",
                    "count": 1,
                },
                {
                    "eventType": "admission",
                    "custodyStatus": "incarcerated_parolee",
                    "admissionType": "drug_treatment_parole_violation",
                    "count": 1,
                },
            ],
            results["data"],
        )


class TestCompositeDimensionParsing(TestCase):
    """Tests for how the config parser reads a composite dimension."""

    def test_composite_dimension_filters_on_one_column(self) -> None:
        collection = ALL_PUBLIC_PATHWAYS_METRICS_BY_NAME[
            "AdmissionsAndReleasesByDimensionCount"
        ].dimension_mapping_collection

        filter_column = collection.columns_for_dimension_operation(
            DimensionOperation.FILTER, Dimension.ADMISSION_TYPE
        )
        group_columns = collection.columns_for_dimension_operation(
            DimensionOperation.GROUP, Dimension.ADMISSION_TYPE
        )

        self.assertEqual("movement_type", filter_column.name)
        self.assertEqual(
            ["event_type", "custody_status", "admission_type"],
            [column.name for column in group_columns],
        )

    def test_group_only_dimension_is_not_filterable(self) -> None:
        collection = ALL_PUBLIC_PATHWAYS_METRICS_BY_NAME[
            "AdmissionsAndReleasesOverTime"
        ].dimension_mapping_collection

        self.assertNotIn(
            Dimension.MOVEMENT_DATE_EVENT_TYPE,
            collection.operable_map[DimensionOperation.FILTER],
        )

    def test_applies_to_without_group_columns_is_rejected(self) -> None:
        parser = MetricQueryBuilderConfigParser(config_path, SchemaType.PUBLIC_PATHWAYS)

        with self.assertRaisesRegex(ValueError, r"\[admission_type\]"):
            parser.parse_dimension_mapping(
                PublicPrisonAdmissionsAndReleases,
                "admission_type",
                {
                    "operations": "ALL",
                    "column": "movement_type",
                    "applies_to": {"movement_category": "admission"},
                },
            )
