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
"""This class implements tests for Pathways metrics."""
import abc
import csv
import os
from datetime import date
from typing import Dict, List, Optional, Union
from unittest.case import TestCase

import pytest

from recidiviz.case_triage.pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.pathways.dimensions.time_period import TimePeriod
from recidiviz.case_triage.pathways.metric_fetcher import PathwaysMetricFetcher
from recidiviz.case_triage.pathways.metric_queries import (
    CountByDimensionMetricParams,
    DimensionOperation,
    FetchMetricParams,
    LibertyToPrisonTransitionsCount,
    MetricQueryBuilder,
    PrisonPopulationByDimensionCount,
    PrisonPopulationOverTimeCount,
    PrisonPopulationPersonLevelMetric,
    PrisonPopulationProjectionMetric,
    PrisonToSupervisionTransitionsCount,
    PrisonToSupervisionTransitionsPersonLevel,
    SupervisionPopulationByDimensionCount,
    SupervisionPopulationOverTimeCount,
    SupervisionPopulationProjectionMetric,
    SupervisionToLibertyTransitionsCount,
    SupervisionToPrisonTransitionsCount,
)
from recidiviz.case_triage.pathways.metrics import get_metrics_for_entity
from recidiviz.common.constants.states import _FakeStateCode
from recidiviz.persistence.database.schema.pathways.schema import (
    LibertyToPrisonTransitions,
    PathwaysBase,
    PrisonPopulationByDimension,
    PrisonPopulationOverTime,
    PrisonPopulationPersonLevel,
    PrisonPopulationProjection,
    PrisonToSupervisionTransitions,
    SupervisionPopulationByDimension,
    SupervisionPopulationOverTime,
    SupervisionPopulationProjection,
    SupervisionToLibertyTransitions,
    SupervisionToPrisonTransitions,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.case_triage.pathways import fixtures
from recidiviz.tools.postgres import local_postgres_helpers


def load_metrics_fixture(model: PathwaysBase, filename: str = None) -> List[Dict]:
    filename = f"{model.__tablename__}.csv" if filename is None else filename
    fixture_path = os.path.join(os.path.dirname(fixtures.__file__), filename)
    results = []
    with open(fixture_path, "r", encoding="UTF-8") as fixture_file:
        reader = csv.DictReader(fixture_file)
        for row in reader:
            results.append(row)

    return results


@pytest.mark.uses_db
class PathwaysMetricTestBase:
    """Base class for testing Pathways metrics."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @property
    @abc.abstractmethod
    def test(self) -> TestCase:
        ...

    @property
    @abc.abstractmethod
    def schema(self) -> PathwaysBase:
        ...

    @property
    @abc.abstractmethod
    def query_builder(self) -> MetricQueryBuilder:
        ...

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey(SchemaType.PATHWAYS, db_name="us_tn")
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)

        with SessionFactory.using_database(self.database_key) as session:
            for metric in load_metrics_fixture(self.schema):
                session.add(self.schema(**metric))

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )


class PathwaysCountByMetricTestBase(PathwaysMetricTestBase):
    @property
    @abc.abstractmethod
    def all_expected_counts(self) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
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
            self.test.assertEqual(expected_counts, results.get(dimension))

        self.test.assertEqual(self.all_expected_counts, results)


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


class TestLibertyToPrisonTransitionsCount(PathwaysCountByMetricTestBase, TestCase):
    """Test for LibertyToPrisonTransitionsCount metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def schema(self) -> PathwaysBase:
        return LibertyToPrisonTransitions

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return LibertyToPrisonTransitionsCount

    @property
    def all_expected_counts(self) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
        return {
            Dimension.YEAR_MONTH: [
                {"year": 2021, "month": 1, "count": 1},
                {"year": 2022, "month": 1, "count": 1},
                {"year": 2022, "month": 2, "count": 1},
                {"year": 2022, "month": 3, "count": 3},
            ],
            Dimension.GENDER: [
                {"gender": "FEMALE", "count": 1},
                {"gender": "MALE", "count": 5},
            ],
            Dimension.AGE_GROUP: [
                {"age_group": "20-25", "count": 2},
                {"age_group": "60+", "count": 4},
            ],
            Dimension.RACE: [
                {"race": "ASIAN", "count": 1},
                {"race": "BLACK", "count": 2},
                {"race": "WHITE", "count": 3},
            ],
            Dimension.JUDICIAL_DISTRICT: [
                {"judicial_district": "D1", "count": 4},
                {"judicial_district": "D2", "count": 1},
                {"judicial_district": "D3", "count": 1},
            ],
            Dimension.PRIOR_LENGTH_OF_INCARCERATION: [
                {
                    "prior_length_of_incarceration": "months_0_3",
                    "count": 5,
                },
                {
                    "prior_length_of_incarceration": "months_3_6",
                    "count": 1,
                },
            ],
        }

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

        self.test.assertEqual([{"gender": "MALE", "count": 3}], results)

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
            [{"gender": "FEMALE", "count": 1}, {"gender": "MALE", "count": 4}], results
        )


class TestPrisonToSupervisionTransitionsCount(PathwaysCountByMetricTestBase, TestCase):
    """Test for PrisonToSupervisionTransitionsCount metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def schema(self) -> PathwaysBase:
        return PrisonToSupervisionTransitions

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return PrisonToSupervisionTransitionsCount

    @property
    def all_expected_counts(self) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
        return {
            Dimension.YEAR_MONTH: [
                {"year": 2021, "month": 1, "count": 1},
                {"year": 2022, "month": 1, "count": 1},
                {"year": 2022, "month": 2, "count": 2},
                {"year": 2022, "month": 3, "count": 3},
            ],
            Dimension.GENDER: [
                {"gender": "FEMALE", "count": 1},
                {"gender": "MALE", "count": 6},
            ],
            Dimension.AGE_GROUP: [
                {"age_group": "20-25", "count": 2},
                {"age_group": "60+", "count": 5},
            ],
            Dimension.FACILITY: [
                {"facility": "ABC", "count": 3},
                {"facility": "DEF", "count": 3},
                {"facility": "GHI", "count": 1},
            ],
            Dimension.RACE: [
                {"race": "ASIAN", "count": 1},
                {"race": "BLACK", "count": 2},
                {"race": "WHITE", "count": 4},
            ],
        }

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
            [{"gender": "FEMALE", "count": 1}, {"gender": "MALE", "count": 2}], results
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
            [{"facility": "ABC", "count": 3}, {"facility": "DEF", "count": 3}], results
        )


class TestSupervisionToPrisonTransitionsCount(PathwaysCountByMetricTestBase, TestCase):
    """Test for SupervisionToPrisonTransitionsCount metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def schema(self) -> PathwaysBase:
        return SupervisionToPrisonTransitions

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return SupervisionToPrisonTransitionsCount

    @property
    def all_expected_counts(self) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
        return {
            Dimension.YEAR_MONTH: [
                {"year": 2021, "month": 1, "count": 1},
                {"year": 2022, "month": 1, "count": 1},
                {"year": 2022, "month": 2, "count": 2},
                {"year": 2022, "month": 3, "count": 1},
            ],
            Dimension.GENDER: [
                {"gender": "FEMALE", "count": 2},
                {"gender": "MALE", "count": 2},
                {"gender": "NON_BINARY", "count": 1},
            ],
            Dimension.AGE_GROUP: [
                {"age_group": "20-25", "count": 1},
                {"age_group": "26-35", "count": 3},
                {"age_group": "60+", "count": 1},
            ],
            Dimension.RACE: [
                {"race": "ASIAN", "count": 1},
                {"race": "BLACK", "count": 1},
                {"race": "WHITE", "count": 3},
            ],
            Dimension.SUPERVISION_TYPE: [
                {"supervision_type": "PAROLE", "count": 2},
                {"supervision_type": "PROBATION", "count": 3},
            ],
            Dimension.SUPERVISION_LEVEL: [
                {"supervision_level": "MAXIMUM", "count": 1},
                {"supervision_level": "MEDIUM", "count": 2},
                {"supervision_level": "MINIMUM", "count": 2},
            ],
            Dimension.SUPERVISION_DISTRICT: [
                {"supervision_district": "DISTRICT_10", "count": 2},
                {"supervision_district": "DISTRICT_18", "count": 3},
            ],
            Dimension.DISTRICT: [
                {"district": "DISTRICT_10", "count": 2},
                {"district": "DISTRICT_18", "count": 3},
            ],
            Dimension.SUPERVISING_OFFICER: [
                {"supervising_officer": "3456", "count": 1},
                {"supervising_officer": "4567", "count": 1},
                {"supervising_officer": "7890", "count": 2},
                {"supervising_officer": "9999", "count": 1},
            ],
            Dimension.LENGTH_OF_STAY: [
                {"length_of_stay": "months_0_3", "count": 1},
                {"length_of_stay": "months_25_60", "count": 1},
                {"length_of_stay": "months_3_6", "count": 1},
                {"length_of_stay": "months_6_9", "count": 2},
            ],
        }

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
            results,
        )

    def test_filter_time_period(self) -> None:
        """Tests that person 5 is not included, as the transition occurred more than 6 months ago"""
        results = PathwaysMetricFetcher(_FakeStateCode.US_TN).fetch(
            self.query_builder,
            CountByDimensionMetricParams(
                group=Dimension.GENDER,
                filters={Dimension.TIME_PERIOD: [TimePeriod.MONTHS_0_6.value]},
            ),
        )

        self.test.assertEqual(
            [
                {"gender": "MALE", "count": 1},
            ],
            results,
        )


class TestPrisonToSupervisionTransitionsPersonLevel(
    PathwaysPersonLevelMetricTestBase, TestCase
):
    """Test for PrisonToSupervisionTransitionsPersonLevel metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def schema(self) -> PathwaysBase:
        return PrisonToSupervisionTransitions

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return PrisonToSupervisionTransitionsPersonLevel

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


class MetricHelpersTest(TestCase):
    def test_get_metrics_by_entity(self) -> None:
        self.assertEqual(
            [LibertyToPrisonTransitionsCount],
            get_metrics_for_entity(LibertyToPrisonTransitions),
        )


class TestSupervisionToLibertyTransitionsCount(PathwaysCountByMetricTestBase, TestCase):
    """Test for SupervisionToLibertyTransitionsCount metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def schema(self) -> PathwaysBase:
        return SupervisionToLibertyTransitions

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return SupervisionToLibertyTransitionsCount

    @property
    def all_expected_counts(self) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
        return {
            Dimension.YEAR_MONTH: [
                {"year": 2021, "month": 1, "count": 1},
                {"year": 2022, "month": 1, "count": 1},
                {"year": 2022, "month": 2, "count": 2},
                {"year": 2022, "month": 3, "count": 1},
            ],
            Dimension.AGE_GROUP: [
                {"age_group": "20-25", "count": 1},
                {"age_group": "26-35", "count": 3},
                {"age_group": "60+", "count": 1},
            ],
            Dimension.GENDER: [
                {"gender": "FEMALE", "count": 2},
                {"gender": "MALE", "count": 2},
                {"gender": "NON_BINARY", "count": 1},
            ],
            Dimension.LENGTH_OF_STAY: [
                {"length_of_stay": "months_0_3", "count": 1},
                {"length_of_stay": "months_24_36", "count": 1},
                {"length_of_stay": "months_3_6", "count": 1},
                {"length_of_stay": "months_6_9", "count": 2},
            ],
            Dimension.RACE: [
                {"race": "ASIAN", "count": 1},
                {"race": "BLACK", "count": 1},
                {"race": "WHITE", "count": 3},
            ],
            Dimension.SUPERVISION_TYPE: [
                {"supervision_type": "PAROLE", "count": 2},
                {"supervision_type": "PROBATION", "count": 3},
            ],
            Dimension.SUPERVISION_LEVEL: [
                {"supervision_level": "MAXIMUM", "count": 1},
                {"supervision_level": "MEDIUM", "count": 2},
                {"supervision_level": "MINIMUM", "count": 2},
            ],
            Dimension.SUPERVISION_DISTRICT: [
                {"supervision_district": "DISTRICT_10", "count": 2},
                {"supervision_district": "DISTRICT_18", "count": 2},
                {"supervision_district": "DISTRICT_20", "count": 1},
            ],
            # TODO(#13552): Remove this once FE uses supervision_district
            Dimension.DISTRICT: [
                {"district": "DISTRICT_10", "count": 2},
                {"district": "DISTRICT_18", "count": 2},
                {"district": "DISTRICT_20", "count": 1},
            ],
            Dimension.SUPERVISING_OFFICER: [
                {"supervising_officer": "3456", "count": 1},
                {"supervising_officer": "4567", "count": 1},
                {"supervising_officer": "7890", "count": 2},
                {"supervising_officer": "9999", "count": 1},
            ],
        }

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
            results,
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
                {"supervision_district": "DISTRICT_10", "count": 2},
                {"supervision_district": "DISTRICT_18", "count": 2},
            ],
            results,
        )


class TestSupervisionPopulationOverTimeCount(PathwaysCountByMetricTestBase, TestCase):
    """Test for SupervisionPopulationCount metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def schema(self) -> PathwaysBase:
        return SupervisionPopulationOverTime

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return SupervisionPopulationOverTimeCount

    @property
    def all_expected_counts(
        self,
    ) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
        return {
            Dimension.YEAR_MONTH: [
                {
                    "year": 2022,
                    "month": 1,
                    "count": 2,
                },
                {
                    "year": 2022,
                    "month": 2,
                    "count": 2,
                },
            ],
        }

    def test_metrics_filter(self) -> None:
        # TODO(#13950): Replace with StateCode
        results = PathwaysMetricFetcher(state_code=_FakeStateCode.US_TN).fetch(
            self.query_builder,
            CountByDimensionMetricParams(
                group=Dimension.YEAR_MONTH,
                filters={
                    Dimension.SUPERVISION_DISTRICT: ["OTHER"],
                },
            ),
        )

        self.test.assertEqual(
            [
                {
                    "year": 2022,
                    "month": 1,
                    "count": 1,
                },
                {
                    "year": 2022,
                    "month": 2,
                    "count": 1,
                },
            ],
            results,
        )

    def test_filter_time_period(self) -> None:
        # TODO(#13950): Replace with StateCode
        results = PathwaysMetricFetcher(_FakeStateCode.US_TN).fetch(
            self.query_builder,
            CountByDimensionMetricParams(
                group=Dimension.YEAR_MONTH,
                filters={Dimension.TIME_PERIOD: [TimePeriod.MONTHS_0_6.value]},
            ),
        )

        self.test.assertEqual(
            [
                {
                    "year": 2022,
                    "month": 2,
                    "count": 2,
                }
            ],
            results,
        )


class TestSupervisionPopulationByDimensionCount(
    PathwaysCountByMetricTestBase, TestCase
):
    """Test for SupervisionPopulationByDimensionCount metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def schema(self) -> PathwaysBase:
        return SupervisionPopulationByDimension

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return SupervisionPopulationByDimensionCount

    @property
    def all_expected_counts(
        self,
    ) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
        return {
            Dimension.SUPERVISION_LEVEL: [
                {"supervision_level": "HIGH", "count": 1},
                {"supervision_level": "MINIMUM", "count": 1},
            ],
            Dimension.DISTRICT: [
                {"district": "District 1", "count": 1},
                {"district": "District 2", "count": 1},
                {"district": "OTHER", "count": 1},
            ],
            Dimension.SUPERVISION_DISTRICT: [
                {"supervision_district": "District 1", "count": 1},
                {"supervision_district": "District 2", "count": 1},
                {"supervision_district": "OTHER", "count": 1},
            ],
            Dimension.RACE: [
                {"race": "HISPANIC", "count": 1},
                {"race": "WHITE", "count": 1},
            ],
        }

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
            results,
        )


class TestPrisonPopulationOverTimeCount(PathwaysCountByMetricTestBase, TestCase):
    """Test for PrisonPopulationOverTimeCount metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def schema(self) -> PathwaysBase:
        return PrisonPopulationOverTime

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return PrisonPopulationOverTimeCount

    @property
    def all_expected_counts(
        self,
    ) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
        return {
            Dimension.YEAR_MONTH: [
                {
                    "year": 2022,
                    "month": 1,
                    "count": 2,
                },
                {
                    "year": 2022,
                    "month": 2,
                    "count": 2,
                },
                {
                    "year": 2022,
                    "month": 3,
                    "count": 2,
                },
            ],
        }


class TestPrisonPopulationByDimensionCount(PathwaysCountByMetricTestBase, TestCase):
    """Test for PrisonPopulationByDimensionCount metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def schema(self) -> PathwaysBase:
        return PrisonPopulationByDimension

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return PrisonPopulationByDimensionCount

    @property
    def all_expected_counts(
        self,
    ) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
        return {
            Dimension.AGE_GROUP: [
                {"age_group": "25-29", "count": 1},
                {"age_group": "60+", "count": 3},
            ],
            Dimension.FACILITY: [
                {"facility": "F1", "count": 4},
            ],
            Dimension.GENDER: [
                {"gender": "FEMALE", "count": 2},
                {"gender": "MALE", "count": 2},
            ],
            Dimension.ADMISSION_REASON: [
                {"admission_reason": "NEW_ADMISSION", "count": 1},
                {"admission_reason": "REVOCATION", "count": 2},
                {"admission_reason": "UNKNOWN", "count": 1},
            ],
            Dimension.RACE: [
                {"race": "BLACK", "count": 2},
                {"race": "WHITE", "count": 2},
            ],
            Dimension.LENGTH_OF_STAY: [{"count": 4, "length_of_stay": "months_0_3"}],
        }


class TestPrisonPopulationProjectionMetric(
    PathwaysPopulationProjectionMetricTestBase, TestCase
):
    """Test for PrisonPopulationProjection metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def schema(self) -> PathwaysBase:
        return PrisonPopulationProjection

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return PrisonPopulationProjectionMetric

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
    def schema(self) -> PathwaysBase:
        return SupervisionPopulationProjection

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return SupervisionPopulationProjectionMetric

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


class TestPrisonPopulationPersonLevel(PathwaysPersonLevelMetricTestBase, TestCase):
    """Test for PrisonPopulationPersonLevel metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def schema(self) -> PathwaysBase:
        return PrisonPopulationPersonLevel

    @property
    def query_builder(self) -> MetricQueryBuilder:
        return PrisonPopulationPersonLevelMetric

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
