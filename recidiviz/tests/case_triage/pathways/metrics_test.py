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

from recidiviz.case_triage.pathways.dimension import Dimension
from recidiviz.case_triage.pathways.metric_fetcher import PathwaysMetricFetcher
from recidiviz.case_triage.pathways.metric_queries import (
    CountByDimensionMetricParams,
    FetchMetricParams,
    LibertyToPrisonTransitionsCount,
    MetricQueryBuilder,
    PrisonToSupervisionTransitionsCount,
    PrisonToSupervisionTransitionsPersonLevel,
    SupervisionToLibertyTransitionsCount,
    SupervisionToPrisonTransitionsCount,
    TimePeriod,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.pathways.schema import (
    LibertyToPrisonTransitions,
    PathwaysBase,
    PrisonToSupervisionTransitions,
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
        metric_fetcher = PathwaysMetricFetcher(StateCode.US_TN)
        for dimension_mapping in self.query_builder.dimension_mappings:
            results[dimension_mapping.dimension] = metric_fetcher.fetch(
                self.query_builder,
                self.query_builder.build_params({"group": dimension_mapping.dimension}),
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
        metric_fetcher = PathwaysMetricFetcher(StateCode.US_TN)
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
        results = PathwaysMetricFetcher(state_code=StateCode.US_TN).fetch(
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
        results = PathwaysMetricFetcher(StateCode.US_TN).fetch(
            self.query_builder,
            CountByDimensionMetricParams(
                group=Dimension.GENDER, time_period=TimePeriod.MONTHS_0_6
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
        results = PathwaysMetricFetcher(state_code=StateCode.US_TN).fetch(
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
        results = PathwaysMetricFetcher(StateCode.US_TN).fetch(
            self.query_builder,
            CountByDimensionMetricParams(
                group=Dimension.FACILITY,
                time_period=TimePeriod.MONTHS_0_6,
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
        results = PathwaysMetricFetcher(state_code=StateCode.US_TN).fetch(
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
        results = PathwaysMetricFetcher(StateCode.US_TN).fetch(
            self.query_builder,
            CountByDimensionMetricParams(
                group=Dimension.GENDER,
                time_period=TimePeriod.MONTHS_0_6,
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
        results = PathwaysMetricFetcher(state_code=StateCode.US_TN).fetch(
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
        results = PathwaysMetricFetcher(StateCode.US_TN).fetch(
            self.query_builder,
            FetchMetricParams(time_period=TimePeriod.MONTHS_0_6),
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
        results = PathwaysMetricFetcher(state_code=StateCode.US_TN).fetch(
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
        results = PathwaysMetricFetcher(StateCode.US_TN).fetch(
            self.query_builder,
            CountByDimensionMetricParams(
                group=Dimension.SUPERVISION_DISTRICT,
                time_period=TimePeriod.MONTHS_0_6,
            ),
        )

        self.test.assertEqual(
            [
                {"supervision_district": "DISTRICT_10", "count": 2},
                {"supervision_district": "DISTRICT_18", "count": 2},
            ],
            results,
        )
