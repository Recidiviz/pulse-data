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
from typing import Dict, List, Optional, Union
from unittest.case import TestCase

import pytest

from recidiviz.case_triage.pathways.dimension import Dimension
from recidiviz.case_triage.pathways.metric_fetcher import PathwaysMetricFetcher
from recidiviz.case_triage.pathways.metric_mappers import (
    CountByDimensionMetricMapper,
    LibertyToPrisonTransitionsCount,
    PrisonToSupervisionTransitionsCount,
)
from recidiviz.case_triage.pathways.metric_queries import FetchMetricParams
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.pathways.schema import (
    LibertyToPrisonTransitions,
    PathwaysBase,
    PrisonToSupervisionTransitions,
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
    def mapper(self) -> CountByDimensionMetricMapper:
        ...

    @property
    @abc.abstractmethod
    def all_expected_counts(self) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
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

    def test_metrics_base(self) -> None:
        results = {}
        metric_fetcher = PathwaysMetricFetcher(StateCode.US_TN)
        for dimension_mapping in self.mapper.dimension_mappings:
            results[dimension_mapping.dimension] = metric_fetcher.fetch(
                self.mapper,
                FetchMetricParams(group=dimension_mapping.dimension),
            )

        self.test.assertEqual(self.all_expected_counts, results)


class TestLibertyToPrisonTransitions(PathwaysMetricTestBase, TestCase):
    """Test for LibertyToPrisonTransitions metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def schema(self) -> PathwaysBase:
        return LibertyToPrisonTransitions

    @property
    def mapper(self) -> CountByDimensionMetricMapper:
        return LibertyToPrisonTransitionsCount

    @property
    def all_expected_counts(self) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
        return {
            Dimension.YEAR_MONTH: [
                {"year": 2022, "month": 1, "count": 1},
                {"year": 2022, "month": 2, "count": 1},
                {"year": 2022, "month": 3, "count": 3},
            ],
            Dimension.GENDER: [
                {"gender": "FEMALE", "count": 1},
                {"gender": "MALE", "count": 4},
            ],
            Dimension.AGE_GROUP: [
                {"age_group": "20-25", "count": 1},
                {"age_group": "60+", "count": 4},
            ],
            Dimension.RACE: [
                {"race": "ASIAN", "count": 1},
                {"race": "BLACK", "count": 2},
                {"race": "WHITE", "count": 2},
            ],
            Dimension.JUDICIAL_DISTRICT: [
                {"judicial_district": "D1", "count": 4},
                {"judicial_district": "D2", "count": 1},
            ],
            Dimension.PRIOR_LENGTH_OF_INCARCERATION: [
                {
                    "prior_length_of_incarceration": "months_0_3",
                    "count": 4,
                },
                {
                    "prior_length_of_incarceration": "months_3_6",
                    "count": 1,
                },
            ],
        }

    def test_metrics_filter(self) -> None:
        results = PathwaysMetricFetcher(state_code=StateCode.US_TN).fetch(
            self.mapper,
            FetchMetricParams(
                group=Dimension.GENDER,
                filters={
                    Dimension.RACE: ["WHITE"],
                },
            ),
        )

        self.test.assertEqual([{"gender": "MALE", "count": 2}], results)

    def test_filter_since(self) -> None:
        results = PathwaysMetricFetcher(StateCode.US_TN).fetch(
            self.mapper,
            FetchMetricParams(group=Dimension.GENDER, since="2022-03-01"),
        )

        self.test.assertEqual(
            [{"gender": "FEMALE", "count": 1}, {"gender": "MALE", "count": 2}], results
        )


class TestPrisonToSupervisionTransitions(PathwaysMetricTestBase, TestCase):
    """Test for PrisonToSupervisionTransitions metric."""

    @property
    def test(self) -> TestCase:
        return self

    @property
    def schema(self) -> PathwaysBase:
        return PrisonToSupervisionTransitions

    @property
    def mapper(self) -> CountByDimensionMetricMapper:
        return PrisonToSupervisionTransitionsCount

    @property
    def all_expected_counts(self) -> Dict[Dimension, List[Dict[str, Union[str, int]]]]:
        return {
            Dimension.YEAR_MONTH: [
                {"year": 2022, "month": 1, "count": 1},
                {"year": 2022, "month": 2, "count": 1},
                {"year": 2022, "month": 3, "count": 3},
            ],
            Dimension.GENDER: [
                {"gender": "FEMALE", "count": 1},
                {"gender": "MALE", "count": 4},
            ],
            Dimension.AGE_GROUP: [
                {"age_group": "20-25", "count": 1},
                {"age_group": "60+", "count": 4},
            ],
            Dimension.FACILITY: [
                {"facility": "ABC", "count": 3},
                {"facility": "DEF", "count": 2},
            ],
        }

    def test_metrics_filter(self) -> None:
        results = PathwaysMetricFetcher(state_code=StateCode.US_TN).fetch(
            self.mapper,
            FetchMetricParams(
                group=Dimension.GENDER,
                filters={
                    Dimension.FACILITY: ["ABC"],
                },
            ),
        )

        self.test.assertEqual(
            [{"gender": "FEMALE", "count": 1}, {"gender": "MALE", "count": 2}], results
        )

    def test_filter_since(self) -> None:
        results = PathwaysMetricFetcher(StateCode.US_TN).fetch(
            self.mapper,
            FetchMetricParams(group=Dimension.GENDER, since="2022-03-01"),
        )

        self.test.assertEqual(
            [{"gender": "FEMALE", "count": 1}, {"gender": "MALE", "count": 2}], results
        )
