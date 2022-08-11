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
"""This class implements test helpers for Pathways metrics."""
import abc
import csv
import os
from typing import Dict, List, Optional
from unittest import TestCase

import pytest

from recidiviz.case_triage.pathways.enabled_metrics import get_metrics_for_entity
from recidiviz.case_triage.pathways.metrics.metric_query_builders import (
    ALL_METRICS_BY_NAME,
)
from recidiviz.case_triage.pathways.metrics.query_builders.metric_query_builder import (
    MetricQueryBuilder,
)
from recidiviz.persistence.database.schema.pathways.schema import (
    LibertyToPrisonTransitions,
    MetricMetadata,
    PathwaysBase,
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
    def schema(self) -> PathwaysBase:
        return self.query_builder.model

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
            for metric_metadata in load_metrics_fixture(MetricMetadata):
                session.add(MetricMetadata(**metric_metadata))

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )


class TestMetricHelpers(TestCase):
    def test_get_metrics_by_entity(self) -> None:
        self.assertEqual(
            [
                ALL_METRICS_BY_NAME["LibertyToPrisonTransitionsOverTime"],
                ALL_METRICS_BY_NAME["LibertyToPrisonTransitionsCount"],
            ],
            get_metrics_for_entity(LibertyToPrisonTransitions),
        )
