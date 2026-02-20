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
"""This class implements test helpers for Public Pathways metrics."""
import abc
import csv
import os
from typing import Dict, List, Optional
from unittest import TestCase

import pytest
from sqlalchemy.engine import Engine

from recidiviz.case_triage.shared_pathways.metric_fetcher import (
    MetricNotEnabledError,
    PathwaysMetricFetcher,
)
from recidiviz.case_triage.shared_pathways.query_builders.metric_query_builder import (
    FetchMetricParams,
    MetricQueryBuilder,
)
from recidiviz.case_triage.shared_pathways.utils import get_metrics_for_entity
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.public_pathways.schema import (
    MetricMetadata,
    PublicPathwaysBase,
    PublicPrisonPopulationOverTime,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.public_pathways.enabled_metrics import (
    ALL_PUBLIC_PATHWAYS_METRICS_BY_STATE_CODE_BY_NAME,
)
from recidiviz.public_pathways.metrics.metric_query_builders import (
    ALL_PUBLIC_PATHWAYS_METRICS_BY_NAME,
)
from recidiviz.tests.public_pathways import fixtures
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult


def load_metrics_fixture(
    model: PublicPathwaysBase, filename: Optional[str] = None
) -> List[Dict]:
    filename = f"{model.__tablename__}.csv" if filename is None else filename
    fixture_path = os.path.join(os.path.dirname(fixtures.__file__), filename)
    results = []
    with open(fixture_path, "r", encoding="UTF-8") as fixture_file:
        reader = csv.DictReader(fixture_file)
        for row in reader:
            results.append(row)

    return results


@pytest.mark.uses_db
class PublicPathwaysMetricTestBase:
    """Base class for testing Public Pathways metrics."""

    # Stores the location of the postgres DB for this test run
    postgres_launch_result: OnDiskPostgresLaunchResult

    @property
    @abc.abstractmethod
    def test(self) -> TestCase:
        ...

    @property
    def schema(self) -> PublicPathwaysBase:
        return self.query_builder.model

    @property
    @abc.abstractmethod
    def query_builder(self) -> MetricQueryBuilder:
        ...

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey(
            SchemaType.PUBLIC_PATHWAYS, db_name="us_ny"
        )
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.database_key
        )

        with SessionFactory.using_database(self.database_key) as session:
            for metric in load_metrics_fixture(self.schema):
                session.add(self.schema(**metric))
            for metric_metadata in load_metrics_fixture(MetricMetadata):
                session.add(MetricMetadata(**metric_metadata))

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )


class TestPublicPathwaysMetricHelpers(TestCase):
    """Tests for the metric helpers"""

    def test_get_metrics_by_entity(self) -> None:
        self.assertEqual(
            [
                ALL_PUBLIC_PATHWAYS_METRICS_BY_NAME["PrisonPopulationOverTime"],
            ],
            get_metrics_for_entity(PublicPrisonPopulationOverTime),
        )


@pytest.mark.uses_db
class TestPublicPathwaysMetricFetcher(TestCase):
    """Tests for the Public PathwaysMetricFetcher"""

    # Stores the location of the postgres DB for this test run
    postgres_launch_result: Optional[str]
    engine: Engine

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey(
            SchemaType.PUBLIC_PATHWAYS, db_name="us_ny"
        )
        self.engine = local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result,
            self.database_key,
            create_tables=False,
        )

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def test_enabled_metric_without_table(self) -> None:
        metric_fetcher = PathwaysMetricFetcher(
            state_code=StateCode.US_NY,
            enabled_states=["US_NY"],
            metric_metadata=MetricMetadata,
            schema_type=SchemaType.PUBLIC_PATHWAYS,
        )

        with self.assertRaises(MetricNotEnabledError):
            metric_fetcher.fetch(
                ALL_PUBLIC_PATHWAYS_METRICS_BY_STATE_CODE_BY_NAME[StateCode.US_NY][
                    "PrisonPopulationOverTime"
                ],
                FetchMetricParams(),
            )
