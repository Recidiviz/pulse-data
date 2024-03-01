#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2024 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  ============================================================================
"""Tests for the Workflows querier"""
import csv
import json
import os
from typing import Dict, List, Optional
from unittest import TestCase

import pytest

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.workflows.schema import (
    Opportunity,
    WorkflowsBase,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.workflows import fixtures
from recidiviz.workflows.querier.querier import WorkflowsQuerier
from recidiviz.workflows.types import OpportunityInfo


def load_model_fixture(
    model: WorkflowsBase, filename: Optional[str] = None
) -> List[Dict]:
    filename = f"{model.__tablename__}.csv" if filename is None else filename
    fixture_path = os.path.join(os.path.dirname(fixtures.__file__), filename)
    results = []
    with open(fixture_path, "r", encoding="UTF-8") as fixture_file:
        reader = csv.DictReader(fixture_file)
        for row in reader:
            for k, v in row.items():
                if k in {"snooze"} and v != "":
                    row[k] = json.loads(v)
                if v == "":
                    row[k] = None
            results.append(row)

    return results


@pytest.mark.uses_db
class TestOutliersQuerier(TestCase):
    """Implements tests for the OutliersQuerier."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey(SchemaType.WORKFLOWS, db_name="us_id")
        local_persistence_helpers.use_on_disk_postgresql_database(self.database_key)

        with SessionFactory.using_database(self.database_key) as session:
            # Restart the sequence in tests as per https://stackoverflow.com/questions/46841912/sqlalchemy-revert-auto-increment-during-testing-pytest
            # session.execute("""ALTER SEQUENCE configurations_id_seq RESTART WITH 1;""")

            for opportunity in load_model_fixture(Opportunity):
                session.add(Opportunity(**opportunity))

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_get_opportunities(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_opportunities()

        expected = [
            OpportunityInfo(state_code="US_ID", opportunity_type="usIdCrcWorkRelease"),
            OpportunityInfo(
                state_code="US_ID",
                opportunity_type="usIdFastFTRD",
                gating_feature_variant="someFeatureVariant",
            ),
            OpportunityInfo(
                state_code="US_ID",
                opportunity_type="usIdSLD",
                gating_feature_variant="someOtherFeatureVariant",
            ),
        ]

        self.assertCountEqual(expected, actual)
