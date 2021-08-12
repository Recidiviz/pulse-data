# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for SQLAlchemyDatabaseKey."""
import os
from unittest import TestCase
from unittest.mock import Mock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.direct_ingest_instance import (
    DirectIngestInstance,
)
from recidiviz.persistence.database.base_schema import CaseTriageBase, StateBase
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


class SQLAlchemyDatabaseKeyTest(TestCase):
    """Tests for SQLAlchemyDatabaseKey."""

    def test_for_schema_throws_state(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "^Must provide db name information to create a STATE database key.$",
        ):
            _ = SQLAlchemyDatabaseKey.for_schema(SchemaType.STATE)

    def test_for_state_code(self) -> None:
        primary = DirectIngestInstance.PRIMARY.database_key_for_state(StateCode.US_MN)
        secondary = DirectIngestInstance.SECONDARY.database_key_for_state(
            StateCode.US_MN
        )

        self.assertEqual(
            SQLAlchemyDatabaseKey(
                schema_type=SchemaType.STATE, db_name="us_mn_primary"
            ),
            primary,
        )

        self.assertEqual(
            SQLAlchemyDatabaseKey(
                schema_type=SchemaType.STATE, db_name="us_mn_secondary"
            ),
            secondary,
        )

    def test_key_attributes_state(self) -> None:
        key = DirectIngestInstance.PRIMARY.database_key_for_state(StateCode.US_MN)

        self.assertEqual(key.declarative_meta, StateBase)

        self.assertTrue(os.path.exists(key.alembic_file))
        self.assertTrue(key.alembic_file.endswith("migrations/state_alembic.ini"))

        self.assertTrue(os.path.exists(key.migrations_location))
        self.assertTrue(key.migrations_location.endswith("/migrations/state"))

        self.assertEqual(key.isolation_level, "SERIALIZABLE")

    def test_key_attributes_case_triage(self) -> None:
        key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)

        self.assertEqual(key.declarative_meta, CaseTriageBase)

        self.assertTrue(os.path.exists(key.alembic_file))
        self.assertTrue(key.alembic_file.endswith("migrations/case_triage_alembic.ini"))

        self.assertTrue(os.path.exists(key.migrations_location))
        self.assertTrue(key.migrations_location.endswith("/migrations/case_triage"))

        self.assertEqual(key.isolation_level, None)

    def test_canonical_for_schema_local_only(self) -> None:
        _ = SQLAlchemyDatabaseKey.canonical_for_schema(schema_type=SchemaType.STATE)

        with patch(
            "recidiviz.utils.environment.get_gcp_environment",
            Mock(return_value="production"),
        ):
            with self.assertRaises(RuntimeError):
                _ = SQLAlchemyDatabaseKey.canonical_for_schema(
                    schema_type=SchemaType.STATE
                )

        _ = SQLAlchemyDatabaseKey.canonical_for_schema(schema_type=SchemaType.STATE)
