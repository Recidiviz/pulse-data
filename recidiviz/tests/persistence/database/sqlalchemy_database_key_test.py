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

from recidiviz.persistence.database.base_schema import CaseTriageBase, OperationsBase
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


class SQLAlchemyDatabaseKeyTest(TestCase):
    """Tests for SQLAlchemyDatabaseKey."""

    def test_for_schema_throws_multi_db(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "^Must provide db name information to create a PATHWAYS database key.$",
        ):
            _ = SQLAlchemyDatabaseKey.for_schema(SchemaType.PATHWAYS)

    def test_key_attributes_operations(self) -> None:
        key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)

        self.assertEqual(key.declarative_meta, OperationsBase)

        self.assertTrue(os.path.exists(key.alembic_file))
        self.assertTrue(key.alembic_file.endswith("migrations/operations_alembic.ini"))

        self.assertTrue(os.path.exists(key.migrations_location))
        self.assertTrue(key.migrations_location.endswith("/migrations/operations"))

        self.assertEqual(key.isolation_level, None)

    def test_key_attributes_case_triage(self) -> None:
        key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)

        self.assertEqual(key.declarative_meta, CaseTriageBase)

        self.assertTrue(os.path.exists(key.alembic_file))
        self.assertTrue(key.alembic_file.endswith("migrations/case_triage_alembic.ini"))

        self.assertTrue(os.path.exists(key.migrations_location))
        self.assertTrue(key.migrations_location.endswith("/migrations/case_triage"))

        self.assertEqual(key.isolation_level, None)

    def test_canonical_for_schema_local_only(self) -> None:
        _ = SQLAlchemyDatabaseKey.canonical_for_schema(
            schema_type=SchemaType.OPERATIONS
        )

        with patch(
            "recidiviz.utils.environment.get_gcp_environment",
            Mock(return_value="production"),
        ):
            with self.assertRaises(RuntimeError):
                _ = SQLAlchemyDatabaseKey.canonical_for_schema(
                    schema_type=SchemaType.OPERATIONS
                )

        _ = SQLAlchemyDatabaseKey.canonical_for_schema(
            schema_type=SchemaType.OPERATIONS
        )
