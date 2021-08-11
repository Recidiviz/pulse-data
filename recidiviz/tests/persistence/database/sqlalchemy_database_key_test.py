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
from recidiviz.persistence.database import sqlalchemy_database_key
from recidiviz.persistence.database.base_schema import CaseTriageBase, StateBase
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import (
    SQLAlchemyDatabaseKey,
    SQLAlchemyStateDatabaseVersion,
)


class SQLAlchemyDatabaseKeyTest(TestCase):
    """Tests for SQLAlchemyDatabaseKey."""

    def test_state_legacy_db(self) -> None:
        db_key_1 = SQLAlchemyDatabaseKey(schema_type=SchemaType.STATE)
        db_key_1_dup = SQLAlchemyDatabaseKey.canonical_for_schema(
            schema_type=SchemaType.STATE
        )
        self.assertEqual(db_key_1, db_key_1_dup)

        # TODO(#7984): Once we have cut over all traffic to non-legacy state DBs and
        #  removed the LEGACY database version, remove this part of the test.
        db_key_legacy = SQLAlchemyDatabaseKey.for_state_code(
            StateCode.US_AK, SQLAlchemyStateDatabaseVersion.LEGACY
        )
        self.assertEqual(db_key_1, db_key_legacy)

    def test_for_schema_throws_state(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "^Must provide db name information to create a STATE database key.$",
        ):
            _ = SQLAlchemyDatabaseKey.for_schema(SchemaType.STATE)

    def test_for_state_code(self) -> None:
        primary = SQLAlchemyDatabaseKey.for_state_code(
            StateCode.US_MN, db_version=SQLAlchemyStateDatabaseVersion.PRIMARY
        )
        secondary = SQLAlchemyDatabaseKey.for_state_code(
            StateCode.US_MN, db_version=SQLAlchemyStateDatabaseVersion.SECONDARY
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
        key = SQLAlchemyDatabaseKey.for_state_code(
            StateCode.US_MN, db_version=SQLAlchemyStateDatabaseVersion.PRIMARY
        )

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

    @patch(
        f"{sqlalchemy_database_key.__name__}.get_existing_direct_ingest_states",
        return_value=[StateCode.US_XX, StateCode.US_WW],
    )
    def test_get_all(self, state_codes_fn) -> None:
        all_keys = SQLAlchemyDatabaseKey.all()

        expected_all_keys = [
            SQLAlchemyDatabaseKey(SchemaType.JAILS, db_name="postgres"),
            SQLAlchemyDatabaseKey(SchemaType.STATE, db_name="postgres"),
            SQLAlchemyDatabaseKey(SchemaType.OPERATIONS, db_name="postgres"),
            SQLAlchemyDatabaseKey(SchemaType.JUSTICE_COUNTS, db_name="postgres"),
            SQLAlchemyDatabaseKey(SchemaType.CASE_TRIAGE, db_name="postgres"),
            SQLAlchemyDatabaseKey(SchemaType.STATE, db_name="us_xx_primary"),
            SQLAlchemyDatabaseKey(SchemaType.STATE, db_name="us_ww_primary"),
            SQLAlchemyDatabaseKey(SchemaType.STATE, db_name="us_xx_secondary"),
            SQLAlchemyDatabaseKey(SchemaType.STATE, db_name="us_ww_secondary"),
        ]

        self.assertCountEqual(expected_all_keys, all_keys)

        state_codes_fn.assert_called()

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
