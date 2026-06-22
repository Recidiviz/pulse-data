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
"""Tests for the Identity Service querier."""
import datetime
import os
import unittest
import uuid

import pytest
from sqlalchemy import text

from recidiviz.common.constants.identity import IdentityStatus, PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.services.identity.querier import IdentityServiceQuerier
from recidiviz.services.identity.types import Identity
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult
from recidiviz.tools.services.identity import fixtures as identity_fixtures
from recidiviz.tools.utils.fixture_helpers import reset_fixtures


@pytest.mark.uses_db
class IdentityServiceQuerierTest(unittest.TestCase):
    """Tests for IdentityServiceQuerier session-management wiring."""

    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.IDENTITY)
        self.engine = local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.database_key
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

    def test_database_key_targets_identity_schema(self) -> None:
        querier = IdentityServiceQuerier()
        self.assertEqual(SchemaType.IDENTITY, querier.database_key.schema_type)

    def test_can_open_and_close_session(self) -> None:
        querier = IdentityServiceQuerier()
        with SessionFactory.using_database(querier.database_key) as session:
            self.assertEqual(1, session.execute(text("SELECT 1")).scalar())

    def test_get_all_identities_returns_seeded_row(self) -> None:
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )

        self.assertEqual(
            [
                Identity(
                    recidiviz_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
                    tenant=Tenant.US_OZ,
                    person_type=PersonType.JII,
                    status=IdentityStatus.ACTIVE,
                    merged_into=None,
                    created_utc=datetime.datetime(
                        2026, 1, 1, tzinfo=datetime.timezone.utc
                    ),
                    last_updated_utc=datetime.datetime(
                        2026, 1, 1, tzinfo=datetime.timezone.utc
                    ),
                )
            ],
            IdentityServiceQuerier().get_all_identities(),
        )
