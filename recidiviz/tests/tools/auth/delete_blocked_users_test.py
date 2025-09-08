# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for the script that deletes users blocked for 30+ days from UserOverride and 
PermissionsOverride."""

from datetime import datetime, timedelta, timezone
from unittest import TestCase
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import select

from recidiviz.persistence.database.schema.case_triage.schema import (
    PermissionsOverride,
    UserOverride,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tests.auth.helpers import (
    generate_fake_permissions_overrides,
    generate_fake_rosters,
    generate_fake_user_overrides,
)
from recidiviz.tools.auth.delete_blocked_users import delete_blocked_users
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult


@pytest.mark.uses_db
class DeleteBlockedUsersTest(TestCase):
    """Tests for the script that deletes users blocked for 30+ days from UserOverride and
    PermissionsOverride."""

    # Stores the location of the postgres DB for this test run
    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        self.overridden_env_vars = (
            local_persistence_helpers.update_local_sqlalchemy_postgres_env_vars(
                self.postgres_launch_result
            )
        )
        engine = SQLAlchemyEngineManager.init_engine_for_postgres_instance(
            database_key=self.database_key,
            db_url=self.postgres_launch_result.url(),
        )

        self.database_key.declarative_meta.metadata.create_all(engine)

        self.past_blocked_on_date_more_than_30_days = datetime(
            2025, 1, 9, 15, 0, 0
        ).replace(tzinfo=timezone.utc)
        self.past_blocked_on_date_less_than_30_days = (
            datetime.now() - timedelta(weeks=1)
        ).replace(tzinfo=timezone.utc)

        self.session = SessionFactory.using_database(self.database_key).__enter__()

    def tearDown(self) -> None:
        self.session.__exit__(None, None, None)
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @patch("recidiviz.tools.auth.delete_blocked_users.prompt_for_confirmation")
    def test_delete_users_blocked_more_than_30_days_not_in_roster(
        self, mock_prompt: MagicMock
    ) -> None:
        mock_prompt.return_value = True

        override_user = generate_fake_user_overrides(
            email="leadership@testdomain.com",
            region_code="US_CO",
            blocked_on=self.past_blocked_on_date_more_than_30_days,
        )
        override_permissions = generate_fake_permissions_overrides(
            email="leadership@testdomain.com", routes={}, feature_variants={}
        )
        self.session.add_all([override_user, override_permissions])

        delete_blocked_users(self.session, state_code="US_CO")
        user_override_result = self.session.execute(select(UserOverride)).scalar()
        self.assertIsNone(user_override_result)

        permissions_override_result = self.session.execute(
            select(PermissionsOverride)
        ).scalar()
        self.assertIsNone(permissions_override_result)

    @patch("recidiviz.tools.auth.delete_blocked_users.prompt_for_confirmation")
    def test_keep_users_blocked_less_than_30_days_not_in_roster(
        self, mock_prompt: MagicMock
    ) -> None:
        mock_prompt.return_value = True

        override_user = generate_fake_user_overrides(
            email="leadership@testdomain.com",
            region_code="US_CO",
            blocked_on=self.past_blocked_on_date_less_than_30_days,
        )
        override_permissions = generate_fake_permissions_overrides(
            email="leadership@testdomain.com", routes={}, feature_variants={}
        )
        self.session.add_all([override_user, override_permissions])

        delete_blocked_users(self.session, state_code="US_CO")
        user_override_result = self.session.execute(select(UserOverride)).scalar()
        self.assertEqual(override_user, user_override_result)

        permissions_override_result = self.session.execute(
            select(PermissionsOverride)
        ).scalar()
        self.assertEqual(override_permissions, permissions_override_result)

    @patch("recidiviz.tools.auth.delete_blocked_users.prompt_for_confirmation")
    def test_keep_users_blocked_more_than_30_days_in_roster(
        self, mock_prompt: MagicMock
    ) -> None:
        mock_prompt.return_value = True

        roster_user = generate_fake_rosters(
            email="leadership@testdomain.com",
            region_code="US_CO",
            external_id="123",
            roles=["leadership_role"],
            district="D1",
            first_name="Test",
            last_name="User",
            pseudonymized_id="pseudo-123",
        )
        override_user = generate_fake_user_overrides(
            email="leadership@testdomain.com",
            region_code="US_CO",
            blocked_on=self.past_blocked_on_date_more_than_30_days,
        )
        override_permissions = generate_fake_permissions_overrides(
            email="leadership@testdomain.com", routes={}, feature_variants={}
        )
        self.session.add_all([roster_user, override_user, override_permissions])

        delete_blocked_users(self.session, state_code="US_CO")
        user_override_result = self.session.execute(select(UserOverride)).scalar()
        self.assertEqual(override_user, user_override_result)

        permissions_override_result = self.session.execute(
            select(PermissionsOverride)
        ).scalar()
        self.assertEqual(override_permissions, permissions_override_result)

    @patch("recidiviz.tools.auth.delete_blocked_users.prompt_for_confirmation")
    def test_non_specified_emails_not_deleted(self, mock_prompt: MagicMock) -> None:
        mock_prompt.return_value = True

        override_user_to_delete_1 = generate_fake_user_overrides(
            email="leadership@testdomain.com",
            region_code="US_CO",
            blocked_on=self.past_blocked_on_date_more_than_30_days,
        )
        override_permissions_to_delete_1 = generate_fake_permissions_overrides(
            email="leadership@testdomain.com", routes={}, feature_variants={}
        )
        override_user_to_delete_2 = generate_fake_user_overrides(
            email="user@testdomain.com",
            region_code="US_CO",
            blocked_on=self.past_blocked_on_date_more_than_30_days,
        )
        override_user_to_keep = generate_fake_user_overrides(
            email="parameter@testdomain.com",
            region_code="US_CO",
            blocked_on=self.past_blocked_on_date_more_than_30_days,
        )
        override_permissions_to_keep = generate_fake_permissions_overrides(
            email="parameter@testdomain.com", routes={}, feature_variants={}
        )
        self.session.add_all(
            [
                override_user_to_delete_1,
                override_permissions_to_delete_1,
                override_user_to_delete_2,
                override_user_to_keep,
                override_permissions_to_keep,
            ]
        )

        delete_blocked_users(
            self.session,
            state_code="US_CO",
            user_emails=["leadership@testdomain.com", "user@testdomain.com"],
        )
        user_override_result = self.session.execute(select(UserOverride)).scalar()
        self.assertEqual(override_user_to_keep, user_override_result)

        permissions_override_result = self.session.execute(
            select(PermissionsOverride)
        ).scalar()
        self.assertEqual(override_permissions_to_keep, permissions_override_result)

    @patch("recidiviz.tools.auth.delete_blocked_users.prompt_for_confirmation")
    def test_non_specified_states_not_deleted(self, mock_prompt: MagicMock) -> None:
        mock_prompt.return_value = True

        override_user = generate_fake_user_overrides(
            email="leadership@testdomain.com",
            region_code="US_CO",
            blocked_on=self.past_blocked_on_date_more_than_30_days,
        )
        override_permissions = generate_fake_permissions_overrides(
            email="leadership@testdomain.com", routes={}, feature_variants={}
        )
        self.session.add_all([override_user, override_permissions])

        delete_blocked_users(self.session, state_code="US_MI")
        user_override_result = self.session.execute(select(UserOverride)).scalar()
        self.assertEqual(override_user, user_override_result)

        permissions_override_result = self.session.execute(
            select(PermissionsOverride)
        ).scalar()
        self.assertEqual(override_permissions, permissions_override_result)

    @patch("recidiviz.tools.auth.delete_blocked_users.prompt_for_confirmation")
    def test_no_confirmation_not_deleted(self, mock_prompt: MagicMock) -> None:
        mock_prompt.return_value = False

        override_user = generate_fake_user_overrides(
            email="leadership@testdomain.com",
            region_code="US_CO",
            blocked_on=self.past_blocked_on_date_more_than_30_days,
        )
        override_permissions = generate_fake_permissions_overrides(
            email="leadership@testdomain.com", routes={}, feature_variants={}
        )
        self.session.add_all([override_user, override_permissions])

        delete_blocked_users(self.session, state_code="US_CO")
        user_override_result = self.session.execute(select(UserOverride)).scalar()
        self.assertEqual(override_user, user_override_result)

        permissions_override_result = self.session.execute(
            select(PermissionsOverride)
        ).scalar()
        self.assertEqual(override_permissions, permissions_override_result)
