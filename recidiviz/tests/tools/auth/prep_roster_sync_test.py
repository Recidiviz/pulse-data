# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Implements tests for the script that prepares data to be ready for roster sync."""

from datetime import datetime
from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import func, select

from recidiviz.persistence.database.schema.case_triage.schema import (
    Roster,
    UserOverride,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tests.auth.helpers import (
    generate_fake_rosters,
    generate_fake_user_overrides,
)
from recidiviz.tools.auth.prep_roster_sync import (
    _EXISTING_USER_QUERY,
    add_user_overrides,
    get_existing_users_missing_from_roster_sync,
    prepare_for_roster_sync,
    remove_users,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers


@pytest.mark.uses_db
@pytest.mark.usefixtures("snapshottest_snapshot")
class PrepRosterSyncTest(TestCase):
    """Implements tests for the script that prepares data to be ready for roster sync."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: str | None

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def setUp(self) -> None:
        self.maxDiff = None
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        self.overridden_env_vars = (
            local_persistence_helpers.update_local_sqlalchemy_postgres_env_vars()
        )
        engine = SQLAlchemyEngineManager.init_engine_for_postgres_instance(
            database_key=self.database_key,
            db_url=local_postgres_helpers.on_disk_postgres_db_url(),
        )

        self.database_key.declarative_meta.metadata.create_all(engine)
        self.session = SessionFactory.using_database(self.database_key).__enter__()

        self.recently_logged_in_user_roster = generate_fake_rosters(
            email="recently_logged_in_user@testdomain.com",
            region_code="US_XX",
            external_id="123",
            role="supervision_officer",
            roles=["supervision_officer"],
            district="D1",
            first_name="Test",
            last_name="User",
            pseudonymized_id="pseudo-123",
            created_datetime=datetime.fromisoformat("2023-01-01"),
        )

        self.recently_logged_in_user_uo = generate_fake_user_overrides(
            email="recently_logged_in_user@testdomain.com",
            region_code="US_XX",
            roles=["supervision_officer", "custom_role"],
            district="D2",
            created_datetime=datetime.fromisoformat("2023-02-01"),
        )

        self.user_in_sync_query = generate_fake_user_overrides(
            email="user_in_sync_query@testdomain.com",
            region_code="US_XX",
            external_id="234",
            role="supervision_officer",
            roles=["supervision_officer"],
            district="D2",
            first_name="Test",
            last_name="User",
            pseudonymized_id="pseudo-234",
            created_datetime=datetime.fromisoformat("2023-01-01"),
        )

        self.recently_created_user = generate_fake_user_overrides(
            email="recently_created_user@testdomain.com",
            region_code="US_XX",
            external_id="345",
            role="supervision_officer",
            roles=["supervision_officer"],
            district="D3",
            first_name="Test",
            last_name="User",
            pseudonymized_id="pseudo-345",
        )

        self.user_to_delete_both_roster = generate_fake_rosters(
            email="user_to_delete_roster_and_uo@testdomain.com",
            region_code="US_XX",
            external_id="456",
            role="supervision_officer",
            roles=["supervision_officer"],
            district="D4",
            first_name="Test",
            last_name="User",
            pseudonymized_id="pseudo-456",
            created_datetime=datetime.fromisoformat("2023-01-01"),
        )

        self.user_to_delete_both_uo = generate_fake_user_overrides(
            email="user_to_delete_roster_and_uo@testdomain.com",
            region_code="US_XX",
            external_id="567",
            pseudonymized_id="pseudo-567",
            created_datetime=datetime.fromisoformat("2023-01-01"),
        )

        self.user_to_delete_uo_only = generate_fake_user_overrides(
            email="user_to_delete_uo_only@testdomain.com",
            region_code="US_XX",
            external_id="678",
            role="supervision_officer",
            roles=["supervision_officer"],
            district="D6",
            first_name="Test",
            last_name="User",
            pseudonymized_id="pseudo-678",
            created_datetime=datetime.fromisoformat("2023-01-01"),
        )

    def tearDown(self) -> None:
        self.session.__exit__(None, None, None)
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @patch(
        "recidiviz.tools.auth.prep_roster_sync.get_recently_logged_in_users_by_email"
    )
    def test_get_missing_users_no_roster_sync_no_logins(
        self, mock_recent_users: MagicMock
    ) -> None:
        self.session.add_all(
            [
                self.user_to_delete_both_roster,
                self.user_to_delete_both_uo,
                self.user_to_delete_uo_only,
            ]
        )

        # Assuming roster sync returns nobody
        roster_sync_users: list[Roster] = []

        # And none of these users have logged in recently
        mock_recent_users.return_value = []

        # None of these users should be kept
        expected_overrides: list[dict] = []
        expected_deletions = {
            "user_to_delete_roster_and_uo@testdomain.com",
            "user_to_delete_uo_only@testdomain.com",
        }

        self.assertEqual(
            get_existing_users_missing_from_roster_sync(
                session=self.session,
                auth0_client=mock.ANY,
                roster_sync_users=roster_sync_users,
                state_code="US_XX",
            ),
            (expected_overrides, expected_deletions),
        )

    @patch(
        "recidiviz.tools.auth.prep_roster_sync.get_recently_logged_in_users_by_email"
    )
    def test_get_missing_users_one_roster_sync_no_logins(
        self, mock_recent_users: MagicMock
    ) -> None:
        self.session.add_all(
            [
                self.user_to_delete_both_roster,
                self.user_to_delete_both_uo,
                self.user_to_delete_uo_only,
                self.user_in_sync_query,
            ]
        )

        # Assuming roster sync returns 1 of our users
        roster_sync_users: list[Roster] = [
            generate_fake_rosters(
                email="user_in_sync_query@testdomain.com",
                region_code="US_XX",
                role="leadership_role",
                roles=["leadership_role"],
            )
        ]

        # And none of our remaining users have logged in recently
        mock_recent_users.return_value = []

        # None of the remaining users should be kept
        expected_overrides: list[dict] = []
        expected_deletions = {
            "user_to_delete_roster_and_uo@testdomain.com",
            "user_to_delete_uo_only@testdomain.com",
        }

        self.assertEqual(
            get_existing_users_missing_from_roster_sync(
                session=self.session,
                auth0_client=mock.ANY,
                roster_sync_users=roster_sync_users,
                state_code="US_XX",
            ),
            (expected_overrides, expected_deletions),
        )

    @patch(
        "recidiviz.tools.auth.prep_roster_sync.get_recently_logged_in_users_by_email"
    )
    def test_get_missing_users_one_roster_sync_one_login(
        self, mock_recent_users: MagicMock
    ) -> None:
        self.session.add_all(
            [
                self.user_to_delete_both_roster,
                self.user_to_delete_both_uo,
                self.user_to_delete_uo_only,
                self.user_in_sync_query,
                self.recently_logged_in_user_roster,
                self.recently_logged_in_user_uo,
            ]
        )

        # Assuming roster sync returns 1 of our users
        roster_sync_users: list[Roster] = [
            generate_fake_rosters(
                email="user_in_sync_query@testdomain.com",
                region_code="US_XX",
                role="leadership_role",
                roles=["leadership_role"],
            )
        ]

        # And one of our remaining users has logged in recently
        mock_recent_users.return_value = ["recently_logged_in_user@testdomain.com"]

        # The recently logged in user should get an override, and the other user should be removed
        expected_overrides = [
            {
                "blocked": False,
                "district": "D2",
                "email_address": "recently_logged_in_user@testdomain.com",
                "external_id": "123",
                "first_name": "Test",
                "last_name": "User",
                "pseudonymized_id": "pseudo-123",
                "role": "supervision_officer",
                "roles": ["supervision_officer", "custom_role"],
                "state_code": "US_XX",
                "user_hash": "On9z4tx1lZK9NfTUmCrAucJRuDsvNDZvT4JknYfHlUU=",
            }
        ]
        expected_deletions = {
            "user_to_delete_roster_and_uo@testdomain.com",
            "user_to_delete_uo_only@testdomain.com",
        }

        self.assertEqual(
            get_existing_users_missing_from_roster_sync(
                session=self.session,
                auth0_client=mock.ANY,
                roster_sync_users=roster_sync_users,
                state_code="US_XX",
            ),
            (expected_overrides, expected_deletions),
        )

    @patch(
        "recidiviz.tools.auth.prep_roster_sync.get_recently_logged_in_users_by_email"
    )
    def test_get_missing_users_one_roster_sync_one_login_one_recently_added(
        self, mock_recent_users: MagicMock
    ) -> None:
        self.session.add_all(
            [
                self.user_to_delete_both_roster,
                self.user_to_delete_both_uo,
                self.user_to_delete_uo_only,
                self.user_in_sync_query,
                self.recently_logged_in_user_roster,
                self.recently_logged_in_user_uo,
                self.recently_created_user,
            ]
        )

        # Assuming roster sync returns 1 of our users
        roster_sync_users: list[Roster] = [
            generate_fake_rosters(
                email="user_in_sync_query@testdomain.com",
                region_code="US_XX",
                role="leadership_role",
                roles=["leadership_role"],
            )
        ]

        # And one of our remaining users has logged in recently
        mock_recent_users.return_value = ["recently_logged_in_user@testdomain.com"]

        # The recently logged in user should get an override, the other original user should be removed, and the recently added user should be ignored
        expected_overrides = [
            {
                "blocked": False,
                "district": "D2",
                "email_address": "recently_logged_in_user@testdomain.com",
                "external_id": "123",
                "first_name": "Test",
                "last_name": "User",
                "pseudonymized_id": "pseudo-123",
                "role": "supervision_officer",
                "roles": ["supervision_officer", "custom_role"],
                "state_code": "US_XX",
                "user_hash": "On9z4tx1lZK9NfTUmCrAucJRuDsvNDZvT4JknYfHlUU=",
            }
        ]
        expected_deletions = {
            "user_to_delete_roster_and_uo@testdomain.com",
            "user_to_delete_uo_only@testdomain.com",
        }

        self.assertEqual(
            get_existing_users_missing_from_roster_sync(
                session=self.session,
                auth0_client=mock.ANY,
                roster_sync_users=roster_sync_users,
                state_code="US_XX",
            ),
            (expected_overrides, expected_deletions),
        )

    def test_remove_users(self) -> None:
        self.session.add_all(
            [
                self.user_to_delete_both_roster,
                self.user_to_delete_both_uo,
                self.user_to_delete_uo_only,
            ]
        )

        users_to_remove = {
            "user_to_delete_roster_and_uo@testdomain.com",
            "user_to_delete_uo_only@testdomain.com",
        }

        # Make sure they start off existing
        query = _EXISTING_USER_QUERY.filter(
            func.coalesce(UserOverride.email_address, Roster.email_address).in_(
                users_to_remove
            )
        )
        self.assertEqual(len(self.session.execute(query).all()), 2)

        # Remove them and make sure they're no longer there
        remove_users(self.session, users_to_remove)
        self.assertEqual(len(self.session.execute(query).all()), 0)

    def test_add_user_overrides(self) -> None:
        overrides: list[dict] = [
            {
                "state_code": "US_XX",
                "email_address": "user1@testdomain.com",
                "external_id": "1234",
                "roles": ["leadership_role"],
                "district": "District 7",
                "first_name": "Test",
                "last_name": "User",
                "user_hash": "user1hash",
                "blocked": True,
            },
            {
                "state_code": "US_XX",
                "email_address": "user2@testdomain.com",
                "roles": ["leadership_role"],
                "user_hash": "user2hash",
            },
        ]

        add_user_overrides(self.session, overrides)

        all_user_overrides = self.session.execute(select(UserOverride)).scalars().all()

        # Direct comparison of UserOverride objects gets murky with created times, so just compare
        # the repr in a snapshotl. We're expecting objects representing both of the above dicts to
        # show up.
        self.snapshot.assert_match(all_user_overrides, name="test_add_user_overrides")  # type: ignore[attr-defined]

    @patch("recidiviz.tools.auth.prep_roster_sync.prompt_for_confirmation")
    @patch("recidiviz.tools.auth.prep_roster_sync.get_roster_sync_output")
    @patch(
        "recidiviz.tools.auth.prep_roster_sync.get_recently_logged_in_users_by_email"
    )
    def test_full(
        self,
        mock_recent_users: MagicMock,
        mock_roster_sync_output: MagicMock,
        mock_prompt: MagicMock,
    ) -> None:
        mock_prompt.return_value = True
        self.session.add_all(
            [
                self.user_to_delete_both_roster,
                self.user_to_delete_uo_only,
                self.user_in_sync_query,
                self.recently_logged_in_user_uo,
                self.recently_logged_in_user_roster,
                self.recently_created_user,
            ]
        )

        # Assuming roster sync returns 1 of our users
        roster_sync_users: list[Roster] = [
            generate_fake_rosters(
                email="user_in_sync_query@testdomain.com",
                region_code="US_XX",
                role="leadership_role",
                roles=["leadership_role"],
            )
        ]
        mock_roster_sync_output.return_value = roster_sync_users

        # And one of our remaining users has logged in recently
        mock_recent_users.return_value = ["recently_logged_in_user@testdomain.com"]

        prepare_for_roster_sync(
            session=self.session,
            dry_run=False,
            state_code="US_XX",
            project_id=mock.ANY,
            sandbox_prefix=mock.ANY,
            bq_client=mock.ANY,
            auth0_client=mock.ANY,
        )

        new_roster = self.session.execute(select(Roster)).scalars().all()
        new_overrides = self.session.execute(select(UserOverride)).scalars().all()

        # We expect the user who will be synced, the user who was added recently, and the user who
        # logged in recently to continue to exist as they were before.
        self.assertEqual(
            {user.email_address for user in new_overrides},
            {
                "user_in_sync_query@testdomain.com",
                "recently_created_user@testdomain.com",
                "recently_logged_in_user@testdomain.com",
            },
        )

        # Also double check that the values match our expectations- specifically,
        # recently_logged_in_user@testdomain.com should have the merged value in UserOverride
        self.snapshot.assert_match(new_roster, name="test_full_roster")  # type: ignore[attr-defined]
        self.snapshot.assert_match(new_overrides, name="test_full_user_override")  # type: ignore[attr-defined]

    @patch("recidiviz.tools.auth.prep_roster_sync.get_roster_sync_output")
    @patch(
        "recidiviz.tools.auth.prep_roster_sync.get_recently_logged_in_users_by_email"
    )
    def test_full_dry_run(
        self,
        mock_recent_users: MagicMock,
        mock_roster_sync_output: MagicMock,
    ) -> None:
        self.session.add_all(
            [
                self.user_to_delete_both_roster,
                self.user_to_delete_uo_only,
                self.user_in_sync_query,
                self.recently_logged_in_user_uo,
                self.recently_logged_in_user_roster,
                self.recently_created_user,
            ]
        )

        # Assuming roster sync returns 1 of our users
        roster_sync_users: list[Roster] = [
            generate_fake_rosters(
                email="user_in_sync_query@testdomain.com",
                region_code="US_XX",
                role="leadership_role",
                roles=["leadership_role"],
            )
        ]
        mock_roster_sync_output.return_value = roster_sync_users

        # And one of our remaining users has logged in recently
        mock_recent_users.return_value = ["recently_logged_in_user@testdomain.com"]

        existing_roster = self.session.execute(select(Roster)).scalars().all()
        existing_overrides = self.session.execute(select(UserOverride)).scalars().all()

        prepare_for_roster_sync(
            session=self.session,
            dry_run=True,
            state_code="US_XX",
            project_id=mock.ANY,
            sandbox_prefix=mock.ANY,
            bq_client=mock.ANY,
            auth0_client=mock.ANY,
        )

        new_roster = self.session.execute(select(Roster)).scalars().all()
        new_overrides = self.session.execute(select(UserOverride)).scalars().all()

        # Since this is a dry run, we expect no changes to have been made
        self.assertEqual(existing_roster, new_roster)
        self.assertEqual(existing_overrides, new_overrides)

    @patch("recidiviz.tools.auth.prep_roster_sync.prompt_for_confirmation")
    @patch("recidiviz.tools.auth.prep_roster_sync.get_roster_sync_output")
    @patch(
        "recidiviz.tools.auth.prep_roster_sync.get_recently_logged_in_users_by_email"
    )
    def test_full_no_confirm(
        self,
        mock_recent_users: MagicMock,
        mock_roster_sync_output: MagicMock,
        mock_prompt: MagicMock,
    ) -> None:
        mock_prompt.return_value = False
        self.session.add_all(
            [
                self.user_to_delete_both_roster,
                self.user_to_delete_uo_only,
                self.user_in_sync_query,
                self.recently_logged_in_user_uo,
                self.recently_logged_in_user_roster,
                self.recently_created_user,
            ]
        )

        # Assuming roster sync returns 1 of our users
        roster_sync_users: list[Roster] = [
            generate_fake_rosters(
                email="user_in_sync_query@testdomain.com",
                region_code="US_XX",
                role="leadership_role",
                roles=["leadership_role"],
            )
        ]
        mock_roster_sync_output.return_value = roster_sync_users

        # And one of our remaining users has logged in recently
        mock_recent_users.return_value = ["recently_logged_in_user@testdomain.com"]

        existing_roster = self.session.execute(select(Roster)).scalars().all()
        existing_overrides = self.session.execute(select(UserOverride)).scalars().all()

        prepare_for_roster_sync(
            session=self.session,
            dry_run=False,
            state_code="US_XX",
            project_id=mock.ANY,
            sandbox_prefix=mock.ANY,
            bq_client=mock.ANY,
            auth0_client=mock.ANY,
        )

        new_roster = self.session.execute(select(Roster)).scalars().all()
        new_overrides = self.session.execute(select(UserOverride)).scalars().all()

        # Since we said "no" on the prompt, we expect no changes to have been made
        self.assertEqual(existing_roster, new_roster)
        self.assertEqual(existing_overrides, new_overrides)
