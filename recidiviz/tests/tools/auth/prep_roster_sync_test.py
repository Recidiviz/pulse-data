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

from datetime import datetime, timezone
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
    find_and_handle_diffs,
    get_existing_users_missing_from_roster_sync,
    get_role_updates,
    prepare_for_roster_sync,
    remove_users,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult


@pytest.mark.uses_db
@pytest.mark.usefixtures("snapshottest_snapshot")
class PrepRosterSyncTest(TestCase):
    """Implements tests for the script that prepares data to be ready for roster sync."""

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
        self.maxDiff = None
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
        self.session = SessionFactory.using_database(self.database_key).__enter__()

        self.recently_logged_in_user_roster = generate_fake_rosters(
            email="recently_logged_in_user@testdomain.com",
            region_code="US_XX",
            external_id="123",
            roles=["supervision_line_staff"],
            district="D1",
            first_name="Test",
            last_name="User",
            pseudonymized_id="pseudo-123",
            created_datetime=datetime.fromisoformat("2023-01-01"),
        )

        self.recently_logged_in_user_uo = generate_fake_user_overrides(
            email="recently_logged_in_user@testdomain.com",
            region_code="US_XX",
            roles=["supervision_line_staff", "custom_role"],
            district="D2",
            created_datetime=datetime.fromisoformat("2023-02-01"),
        )

        self.user_in_sync_query = generate_fake_user_overrides(
            email="user_in_sync_query@testdomain.com",
            region_code="US_XX",
            external_id="234",
            roles=["supervision_staff"],
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
            roles=["supervision_line_staff"],
            district="D3",
            first_name="Test",
            last_name="User",
            pseudonymized_id="pseudo-345",
        )

        self.user_to_delete_both_roster = generate_fake_rosters(
            email="user_to_delete_roster_and_uo@testdomain.com",
            region_code="US_XX",
            external_id="456",
            roles=["supervision_line_staff"],
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
            roles=["supervision_line_staff"],
            district="D6",
            first_name="Test",
            last_name="User",
            pseudonymized_id="pseudo-678",
            created_datetime=datetime.fromisoformat("2023-01-01"),
        )

        self.user_with_equivalent_role = generate_fake_user_overrides(
            email="user_with_equivalent_role@testdomain.com",
            region_code="US_XX",
            external_id="789",
            roles=["supervision_staff", "tt group"],
            district="D7",
            first_name="Test",
            last_name="User",
            pseudonymized_id="pseudo-789",
            created_datetime=datetime.fromisoformat("2023-01-01"),
        )

        self.user_with_different_role = generate_fake_user_overrides(
            email="user_with_different_role@testdomain.com",
            region_code="US_XX",
            external_id="890",
            roles=["leadership_role", "tt group"],
            district="D8",
            first_name="Test",
            last_name="User",
            pseudonymized_id="pseudo-890",
            created_datetime=datetime.fromisoformat("2023-01-01"),
        )

        self.user_with_different_district = generate_fake_user_overrides(
            email="user_with_different_district@testdomain.com",
            region_code="US_XX",
            external_id="901",
            roles=["supervision_staff"],
            district="D9",
            first_name="Test",
            last_name="User",
            pseudonymized_id="pseudo-901",
            created_datetime=datetime.fromisoformat("2023-01-01"),
        )

        self.user_with_multiple_diffs = generate_fake_user_overrides(
            email="user_with_multiple_diffs@testdomain.com",
            region_code="US_XX",
            external_id="012",
            roles=["supervision_staff"],
            district="D0",
            first_name="Test",
            last_name="User",
            pseudonymized_id="pseudo-012",
            created_datetime=datetime.fromisoformat("2023-01-01"),
        )

        self.user_to_keep_unchanged = generate_fake_rosters(
            email="user_to_keep_unchanged@testdomain.com",
            region_code="US_XX",
            external_id="1234",
            roles=["supervision_staff"],
            district="D12",
            first_name="Test",
            last_name="User",
            pseudonymized_id="pseudo-1234",
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
                emails_of_users_to_keep_unchanged=[],
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
                emails_of_users_to_keep_unchanged=[],
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
                roles=["leadership_role"],
            )
        ]

        # And one of our remaining users has logged in recently
        mock_recent_users.return_value = ["recently_logged_in_user@testdomain.com"]

        # The recently logged in user should get an override, and the other user should be removed
        expected_overrides = [
            {
                "blocked_on": None,
                "district": "D2",
                "email_address": "recently_logged_in_user@testdomain.com",
                "external_id": "123",
                "first_name": "Test",
                "last_name": "User",
                "pseudonymized_id": "pseudo-123",
                "roles": ["supervision_line_staff", "custom_role"],
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
                emails_of_users_to_keep_unchanged=[],
            ),
            (expected_overrides, expected_deletions),
        )

    @patch(
        "recidiviz.tools.auth.prep_roster_sync.get_recently_logged_in_users_by_email"
    )
    def test_get_missing_users_unchanged_user(
        self, mock_recent_users: MagicMock
    ) -> None:
        self.session.add_all(
            [
                self.user_to_delete_both_roster,
                self.user_to_delete_both_uo,
                self.user_to_delete_uo_only,
                self.user_to_keep_unchanged,
            ]
        )

        # Assuming roster sync returns nobody
        roster_sync_users: list[Roster] = []

        # And none of these users have logged in recently
        mock_recent_users.return_value = []

        # The user to keep unchanged should be kept
        expected_overrides: list[dict] = [
            {
                "blocked_on": None,
                "district": "D12",
                "email_address": "user_to_keep_unchanged@testdomain.com",
                "external_id": "1234",
                "first_name": "Test",
                "last_name": "User",
                "pseudonymized_id": "pseudo-1234",
                "roles": ["supervision_staff"],
                "state_code": "US_XX",
                "user_hash": "HDT8/pUJRRPlwYzN8Ds5PsZAV5//h1UzKb+lzBA9qVY=",
            },
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
                emails_of_users_to_keep_unchanged=[
                    "user_to_keep_unchanged@testdomain.com"
                ],
            ),
            (expected_overrides, expected_deletions),
        )

    @patch(
        "recidiviz.tools.auth.prep_roster_sync.get_recently_logged_in_users_by_email"
    )
    def test_get_missing_users_one_roster_sync_one_login_one_recently_added_one_unchanged(
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
                self.user_to_keep_unchanged,
            ]
        )

        # Assuming roster sync returns 1 of our users
        roster_sync_users: list[Roster] = [
            generate_fake_rosters(
                email="user_in_sync_query@testdomain.com",
                region_code="US_XX",
                roles=["leadership_role"],
            )
        ]

        # And one of our remaining users has logged in recently
        mock_recent_users.return_value = ["recently_logged_in_user@testdomain.com"]

        # The recently logged in user and user to keep unchanged should get overrides, the other
        # original user should be removed, and the recently added user should be ignored
        expected_overrides = [
            {
                "blocked_on": None,
                "district": "D2",
                "email_address": "recently_logged_in_user@testdomain.com",
                "external_id": "123",
                "first_name": "Test",
                "last_name": "User",
                "pseudonymized_id": "pseudo-123",
                "roles": ["supervision_line_staff", "custom_role"],
                "state_code": "US_XX",
                "user_hash": "On9z4tx1lZK9NfTUmCrAucJRuDsvNDZvT4JknYfHlUU=",
            },
            {
                "blocked_on": None,
                "district": "D12",
                "email_address": "user_to_keep_unchanged@testdomain.com",
                "external_id": "1234",
                "first_name": "Test",
                "last_name": "User",
                "pseudonymized_id": "pseudo-1234",
                "roles": ["supervision_staff"],
                "state_code": "US_XX",
                "user_hash": "HDT8/pUJRRPlwYzN8Ds5PsZAV5//h1UzKb+lzBA9qVY=",
            },
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
                emails_of_users_to_keep_unchanged=[
                    "user_to_keep_unchanged@testdomain.com"
                ],
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
                "blocked_on": datetime.fromisoformat("2025-01-09T14:00:00").replace(
                    tzinfo=timezone.utc
                ),
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

    def test_get_role_updates_equivalent(self) -> None:
        # the roster sync query returns "roles" as a string, so construct our test one that way
        roster_sync_user = Roster(
            email_address="user_in_sync_query@testdomain.com",
            state_code="US_XX",
            roles="SUPERVISION_LINE_STAFF",
        )
        current_user = generate_fake_user_overrides(
            email="user_in_sync_query@testdomain.com",
            region_code="US_XX",
            roles=["supervision_staff"],
        )

        expected_updates = ["supervision_line_staff"]
        self.assertEqual(
            get_role_updates(current_user, roster_sync_user), expected_updates
        )

    def test_get_role_updates_not_equivalent(self) -> None:
        # the roster sync query returns "roles" as a string, so construct our test one that way
        roster_sync_user = Roster(
            email_address="user_in_sync_query@testdomain.com",
            state_code="US_XX",
            roles="SUPERVISION_LINE_STAFF",
        )
        current_user = generate_fake_user_overrides(
            email="user_in_sync_query@testdomain.com",
            region_code="US_XX",
            roles=["supervision_leadership"],
        )

        expected_updates = ["supervision_leadership", "supervision_line_staff"]
        self.assertEqual(
            get_role_updates(current_user, roster_sync_user), expected_updates
        )

    def test_get_role_updates_multiple_roles(self) -> None:
        # the roster sync query returns "roles" as a string, so construct our test one that way
        roster_sync_user = Roster(
            email_address="user_in_sync_query@testdomain.com",
            state_code="US_XX",
            roles="SUPERVISION_LINE_STAFF",
        )
        current_user = generate_fake_user_overrides(
            email="user_in_sync_query@testdomain.com",
            region_code="US_XX",
            roles=["supervision_leadership", "supervision_staff", "tt group"],
        )

        expected_updates = [
            "supervision_leadership",
            "supervision_line_staff",
            "tt group",
        ]
        self.assertEqual(
            get_role_updates(current_user, roster_sync_user), expected_updates
        )

    def test_get_role_updates_unknown_role(self) -> None:
        # the roster sync query returns "roles" as a string, so construct our test one that way
        roster_sync_user = Roster(
            email_address="user_in_sync_query@testdomain.com",
            state_code="US_XX",
            roles="UNKNOWN",
        )
        current_user = generate_fake_user_overrides(
            email="user_in_sync_query@testdomain.com",
            region_code="US_XX",
            roles=["supervision_leadership", "supervision_staff"],
        )

        expected_updates = ["supervision_leadership", "supervision_staff"]
        self.assertEqual(
            get_role_updates(current_user, roster_sync_user), expected_updates
        )

    def test_find_and_handle_diffs(self) -> None:
        self.session.add_all(
            [
                self.user_to_delete_both_roster,
                self.user_to_delete_uo_only,
                self.user_in_sync_query,
                self.recently_logged_in_user_uo,
                self.recently_logged_in_user_roster,
                self.recently_created_user,
                self.user_with_equivalent_role,
                self.user_with_different_role,
                self.user_with_different_district,
                self.user_to_keep_unchanged,
            ]
        )
        # the roster sync query returns "roles" as a string, so construct our test ones that way
        roster_sync_users = [
            Roster(
                email_address="user_in_sync_query@testdomain.com",
                state_code="US_XX",
                roles="UNKNOWN",
            ),
            Roster(
                email_address="user_with_equivalent_role@testdomain.com",
                state_code="US_XX",
                roles="SUPERVISION_LINE_STAFF",
            ),
            Roster(
                email_address="user_with_different_role@testdomain.com",
                state_code="US_XX",
                roles="SUPERVISION_LINE_STAFF",
            ),
            Roster(
                email_address="user_with_different_district@testdomain.com",
                state_code="US_XX",
                roles="UNKNOWN",
                district="changed district",
            ),
            Roster(
                email_address="user_with_multiple_diffs@testdomain.com",
                state_code="US_XX",
                roles="SUPERVISION_LINE_STAFF",
                district="changed district",
                first_name="changed name",
            ),
            Roster(
                email_address="user_to_keep_unchanged@testdomain.com",
                state_code="US_XX",
                roles="UNKNOWN",
            ),
        ]
        self.snapshot.assert_match(  # type: ignore[attr-defined]
            find_and_handle_diffs(
                session=self.session,
                roster_sync_users=roster_sync_users,
                state_code="US_XX",
                emails_of_users_to_keep_unchanged=[
                    "user_to_keep_unchanged@testdomain.com"
                ],
            ),
            name="test_find_and_handle_diffs",
        )

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
                self.user_with_equivalent_role,
                self.user_with_different_role,
                self.user_with_different_district,
                self.user_with_multiple_diffs,
                self.user_to_keep_unchanged,
            ]
        )

        # Assuming roster sync returns 5 of our users
        roster_sync_users: list[Roster] = [
            # the roster sync query returns "roles" as a string, so construct our test ones that way
            Roster(
                email_address="user_in_sync_query@testdomain.com",
                state_code="US_XX",
                roles="leadership_role",
            ),
            Roster(
                email_address="user_with_equivalent_role@testdomain.com",
                state_code="US_XX",
                roles="SUPERVISION_LINE_STAFF",
            ),
            Roster(
                email_address="user_with_different_role@testdomain.com",
                state_code="US_XX",
                roles="SUPERVISION_LINE_STAFF",
            ),
            Roster(
                email_address="user_with_different_district@testdomain.com",
                state_code="US_XX",
                roles="UNKNOWN",
                district="changed district",
            ),
            Roster(
                email_address="user_with_multiple_diffs@testdomain.com",
                state_code="US_XX",
                roles="SUPERVISION_LINE_STAFF",
                district="changed district",
                first_name="changed name",
            ),
            Roster(
                email_address="user_to_keep_unchanged@testdomain.com",
                state_code="US_XX",
                roles="UNKNOWN",
                district="changed district",
            ),
        ]
        mock_roster_sync_output.return_value = roster_sync_users

        # And one of our remaining users has logged in recently
        mock_recent_users.return_value = ["recently_logged_in_user@testdomain.com"]

        prepare_for_roster_sync(
            session=self.session,
            dry_run=False,
            state_code="US_XX",
            emails_of_users_to_keep_unchanged=["user_to_keep_unchanged@testdomain.com"],
            project_id=mock.ANY,
            sandbox_prefix=mock.ANY,
            bq_client=mock.ANY,
            auth0_client=mock.ANY,
        )

        new_roster = self.session.execute(select(Roster)).scalars().all()
        new_overrides = self.session.execute(select(UserOverride)).scalars().all()

        # We expect the user who will be synced, the user who was added recently, the user who
        # logged in recently, and the user we asked to keep unchanged to continue to exist as they
        # were before. We also expect the users with diffs to have overrides.
        self.assertEqual(
            {user.email_address for user in new_overrides},
            {
                "user_in_sync_query@testdomain.com",
                "recently_created_user@testdomain.com",
                "recently_logged_in_user@testdomain.com",
                "user_to_keep_unchanged@testdomain.com",
                "user_with_different_role@testdomain.com",
                "user_with_equivalent_role@testdomain.com",
                "user_with_different_district@testdomain.com",
                "user_with_multiple_diffs@testdomain.com",
            },
        )

        # Also double check that the values match our expectations:
        # - recently_logged_in_user@testdomain.com should have the merged value in UserOverride
        # - the users with role diffs should have the correct roles
        # - the users with other diffs should have the changed values for the fields with diffs
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
                self.user_with_equivalent_role,
                self.user_with_different_role,
                self.user_with_different_district,
                self.user_with_multiple_diffs,
                self.user_to_keep_unchanged,
            ]
        )

        # Assuming roster sync returns 3 of our users
        roster_sync_users: list[Roster] = [
            # the roster sync query returns "roles" as a string, so construct our test ones that way
            Roster(
                email_address="user_in_sync_query@testdomain.com",
                state_code="US_XX",
                roles="leadership_role",
            ),
            Roster(
                email_address="user_with_equivalent_role@testdomain.com",
                state_code="US_XX",
                roles="SUPERVISION_LINE_STAFF",
            ),
            Roster(
                email_address="user_with_different_role@testdomain.com",
                state_code="US_XX",
                roles="SUPERVISION_LINE_STAFF",
            ),
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
            emails_of_users_to_keep_unchanged=["user_to_keep_unchanged@testdomain.com"],
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
                self.user_with_equivalent_role,
                self.user_with_different_role,
                self.user_with_different_district,
                self.user_with_multiple_diffs,
                self.user_to_keep_unchanged,
            ]
        )

        # Assuming roster sync returns 3 of our users
        roster_sync_users: list[Roster] = [
            # the roster sync query returns "roles" as a string, so construct our test ones that way
            Roster(
                email_address="user_in_sync_query@testdomain.com",
                state_code="US_XX",
                roles="leadership_role",
            ),
            Roster(
                email_address="user_with_equivalent_role@testdomain.com",
                state_code="US_XX",
                roles="SUPERVISION_LINE_STAFF",
            ),
            Roster(
                email_address="user_with_different_role@testdomain.com",
                state_code="US_XX",
                roles="SUPERVISION_LINE_STAFF",
            ),
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
            emails_of_users_to_keep_unchanged=["user_to_keep_unchanged@testdomain.com"],
        )

        new_roster = self.session.execute(select(Roster)).scalars().all()
        new_overrides = self.session.execute(select(UserOverride)).scalars().all()

        # Since we said "no" on the prompt, we expect no changes to have been made
        self.assertEqual(existing_roster, new_roster)
        self.assertEqual(existing_overrides, new_overrides)
