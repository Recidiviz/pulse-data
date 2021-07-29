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
"""Implements tests for the core AuthorizationStore of Case Triage."""
import os
from datetime import date
from pathlib import Path
from typing import List, Optional
from unittest import TestCase, mock

import pytest
from freezegun import freeze_time
from parameterized import parameterized
from sqlalchemy.orm.exc import NoResultFound

from recidiviz.case_triage.authorization import AccessPermissions, AuthorizationStore
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.schema.case_triage.schema import (
    DashboardUserRestrictions,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.auth.helpers import generate_fake_user_restrictions
from recidiviz.tests.case_triage.case_triage_helpers import generate_fake_officer
from recidiviz.tools.postgres import local_postgres_helpers


def _test_get_local_file(file_path: GcsfsFilePath) -> str:
    local_path = os.path.join(
        os.path.realpath(os.path.dirname(os.path.realpath(__file__))), "auth_fixtures"
    )
    return Path(os.path.join(local_path, file_path.abs_path())).read_text()


class TestAuthorizationStore(TestCase):
    """Class to test AuthorizationStore"""

    def setUp(self) -> None:
        self.get_local_patcher = mock.patch(
            "recidiviz.case_triage.authorization.get_local_file",
            new=_test_get_local_file,
        )
        self.get_local_patcher.start()

        self.auth_store = AuthorizationStore()
        self.auth_store.refresh()

    def tearDown(self) -> None:
        self.get_local_patcher.stop()

    @parameterized.expand(
        [
            ("non-user@not-recidiviz.org", False, False),
            ("user@not-recidiviz.org", True, False),
            ("demoer@not-recidiviz.org", True, True),
            ("admin@not-recidiviz.org", True, True),
        ]
    )
    def test_basic_auth(self, email: str, is_allowed: bool, can_see_demo: bool) -> None:
        self.assertEqual(email in self.auth_store.case_triage_allowed_users, is_allowed)
        self.assertEqual(self.auth_store.can_see_demo_data(email), can_see_demo)

    @parameterized.expand(
        [
            ("non-experiment@not-recidiviz.org", None, None),
            ("eventually@not-recidiviz.org", None, "in-experiment"),
            ("always@not-recidiviz.org", "in-experiment", "in-experiment"),
            ("second-variant@not-recidiviz.org", "second-variant", "second-variant"),
        ]
    )
    @freeze_time("2021-01-01 00:00:00")
    def test_feature_gating(
        self, email: str, current_variant: Optional[str], future_variant: Optional[str]
    ) -> None:
        feature = "can-see-test-feature"

        self.assertEqual(
            self.auth_store.get_feature_variant(feature, email),
            current_variant,
            msg="Incorrect variant returned for current date",
        )
        self.assertEqual(
            self.auth_store.get_feature_variant(
                feature, email, on_date=date(2022, 2, 2)
            ),
            future_variant,
            msg="Incorrect variant returned for future date",
        )


@pytest.mark.uses_db
class TestAccessPermissions(TestCase):
    """Implements tests for the authorization store that make use of the database as
    a secondary store of permissions to check for frontend app access."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def setUp(self) -> None:
        self.get_local_patcher = mock.patch(
            "recidiviz.case_triage.authorization.get_local_file",
            new=_test_get_local_file,
        )
        self.get_local_patcher.start()

        self.auth_store = AuthorizationStore()
        self.auth_store.refresh()

        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)

        self.case_triage_user = generate_fake_user_restrictions(
            "US_XX",
            "case-triage@not-recidiviz.org",
            can_access_leadership_dashboard=False,
            can_access_case_triage=True,
        )
        self.dashboard_user = generate_fake_user_restrictions(
            "US_XX",
            "leadership@not-recidiviz.org",
            can_access_leadership_dashboard=True,
            can_access_case_triage=False,
        )
        self.both_user = generate_fake_user_restrictions(
            "US_XX",
            "both@not-recidiviz.org",
            can_access_leadership_dashboard=True,
            can_access_case_triage=True,
        )

        self.overridden_user = generate_fake_user_restrictions(
            "US_XX",
            "user@not-recidiviz.org",
            can_access_leadership_dashboard=True,
            can_access_case_triage=False,
        )

        self.both_user_different_state = generate_fake_user_restrictions(
            "US_YY",
            "different-state@not-recidiviz.org",
            can_access_leadership_dashboard=True,
            can_access_case_triage=True,
        )

        self.officer = generate_fake_officer(
            "test", "case-triage@not-recidiviz.org", state_code="US_XX"
        )

        with SessionFactory.using_database(self.database_key) as session:
            session.expire_on_commit = False
            session.add_all(
                [
                    self.case_triage_user,
                    self.dashboard_user,
                    self.both_user,
                    self.overridden_user,
                    self.both_user_different_state,
                    self.officer,
                ]
            )

    def tearDown(self) -> None:
        self.get_local_patcher.stop()
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    def assert_email_has_permissions(
        self,
        email: str,
        *,
        can_access_case_triage: bool,
        can_access_leadership_dashboard: bool,
        impersonatable_state_codes: List[str]
    ) -> None:
        self.assertEqual(
            self.auth_store.get_access_permissions(email),
            AccessPermissions(
                can_access_case_triage=can_access_case_triage,
                can_access_leadership_dashboard=can_access_leadership_dashboard,
                impersonatable_state_codes=impersonatable_state_codes,
            ),
        )

    def test_basic_db_permissions(self) -> None:
        self.assert_email_has_permissions(
            self.case_triage_user.restricted_user_email,
            can_access_case_triage=True,
            can_access_leadership_dashboard=False,
            impersonatable_state_codes=[],
        )
        self.assert_email_has_permissions(
            self.dashboard_user.restricted_user_email,
            can_access_case_triage=False,
            can_access_leadership_dashboard=True,
            impersonatable_state_codes=["US_XX"],
        )
        self.assert_email_has_permissions(
            self.both_user.restricted_user_email,
            can_access_case_triage=True,
            can_access_leadership_dashboard=True,
            impersonatable_state_codes=["US_XX"],
        )
        self.assert_email_has_permissions(
            "nonexistent@not-recidiviz.org",
            can_access_case_triage=False,
            can_access_leadership_dashboard=False,
            impersonatable_state_codes=[],
        )
        self.assert_email_has_permissions(
            self.both_user_different_state.restricted_user_email,
            can_access_case_triage=True,
            can_access_leadership_dashboard=True,
            impersonatable_state_codes=["US_YY"],
        )

    def test_allowlist_override_succeeds(self) -> None:
        # User does not have case triage in database, but is in allowlist_v2.json
        self.assert_email_has_permissions(
            self.overridden_user.restricted_user_email,
            can_access_case_triage=True,
            can_access_leadership_dashboard=True,
            impersonatable_state_codes=["US_XX"],
        )

    @parameterized.expand(
        [
            ("test@recidiviz.org", False),
            ("leadership@not-recidiviz.org", True),
            ("both@not-recidiviz.org", True),
            ("user@not-recidiviz.org", True),
            ("different-state@not-recidiviz.org", False),
            ("case-triage@not-recidiviz.org", False),
        ]
    )
    def test_can_impersonate_state_US_XX(
        self, email: str, can_impersonate: bool
    ) -> None:
        self.assertEqual(
            self.auth_store.can_impersonate(
                email,
                generate_fake_officer(
                    officer_id="test",
                    email="case-triage@not-recidiviz.org",
                    state_code="US_XX",
                ),
            ),
            can_impersonate,
        )

    def test_admin_can_impersonate_all_states(self) -> None:
        self.assertTrue(
            self.auth_store.can_impersonate(
                "admin@not-recidiviz.org",
                generate_fake_officer(
                    officer_id="test",
                    email="case-triage@not-recidiviz.org",
                ),
            )
        )

    def test_cannot_impersonate_own_self(self) -> None:
        self.assertFalse(
            self.auth_store.can_impersonate(
                "case-triage@not-recidiviz.org",
                generate_fake_officer(
                    officer_id="test",
                    email="case-triage@not-recidiviz.org",
                    state_code="US_XX",
                ),
            )
        )

    def test_still_can_access_if_not_in_database(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            with self.assertRaises(NoResultFound):
                session.query(DashboardUserRestrictions).filter(
                    DashboardUserRestrictions.restricted_user_email
                    == "demoer@not-recidiviz.org"
                ).one()

        self.assert_email_has_permissions(
            "demoer@not-recidiviz.org",
            can_access_case_triage=True,
            can_access_leadership_dashboard=False,
            impersonatable_state_codes=[],
        )
