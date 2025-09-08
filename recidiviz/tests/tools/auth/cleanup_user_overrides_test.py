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
"""Implements tests for the script that cleans up extra data in the UserOverride table."""

from datetime import datetime
from unittest import TestCase

import pytest
from sqlalchemy import select

from recidiviz.persistence.database.schema.case_triage.schema import UserOverride
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
from recidiviz.tools.auth.cleanup_user_overrides import cleanup_user_overrides
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult


@pytest.mark.uses_db
class CleanupUserOverridesTest(TestCase):
    """Implements tests for the script that cleans up extra data in the UserOverride table."""

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
        self.roster_user = generate_fake_rosters(
            email="parameter@domain.org",
            region_code="US_XX",
            external_id="123",
            roles=["supervision_officer", "leadership_role"],
            district="D1",
            first_name="Test",
            last_name="User",
            pseudonymized_id="pseudo-123",
        )
        self.switched_roles = self.roster_user.roles.copy()
        self.switched_roles[0], self.switched_roles[1] = (
            self.switched_roles[1],
            self.switched_roles[0],
        )

        self.session = SessionFactory.using_database(self.database_key).__enter__()
        self.session.add(self.roster_user)

    def tearDown(self) -> None:
        self.session.__exit__(None, None, None)
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    def test_same_external_id_set_to_null(self) -> None:
        self.session.add(
            generate_fake_user_overrides(
                email=self.roster_user.email_address,
                region_code=self.roster_user.state_code,
                external_id=self.roster_user.external_id,
                # add one different attribute so it doesn't get deleted
                district="different district",
            )
        )
        cleanup_user_overrides(
            self.session, dry_run=False, state_code=self.roster_user.state_code
        )
        modified_user = self.session.execute(
            select(UserOverride).where(
                UserOverride.email_address == self.roster_user.email_address
            )
        ).scalar()
        self.assertIsNone(modified_user.external_id)

    def test_same_district_set_to_null(self) -> None:
        self.session.add(
            generate_fake_user_overrides(
                email=self.roster_user.email_address,
                region_code=self.roster_user.state_code,
                district=self.roster_user.district,
                # add one different attribute so it doesn't get deleted
                external_id="different",
            )
        )
        cleanup_user_overrides(
            self.session, dry_run=False, state_code=self.roster_user.state_code
        )
        modified_user = self.session.execute(
            select(UserOverride).where(
                UserOverride.email_address == self.roster_user.email_address
            )
        ).scalar()
        self.assertIsNone(modified_user.district)

    def test_same_first_name_set_to_null(self) -> None:
        self.session.add(
            generate_fake_user_overrides(
                email=self.roster_user.email_address,
                region_code=self.roster_user.state_code,
                first_name=self.roster_user.first_name,
                # add one different attribute so it doesn't get deleted
                district="different",
            )
        )
        cleanup_user_overrides(
            self.session, dry_run=False, state_code=self.roster_user.state_code
        )
        modified_user = self.session.execute(
            select(UserOverride).where(
                UserOverride.email_address == self.roster_user.email_address
            )
        ).scalar()
        self.assertIsNone(modified_user.first_name)

    def test_same_first_name_different_case_set_to_null(self) -> None:
        self.session.add(
            generate_fake_user_overrides(
                email=self.roster_user.email_address,
                region_code=self.roster_user.state_code,
                first_name=self.roster_user.first_name.upper(),
                # add one different attribute so it doesn't get deleted
                district="different",
            )
        )
        cleanup_user_overrides(
            self.session, dry_run=False, state_code=self.roster_user.state_code
        )
        modified_user = self.session.execute(
            select(UserOverride).where(
                UserOverride.email_address == self.roster_user.email_address
            )
        ).scalar()
        self.assertIsNone(modified_user.first_name)

    def test_same_last_name_set_to_null(self) -> None:
        self.session.add(
            generate_fake_user_overrides(
                email=self.roster_user.email_address,
                region_code=self.roster_user.state_code,
                last_name=self.roster_user.last_name,
                # add one different attribute so it doesn't get deleted
                district="different",
            )
        )
        cleanup_user_overrides(
            self.session, dry_run=False, state_code=self.roster_user.state_code
        )
        modified_user = self.session.execute(
            select(UserOverride).where(
                UserOverride.email_address == self.roster_user.email_address
            )
        ).scalar()
        self.assertIsNone(modified_user.last_name)

    def test_same_last_name_different_case_set_to_null(self) -> None:
        self.session.add(
            generate_fake_user_overrides(
                email=self.roster_user.email_address,
                region_code=self.roster_user.state_code,
                last_name=self.roster_user.last_name.lower(),
                # add one different attribute so it doesn't get deleted
                district="different",
            )
        )
        cleanup_user_overrides(
            self.session, dry_run=False, state_code=self.roster_user.state_code
        )
        modified_user = self.session.execute(
            select(UserOverride).where(
                UserOverride.email_address == self.roster_user.email_address
            )
        ).scalar()
        self.assertIsNone(modified_user.last_name)

    def test_same_role_set_to_null(self) -> None:
        self.session.add(
            generate_fake_user_overrides(
                email=self.roster_user.email_address,
                region_code=self.roster_user.state_code,
                roles=self.roster_user.roles,
                # add one different attribute so it doesn't get deleted
                district="different",
            )
        )
        cleanup_user_overrides(
            self.session, dry_run=False, state_code=self.roster_user.state_code
        )
        modified_user = self.session.execute(
            select(UserOverride).where(
                UserOverride.email_address == self.roster_user.email_address
            )
        ).scalar()
        self.assertIsNone(modified_user.roles)

    def test_same_roles_different_order_set_to_null(self) -> None:
        self.session.add(
            generate_fake_user_overrides(
                email=self.roster_user.email_address,
                region_code=self.roster_user.state_code,
                roles=self.switched_roles,
                # add one different attribute so it doesn't get deleted
                district="different",
            )
        )
        cleanup_user_overrides(
            self.session, dry_run=False, state_code=self.roster_user.state_code
        )
        modified_user = self.session.execute(
            select(UserOverride).where(
                UserOverride.email_address == self.roster_user.email_address
            )
        ).scalar()
        self.assertIsNone(modified_user.roles)

    def test_equivalent_supervision_staff_role_set_to_null(self) -> None:
        self.session.add(
            generate_fake_user_overrides(
                email=self.roster_user.email_address,
                region_code=self.roster_user.state_code,
                roles=["supervision_staff"],
                # add one different attribute so it doesn't get deleted
                district="different",
            )
        )
        cleanup_user_overrides(
            self.session, dry_run=False, state_code=self.roster_user.state_code
        )
        modified_user = self.session.execute(
            select(UserOverride).where(
                UserOverride.email_address == self.roster_user.email_address
            )
        ).scalar()
        self.assertIsNone(modified_user.roles)

    def test_equivalent_leadership_role_set_to_null(self) -> None:
        self.roster_user.roles = ["supervision_leadership"]
        self.session.add(
            generate_fake_user_overrides(
                email=self.roster_user.email_address,
                region_code=self.roster_user.state_code,
                roles=["leadership_role"],
                # add one different attribute so it doesn't get deleted
                district="different",
            )
        )
        cleanup_user_overrides(
            self.session, dry_run=False, state_code=self.roster_user.state_code
        )
        modified_user = self.session.execute(
            select(UserOverride).where(
                UserOverride.email_address == self.roster_user.email_address
            )
        ).scalar()
        self.assertIsNone(modified_user.roles)

    def test_non_equivalent_role_does_not_change(self) -> None:
        self.session.add(
            generate_fake_user_overrides(
                email=self.roster_user.email_address,
                region_code=self.roster_user.state_code,
                roles=["leadership_role"],
                # add one different attribute so it doesn't get deleted
                district="different",
            )
        )
        cleanup_user_overrides(
            self.session, dry_run=False, state_code=self.roster_user.state_code
        )
        modified_user = self.session.execute(
            select(UserOverride).where(
                UserOverride.email_address == self.roster_user.email_address
            )
        ).scalar()
        self.assertEqual(modified_user.roles, ["leadership_role"])

    def test_delete_user_all_null_attributes(self) -> None:
        self.session.add(
            generate_fake_user_overrides(
                email=self.roster_user.email_address,
                region_code=self.roster_user.state_code,
            )
        )
        cleanup_user_overrides(
            self.session, dry_run=False, state_code=self.roster_user.state_code
        )
        modified_user = self.session.execute(
            select(UserOverride).where(
                UserOverride.email_address == self.roster_user.email_address
            )
        ).scalar()
        self.assertIsNone(modified_user)

    def test_delete_user_matching_attributes(self) -> None:
        self.session.add(
            generate_fake_user_overrides(
                email=self.roster_user.email_address,
                region_code=self.roster_user.state_code,
                district=self.roster_user.district,
                roles=["supervision_staff"],
            )
        )
        cleanup_user_overrides(
            self.session, dry_run=False, state_code=self.roster_user.state_code
        )
        modified_user = self.session.execute(
            select(UserOverride).where(
                UserOverride.email_address == self.roster_user.email_address
            )
        ).scalar()
        self.assertIsNone(modified_user)

    def test_blocked_user_all_null_attributes_is_not_deleted(self) -> None:
        self.session.add(
            generate_fake_user_overrides(
                email=self.roster_user.email_address,
                region_code=self.roster_user.state_code,
                blocked_on=datetime.fromisoformat("2023-01-01"),
            )
        )
        cleanup_user_overrides(
            self.session, dry_run=False, state_code=self.roster_user.state_code
        )
        modified_user = self.session.execute(
            select(UserOverride).where(
                UserOverride.email_address == self.roster_user.email_address
            )
        ).scalar()
        self.assertIsNotNone(modified_user)

    def test_non_specified_states_not_modified(self) -> None:
        user_override = generate_fake_user_overrides(
            email=self.roster_user.email_address,
            region_code=self.roster_user.state_code,
            external_id=self.roster_user.external_id,
            # add one different attribute so it doesn't get deleted
            district="different district",
        )
        self.session.add(user_override)
        cleanup_user_overrides(self.session, dry_run=False, state_code="US_YY")
        modified_user = self.session.execute(
            select(UserOverride).where(
                UserOverride.email_address == self.roster_user.email_address
            )
        ).scalar()
        self.assertEqual(modified_user, user_override)

    def test_non_specified_states_not_deleted(self) -> None:
        user_override = generate_fake_user_overrides(
            email=self.roster_user.email_address,
            region_code=self.roster_user.state_code,
        )
        self.session.add(user_override)
        cleanup_user_overrides(self.session, dry_run=False, state_code="US_YY")
        modified_user = self.session.execute(
            select(UserOverride).where(
                UserOverride.email_address == self.roster_user.email_address
            )
        ).scalar()
        self.assertEqual(modified_user, user_override)
