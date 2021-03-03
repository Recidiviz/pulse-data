# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
# ============================================================================

"""Basic tests that migrations are working properly."""

import abc
from collections import defaultdict
from typing import Dict, Set
from unittest.case import TestCase

import pytest
from alembic.autogenerate import render_python_code
from pytest_alembic import runner  # type: ignore
from sqlalchemy import create_engine

from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
    SchemaType,
)
from recidiviz.tools.postgres import local_postgres_helpers


@pytest.mark.uses_db
class MigrationsTestBase:
    """This is the base class for testing that migrations work.

    The default tests in this class match the default tests used by pytest_alembic.
    NB: pytest_alembic doesn't make it easy to run the default tests across multiple databases,
    which is why we have redefined them here.
    See: https://github.com/schireson/pytest-alembic/blob/master/src/pytest_alembic/tests.py
    """

    def setUp(self) -> None:
        self.db_dir = local_postgres_helpers.start_on_disk_postgresql_database()
        self.overridden_env_vars = (
            local_postgres_helpers.update_local_sqlalchemy_postgres_env_vars()
        )
        self.engine = create_engine(
            local_postgres_helpers.postgres_db_url_from_env_vars()
        )

    def tearDown(self) -> None:
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(self.db_dir)

    def fetch_all_enums(self) -> Dict[str, Set[str]]:
        engine = create_engine(local_postgres_helpers.postgres_db_url_from_env_vars())

        conn = engine.connect()
        rows = conn.execute(
            """
        SELECT t.typname as enum_name,
            e.enumlabel as enum_value
        FROM pg_type t
            JOIN pg_enum e ON t.oid = e.enumtypid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        WHERE
            n.nspname = 'public';
        """
        )

        enums = defaultdict(set)
        for row in rows:
            enums[row[0]].add(row[1])

        return enums

    def default_config(self) -> Dict[str, str]:
        return {
            "file": SQLAlchemyEngineManager.get_alembic_file(self.schema_type),
            "script_location": SQLAlchemyEngineManager.get_migrations_location(
                self.schema_type
            ),
        }

    @property
    @abc.abstractmethod
    def schema_type(self) -> SchemaType:
        raise NotImplementedError

    def test_enums_match_schema(self):
        with runner(self.default_config(), self.engine) as r:
            r.migrate_up_to("head")

        # Fetch enum values
        migration_enums = self.fetch_all_enums()

        # Doing teardown/setup to generate a new postgres instance
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(self.db_dir)

        self.db_dir = local_postgres_helpers.start_on_disk_postgresql_database()
        self.overridden_env_vars = (
            local_postgres_helpers.update_local_sqlalchemy_postgres_env_vars()
        )

        declarative_base = SQLAlchemyEngineManager.declarative_method_for_schema(
            self.schema_type
        )
        local_postgres_helpers.use_on_disk_postgresql_database(declarative_base)

        # Check enum values
        schema_enums = self.fetch_all_enums()

        # Assert that they all match
        self.assertEqual(len(migration_enums), len(schema_enums))
        for enum_name, migration_values in migration_enums.items():
            schema_values = schema_enums[enum_name]
            self.assertEqual(
                len(migration_values),
                len(schema_values),
                msg=f"{enum_name} lengths differ",
            )
            self.assertEqual(
                len(migration_values),
                len(migration_values.intersection(schema_values)),
                msg=f"{enum_name} values differ",
            )

        # Cleanup needed for this method.
        local_postgres_helpers.teardown_on_disk_postgresql_database(declarative_base)

    def test_full_upgrade(self):
        """Enforce that migrations can be run forward to completion."""
        with runner(self.default_config(), self.engine) as r:
            r.migrate_up_to("head")

    def test_single_head_revision(self):
        """Enforce that there is exactly one head revision."""
        with runner(self.default_config(), self.engine) as r:
            self.assertEqual(len(r.heads), 1)

    def test_up_down(self):
        """Enforce that migrations can be run all the way up, back, and up again."""
        with runner(self.default_config(), self.engine) as r:
            revisions = r.history.revisions
            r.migrate_up_to("head")
            for rev in reversed(revisions):
                try:
                    r.migrate_down_to(rev)
                except Exception:
                    self.fail(f"Migrate down failed at revision: {rev}")
            for rev in revisions:
                try:
                    r.migrate_up_to(rev)
                except Exception:
                    self.fail(f"Migrate back up failed at revision: {rev}")

    def test_migrate_matches_defs(self):
        """Enforces that after all migrations, database state matches known models

        Important note: This test will not detect changes made to enums that have failed to
        be incorporated by existing migrations. It only reliably handles table schema.
        """

        def verify_is_empty(_, __, directives):
            script = directives[0]

            migration_is_empty = script.upgrade_ops.is_empty()
            if not migration_is_empty:
                raise RuntimeError(
                    "expected empty autogenerated migration. actual contained these operations:\n"
                    f"{render_python_code(script.upgrade_ops)}"
                )

        with runner(self.default_config(), self.engine) as r:
            r.migrate_up_to("head")
            r.generate_revision(
                message="test_rev",
                autogenerate=True,
                process_revision_directives=verify_is_empty,
            )


class TestCaseTriageMigrations(MigrationsTestBase, TestCase):
    @property
    def schema_type(self) -> SchemaType:
        return SchemaType.CASE_TRIAGE


class TestJailsMigrations(MigrationsTestBase, TestCase):
    @property
    def schema_type(self) -> SchemaType:
        return SchemaType.JAILS


class TestJusticeCountsMigrations(MigrationsTestBase, TestCase):
    @property
    def schema_type(self) -> SchemaType:
        return SchemaType.JUSTICE_COUNTS


class TestOperationsMigrations(MigrationsTestBase, TestCase):
    @property
    def schema_type(self) -> SchemaType:
        return SchemaType.OPERATIONS


class TestStateMigrations(MigrationsTestBase, TestCase):
    @property
    def schema_type(self) -> SchemaType:
        return SchemaType.STATE
