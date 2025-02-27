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
from pytest_alembic import runner
from pytest_alembic.config import Config
from sqlalchemy import create_engine

from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_region_codes,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers


@pytest.mark.uses_db
class MigrationsTestBase(TestCase):
    """This is the base class for testing that migrations work.

    The default tests in this class match the default tests used by pytest_alembic.
    NB: pytest_alembic doesn't make it easy to run the default tests across multiple databases,
    which is why we have redefined them here.
    See: https://github.com/schireson/pytest-alembic/blob/master/src/pytest_alembic/tests.py
    """

    # We set __test__ to False to tell `pytest` not to collect this class for running tests
    # (as successful runs rely on the implementation of an abstract method).
    # In sub-classes, __test__ should be re-set to True.
    __test__ = False

    def setUp(self) -> None:
        self.db_dir = local_postgres_helpers.start_on_disk_postgresql_database()
        self.database_key = SQLAlchemyDatabaseKey.canonical_for_schema(self.schema_type)
        self.overridden_env_vars = (
            local_persistence_helpers.update_local_sqlalchemy_postgres_env_vars()
        )
        self.engine = create_engine(
            local_persistence_helpers.postgres_db_url_from_env_vars()
        )

    def tearDown(self) -> None:
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(self.db_dir)

    def fetch_all_enums(self) -> Dict[str, Set[str]]:
        engine = create_engine(
            local_persistence_helpers.postgres_db_url_from_env_vars()
        )

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

    def fetch_all_constraints(self) -> Dict[str, str]:
        """Returns a dictionary mapping constraint name to constraint definition for all
        CHECK and UNIQUENESS constraints defined in the database.
        """
        engine = create_engine(
            local_persistence_helpers.postgres_db_url_from_env_vars()
        )

        conn = engine.connect()
        rows = conn.execute(
            """
       SELECT con.conname, pg_get_constraintdef(con.oid)
       FROM pg_catalog.pg_constraint con
            INNER JOIN pg_catalog.pg_class rel
                       ON rel.oid = con.conrelid
            INNER JOIN pg_catalog.pg_namespace nsp
                       ON nsp.oid = connamespace
       WHERE nsp.nspname = 'public' AND con.contype IN ('c', 'u');
       """
        )

        return {row[0]: row[1] for row in rows}

    def fetch_all_indices(self) -> Dict[str, str]:
        """Returns a list of all indices defined in the database."""
        engine = create_engine(
            local_persistence_helpers.postgres_db_url_from_env_vars()
        )

        conn = engine.connect()
        rows = conn.execute(
            """
       SELECT
            indexname,
            indexdef
        FROM
            pg_indexes
        WHERE
            schemaname = 'public'
        ORDER BY
            indexname;
       """
        )

        # The alembic_version_pkc constraint is only added when migrations are run
        return {row[0]: row[1] for row in rows if row[0] != "alembic_version_pkc"}

    def default_config(self) -> Config:
        return Config.from_raw_config(
            {
                "file": self.database_key.alembic_file,
                "script_location": self.database_key.migrations_location,
            }
        )

    @property
    @abc.abstractmethod
    def schema_type(self) -> SchemaType:
        raise NotImplementedError

    def test_constraints_match_schema(self) -> None:
        with runner(self.default_config(), self.engine) as r:
            r.migrate_up_to("head")

        # Fetch constraints after loading the schema after running all the migrations
        migration_constraints = self.fetch_all_constraints()

        # Doing teardown/setup to generate a new postgres instance
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(self.db_dir)

        self.db_dir = local_postgres_helpers.start_on_disk_postgresql_database()
        self.overridden_env_vars = (
            local_persistence_helpers.update_local_sqlalchemy_postgres_env_vars()
        )

        local_persistence_helpers.use_on_disk_postgresql_database(self.database_key)

        # Fetch constraints after having SQLAlchemy load the schema
        schema_constraints = self.fetch_all_constraints()

        # Assert that they all match
        constraints_not_in_schema = (
            set(migration_constraints)
            - set(schema_constraints)
            # This constraint is only added when migrations are run
            - {"alembic_version_pkc"}
        )
        if constraints_not_in_schema:
            raise ValueError(
                f"Found constraints defined in migrations but not in schema.py: {constraints_not_in_schema}"
            )
        constraints_not_in_migrations = set(schema_constraints) - set(
            migration_constraints
        )
        if constraints_not_in_migrations:
            raise ValueError(
                f"Found constraints defined in schema.py but not in migrations: {constraints_not_in_migrations}"
            )

        for (
            constraint_name,
            schema_constraint_definition,
        ) in schema_constraints.items():
            migration_constraint_definition = migration_constraints[constraint_name]
            self.assertEqual(
                migration_constraint_definition,
                schema_constraint_definition,
                f"Found constraint {constraint_name} with conflicting definitions in migrations and schema.py definition",
            )

        # Cleanup needed for this method.
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    def test_indices_match_schema(self) -> None:
        with runner(self.default_config(), self.engine) as r:
            r.migrate_up_to("head")

        # Fetch indices after loading the schema after running all the migrations
        migration_indices = self.fetch_all_indices()

        # Doing teardown/setup to generate a new postgres instance
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(self.db_dir)

        self.db_dir = local_postgres_helpers.start_on_disk_postgresql_database()
        self.overridden_env_vars = (
            local_persistence_helpers.update_local_sqlalchemy_postgres_env_vars()
        )

        local_persistence_helpers.use_on_disk_postgresql_database(self.database_key)

        # Fetch indices after having SQLAlchemy load the schema
        schema_indices = self.fetch_all_indices()

        # Assert that they all match
        indices_not_in_schema = set(migration_indices) - set(schema_indices)
        if indices_not_in_schema:
            raise ValueError(
                f"Found indices defined in migrations but not in schema.py: {indices_not_in_schema}."
            )
        # TODO(#17979): Remove `etl_clients_pkey` from exemption below once duplicates from `etl_clients`
        # are removed or the table itself is deleted.
        indices_not_in_migrations = (
            set(schema_indices) - set(migration_indices) - {"etl_clients_pkey"}
        )
        if indices_not_in_migrations:
            raise ValueError(
                f"Found indices defined in schema.py but not in migrations: {indices_not_in_migrations}"
            )

        for (
            index_name,
            schema_index_definition,
        ) in schema_indices.items():
            # TODO(#17979): Remove `etl_clients_pkey` from exemption below once duplicates from `etl_clients`
            # are removed or the table itself is deleted.
            if index_name != "etl_clients_pkey":
                migration_index_definition = migration_indices[index_name]
                self.assertEqual(
                    migration_index_definition,
                    schema_index_definition,
                    f"Found index {index_name} with conflicting definitions in migrations and schema.py definition",
                )

        # Cleanup needed for this method.
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    def test_enums_match_schema(self) -> None:
        with runner(self.default_config(), self.engine) as r:
            r.migrate_up_to("head")

        # Fetch enum values
        migration_enums = self.fetch_all_enums()

        # Doing teardown/setup to generate a new postgres instance
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(self.db_dir)

        self.db_dir = local_postgres_helpers.start_on_disk_postgresql_database()
        self.overridden_env_vars = (
            local_persistence_helpers.update_local_sqlalchemy_postgres_env_vars()
        )

        local_persistence_helpers.use_on_disk_postgresql_database(self.database_key)

        # Check enum values
        schema_enums = self.fetch_all_enums()

        # Assert that they all match
        self.assertEqual(len(migration_enums), len(schema_enums))
        for enum_name, migration_values in migration_enums.items():
            schema_values = schema_enums[enum_name]
            self.assertCountEqual(migration_values, schema_values, f"{enum_name}")

        # Cleanup needed for this method.
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    def test_full_upgrade(self) -> None:
        """Enforce that migrations can be run forward to completion."""
        with runner(self.default_config(), self.engine) as r:
            r.migrate_up_to("head")

    def test_single_head_revision(self) -> None:
        """Enforce that there is exactly one head revision."""
        with runner(self.default_config(), self.engine) as r:
            self.assertEqual(len(r.heads), 1)

    def test_up_down(self) -> None:
        """Enforce that migrations can be run all the way up, back, and up again."""
        with runner(self.default_config(), self.engine) as r:
            revisions = r.history.revisions
            r.migrate_up_to("head")
            for rev in reversed(revisions):
                try:
                    r.migrate_down_to(rev)
                except Exception:
                    self.fail(f"Migrate down failed at revision: {rev}")

            # Check that schema is empty
            if enums := self.fetch_all_enums():
                self.fail(
                    f"Found enums still defined after all downgrade migrations run: {sorted(enums.keys())}"
                )

            if indices := self.fetch_all_indices():
                self.fail(
                    f"Found indices still defined after all downgrade migrations run: {sorted(indices.keys())}"
                )

            if schema_constraints := self.fetch_all_constraints():
                self.fail(
                    f"Found schema_constraints still defined after all downgrade migrations run: {sorted(schema_constraints.keys())}"
                )

            for rev in revisions:
                try:
                    r.migrate_up_to(rev)
                except Exception:
                    self.fail(f"Migrate back up failed at revision: {rev}")

    def test_migrate_matches_defs(self) -> None:
        """Enforces that after all migrations, database state matches known models

        Important note: This test will not detect changes made to enums that have failed to
        be incorporated by existing migrations. It only reliably handles table schema.
        """

        def verify_is_empty(_, __, directives) -> None:
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


class TestCaseTriageMigrations(MigrationsTestBase):
    __test__ = True

    @property
    def schema_type(self) -> SchemaType:
        return SchemaType.CASE_TRIAGE


class TestJusticeCountsMigrations(MigrationsTestBase):
    __test__ = True

    @property
    def schema_type(self) -> SchemaType:
        return SchemaType.JUSTICE_COUNTS


class TestOperationsMigrations(MigrationsTestBase):
    """Organizes migrations tests for operations db."""

    __test__ = True

    @property
    def schema_type(self) -> SchemaType:
        return SchemaType.OPERATIONS

    def test_direct_ingest_instance_status_contains_data_for_all_states(
        self,
    ) -> None:
        '''Enforces that after all migrations the set of direct ingest instance statuses
        matches the list of known states.

        If this test fails, you will likely have to add a new migration because a new state
        was recently created. To do so, first run:
        ```
        python -m recidiviz.tools.migrations.autogenerate_migration \
            --database OPERATIONS \
            --message add_us_xx_initial_ingest_statuses
        ```

        This will generate a blank migration. You should then modify the migration, changing
        the `upgrade` method to look like:
        ```
        def upgrade() -> None:
            op.execute("""
                INSERT INTO direct_ingest_instance_status (region_code, instance, status, status_timestamp) VALUES
                ('US_XX', 'PRIMARY', 'INITIAL_STATE', '20XX-YY-ZZT00:00:00.000000'),
                ('US_XX', 'SECONDARY', 'NO_RAW_DATA_REIMPORT_IN_PROGRESS', '20XX-YY-YYT00:00:00.000000');
            """)

        def downgrade() -> None:
            op.execute(
                f"""
                   DELETE FROM direct_ingest_instance_status
                   WHERE region_code = 'US_XX';
               """
            )
        ```

        Afterwards, this test should ideally pass.
        '''

        with runner(self.default_config(), self.engine) as r:
            r.migrate_up_to("head")

            engine = create_engine(
                local_persistence_helpers.postgres_db_url_from_env_vars()
            )

            conn = engine.connect()
            rows = conn.execute(
                "SELECT region_code, instance FROM direct_ingest_instance_status;"
            )

            instance_to_state_codes = defaultdict(set)
            for row in rows:
                instance_to_state_codes[DirectIngestInstance(row[1])].add(row[0])

            required_states = {name.upper() for name in get_existing_region_codes()}

            for instance in DirectIngestInstance:
                self.assertEqual(required_states, instance_to_state_codes[instance])


class TestStateMigrations(MigrationsTestBase):
    __test__ = True

    @property
    def schema_type(self) -> SchemaType:
        return SchemaType.STATE
