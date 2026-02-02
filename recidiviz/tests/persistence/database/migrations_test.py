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
import warnings
from collections import defaultdict
from typing import Any, Dict, Set
from unittest.case import TestCase

import pytest
from alembic.autogenerate import render_python_code
from pytest_alembic import runner
from pytest_alembic.config import Config
from sqlalchemy import create_engine
from sqlalchemy.exc import SAWarning

from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_region_codes,
)
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
        self.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )
        self.database_key = SQLAlchemyDatabaseKey.canonical_for_schema(
            self.schema_type()
        )
        self.overridden_env_vars = (
            local_persistence_helpers.update_local_sqlalchemy_postgres_env_vars(
                self.postgres_launch_result
            )
        )
        self.engine = create_engine(
            local_persistence_helpers.postgres_db_url_from_env_vars()
        )

    def tearDown(self) -> None:
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            self.postgres_launch_result
        )

    def expected_missing_indices(self) -> Set[str]:
        return set()

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

    @classmethod
    def schema_type(cls) -> SchemaType:
        raise NotImplementedError

    def test_constraints_match_schema(self) -> None:
        with runner(self.default_config(), self.engine) as r:
            r.migrate_up_to("head")

        # Fetch constraints after loading the schema after running all the migrations
        migration_constraints = self.fetch_all_constraints()

        # Doing teardown/setup to generate a new postgres instance
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            self.postgres_launch_result
        )

        self.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )
        self.overridden_env_vars = (
            local_persistence_helpers.update_local_sqlalchemy_postgres_env_vars(
                self.postgres_launch_result
            )
        )

        local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.database_key
        )

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
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            self.postgres_launch_result
        )

        self.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )
        self.overridden_env_vars = (
            local_persistence_helpers.update_local_sqlalchemy_postgres_env_vars(
                self.postgres_launch_result
            )
        )

        local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.database_key
        )

        # Fetch indices after having SQLAlchemy load the schema
        schema_indices = self.fetch_all_indices()

        # Assert that they all match
        indices_not_in_schema = (
            set(migration_indices)
            - set(schema_indices)
            - self.expected_missing_indices()
        )
        if indices_not_in_schema:
            raise ValueError(
                f"Found indices defined in migrations but not in schema.py: {indices_not_in_schema}."
            )

        indices_not_in_migrations = (
            set(schema_indices)
            - set(migration_indices)
            - self.expected_missing_indices()
        )
        print("***", schema_indices, migration_indices, self.expected_missing_indices())
        if indices_not_in_migrations:
            raise ValueError(
                f"Found indices defined in schema.py but not in migrations. If you are "
                f"*sure* that you don't want a migration defined for this index (e.g. "
                f"if the specific table's schema is managed by a process outside of "
                f"alembic), you can add it to the `expected_missing_indices` list for "
                f"this test class. Missing indices: {indices_not_in_migrations}"
            )

        for (
            index_name,
            schema_index_definition,
        ) in schema_indices.items():
            if index_name in self.expected_missing_indices():
                continue

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
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            self.postgres_launch_result
        )

        self.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )
        self.overridden_env_vars = (
            local_persistence_helpers.update_local_sqlalchemy_postgres_env_vars(
                self.postgres_launch_result
            )
        )

        local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.database_key
        )

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

        def verify_is_empty(_: Any, __: Any, directives: Any) -> None:
            script = directives[0]

            migration_is_empty = script.upgrade_ops.is_empty()
            if not migration_is_empty:
                raise RuntimeError(
                    "expected empty autogenerated migration. actual contained these operations:\n"
                    f"{render_python_code(script.upgrade_ops)}"
                )

        with runner(self.default_config(), self.engine) as r:
            r.migrate_up_to("head")

            with warnings.catch_warnings():
                # This warning is safe to ignore because SQLAlchemy can't reflect expression-based indexes.
                # The index is defined manually in a migration and does not impact autogeneration logic.
                warnings.filterwarnings(
                    "ignore",
                    category=SAWarning,
                    message=".*expression-based index.*",
                )

                r.generate_revision(
                    message="test_rev",
                    autogenerate=True,
                    process_revision_directives=verify_is_empty,
                )


class TestCaseTriageMigrations(MigrationsTestBase):
    __test__ = True

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.CASE_TRIAGE


class TestJusticeCountsMigrations(MigrationsTestBase):
    __test__ = True

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.JUSTICE_COUNTS

    # This index uses a COALESCE expression and is defined via raw SQL in the migration,
    # so it cannot be reflected by SQLAlchemy and is not represented in schema.py
    def expected_missing_indices(self) -> Set[str]:
        return {
            "unique_report_datapoint_normalized",
        }


class TestOperationsMigrations(MigrationsTestBase):
    """Organizes migrations tests for operations db."""

    __test__ = True

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.OPERATIONS

    def test_direct_ingest_raw_data_flash_status_contains_data_for_all_states(
        self,
    ) -> None:
        '''Enforces that after all migrations the set of region_codes that have rows in 
        direct_ingest_raw_data_flash_status matches the list of known states.

        If this test fails, you will likely have to add a new migration because a new state
        was recently created. To do so, first run:
        ```
        python -m recidiviz.tools.migrations.autogenerate_migration \
            --database OPERATIONS \
            --message add_us_xx_initial_flash_status
        ```

        This will generate a blank migration. You should then modify the migration, changing
        the `upgrade` method to look like:
        ```
        def upgrade() -> None:
            op.execute("""
                INSERT INTO direct_ingest_raw_data_flash_status (region_code, status_timestamp, flashing_in_progress) VALUES
                ('US_XX', '20XX-YY-ZZT00:00:00.000000', '0')
            """)

        def downgrade() -> None:
            op.execute(
                f"""
                   DELETE FROM direct_ingest_raw_data_flash_status
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
                "SELECT region_code FROM direct_ingest_raw_data_flash_status;"
            )

            actual_states = {row[0] for row in rows}
            expected_states = {name.upper() for name in get_existing_region_codes()}

            assert actual_states == expected_states


class TestInsightsMigrations(MigrationsTestBase):
    __test__ = True

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.INSIGHTS

    def expected_missing_indices(self) -> Set[str]:
        # We don't manage the schemas of some of the Insights tables via alembic
        # migrations because they are loaded to our Insights DB via a separate ETL
        # process. For indices on these tables, we're ok if they are not defined in
        # migrations.
        # TODO(#27159): Consider fully managing the Insights schema via Alembic
        return {
            "supervision_officer_metrics_pkey",
            "supervision_officer_supervisors_pkey",
            "supervision_district_managers_pkey",
            "supervision_districts_pkey",
            "supervision_clients_pkey",
            "supervision_client_events_pkey",
            "metric_benchmarks_pkey",
            "supervision_officer_outlier_status_pkey",
            "supervision_officers_pkey",
        }


class TestPathwaysMigrations(MigrationsTestBase):
    __test__ = True

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.PATHWAYS

    def expected_missing_indices(self) -> Set[str]:
        # We don't manage the schemas of some of the Pathways tables via alembic
        # migrations because they are loaded to our Pathways DB via a separate ETL
        # process. For indices on these tables, we're ok if they are not defined in
        # migrations.
        # TODO(#27159): Consider fully managing the Pathways schema via Alembic
        return {
            "liberty_to_prison_transitions_pkey",
            "prison_population_by_dimension_admission_reason",
            "prison_population_by_dimension_age_group",
            "prison_population_by_dimension_facility",
            "prison_population_by_dimension_gender",
            "prison_population_by_dimension_sex",
            "prison_population_by_dimension_pk",
            "prison_population_by_dimension_pkey",
            "prison_population_by_dimension_race",
            "prison_population_by_dimension_offense_type",
            "prison_population_by_dimension_sentence_length_max",
            "prison_population_by_dimension_sentence_length_min",
            "prison_population_by_dimension_charge_county_code",
            "prison_population_over_time_pk",
            "prison_population_over_time_pkey",
            "prison_population_over_time_time_series",
            "prison_population_over_time_watermark",
            "prison_population_person_level_pkey",
            "prison_population_projection_pkey",
            "prison_to_supervision_transitions_pkey",
            "supervision_population_by_dimension_pk",
            "supervision_population_by_dimension_pkey",
            "supervision_population_by_dimension_race",
            "supervision_population_by_dimension_supervision_district",
            "supervision_population_by_dimension_supervision_level",
            "supervision_population_over_time_pk",
            "supervision_population_over_time_pkey",
            "supervision_population_over_time_time_series",
            "supervision_population_over_time_watermark",
            "supervision_population_projection_pkey",
            "supervision_to_liberty_transitions_pkey",
            "supervision_to_prison_transitions_pkey",
        }


class TestPublicPathwaysMigrations(MigrationsTestBase):
    __test__ = True

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.PUBLIC_PATHWAYS

    def expected_missing_indices(self) -> Set[str]:
        # We don't manage the schemas of some of the Public Pathways tables via alembic
        # migrations because they are loaded to our Public Pathways DB via a separate ETL
        # process. For indices on these tables, we're ok if they are not defined in
        # migrations.
        return {
            "prison_population_by_dimension_age_group",
            "prison_population_by_dimension_facility",
            "prison_population_by_dimension_gender",
            "prison_population_by_dimension_sex",
            "prison_population_by_dimension_offense_type",
            "prison_population_by_dimension_sentence_length_max",
            "prison_population_by_dimension_sentence_length_min",
            "prison_population_by_dimension_charge_county_code",
            "prison_population_by_dimension_pk",
            "prison_population_by_dimension_pkey",
            "prison_population_by_dimension_race",
            "prison_population_over_time_pk",
            "prison_population_over_time_pkey",
            "prison_population_over_time_time_series",
            "prison_population_over_time_watermark",
        }


class TestWorkflowsMigrations(MigrationsTestBase):
    __test__ = True

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.WORKFLOWS


class MigrationsTestTest(TestCase):
    """Tests that any new schema added has migration tests defined."""

    def test_all_schemas_have_tests(self) -> None:
        schemas_with_coverage = {
            test_case_cls.schema_type()
            for test_case_cls in MigrationsTestBase.__subclasses__()
        }

        all_schemas = set(SchemaType)
        exempt_schemas = {
            # The STATE schema is exempt from test coverage because this schema is no
            # longer deployed in BQ and we just use the SQLAlchemy schema definition
            # for certain schema introspection conveniences.
            SchemaType.STATE,
        }

        if missing_coverage := all_schemas - schemas_with_coverage - exempt_schemas:
            raise ValueError(
                f"Found schema types with missing migrations test coverage: "
                f"{missing_coverage}."
            )
