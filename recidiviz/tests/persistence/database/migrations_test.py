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
import os
from typing import Dict
from unittest.case import TestCase

from pytest_alembic import runner  # type: ignore

from recidiviz.tests.utils import fakes


class MigrationsTestBase:
    """This is the base class for testing that migrations work.

    The default tests in this class match the default tests used by pytest_alembic.
    NB: pytest_alembic doesn't make it easy to run the default tests across multiple databases,
    which is why we have redefined them here.
    See: https://github.com/schireson/pytest-alembic/blob/master/src/pytest_alembic/tests.py
    """

    def setUp(self) -> None:
        self.db_dir = fakes.start_on_disk_postgresql_database(create_temporary_db=True)
        os.environ['SQLALCHEMY_DB_NAME'] = fakes.TEST_POSTGRES_DB_NAME
        os.environ['SQLALCHEMY_DB_HOST'] = 'localhost'
        os.environ['SQLALCHEMY_USE_SSL'] = '0'
        os.environ['SQLALCHEMY_DB_USER'] = fakes.TEST_POSTGRES_USER_NAME
        os.environ['SQLALCHEMY_DB_PASSWORD'] = ''

    def tearDown(self) -> None:
        fakes.stop_and_clear_on_disk_postgresql_database(self.db_dir)

    @property
    @abc.abstractmethod
    def alembic_path_prefix(self) -> str:
        raise NotImplementedError

    def default_config(self) -> Dict[str, str]:
        return {
            'file': f'{self.alembic_path_prefix}_alembic.ini',
            'script_location': self.alembic_path_prefix,
        }

    def test_full_upgrade(self):
        """Enforce that migrations can be run forward to completion."""
        with runner(self.default_config()) as r:
            r.migrate_up_to('head')

    def test_single_head_revision(self):
        """Enforce that there is exactly one head revision."""
        with runner(self.default_config()) as r:
            self.assertEqual(len(r.heads), 1)

    def test_up_down(self):
        """Enforce that migrations can be run all the way up and back."""
        with runner(self.default_config()) as r:
            revisions = reversed(r.history.revisions)
            r.migrate_up_to('head')
            for rev in revisions:
                try:
                    r.migrate_down_to(rev)
                except Exception:
                    self.fail(f'Migrate down failed at revision: {rev}')

    def test_migrate_matches_defs(self):
        """Enforces that after all migrations, database state matches known models

        Important note: This test will not detect changes made to enums that have failed to
        be incorporated by existing migrations. It only reliably handles table schema.

        TODO(#4604): Add tests to ensure that enums rendered with migration match our schema.
        """
        def verify_is_empty(_, __, directives):
            script = directives[0]

            migration_is_empty = script.upgrade_ops.is_empty()
            if not migration_is_empty:
                raise RuntimeError('migration should be empty')

        with runner(self.default_config()) as r:
            r.migrate_up_to('head')
            r.generate_revision(message="test_rev", autogenerate=True, process_revision_directives=verify_is_empty)


class TestJusticeCountsMigrations(MigrationsTestBase, TestCase):
    @property
    def alembic_path_prefix(self) -> str:
        return './recidiviz/persistence/database/migrations/justice_counts'


class TestOperationsMigrations(MigrationsTestBase, TestCase):
    @property
    def alembic_path_prefix(self) -> str:
        return './recidiviz/persistence/database/migrations/operations'
