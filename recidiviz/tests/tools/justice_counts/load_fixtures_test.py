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
"""Implements tests for the script that loads fixture data into the Justice Counts DB."""

from recidiviz.persistence.database.schema.justice_counts.schema import Report, Source
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils import JusticeCountsDatabaseTestCase
from recidiviz.tools.justice_counts.control_panel.load_fixtures import (
    reset_justice_counts_fixtures,
)


class TestJusticeCountsLoadFixtures(JusticeCountsDatabaseTestCase):
    """Implements tests for the load fixtures script."""

    def test_load_fixtures_succeeds(self) -> None:
        # Clear DB and then load in fixture data
        reset_justice_counts_fixtures(in_test=True)

        # Assert that DB is now non-empty
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as read_session:
            # TODO(#11588): Add fixture data for all Justice Counts schema tables
            for fixture_class in [Source, Report]:
                self.assertTrue(len(read_session.query(fixture_class).all()) > 0)
