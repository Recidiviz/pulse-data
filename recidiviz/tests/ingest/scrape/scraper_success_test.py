# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for scaper_success.py."""
import datetime
from unittest import TestCase

from mock import patch
from more_itertools import one

from recidiviz.ingest.scrape import constants, scrape_phase, sessions
from recidiviz.persistence.database.base_schema import JailsBase
from recidiviz.persistence.database.schema.county.schema import ScraperSuccess
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.utils import regions
from recidiviz.tests.utils import fakes
from recidiviz.utils.regions import Region

_REGION_CODE = "us_al_jackson"
_TODAY = datetime.date(2000, 1, 1)


class TestScraperStop(TestCase):
    """Tests for requests to the Scraper Stop API."""

    def setUp(self) -> None:
        fakes.use_in_memory_sqlite_database(JailsBase)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    # pylint:disable=unused-argument
    @patch("google.cloud.datastore.Client")
    @patch("recidiviz.utils.regions.get_region")
    def test_scraper_success(self, mock_get_region, mock_client):
        mock_get_region.return_value = _mock_region()

        session = sessions.ScrapeSession.new(
            key=None,
            region=_REGION_CODE,
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.RELEASE,
        )
        session.start = datetime.datetime.strptime("2000-01-01 9:01", "%Y-%d-%m %H:%M")

        sessions.update_phase(session, scrape_phase.ScrapePhase.DONE)

        query = SessionFactory.for_schema_base(JailsBase).query(ScraperSuccess)
        result = one(query.all())

        region = regions.get_region(_REGION_CODE)
        self.assertEqual(result.jid, region.jurisdiction_id)
        self.assertEqual(result.date, _TODAY)


def _mock_region():
    return Region(
        region_code=_REGION_CODE,
        shared_queue="queue",
        agency_name="the agency",
        agency_type="benevolent",
        base_url="localhost:3000",
        names_file="names.txt",
        timezone="America/Chicago",
        environment="production",
        jurisdiction_id="01071001",
        is_stoppable=False,
    )
