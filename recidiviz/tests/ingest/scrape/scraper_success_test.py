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

from recidiviz.ingest.scrape import (constants, scrape_phase, sessions)
from recidiviz.persistence.database.base_schema import \
    JailsBase
from recidiviz.persistence.database.schema.county.schema import ScraperSuccess
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.utils import regions
from recidiviz.tests.utils import fakes

_REGION_CODE = 'us_al_jackson'
_TODAY = datetime.date(2000, 1, 1)


class MockDate(datetime.date):
    @classmethod
    def today(cls):
        return cls(2000, 1, 1)


class TestScraperStop(TestCase):
    """Tests for requests to the Scraper Stop API."""

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database(JailsBase)

    @patch('recidiviz.ingest.models.model_utils.datetime.date', MockDate)
    @patch('recidiviz.ingest.models.single_count.datetime.date', MockDate)
    @patch('google.cloud.datastore.batch.Batch.put')
    def test_scraper_success(self, mock_put):
        mock_put.return_value = None
        session = sessions.ScrapeSession.new(
            key=None, region=_REGION_CODE,
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.RELEASE
        )

        sessions.update_phase(session, scrape_phase.ScrapePhase.DONE)

        query = SessionFactory.for_schema_base(JailsBase).query(ScraperSuccess)
        result = one(query.all())

        region = regions.get_region(_REGION_CODE)
        self.assertEqual(result.jid, region.jurisdiction_id)
        self.assertEqual(result.date, _TODAY)
