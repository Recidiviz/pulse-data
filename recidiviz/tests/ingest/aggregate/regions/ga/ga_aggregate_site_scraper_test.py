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
"""Tests for ga_aggregate_ingest.py."""
from unittest import TestCase
from mock import patch, Mock
import requests

from recidiviz.ingest.aggregate.regions.ga import ga_aggregate_site_scraper
from recidiviz.tests.ingest import fixtures

REPORTS_LANDING_HTML = fixtures.as_string(
    "aggregate/regions/ga", "reports_landing.html"
)
REPORTS_YEAR_2015 = fixtures.as_string("aggregate/regions/ga", "reports_year_2015.html")
REPORTS_YEAR_2019 = fixtures.as_string("aggregate/regions/ga", "reports_year_2019.html")


class TestGaAggregateSiteScraper(TestCase):
    """Test that ga_aggregate_site_scraper correctly scrapes urls."""

    @patch.object(requests, "get")
    def testGetAllUrls(self, mockget):
        def _MockGet(url):
            response = Mock()
            if "node/5617" in url:
                response.text = REPORTS_YEAR_2019
            elif "node/4036" in url:
                response.text = REPORTS_YEAR_2015
            else:
                response.text = REPORTS_LANDING_HTML
            return response

        mockget.side_effect = _MockGet
        url1 = "https://www.dca.ga.gov/sites/default/files/jail_report_jan19.pdf"
        url2 = "https://www.dca.ga.gov/sites/default/files/mar15_jail_report.pdf"
        expected_urls = {url1, url2}

        urls = ga_aggregate_site_scraper.get_urls_to_download()
        self.assertEqual(expected_urls, urls)
