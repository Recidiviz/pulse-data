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
"""Tests for hi_aggregate_ingest.py."""
from unittest import TestCase
from mock import patch, Mock
import requests

from recidiviz.ingest.aggregate.regions.ky import ky_aggregate_site_scraper
from recidiviz.tests.ingest import fixtures


class TestKyAggregateSiteScraper(TestCase):
    """Test that ky_aggregate_site_scraper correctly scrapes urls."""

    @patch.object(requests, "get")
    def testGetAllUrls(self, mockget: Mock) -> None:
        mockresponse = Mock()
        mockget.return_value = mockresponse
        mockresponse.text = fixtures.as_string("aggregate/regions/ky", "report.html")
        url1 = (
            "https://corrections.ky.gov/About/researchandstats/Documents/"
            "Weekly Jail/2018/08-18-18.pdf"
        )
        expected_urls = {url1}

        urls = ky_aggregate_site_scraper.get_urls_to_download()
        self.assertEqual(expected_urls, urls)
