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
"""Tests for ma_aggregate_site_scraper_test.py"""
from unittest import TestCase

import requests
from mock import Mock, patch

from recidiviz.ingest.aggregate.regions.ma import ma_aggregate_site_scraper
from recidiviz.tests.ingest import fixtures


class TestMaAggregateSiteScraper(TestCase):
    """Test that ma_aggregate_site_scraper correctly scrapes urls."""

    @patch.object(requests, "get")
    def testGetAllUrls(self, mockget: Mock) -> None:
        mock_landing = Mock()
        mock_landing.text = fixtures.as_string(
            "aggregate/regions/ma", "reports_landing.html"
        )

        mock_year = Mock()
        mock_year.text = fixtures.as_string("aggregate/regions/ma", "year_page.html")

        mockget.side_effect = [mock_landing, mock_year]

        url1 = "https://www.mass.gov/doc/weekly-inmate-count-12252017/download"
        expected_urls = {url1}

        urls = ma_aggregate_site_scraper.get_urls_to_download()
        self.assertEqual(expected_urls, urls)
