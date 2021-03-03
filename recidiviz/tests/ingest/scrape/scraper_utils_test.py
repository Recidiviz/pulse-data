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

"""Tests for ingest/scraper_utils.py."""
import unittest

from mock import patch
import pytest

from recidiviz.ingest.scrape import scraper_utils
from recidiviz.ingest.models.ingest_info import IngestInfo


class TestOne(unittest.TestCase):
    """Tests for the |one| method in the module."""

    def test_onePerson_passes(self):
        ii = IngestInfo()
        p = ii.create_person()
        self.assertIs(p, scraper_utils.one("person", ii))

    def test_twoPeople_raises(self):
        ii = IngestInfo()
        ii.create_person().create_booking()
        ii.create_person()
        with self.assertRaises(ValueError):
            scraper_utils.one("booking", ii)

    def test_noSentence_raises(self):
        ii = IngestInfo()
        ii.create_person().create_booking().create_charge().create_bond()
        with self.assertRaises(ValueError):
            scraper_utils.one("sentence", ii)

    def test_oneBooking_passes(self):
        ii = IngestInfo()
        b = ii.create_person().create_booking()
        b.create_arrest()
        self.assertIs(b, scraper_utils.one("booking", ii))

    def test_oneBond_passes(self):
        ii = IngestInfo()
        b = ii.create_person().create_booking().create_charge().create_bond()
        self.assertIs(b, scraper_utils.one("bond", ii))


class TestGetProxies(unittest.TestCase):
    """Tests for the get_proxies method in the module."""

    @patch("recidiviz.utils.secrets.get_secret")
    @patch("random.random")
    @patch("recidiviz.utils.environment.in_gcp")
    def test_get_proxies_prod(self, mock_in_gcp, mock_rand, mock_secret):
        mock_in_gcp.return_value = True
        mock_rand.return_value = 10
        test_secrets = {
            "proxy_url": "proxy.net/",
            "proxy_user": "real_user",
            "proxy_password": "real_password",
        }
        mock_secret.side_effect = test_secrets.get

        proxies = scraper_utils.get_proxies()
        assert proxies == {
            "http": "http://real_user-session-10:real_password@proxy.net/",
            "https": "http://real_user-session-10:real_password@proxy.net/",
        }

    @patch("recidiviz.utils.secrets.get_secret")
    @patch("recidiviz.utils.environment.in_gcp")
    def test_get_proxies_local_no_user(self, mock_in_gcp, mock_secret):
        mock_in_gcp.return_value = True
        test_secrets = {
            "proxy_url": "proxy.net/",
            "proxy_password": "real_password",
        }
        mock_secret.side_effect = test_secrets.get

        with pytest.raises(Exception) as exception:
            scraper_utils.get_proxies()
        assert str(exception.value) == "No proxy user/pass"

    @patch("recidiviz.utils.secrets.get_secret")
    @patch("recidiviz.utils.environment.in_gcp")
    def test_get_proxies_local(self, mock_in_gcp, mock_secret):
        mock_in_gcp.return_value = False
        test_secrets = {
            "proxy_url": "proxy.biz/",
            "test_proxy_user": "user",
            "test_proxy_password": "password",
        }
        mock_secret.side_effect = test_secrets.get

        proxies = scraper_utils.get_proxies()
        assert proxies is None


class TestGetHeaders:
    """Tests for the get_headers method in the module."""

    @patch("recidiviz.utils.secrets.get_secret")
    @patch("recidiviz.utils.environment.in_gcp")
    def test_get_headers(self, mock_in_gcp, mock_secret):
        # This is prod behaviour
        mock_in_gcp.return_value = True
        user_agent = "test_user_agent"

        test_secrets = {"user_agent": user_agent}
        mock_secret.side_effect = test_secrets.get

        headers = scraper_utils.get_headers()
        assert headers == {"User-Agent": user_agent}

    @patch("recidiviz.utils.secrets.get_secret")
    @patch("recidiviz.utils.environment.in_gcp")
    def test_get_headers_missing_user_agent_in_prod(self, mock_in_gcp, mock_secret):
        mock_in_gcp.return_value = True
        mock_secret.return_value = None
        with pytest.raises(Exception) as exception:
            scraper_utils.get_headers()
        assert str(exception.value) == "No user agent string"

    @patch("recidiviz.utils.environment.in_gcp")
    def test_get_headers_local(self, mock_in_gcp):
        mock_in_gcp.return_value = False
        headers = scraper_utils.get_headers()
        assert headers == {
            "User-Agent": (
                "For any issues, concerns, or rate constraints,"
                "e-mail alerts@recidiviz.com"
            )
        }
