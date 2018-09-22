# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

"""Tests for ingest/scraper_control.py."""


import webapp2

from recidiviz.ingest import scraper_control


def test_invalid_input():
    response = webapp2.Response()
    scraper_control.invalid_input(response, "Mr. Brightside")

    assert response.status_code == 400
    assert response.body == "Missing or invalid parameters, see service logs."


def test_get_and_validate_params():
    params = [("region", "us_ny"),
              ("scrape_type", "snapshot"),
              ("album", "Hot Fuss")]

    results = scraper_control.get_and_validate_params(params)
    assert results == (["us_ny"], ["snapshot"], params)


def test_get_and_validate_params_all():
    params = [("region", "all"), ("scrape_type", "all")]

    results = scraper_control.get_and_validate_params(params)
    assert results == (["us_ny", "us_vt"], ["background", "snapshot"], params)


def test_get_and_validate_params_invalid_region():
    params = [("region", "ca_bc"), ("scrape_type", "snapshot")]
    assert not scraper_control.get_and_validate_params(params)


def test_get_and_validate_params_invalid_scrape_type():
    params = [("region", "us_ny"), ("scrape_type", "When You Were Young")]
    assert not scraper_control.get_and_validate_params(params)


def test_get_and_validate_params_defaukt_scrape_type():
    params = [("region", "us_ny"), ("album", "Sam's Town")]

    results = scraper_control.get_and_validate_params(params)
    assert results == (["us_ny"], ["background"], params)
