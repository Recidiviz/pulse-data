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

"""Tests for ingest/models/scrape_key.py."""


from recidiviz.ingest.models.scrape_key import ScrapeKey


def test_eq_different_regions():
    left = ScrapeKey("us_ny", "background")
    right = ScrapeKey("us_fl", "background")

    assert left != right


def test_eq_different_types():
    left = ScrapeKey("us_ny", "background")
    right = ScrapeKey("us_ny", "snapshot")

    assert left != right


def test_eq_different_everything():
    left = ScrapeKey("us_ny", "snapshot")
    right = ScrapeKey("us_fl", "background")

    assert left != right


def test_eq_same():
    left = ScrapeKey("us_ny", "background")
    right = ScrapeKey("us_ny", "background")

    assert left == right


def test_repr():
    scrape_key = ScrapeKey("us_ut", "snapshot")

    representation = scrape_key.__repr__()

    assert representation == "<ScrapeKey region_code: us_ut, " \
                             "scrape_type: snapshot>"
