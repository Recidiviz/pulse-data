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


import pytest

from recidiviz.ingest import constants
from recidiviz.ingest.models.scrape_key import ScrapeKey


def test_eq_different_regions():
    left = ScrapeKey("us_ny", constants.BACKGROUND_SCRAPE)
    right = ScrapeKey("us_fl", constants.BACKGROUND_SCRAPE)

    assert left != right


def test_eq_different_types():
    left = ScrapeKey("us_ny", constants.BACKGROUND_SCRAPE)
    right = ScrapeKey("us_ny", constants.SNAPSHOT_SCRAPE)

    assert left != right


def test_eq_different_everything():
    left = ScrapeKey("us_ny", constants.SNAPSHOT_SCRAPE)
    right = ScrapeKey("us_fl", constants.BACKGROUND_SCRAPE)

    assert left != right


def test_eq_same():
    left = ScrapeKey("us_ny", constants.BACKGROUND_SCRAPE)
    right = ScrapeKey("us_ny", constants.BACKGROUND_SCRAPE)

    assert left == right


def test_eq_different_objects():
    left = ScrapeKey("us_ny", constants.BACKGROUND_SCRAPE)
    right = "We don't read the papers, we don't read the news"

    assert not left.__eq__(right)


def test_repr():
    scrape_key = ScrapeKey("us_ut", constants.SNAPSHOT_SCRAPE)

    representation = scrape_key.__repr__()

    assert representation == "<ScrapeKey region_code: us_ut, " \
                             "scrape_type: snapshot>"


def test_no_region():
    with pytest.raises(ValueError) as exception:
        ScrapeKey(None, constants.SNAPSHOT_SCRAPE)
    assert exception.value.message == 'A scrape key must include both ' \
                                      'a region code and a scrape type'


def test_no_scrape_type():
    with pytest.raises(ValueError) as exception:
        ScrapeKey("us_ut", None)
    assert exception.value.message == 'A scrape key must include both ' \
                                      'a region code and a scrape type'
