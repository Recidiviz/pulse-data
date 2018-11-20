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

"""Tests for utils/params.py."""


from werkzeug.datastructures import MultiDict
from recidiviz.ingest import constants
from recidiviz.utils import params


PARAMS = MultiDict([("region", "us_mo"),
                    ("scrape_type", constants.BACKGROUND_SCRAPE),
                    ("region", "us_wa")])


def test_get_value():
    assert params.get_value("region", PARAMS) == "us_mo"


def test_get_values():
    assert params.get_values("region", PARAMS) == ["us_mo", "us_wa"]


def test_get_value_default():
    assert params.get_value("foo", PARAMS, default="bar") == "bar"


def test_get_value_no_default():
    assert not params.get_value("foo", PARAMS)


def test_get_value_explicitly_none_default():
    assert not params.get_value("foo", PARAMS, default=None)
